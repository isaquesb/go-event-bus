package jetstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
)

// --- fakes (prefix rp) ---

// rpPubResult is one scripted PublishMsg outcome.
type rpPubResult struct {
	ack *jetstream.PubAck
	err error
}

// rpJS implements replayBackend. It records consumer configs and published
// messages and replays scripted publish results in order.
type rpJS struct {
	mu        sync.Mutex
	streams   []string
	cfgs      []jetstream.ConsumerConfig
	consumer  jetstream.Consumer
	createErr error
	published []*nats.Msg
	results   []rpPubResult
}

func (j *rpJS) CreateOrUpdateConsumer(_ context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.streams = append(j.streams, stream)
	j.cfgs = append(j.cfgs, cfg)
	if j.createErr != nil {
		return nil, j.createErr
	}
	return j.consumer, nil
}

func (j *rpJS) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.published = append(j.published, msg)
	if len(j.results) == 0 {
		return &jetstream.PubAck{}, nil
	}
	res := j.results[0]
	j.results = j.results[1:]
	if res.err != nil {
		return nil, res.err
	}
	if res.ack != nil {
		return res.ack, nil
	}
	return &jetstream.PubAck{}, nil
}

// rpBatch is one scripted FetchNoWait result.
type rpBatch struct {
	msgs []*rpMsg
	err  error
}

type rpBatchImpl struct {
	ch  chan jetstream.Msg
	err error
}

func (b *rpBatchImpl) Messages() <-chan jetstream.Msg { return b.ch }
func (b *rpBatchImpl) Error() error                   { return b.err }

// rpConsumer implements jetstream.Consumer. Only FetchNoWait is scripted; an
// exhausted script yields empty batches. Every other method panics, so a
// blocking fetch (Next, Fetch) fails the test loudly.
type rpConsumer struct {
	batches  []rpBatch
	fetchErr error
	ns       []int
}

func (c *rpConsumer) FetchNoWait(n int) (jetstream.MessageBatch, error) {
	c.ns = append(c.ns, n)
	if c.fetchErr != nil {
		return nil, c.fetchErr
	}
	var b rpBatch
	if len(c.batches) > 0 {
		b = c.batches[0]
		c.batches = c.batches[1:]
	}
	ch := make(chan jetstream.Msg, len(b.msgs))
	for _, m := range b.msgs {
		ch <- m
	}
	close(ch)
	return &rpBatchImpl{ch: ch, err: b.err}, nil
}

func (c *rpConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	panic("rpConsumer: Fetch not implemented")
}
func (c *rpConsumer) FetchBytes(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	panic("rpConsumer: FetchBytes not implemented")
}
func (c *rpConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	panic("rpConsumer: Messages not implemented")
}
func (c *rpConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) {
	panic("rpConsumer: Next (30 s blocking fetch) must not be used by the Replayer")
}
func (c *rpConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	panic("rpConsumer: Info not implemented")
}
func (c *rpConsumer) CachedInfo() *jetstream.ConsumerInfo {
	panic("rpConsumer: CachedInfo not implemented")
}
func (c *rpConsumer) Consume(jetstream.MessageHandler, ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	panic("rpConsumer: Consume not implemented")
}

// rpMsg implements jetstream.Msg for one DLQ record.
type rpMsg struct {
	data    []byte
	headers nats.Header
	seq     uint64
	metaErr error
	acks    int
	naks    int
}

func (m *rpMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}
func (m *rpMsg) Data() []byte                     { return m.data }
func (m *rpMsg) Headers() nats.Header             { return m.headers }
func (m *rpMsg) Subject() string                  { return "dlq.orders.created" }
func (m *rpMsg) Reply() string                    { return "" }
func (m *rpMsg) Ack() error                       { m.acks++; return nil }
func (m *rpMsg) DoubleAck(context.Context) error  { m.acks++; return nil }
func (m *rpMsg) Nak() error                       { m.naks++; return nil }
func (m *rpMsg) NakWithDelay(time.Duration) error { m.naks++; return nil }
func (m *rpMsg) InProgress() error                { return nil }
func (m *rpMsg) Term() error                      { return nil }
func (m *rpMsg) TermWithReason(string) error      { return nil }

// rpRecord builds a DLQ record the way NewDLQ writes it.
func rpRecord(seq uint64, subject, eventID string, data []byte) *rpMsg {
	h := nats.Header{}
	if subject != "" {
		h.Set("Eventbus-Dlq-Original-Subject", subject)
	}
	h.Set("Eventbus-Dlq-Reason", "terminal")
	if eventID != "" {
		h.Set("Eventbus-Dlq-Event-Id", eventID)
		h.Set("Eventbus-Dlq-Original-Msg-Id", eventID)
	}
	h.Set("Nats-Msg-Id", "dlq-"+eventID)
	h.Set("tenant", "acme")
	return &rpMsg{data: data, headers: h, seq: seq}
}

// --- tests ---

func TestReplayer_RoundTripToOriginalSubject(t *testing.T) {
	envelope := []byte(`{"id":"evt-1","name":"orders.created","data":{"n":1}}`)
	rec := rpRecord(42, "orders.created", "evt-1", envelope)
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js := &rpJS{consumer: cons}

	res, err := NewReplayer(js).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res != (ReplayResult{Replayed: 1, Failed: 0}) {
		t.Fatalf("result = %+v, want {Replayed:1 Failed:0}", res)
	}
	if len(js.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(js.published))
	}
	got := js.published[0]
	if got.Subject != "orders.created" {
		t.Errorf("subject = %q, want orders.created", got.Subject)
	}
	if !bytes.Equal(got.Data, envelope) {
		t.Errorf("data = %q, want the envelope bytes untouched", got.Data)
	}
	if v := got.Header.Get("tenant"); v != "acme" {
		t.Errorf("tenant header = %q, want acme (user headers are kept)", v)
	}
	for k := range got.Header {
		if strings.HasPrefix(strings.ToLower(k), "eventbus-dlq-") {
			t.Errorf("header %q leaked into the replay", k)
		}
	}
	if v := got.Header.Get("Nats-Msg-Id"); v != "evt-1:replay:42" {
		t.Errorf("Nats-Msg-Id = %q, want evt-1:replay:42", v)
	}
	if rec.acks != 1 || rec.naks != 0 {
		t.Errorf("record acks=%d naks=%d, want 1 and 0", rec.acks, rec.naks)
	}
	if len(js.cfgs) != 1 {
		t.Fatalf("consumer created %d times, want 1", len(js.cfgs))
	}
	cfg := js.cfgs[0]
	if cfg.Name != "replay_DLQ" || cfg.Durable != "replay_DLQ" {
		t.Errorf("consumer Name=%q Durable=%q, want replay_DLQ for both", cfg.Name, cfg.Durable)
	}
	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("AckPolicy = %v, want explicit", cfg.AckPolicy)
	}
	if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %v, want deliver-all", cfg.DeliverPolicy)
	}
	if js.streams[0] != "DLQ" {
		t.Errorf("consumer stream = %q, want DLQ", js.streams[0])
	}
}

func rpReplay(t *testing.T, cons *rpConsumer, opts ReplayOptions) (*rpJS, ReplayResult, error) {
	t.Helper()
	js := &rpJS{consumer: cons}
	if opts.FromStream == "" {
		opts.FromStream = "DLQ"
	}
	res, err := NewReplayer(js).Replay(context.Background(), opts)
	return js, res, err
}

// TestReplayer_EmptyBodyRecordRepublishedVerbatim pins PD-2: the dead letter of
// an undecodable empty message is republished empty, like any other record, so
// the original consumer dead-letters it again and nothing is lost.
func TestReplayer_EmptyBodyRecordRepublishedVerbatim(t *testing.T) {
	rec := rpRecord(9, "orders.created", "", nil)
	rec.headers.Set("Eventbus-Dlq-Reason", "decode_malformed")
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}

	js, res, err := rpReplay(t, cons, ReplayOptions{})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res != (ReplayResult{Replayed: 1, Failed: 0}) {
		t.Fatalf("result = %+v, want {Replayed:1 Failed:0}", res)
	}
	if len(js.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(js.published))
	}
	got := js.published[0]
	if got.Subject != "orders.created" {
		t.Errorf("subject = %q, want orders.created", got.Subject)
	}
	if len(got.Data) != 0 {
		t.Errorf("data = %q, want a zero-length body", got.Data)
	}
	if v := got.Header.Get("tenant"); v != "acme" {
		t.Errorf("tenant header = %q, want acme (user headers are kept)", v)
	}
	for k := range got.Header {
		if strings.HasPrefix(strings.ToLower(k), "eventbus-dlq-") {
			t.Errorf("header %q leaked into the replay", k)
		}
	}
	if v := got.Header.Get("Nats-Msg-Id"); v != "replay:DLQ:9" {
		t.Errorf("Nats-Msg-Id = %q, want replay:DLQ:9", v)
	}
	if rec.acks != 1 || rec.naks != 0 {
		t.Errorf("record acks=%d naks=%d, want 1 and 0", rec.acks, rec.naks)
	}
}

func TestReplayer_DrainedReturnsPromptly(t *testing.T) {
	cases := map[string]*rpConsumer{
		"empty first batch":          {batches: []rpBatch{{}}},
		"ErrNoMessages batch error":  {batches: []rpBatch{{err: jetstream.ErrNoMessages}}},
		"ErrNoMessages fetch return": {fetchErr: jetstream.ErrNoMessages},
	}
	for name, cons := range cases {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			js, res, err := rpReplay(t, cons, ReplayOptions{})
			if err != nil {
				t.Fatalf("Replay on a drained DLQ: %v", err)
			}
			if res != (ReplayResult{}) {
				t.Errorf("result = %+v, want zero", res)
			}
			if len(js.published) != 0 {
				t.Errorf("published %d messages from an empty DLQ", len(js.published))
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("Replay took %v on an empty DLQ, want under 1 s", d)
			}
		})
	}
}

func TestReplayer_LimitStopsEarly(t *testing.T) {
	var first, second []*rpMsg
	for i := uint64(1); i <= 3; i++ {
		first = append(first, rpRecord(i, "orders.created", fmt.Sprintf("evt-%d", i), []byte("x")))
	}
	for i := uint64(4); i <= 5; i++ {
		second = append(second, rpRecord(i, "orders.created", fmt.Sprintf("evt-%d", i), []byte("x")))
	}
	cons := &rpConsumer{batches: []rpBatch{{msgs: first}, {msgs: second}}}
	js, res, err := rpReplay(t, cons, ReplayOptions{Limit: 3})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 3 || res.Failed != 0 {
		t.Errorf("result = %+v, want Replayed 3", res)
	}
	if len(js.published) != 3 {
		t.Errorf("published %d, want 3", len(js.published))
	}
	for _, n := range cons.ns {
		if n > 3 {
			t.Errorf("FetchNoWait(%d) with Limit 3, want n <= 3", n)
		}
	}
	for _, m := range second {
		if m.acks != 0 || m.naks != 0 {
			t.Errorf("record beyond the limit was settled (acks=%d naks=%d)", m.acks, m.naks)
		}
	}
}

func TestReplayer_DurableConsumerName(t *testing.T) {
	cons := &rpConsumer{}
	js := &rpJS{consumer: cons}
	r := NewReplayer(js)
	for i := 0; i < 2; i++ {
		if _, err := r.Replay(context.Background(), ReplayOptions{FromStream: "DLQ", Consumer: "ops"}); err != nil {
			t.Fatalf("Replay %d: %v", i, err)
		}
	}
	if len(js.cfgs) != 2 {
		t.Fatalf("consumer configured %d times, want 2", len(js.cfgs))
	}
	for i, cfg := range js.cfgs {
		if cfg.Name != "ops" || cfg.Durable != "ops" {
			t.Errorf("call %d: Name=%q Durable=%q, want ops for both", i, cfg.Name, cfg.Durable)
		}
	}
}

func TestReplayer_DuplicatePubAckCountsReplayed(t *testing.T) {
	rec := rpRecord(7, "orders.created", "evt-7", []byte("x"))
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js := &rpJS{consumer: cons, results: []rpPubResult{{ack: &jetstream.PubAck{Duplicate: true}}}}
	res, err := NewReplayer(js).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res != (ReplayResult{Replayed: 1}) {
		t.Errorf("result = %+v, want Replayed 1", res)
	}
	if rec.acks != 1 || rec.naks != 0 {
		t.Errorf("record acks=%d naks=%d, want 1 and 0", rec.acks, rec.naks)
	}
}

func TestReplayer_PublishErrorNaksAndReturns(t *testing.T) {
	recs := []*rpMsg{
		rpRecord(1, "orders.created", "evt-1", []byte("x")),
		rpRecord(2, "orders.created", "evt-2", []byte("x")),
		rpRecord(3, "orders.created", "evt-3", []byte("x")),
	}
	boom := errors.New("publish refused")
	cons := &rpConsumer{batches: []rpBatch{{msgs: recs}, {}}}
	js := &rpJS{consumer: cons, results: []rpPubResult{{}, {err: boom}}}
	res, err := NewReplayer(js).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the publish error", err)
	}
	if !strings.Contains(err.Error(), "seq 2") {
		t.Errorf("err = %q, want it to name stream sequence 2", err)
	}
	if res != (ReplayResult{Replayed: 1, Failed: 1}) {
		t.Errorf("result = %+v, want {Replayed:1 Failed:1}", res)
	}
	if recs[0].acks != 1 || recs[0].naks != 0 {
		t.Errorf("record 1 acks=%d naks=%d, want 1 and 0", recs[0].acks, recs[0].naks)
	}
	for i, m := range recs[1:] {
		if m.naks != 1 || m.acks != 0 {
			t.Errorf("record %d acks=%d naks=%d, want 0 and 1", i+2, m.acks, m.naks)
		}
	}
	if len(js.published) != 2 {
		t.Errorf("published %d, want 2 (the record after the failure is not attempted)", len(js.published))
	}
}

func TestReplayer_MissingSubjectFails(t *testing.T) {
	rec := rpRecord(9, "", "evt-9", []byte("x"))
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}}}
	js, res, err := rpReplay(t, cons, ReplayOptions{})
	if !errors.Is(err, ErrReplaySubject) {
		t.Fatalf("err = %v, want ErrReplaySubject", err)
	}
	if res != (ReplayResult{Failed: 1}) {
		t.Errorf("result = %+v, want Failed 1", res)
	}
	if rec.naks != 1 || rec.acks != 0 {
		t.Errorf("record acks=%d naks=%d, want 0 and 1", rec.acks, rec.naks)
	}
	if len(js.published) != 0 {
		t.Errorf("published %d messages without a target subject", len(js.published))
	}
}

func TestReplayer_InvalidSubjectsRejected(t *testing.T) {
	for _, subj := range []string{"a.*", "a.>", "*", ">", "a b", "a\tb", "a\nb", "a..b", ".a", "a.", "a.*.b"} {
		t.Run(fmt.Sprintf("%q", subj), func(t *testing.T) {
			rec := rpRecord(1, subj, "evt-1", []byte("x"))
			cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}}}
			js, res, err := rpReplay(t, cons, ReplayOptions{})
			if !errors.Is(err, ErrReplaySubject) {
				t.Fatalf("subject %q: err = %v, want ErrReplaySubject", subj, err)
			}
			if res.Failed != 1 || res.Replayed != 0 {
				t.Errorf("result = %+v, want Failed 1", res)
			}
			if rec.naks != 1 || len(js.published) != 0 {
				t.Errorf("naks=%d published=%d, want 1 and 0", rec.naks, len(js.published))
			}
		})
	}
	for _, subj := range []string{"orders.created", "a", "chat.message.x", "a-b.c_d"} {
		if !validReplaySubject(subj) {
			t.Errorf("validReplaySubject(%q) = false, want true", subj)
		}
	}
}

func TestReplayer_ToSubjectOverride(t *testing.T) {
	rec := rpRecord(3, "orders.created", "evt-3", []byte("x"))
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js, res, err := rpReplay(t, cons, ReplayOptions{ToSubject: "retry.orders"})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 || len(js.published) != 1 {
		t.Fatalf("result = %+v published = %d, want one replay", res, len(js.published))
	}
	if got := js.published[0].Subject; got != "retry.orders" {
		t.Errorf("subject = %q, want retry.orders", got)
	}
}

func TestReplayer_FilterSubjectPropagated(t *testing.T) {
	js, _, err := rpReplay(t, &rpConsumer{}, ReplayOptions{FilterSubject: "dlq.>"})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(js.cfgs) != 1 || js.cfgs[0].FilterSubject != "dlq.>" {
		t.Errorf("consumer configs = %+v, want FilterSubject dlq.>", js.cfgs)
	}
}

func TestReplayer_EmptyStreamRequired(t *testing.T) {
	js := &rpJS{consumer: &rpConsumer{}}
	_, err := NewReplayer(js).Replay(context.Background(), ReplayOptions{})
	if !errors.Is(err, ErrStreamRequired) {
		t.Fatalf("err = %v, want ErrStreamRequired", err)
	}
	if len(js.cfgs) != 0 {
		t.Errorf("consumer created %d times for an empty stream", len(js.cfgs))
	}
}

func TestReplayer_MetadataErrorPublishesWithoutMsgID(t *testing.T) {
	rec := rpRecord(5, "orders.created", "evt-5", []byte("x"))
	rec.metaErr = errors.New("not a jetstream message")
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js, res, err := rpReplay(t, cons, ReplayOptions{})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 || len(js.published) != 1 {
		t.Fatalf("result = %+v published = %d, want one replay", res, len(js.published))
	}
	if _, ok := js.published[0].Header[jetstream.MsgIDHeader]; ok {
		t.Errorf("Nats-Msg-Id = %q, want none when Metadata fails", js.published[0].Header.Get(jetstream.MsgIDHeader))
	}
}

// --- replay span fakes (prefix ox) ---

// oxSpanData is one span an oxTracerProvider tracer started.
type oxSpanData struct {
	name  string
	kind  trace.SpanKind
	attrs map[attribute.Key]attribute.Value
	links []trace.Link
	errs  []error
	code  codes.Code
	ended bool
}

// oxTracerProvider is a recording tracer provider built from the API packages
// only. Every span it starts reports the fixed span context sc.
type oxTracerProvider struct {
	embedded.TracerProvider

	sc    trace.SpanContext
	mu    sync.Mutex
	spans []*oxSpanData
}

func (p *oxTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return &oxTracer{tp: p}
}

// started returns the recorded spans.
func (p *oxTracerProvider) started() []*oxSpanData {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*oxSpanData(nil), p.spans...)
}

type oxTracer struct {
	embedded.Tracer
	tp *oxTracerProvider
}

func (t *oxTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	d := &oxSpanData{
		name:  name,
		kind:  cfg.SpanKind(),
		attrs: map[attribute.Key]attribute.Value{},
		links: cfg.Links(),
	}
	for _, kv := range cfg.Attributes() {
		d.attrs[kv.Key] = kv.Value
	}
	t.tp.mu.Lock()
	t.tp.spans = append(t.tp.spans, d)
	t.tp.mu.Unlock()

	_, inner := noop.NewTracerProvider().Tracer("").Start(ctx, name)
	sp := &oxSpan{Span: inner, tp: t.tp, d: d}
	return trace.ContextWithSpan(ctx, sp), sp
}

type oxSpan struct {
	trace.Span
	tp *oxTracerProvider
	d  *oxSpanData
}

func (s *oxSpan) SpanContext() trace.SpanContext { return s.tp.sc }

func (s *oxSpan) RecordError(err error, _ ...trace.EventOption) {
	s.tp.mu.Lock()
	s.d.errs = append(s.d.errs, err)
	s.tp.mu.Unlock()
}

func (s *oxSpan) SetStatus(code codes.Code, _ string) {
	s.tp.mu.Lock()
	s.d.code = code
	s.tp.mu.Unlock()
}

func (s *oxSpan) End(...trace.SpanEndOption) {
	s.tp.mu.Lock()
	s.d.ended = true
	s.tp.mu.Unlock()
}

const (
	oxReplayTraceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oxReplaySpanID  = "bbbbbbbbbbbbbbbb"
	oxReplayTP      = "00-" + oxReplayTraceID + "-" + oxReplaySpanID + "-01"
)

// oxReplayProvider returns a recording provider whose spans carry the fixed
// replay span context.
func oxReplayProvider(t *testing.T) *oxTracerProvider {
	t.Helper()
	return &oxTracerProvider{sc: oxFakeSpanContext(t, oxReplayTraceID, oxReplaySpanID)}
}

// oxRecordWithDLQTrace is rpRecord plus the dead-letter traceparent.
func oxRecordWithDLQTrace(seq uint64, tp string) *rpMsg {
	rec := rpRecord(seq, "orders.created", "evt-1", []byte(`{"id":"evt-1"}`))
	if tp != "" {
		rec.headers.Set(event.HeaderDLQTraceparent, tp)
	}
	return rec
}

func TestReplayer_ReplaySpanLinkAndHeaders(t *testing.T) {
	tp := oxReplayProvider(t)
	rec := oxRecordWithDLQTrace(42, oxDeadTP)
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js := &rpJS{consumer: cons}

	res, err := NewReplayer(js, WithReplayTracerProvider(tp)).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Replayed != 1 {
		t.Fatalf("result = %+v, want 1 replayed", res)
	}

	spans := tp.started()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	sp := spans[0]
	if sp.name != "replay orders.created" {
		t.Errorf("span name = %q, want %q", sp.name, "replay orders.created")
	}
	if sp.kind != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want Producer", sp.kind)
	}
	if !sp.ended {
		t.Error("span was not ended")
	}
	if len(sp.links) != 1 {
		t.Fatalf("links = %d, want 1", len(sp.links))
	}
	l := sp.links[0]
	if l.SpanContext.TraceID().String() != oxDeadTraceID || l.SpanContext.SpanID().String() != oxDeadSpanID {
		t.Errorf("link target = %s/%s, want the dead-letter span %s/%s",
			l.SpanContext.TraceID(), l.SpanContext.SpanID(), oxDeadTraceID, oxDeadSpanID)
	}
	var linkType string
	for _, kv := range l.Attributes {
		if kv.Key == "eventbus.link.type" {
			linkType = kv.Value.AsString()
		}
	}
	if linkType != "dead_letter" {
		t.Errorf("link type = %q, want dead_letter", linkType)
	}
	for k, want := range map[attribute.Key]string{
		"messaging.system":           "nats",
		"messaging.operation.name":   "replay",
		"messaging.destination.name": "orders.created",
	} {
		if got := sp.attrs[k].AsString(); got != want {
			t.Errorf("attribute %s = %q, want %q", k, got, want)
		}
	}
	if got := sp.attrs["eventbus.dlq.sequence"]; got.AsInt64() != 42 {
		t.Errorf("attribute eventbus.dlq.sequence = %v, want 42", got)
	}

	if len(js.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(js.published))
	}
	pub := js.published[0]
	for k, want := range map[string]string{
		event.HeaderReplayTraceparent:       oxReplayTP,
		event.HeaderReplayFailedTraceparent: oxDeadTP,
		event.HeaderReplayDLQSequence:       "42",
	} {
		if got := pub.Header.Values(k); len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want %q", k, got, want)
		}
	}
	for k := range pub.Header {
		if strings.HasPrefix(strings.ToLower(k), "eventbus-dlq-") {
			t.Errorf("header %q leaked into the replay", k)
		}
	}
	if !bytes.Equal(pub.Data, rec.data) {
		t.Errorf("data = %q, want the record bytes untouched", pub.Data)
	}
}

func TestReplayer_StaleReplayHeadersReplaced(t *testing.T) {
	tp := oxReplayProvider(t)
	rec := oxRecordWithDLQTrace(42, oxDeadTP)
	// A record re-dead-lettered after an earlier replay carries that replay's
	// headers, in any case.
	rec.headers[event.HeaderReplayTraceparent] = []string{"00-11111111111111111111111111111111-2222222222222222-01"}
	rec.headers[event.HeaderReplayDLQSequence] = []string{"7"}
	rec.headers["eventbus-replay-failed-traceparent"] = []string{"stale"}
	rec.headers["Eventbus-Replay-Extra"] = []string{"stale"}
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js := &rpJS{consumer: cons}

	if _, err := NewReplayer(js, WithReplayTracerProvider(tp)).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	pub := js.published[0]
	for k, want := range map[string]string{
		event.HeaderReplayTraceparent:       oxReplayTP,
		event.HeaderReplayFailedTraceparent: oxDeadTP,
		event.HeaderReplayDLQSequence:       "42",
	} {
		if got := pub.Header.Values(k); len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want the single fresh value %q", k, got, want)
		}
	}
	for k, v := range pub.Header {
		if lk := strings.ToLower(k); lk == "eventbus-replay-extra" || k == "eventbus-replay-failed-traceparent" {
			t.Errorf("stale replay header %q survived: %v", k, v)
		}
	}
}

func TestReplayer_MissingOrInvalidTraceparentNoLink(t *testing.T) {
	cases := map[string]string{
		"missing":   "",
		"garbage":   "not-a-traceparent",
		"zero ids":  "00-00000000000000000000000000000000-0000000000000000-01",
		"truncated": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
	}
	for name, dtp := range cases {
		t.Run(name, func(t *testing.T) {
			tp := oxReplayProvider(t)
			rec := oxRecordWithDLQTrace(42, dtp)
			cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
			js := &rpJS{consumer: cons}

			res, err := NewReplayer(js, WithReplayTracerProvider(tp)).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"})
			if err != nil || res.Replayed != 1 {
				t.Fatalf("Replay = %+v, %v; a bad traceparent must not stop the replay", res, err)
			}
			spans := tp.started()
			if len(spans) != 1 {
				t.Fatalf("spans = %d, want 1", len(spans))
			}
			if len(spans[0].links) != 0 {
				t.Errorf("links = %d, want none for %q", len(spans[0].links), dtp)
			}
			pub := js.published[0]
			if got := pub.Header.Values(event.HeaderReplayFailedTraceparent); len(got) != 0 {
				t.Errorf("%s = %v, want it absent", event.HeaderReplayFailedTraceparent, got)
			}
			if got := pub.Header.Get(event.HeaderReplayDLQSequence); got != "42" {
				t.Errorf("%s = %q, want 42", event.HeaderReplayDLQSequence, got)
			}
			if got := pub.Header.Get(event.HeaderReplayTraceparent); got != oxReplayTP {
				t.Errorf("%s = %q, want %q", event.HeaderReplayTraceparent, got, oxReplayTP)
			}
		})
	}
}

func TestReplayer_GlobalTracerProviderFallback(t *testing.T) {
	tp := oxReplayProvider(t)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	rec := oxRecordWithDLQTrace(5, oxDeadTP)
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}, {}}}
	js := &rpJS{consumer: cons}

	if _, err := NewReplayer(js).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	spans := tp.started()
	if len(spans) != 1 || spans[0].name != "replay orders.created" {
		t.Fatalf("global provider spans = %+v, want one named %q", spans, "replay orders.created")
	}
	if got := js.published[0].Header.Get(event.HeaderReplayTraceparent); got != oxReplayTP {
		t.Errorf("%s = %q, want %q", event.HeaderReplayTraceparent, got, oxReplayTP)
	}
}

func TestReplayer_PublishErrorRecordedOnSpan(t *testing.T) {
	tp := oxReplayProvider(t)
	rec := oxRecordWithDLQTrace(42, oxDeadTP)
	cons := &rpConsumer{batches: []rpBatch{{msgs: []*rpMsg{rec}}}}
	boom := errors.New("publish refused")
	js := &rpJS{consumer: cons, results: []rpPubResult{{err: boom}}}

	res, err := NewReplayer(js, WithReplayTracerProvider(tp)).Replay(context.Background(), ReplayOptions{FromStream: "DLQ"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the publish error", err)
	}
	if res.Failed != 1 || res.Replayed != 0 {
		t.Errorf("result = %+v, want 1 failed", res)
	}
	if rec.naks != 1 || rec.acks != 0 {
		t.Errorf("record acks=%d naks=%d, want a Nak and no Ack", rec.acks, rec.naks)
	}
	spans := tp.started()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	sp := spans[0]
	if len(sp.errs) != 1 || !errors.Is(sp.errs[0], boom) {
		t.Errorf("recorded errors = %v, want the publish error", sp.errs)
	}
	if sp.code != codes.Error {
		t.Errorf("span status = %v, want Error", sp.code)
	}
	if !sp.ended {
		t.Error("span was not ended on the error path")
	}
}
