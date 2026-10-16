package jetstream

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
