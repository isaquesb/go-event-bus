package jetstream

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// dlqtJS is a capturing dlqBackend. errs scripts the result of each PublishMsg
// call in order; a call beyond the script succeeds.
type dlqtJS struct {
	mu   sync.Mutex
	msgs []*nats.Msg
	opts [][]jetstream.PublishOpt
	errs []error
}

func (j *dlqtJS) PublishMsg(_ context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.msgs = append(j.msgs, msg)
	j.opts = append(j.opts, opts)
	n := len(j.msgs)
	if n <= len(j.errs) && j.errs[n-1] != nil {
		return nil, j.errs[n-1]
	}
	return &jetstream.PubAck{Stream: "DLQ", Sequence: uint64(n)}, nil
}

func (j *dlqtJS) calls() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.msgs)
}

func (j *dlqtJS) msg(i int) *nats.Msg {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.msgs[i]
}

var dlqtRaw = []byte(`{"name":"orders.created","version":1,"id":"evt-1","payload":{"n":1}}`)

func dlqtRecord() event.DLQRecord {
	return event.DLQRecord{
		Raw: bytes.Clone(dlqtRaw),
		Headers: map[string][]string{
			"Nats-Msg-Id":          {"evt-1"},
			"Nats-Expected-Stream": {"S"},
			"tenant":               {"acme"},
			"traceparent":          {"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		},
		EventID:   "evt-1",
		EventName: "orders.created",
		Cause:     errors.New("boom"),
		Reason:    event.ReasonTerminal,
		Handler:   "h",
		Subject:   "orders.created",
		Attempts:  2,
		Delivery:  event.Delivery{Count: 3, Max: 3, Stream: "S", StreamSeq: 42},
		FailedAt:  time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestDLQ_PublishesRawVerbatimWithFailureHeaders(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "")

	rec := dlqtRecord()
	if err := d.Publish(context.Background(), rec); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if js.calls() != 1 {
		t.Fatalf("publish calls = %d, want 1", js.calls())
	}
	m := js.msg(0)
	if m.Subject != "dlq.orders.created" {
		t.Errorf("subject = %q, want dlq.orders.created", m.Subject)
	}
	if !bytes.Equal(m.Data, dlqtRaw) {
		t.Errorf("data = %q, want the original envelope byte for byte", m.Data)
	}
	for _, k := range []string{"Nats-Msg-Id", "Nats-Expected-Stream"} {
		if _, ok := m.Header[k]; ok {
			t.Errorf("header %s must be stripped (D-28)", k)
		}
	}
	want := map[string]string{
		"tenant":                        "acme",
		"traceparent":                   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		event.HeaderDLQOriginalMsgID:    "evt-1",
		event.HeaderDLQReason:           event.ReasonTerminal,
		event.HeaderDLQCause:            "boom",
		event.HeaderDLQAttempts:         "2",
		event.HeaderDLQDeliveryCount:    "3",
		event.HeaderDLQHandler:          "h",
		event.HeaderDLQOriginalSubject:  "orders.created",
		event.HeaderDLQOriginalStream:   "S",
		event.HeaderDLQOriginalSequence: "42",
		event.HeaderDLQEventID:          "evt-1",
		event.HeaderDLQEventName:        "orders.created",
	}
	for k, v := range want {
		if got := m.Header.Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
	// The record the caller passed in is not mutated.
	if _, ok := rec.Headers["Nats-Msg-Id"]; !ok {
		t.Error("Publish mutated rec.Headers")
	}
}

func TestDLQ_DefaultPrefix(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "")
	if DefaultDLQPrefix != "dlq." {
		t.Fatalf("DefaultDLQPrefix = %q", DefaultDLQPrefix)
	}
	if err := d.Publish(context.Background(), dlqtRecord()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := js.msg(0).Subject; got != "dlq.orders.created" {
		t.Errorf("subject = %q, want dlq.orders.created", got)
	}
}

func TestDLQ_CustomPrefix(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "legacy.")
	if err := d.Publish(context.Background(), dlqtRecord()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := js.msg(0).Subject; got != "legacy.orders.created" {
		t.Errorf("subject = %q, want legacy.orders.created", got)
	}
}

func TestDLQ_NoMessageIDOnPublish(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "")
	if err := d.Publish(context.Background(), dlqtRecord()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(js.opts[0]) != 0 {
		t.Errorf("publish options = %d, want none (no WithMsgID, D-13)", len(js.opts[0]))
	}
	for k := range js.msg(0).Header {
		if len(k) >= 5 && (k[:5] == "Nats-" || k[:5] == "nats-") {
			t.Errorf("published header %s carries a broker system header", k)
		}
	}
}

// dlqtMetrics counts counter increments by name.
type dlqtMetrics struct {
	mu     sync.Mutex
	counts map[string]int64
}

func (m *dlqtMetrics) IncCounter(name string, v int64, _ invoker.Labels) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]int64{}
	}
	m.counts[name] += v
}

func (m *dlqtMetrics) ObserveHistogram(string, float64, invoker.Labels) {}
func (m *dlqtMetrics) SetGauge(string, float64, invoker.Labels)         {}

func (m *dlqtMetrics) get(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[name]
}

func TestDLQ_LocalBusRecordEncodedWithRegistry(t *testing.T) {
	js := &dlqtJS{}
	reg := newMdRegistry("orders.created")
	d := NewDLQ(js, nil, "", WithDLQRegistry(reg))

	evt := &mdEvent{name: "orders.created", EventID: "m-1", Payload: "p"}
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "m-1", Name: "orders.created"})
	rec := event.DLQRecord{
		Evt:       evt,
		EventID:   "m-1",
		EventName: "orders.created",
		Cause:     errors.New("boom"),
		Reason:    event.ReasonTerminal,
		Handler:   "h",
		Subject:   "orders.created",
		Attempts:  1,
	}
	if err := d.Publish(ctx, rec); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if js.calls() != 1 {
		t.Fatalf("publish calls = %d, want 1", js.calls())
	}
	m := js.msg(0)
	if m.Subject != "dlq.orders.created" {
		t.Errorf("subject = %q", m.Subject)
	}
	dctx, got, err := reg.Decode(context.Background(), m.Data)
	if err != nil {
		t.Fatalf("published data does not decode: %v", err)
	}
	if got.Name() != "orders.created" {
		t.Errorf("decoded name = %q", got.Name())
	}
	md, ok := event.MetadataFromContext(dctx)
	if !ok || md.ID != "m-1" {
		t.Errorf("decoded Metadata = %+v ok=%v, want ID m-1", md, ok)
	}
}

func TestDLQ_NoEnvelopeWithoutRegistry(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "")
	rec := event.DLQRecord{Evt: &mdEvent{name: "orders.created"}, Subject: "orders.created"}
	err := d.Publish(context.Background(), rec)
	if !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("err = %v, want ErrNoEnvelope", err)
	}
	if js.calls() != 0 {
		t.Errorf("publish calls = %d, want 0", js.calls())
	}
}

// TestDLQ_DecodeFailureEmptyBodyPublishedVerbatim is the invariant for
// 08-REVIEW CR-02: a record without an event is a decode failure whose Raw is
// the complete wire form, so it is published as it is, even when empty, and
// never gets ErrNoEnvelope or a registry lookup. Every record shape a transport
// builds for a decode failure is a row.
func TestDLQ_DecodeFailureEmptyBodyPublishedVerbatim(t *testing.T) {
	decodeErr := &event.DecodeError{Err: event.ErrMalformedEnvelope}
	for _, tc := range []struct {
		name     string
		rec      event.DLQRecord
		registry event.Registry
		subject  string
		want     map[string]string
		absent   []string
	}{
		{
			name: "jetstream delivery",
			rec: event.DLQRecord{
				Headers: map[string][]string{"tenant": {"acme"}, "Nats-Msg-Id": {"m-1"}},
				Subject: "orders.created",
				Cause:   decodeErr,
				Reason:  event.ReasonDecodeMalformed,
				Handler: "h",
				Delivery: event.Delivery{
					Count: 1, Max: 3, Subject: "orders.created", Stream: "S", StreamSeq: 42,
				},
			},
			subject: "dlq.orders.created",
			want: map[string]string{
				event.HeaderDLQReason:           event.ReasonDecodeMalformed,
				event.HeaderDLQOriginalStream:   "S",
				event.HeaderDLQOriginalSequence: "42",
				event.HeaderDLQOriginalMsgID:    "m-1",
				"tenant":                        "acme",
			},
			absent: []string{"Nats-Msg-Id"},
		},
		{
			name: "nats core delivery",
			rec: event.DLQRecord{
				Raw:      []byte{},
				Headers:  map[string][]string{"tenant": {"acme"}},
				Subject:  "orders.created",
				Cause:    decodeErr,
				Reason:   event.ReasonDecodeMalformed,
				Delivery: event.Delivery{Count: 1, Max: 1, Subject: "orders.created"},
			},
			subject: "dlq.orders.created",
			want: map[string]string{
				event.HeaderDLQReason: event.ReasonDecodeMalformed,
				"tenant":              "acme",
			},
			absent: []string{event.HeaderDLQOriginalSequence},
		},
		{
			name:     "bare record with registry",
			rec:      event.DLQRecord{Subject: "orders.created", Reason: event.ReasonDecodeMalformed},
			registry: newMdRegistry("orders.created"),
			subject:  "dlq.orders.created",
			want:     map[string]string{event.HeaderDLQReason: event.ReasonDecodeMalformed},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			js := &dlqtJS{}
			var opts []DLQOption
			if tc.registry != nil {
				opts = append(opts, WithDLQRegistry(tc.registry))
			}
			d := NewDLQ(js, nil, "", opts...)

			err := d.Publish(context.Background(), tc.rec)
			if errors.Is(err, ErrNoEnvelope) {
				t.Fatalf("Publish = %v, a record without an event must not get ErrNoEnvelope", err)
			}
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if js.calls() != 1 {
				t.Fatalf("publish calls = %d, want 1", js.calls())
			}
			m := js.msg(0)
			if m.Subject != tc.subject {
				t.Errorf("subject = %q, want %q", m.Subject, tc.subject)
			}
			if len(m.Data) != 0 {
				t.Errorf("data = %q, want a zero-length body", m.Data)
			}
			for k, v := range tc.want {
				if got := m.Header.Get(k); got != v {
					t.Errorf("header %s = %q, want %q", k, got, v)
				}
			}
			for _, k := range tc.absent {
				if _, ok := m.Header[k]; ok {
					t.Errorf("header %s must be absent", k)
				}
			}
		})
	}
}

func TestDLQ_MaxPayloadFallsBackToMinimalHeaders(t *testing.T) {
	js := &dlqtJS{errs: []error{nats.ErrMaxPayload}}
	d := NewDLQ(js, nil, "")
	rec := dlqtRecord()
	rec.Cause = errors.New(strings.Repeat("c", 600))
	rec.Execution = event.Execution{Handler: "h", StartedAt: time.Date(2026, 10, 16, 11, 59, 0, 0, time.UTC)}

	if err := d.Publish(context.Background(), rec); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if js.calls() != 2 {
		t.Fatalf("publish calls = %d, want 2", js.calls())
	}
	first, second := js.msg(0), js.msg(1)
	if first.Header.Get(event.HeaderDLQFirstAttemptAt) == "" {
		t.Fatal("first attempt should carry the full header set")
	}
	if !bytes.Equal(second.Data, dlqtRaw) || second.Subject != "dlq.orders.created" {
		t.Errorf("retry data/subject changed: %q %q", second.Data, second.Subject)
	}
	for k, want := range map[string]string{
		event.HeaderDLQReason:          event.ReasonTerminal,
		event.HeaderDLQOriginalSubject: "orders.created",
		event.HeaderDLQEventID:         "evt-1",
		event.HeaderDLQHandler:         "h",
	} {
		if got := second.Header.Get(k); got != want {
			t.Errorf("minimal header %s = %q, want %q", k, got, want)
		}
	}
	if c := second.Header.Get(event.HeaderDLQCause); c == "" || len(c) > 128 {
		t.Errorf("minimal cause length = %d, want 1..128", len(c))
	}
	for _, k := range []string{event.HeaderDLQFirstAttemptAt, event.HeaderDLQFailedAt, event.HeaderDLQAttempts} {
		if _, ok := second.Header[k]; ok {
			t.Errorf("minimal retry still carries %s", k)
		}
	}
	if second.Header.Get("tenant") != "acme" {
		t.Error("minimal retry dropped the original user headers")
	}
}

func TestDLQ_MaxPayloadTwiceReturnsError(t *testing.T) {
	js := &dlqtJS{errs: []error{nats.ErrMaxPayload, nats.ErrMaxPayload}}
	m := &dlqtMetrics{}
	d := NewDLQ(js, m, "")
	err := d.Publish(context.Background(), dlqtRecord())
	if !errors.Is(err, nats.ErrMaxPayload) {
		t.Fatalf("err = %v, want nats.ErrMaxPayload", err)
	}
	if js.calls() != 2 {
		t.Errorf("publish calls = %d, want 2", js.calls())
	}
	if got := m.get("dlq_publish_error_total"); got != 1 {
		t.Errorf("dlq_publish_error_total = %v, want 1 (once per failed Publish call)", got)
	}
}

func TestDLQ_LongCauseTruncated(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "")
	rec := dlqtRecord()
	rec.Cause = errors.New(strings.Repeat("x", 5000))
	if err := d.Publish(context.Background(), rec); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	c := js.msg(0).Header.Get(event.HeaderDLQCause)
	if len(c) > 1024 {
		t.Errorf("cause length = %d, want <= 1024", len(c))
	}
	if !strings.HasSuffix(c, event.TruncationMarker) {
		t.Errorf("cause %q does not end with the truncation marker", c[len(c)-20:])
	}
}

func TestDLQ_StaleFailureHeadersReplaced(t *testing.T) {
	js := &dlqtJS{}
	d := NewDLQ(js, nil, "")
	rec := dlqtRecord()
	rec.Headers["Eventbus-Dlq-Reason"] = []string{"stale"}
	rec.Headers["eventbus-dlq-handler"] = []string{"old"}
	rec.Headers["Eventbus-Trace"] = []string{"keep-me"}
	if err := d.Publish(context.Background(), rec); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	h := js.msg(0).Header
	if got := h.Values(event.HeaderDLQReason); len(got) != 1 || got[0] != event.ReasonTerminal {
		t.Errorf("Reason values = %v, want single %q", got, event.ReasonTerminal)
	}
	if got := h.Values(event.HeaderDLQHandler); len(got) != 1 || got[0] != "h" {
		t.Errorf("Handler values = %v, want single h", got)
	}
	if got := h.Get("Eventbus-Trace"); got != "keep-me" {
		t.Errorf("other Eventbus-* header = %q, want kept verbatim", got)
	}
}

func TestDLQ_Metrics(t *testing.T) {
	m := &dlqtMetrics{}
	js := &dlqtJS{errs: []error{nil, errors.New("down")}}
	d := NewDLQ(js, m, "")

	if err := d.Publish(context.Background(), dlqtRecord()); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	if err := d.Publish(context.Background(), dlqtRecord()); err == nil {
		t.Fatal("second Publish: want error")
	}
	if got := m.get("dlq_published_total"); got != 1 {
		t.Errorf("dlq_published_total = %v, want 1", got)
	}
	if got := m.get("dlq_publish_error_total"); got != 1 {
		t.Errorf("dlq_publish_error_total = %v, want 1", got)
	}
}
