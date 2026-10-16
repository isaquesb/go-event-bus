package jetstream

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
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
