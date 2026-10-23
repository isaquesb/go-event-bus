package invoker_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// dqPublisher records every event.DLQRecord it receives. When err is set it
// fails every publish.
type dqPublisher struct {
	mu   sync.Mutex
	recs []event.DLQRecord
	err  error
}

func (p *dqPublisher) Publish(_ context.Context, rec event.DLQRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recs = append(p.recs, rec)
	return p.err
}

func (p *dqPublisher) records() []event.DLQRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event.DLQRecord(nil), p.recs...)
}

func dqInvoke(ctx context.Context, p invoker.DLQPublisher, err error) error {
	return invoker.NewDLQ(p).Invoke(ctx, &testEvent{name: "test.event"}, "handler", func(context.Context) error {
		return err
	})
}

func dqDelivery(count, max int) context.Context {
	return event.WithDelivery(context.Background(), event.Delivery{Count: count, Max: max})
}

func dqTerminal(cause error) error {
	return &event.FailureError{Cause: invoker.PermanentError{Err: cause}, Reason: event.ReasonTerminal, Attempts: 1}
}

func TestDLQ_SuccessNoPublish(t *testing.T) {
	pub := &dqPublisher{}

	if err := dqInvoke(context.Background(), pub, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("DLQ must not publish on success, got %d records", n)
	}
}

func TestDLQ_TerminalPublishesOnAnyDelivery(t *testing.T) {
	pub := &dqPublisher{}
	cause := errors.New("bad input")

	err := dqInvoke(dqDelivery(1, 3), pub, dqTerminal(cause))

	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("expected ErrDeadLettered, got %v", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("expected the cause to be preserved, got %v", err)
	}
	recs := pub.records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Reason != event.ReasonTerminal {
		t.Errorf("expected reason terminal, got %q", recs[0].Reason)
	}
}

func TestDLQ_TransientNotFinalPassesThrough(t *testing.T) {
	pub := &dqPublisher{}
	transient := errors.New("transient")

	err := dqInvoke(dqDelivery(1, 3), pub, transient)

	if err != transient {
		t.Errorf("expected the same error back, got %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("expected no publish before the final delivery, got %d", n)
	}
}

func TestDLQ_TransientFinalPublishes(t *testing.T) {
	pub := &dqPublisher{}
	transient := errors.New("transient")

	err := dqInvoke(dqDelivery(3, 3), pub, transient)

	if !errors.Is(err, event.ErrDeadLettered) || !errors.Is(err, transient) {
		t.Errorf("expected ErrDeadLettered wrapping the cause, got %v", err)
	}
	recs := pub.records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Reason != event.ReasonDeliveryExhausted {
		t.Errorf("expected reason delivery_exhausted, got %q", recs[0].Reason)
	}
}

func TestDLQ_NoDeliveryIsFinal(t *testing.T) {
	pub := &dqPublisher{}

	err := dqInvoke(context.Background(), pub, errors.New("boom"))

	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("expected ErrDeadLettered, got %v", err)
	}
	if n := len(pub.records()); n != 1 {
		t.Errorf("expected 1 record without a Delivery in ctx, got %d", n)
	}
}

func TestDLQ_PublishErrorTyped(t *testing.T) {
	publishErr := errors.New("publish failed")
	pub := &dqPublisher{err: publishErr}

	err := dqInvoke(dqDelivery(1, 3), pub, dqTerminal(errors.New("bad")))

	if !errors.Is(err, event.ErrDeadLetterFailed) {
		t.Errorf("expected ErrDeadLetterFailed, got %v", err)
	}
	if errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("a failed publish must not match ErrDeadLettered: %v", err)
	}
	if !errors.Is(err, publishErr) {
		t.Errorf("expected the publish error to be preserved, got %v", err)
	}
}

func TestDLQ_InterruptedNotPublished(t *testing.T) {
	pub := &dqPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := dqInvoke(ctx, pub, context.Canceled)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("a subscription cancellation must not be published, got %d", n)
	}
}

func TestDLQ_NilPublisherPassesThrough(t *testing.T) {
	boom := dqTerminal(errors.New("bad"))

	err := dqInvoke(context.Background(), nil, boom)

	if err != boom {
		t.Errorf("expected the error unchanged with a nil publisher, got %v", err)
	}
}

func TestDLQ_AlreadyDeadLetteredNotRepublished(t *testing.T) {
	pub := &dqPublisher{}
	already := &event.DeadLetteredError{Cause: errors.New("bad"), Reason: event.ReasonTerminal}

	err := dqInvoke(dqDelivery(3, 3), pub, already)

	if err != error(already) {
		t.Errorf("expected the error unchanged, got %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("an already dead-lettered error must not be republished, got %d", n)
	}
}

func TestDLQ_RecordFields(t *testing.T) {
	pub := &dqPublisher{}
	raw := []byte(`{"id":"e1"}`)
	ctx := event.WithDelivery(context.Background(), event.Delivery{
		Count: 2, Max: 5, Subject: "orders.created", Raw: raw,
	})
	ctx = event.WithExecution(ctx, event.Execution{Handler: "ctx-handler"})
	ctx = event.WithMetadata(ctx, event.Metadata{ID: "e1", Name: "test.event"})

	cause := errors.New("bad input")
	_ = dqInvoke(ctx, pub, &event.FailureError{
		Cause: invoker.PermanentError{Err: cause}, Reason: event.ReasonTerminal, Attempts: 4,
	})

	recs := pub.records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	rec := recs[0]
	if string(rec.Raw) != string(raw) {
		t.Errorf("Raw = %q, want %q", rec.Raw, raw)
	}
	if rec.Subject != "orders.created" {
		t.Errorf("Subject = %q", rec.Subject)
	}
	if rec.Handler != "handler" {
		t.Errorf("Handler = %q, want the chain handler name", rec.Handler)
	}
	if rec.EventID != "e1" {
		t.Errorf("EventID = %q", rec.EventID)
	}
	if rec.Attempts != 4 {
		t.Errorf("Attempts = %d, want 4 from the FailureError", rec.Attempts)
	}
	if rec.Delivery.Count != 2 || rec.Delivery.Max != 5 {
		t.Errorf("Delivery = %+v", rec.Delivery)
	}
}
