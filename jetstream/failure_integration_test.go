//go:build integration

package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/nats-io/nats.go/jetstream"
)

// termAdvisorySubject is the MSG_TERMINATED advisory subject of one consumer.
func termAdvisorySubject(stream, consumer string) string {
	return "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED." + stream + "." + consumer
}

// maxDeliveriesAdvisorySubject is the MAX_DELIVERIES advisory subject of one
// consumer.
func maxDeliveriesAdvisorySubject(stream, consumer string) string {
	return "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + stream + "." + consumer
}

// TestIntegration_ChainDeadLetterVerbatimRecord is the tracer of the failure
// semantics: with the documented chain (DLQ outside Retry) a terminal handler
// error is dead-lettered once into a DLQ stream whose message data is
// byte-identical to the original stream message, carries the Eventbus-Dlq-*
// failure headers, and the original message is Acked (FAIL-01, FAIL-04,
// FAIL-06, D-09).
func TestIntegration_ChainDeadLetterVerbatimRecord(t *testing.T) {
	nc, js := connect(t)

	base := uniqueName(t, "it")
	subject := base + ".order"
	stream := newStream(t, js, base+".>")
	prefix, dlqStream := itNewDLQStream(t, js)
	consumer := uniqueName(t, "C")

	dlq := NewDLQ(js, nil, prefix)
	onErr := make(chan error, 8)
	bus := NewBus(js, itRegistry(), BusOptions{
		Invoker: invoker.NewChain(
			invoker.NewDLQ(dlq, nil),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 2, BaseDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond}, nil),
		),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
		DLQPublisher:     dlq,
		OnErr: func(_ context.Context, _ event.Event, err error, _ string) {
			onErr <- err
		},
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The chain dead-letters, so the backstop must never terminate this message.
	terminated := itAdvisories(t, nc, termAdvisorySubject(stream, consumer))

	handler := func(context.Context, event.Event) error {
		return invoker.PermanentError{Err: errors.New("boom")}
	}
	if err := bus.Subscribe(ctx, subject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{100 * time.Millisecond, 100 * time.Millisecond}),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Emit(ctx, &itOrder{ID: "o-1"}, event.WithSubject(subject)); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	itWaitStreamMsgs(t, js, dlqStream, 1)

	orig := itStreamMsg(t, js, stream, 1)
	rec := itStreamMsg(t, js, dlqStream, 1)
	if !bytes.Equal(orig.Data, rec.Data) {
		t.Fatalf("DLQ data is not byte-identical to the original\n orig: %s\n dlq:  %s", orig.Data, rec.Data)
	}
	if want := prefix + subject; rec.Subject != want {
		t.Errorf("DLQ subject = %q, want %q", rec.Subject, want)
	}

	var env event.Envelope
	if err := json.Unmarshal(rec.Data, &env); err != nil {
		t.Fatalf("DLQ data is not an envelope: %v", err)
	}
	if env.ID != "o-1" {
		t.Errorf("envelope id = %q, want o-1", env.ID)
	}

	for header, want := range map[string]string{
		event.HeaderDLQReason:          event.ReasonTerminal,
		event.HeaderDLQOriginalSubject: subject,
		event.HeaderDLQHandler:         consumer,
		event.HeaderDLQDeliveryCount:   "1",
		event.HeaderDLQAttempts:        "1",
	} {
		if got := rec.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if got := rec.Header.Get(event.HeaderDLQEventID); got == "" || got != env.ID {
		t.Errorf("%s = %q, want the envelope id %q", event.HeaderDLQEventID, got, env.ID)
	}

	// Acked: the ack floor advanced and nothing is pending. Term would also move
	// the floor, so the MSG_TERMINATED advisory is asserted absent below.
	waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.AckFloor.Consumer == 1 && ci.NumAckPending == 0
	})

	select {
	case err := <-onErr:
		if !errors.Is(err, event.ErrDeadLettered) {
			t.Errorf("OnErr error = %v, want it to match event.ErrDeadLettered", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnErr was not called within 5s")
	}

	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-onErr:
		t.Errorf("OnErr called a second time: %v", err)
	default:
	}
	select {
	case data := <-terminated:
		t.Errorf("MSG_TERMINATED advisory raised although the chain dead-lettered and Acked: %s", data)
	default:
	}
	if n := itStreamMsgs(t, js, dlqStream); n != 1 {
		t.Errorf("DLQ stream holds %d messages, want exactly 1", n)
	}
}
