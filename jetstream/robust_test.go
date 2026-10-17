package jetstream

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
)

// rbPanicInvoker is an Invoker that panics before calling next.
type rbPanicInvoker struct{}

func (rbPanicInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	panic("rb: invoker exploded")
}

// rbPolicy is the delivery policy of the panic tests: three deliveries.
func rbPolicy() deliveryPolicy {
	return deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
}

// rbEmit emits an mdEvent named rb.order through bus and returns the payload.
func rbEmit(t *testing.T, bus *Bus, js *mdJS) []byte {
	t.Helper()
	if err := bus.Emit(context.Background(), &mdEvent{name: "rb.order"}, event.WithID("id-rb")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return js.payload(t, 0).data
}

func TestJetStreamBus_PanicDeadLetteredThenTermed(t *testing.T) {
	js := &mdJS{}
	dlq := event.NewMemoryDLQ()
	var errs dispErrCounter
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
	payload := rbEmit(t, bus, js)

	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		panic("rb: handler exploded")
	}, rbPolicy())(msg)

	got := msg.snapshot()
	if want := []string{"dead_lettered: panic"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}

	recs := dlq.Records()
	if len(recs) != 1 || recs[0].Reason != event.ReasonPanic {
		t.Fatalf("records = %+v, want one with reason panic", recs)
	}
	if stack := recs[0].FailureHeaders()[event.HeaderDLQPanicStack]; stack == "" {
		t.Error("DLQ record has no panic stack header")
	}

	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	last := errs.lastErr()
	if !errors.Is(last, event.ErrDeadLettered) || !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("OnErr err = %v, want to match ErrDeadLettered and ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_PanicWithoutPublisherDropped(t *testing.T) {
	js := &mdJS{}
	var errs dispErrCounter
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{OnErr: errs.onErr})
	payload := rbEmit(t, bus, js)

	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		panic("rb: handler exploded")
	}, rbPolicy())(msg)

	got := msg.snapshot()
	if want := []string{"dropped: panic"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if n := errs.calls.Load(); n != 1 {
		t.Fatalf("OnErr calls = %d, want 1", n)
	}
	if last := errs.lastErr(); !errors.Is(last, event.ErrHandlerPanic) {
		t.Errorf("OnErr err = %v, want to match ErrHandlerPanic", last)
	}
}

func TestJetStreamBus_InvokerPanicRecovered(t *testing.T) {
	js := &mdJS{}
	bus := newMdBus(js, newMdRegistry("rb.order"), BusOptions{
		Invoker:      rbPanicInvoker{},
		DLQPublisher: event.NewMemoryDLQ(),
	})
	payload := rbEmit(t, bus, js)

	ran := false
	msg := &mdMsg{data: payload, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		ran = true
		return nil
	}, rbPolicy())(msg)

	if ran {
		t.Error("handler ran although the invoker panicked before next")
	}
	got := msg.snapshot()
	if len(got.termReasons) != 1 || !strings.HasSuffix(got.termReasons[0], "panic") {
		t.Errorf("termReasons = %q, want one reason ending in panic", got.termReasons)
	}
}
