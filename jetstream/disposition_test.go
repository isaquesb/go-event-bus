package jetstream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
)

// dispPermanent is an error that is never worth retrying.
type dispPermanent struct{}

func (dispPermanent) Error() string  { return "disp: permanent failure" }
func (dispPermanent) Terminal() bool { return true }

// dispEvent is the event of the disposition tests. Its OnErr method counts the
// terminal reports it receives through dispEventOnErr.
type dispEvent struct {
	name    string
	EventID string `json:"event_id"`
}

var dispEventOnErr atomic.Int64

func (e *dispEvent) Name() string { return e.name }
func (e *dispEvent) Id() string   { return e.EventID }
func (e *dispEvent) OnErr(context.Context, error, string) {
	dispEventOnErr.Add(1)
}

// dispRegistry returns an mdRegistry that decodes name into a dispEvent.
func dispRegistry(name string) *mdRegistry {
	r := newMdRegistry()
	r.Register(name, func() event.Event { return &dispEvent{name: name} }, 1)
	return r
}

// dispErrCounter counts OnErr calls and keeps the last error.
type dispErrCounter struct {
	calls atomic.Int64
	last  atomic.Value // error
}

func (c *dispErrCounter) onErr(_ context.Context, _ event.Event, err error, _ string) {
	c.calls.Add(1)
	c.last.Store(&err)
}

func (c *dispErrCounter) lastErr() error {
	if p, ok := c.last.Load().(*error); ok {
		return *p
	}
	return nil
}

// dispEmit emits a dispEvent through bus and returns the recorded payload.
func dispEmit(t *testing.T, bus *Bus, js *mdJS, name string) []byte {
	t.Helper()
	if err := bus.Emit(context.Background(), &dispEvent{name: name}, event.WithID("id-disp")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	return js.payload(t, 0).data
}

func TestJetStreamBus_FinalDeliveryTerminalDeadLetteredThenTermed(t *testing.T) {
	const name = "disp.order"
	dispEventOnErr.Store(0)
	js := &mdJS{}
	dlq := event.NewMemoryDLQ()
	var errs dispErrCounter
	bus := newMdBus(js, dispRegistry(name), BusOptions{DLQPublisher: dlq, OnErr: errs.onErr})
	payload := dispEmit(t, bus, js, name)

	msg := &mdMsg{data: payload, numDelivered: 3}
	policy := deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		return dispPermanent{}
	}, policy)(msg)

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	rec := recs[0]
	if string(rec.Raw) != string(payload) {
		t.Errorf("record Raw = %q, want the emitted payload %q", rec.Raw, payload)
	}
	if rec.Delivery.Count != 3 || !rec.Delivery.Final() {
		t.Errorf("record Delivery = %+v, want Count 3 and Final", rec.Delivery)
	}
	if rec.Delivery.StreamSeq != 7 {
		t.Errorf("record StreamSeq = %d, want 7", rec.Delivery.StreamSeq)
	}

	got := msg.snapshot()
	if want := []string{"dead_lettered: terminal"}; len(got.termReasons) != 1 || got.termReasons[0] != want[0] {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 || got.plainTerms != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if n := errs.calls.Load(); n != 1 {
		t.Errorf("bus OnErr calls = %d, want 1", n)
	}
	if !errors.Is(errs.lastErr(), event.ErrDeadLettered) {
		t.Errorf("bus OnErr error = %v, want it to match event.ErrDeadLettered", errs.lastErr())
	}
	if n := dispEventOnErr.Load(); n != 1 {
		t.Errorf("event OnErr calls = %d, want 1 (D-16)", n)
	}
}
