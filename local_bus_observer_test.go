package event_test

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	event "github.com/isaquesb/go-event-bus"
)

// lbRecord is one captured slog record.
type lbRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// lbCapture is a slog.Handler that records every record under a mutex.
type lbCapture struct {
	mu   sync.Mutex
	recs []lbRecord
}

func (c *lbCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *lbCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *lbCapture) WithGroup(string) slog.Handler            { return c }
func (c *lbCapture) Handle(_ context.Context, r slog.Record) error {
	rec := lbRecord{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	c.mu.Lock()
	c.recs = append(c.recs, rec)
	c.mu.Unlock()
	return nil
}

// withMsg returns the records whose message is msg.
func (c *lbCapture) withMsg(msg string) []lbRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []lbRecord
	for _, r := range c.recs {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

// lbInstallCapture makes a lbCapture the slog default until t ends.
func lbInstallCapture(t *testing.T) *lbCapture {
	t.Helper()
	prev := slog.Default()
	c := &lbCapture{}
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

// lbEvent is the event of these tests: its own OnErr method counts every call.
type lbEvent struct {
	onErr atomic.Int32
}

func (*lbEvent) Name() string                           { return "lb.order" }
func (e *lbEvent) OnErr(context.Context, error, string) { e.onErr.Add(1) }

// lbPermanent is a local error type with Terminal() true.
type lbPermanent struct{}

func (lbPermanent) Error() string  { return "lb: permanent failure" }
func (lbPermanent) Terminal() bool { return true }

const lbFailureMsg = "eventbus: message failed"

// lbPermanentBus builds a bus with the given observer, subscribes "billing" on
// "lb.order" and returns it. A nil observer is the zero-configuration case.
func lbPermanentBus(t *testing.T, obs event.Observer) *event.LocalBus {
	t.Helper()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:  &passthroughInvoker{},
		Observer: obs,
	})
	if err := bus.Subscribe(context.Background(), "lb.order",
		func(context.Context, event.Event) error { return lbPermanent{} },
		event.WithHandlerName("billing")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return bus
}

// TestLocalBus_ZeroConfigLogsFailureOnce is the tracer of plan 10-09: a LocalBus
// built with neither Observer nor DLQPublisher still logs a terminal failure
// exactly once, through the default SlogObserver on slog.Default.
func TestLocalBus_ZeroConfigLogsFailureOnce(t *testing.T) {
	logs := lbInstallCapture(t)
	bus := lbPermanentBus(t, nil)

	evt := &lbEvent{}
	_, _ = bus.EmitSync(context.Background(), evt, event.WithID("lb-1"))

	recs := logs.withMsg(lbFailureMsg)
	if len(recs) != 1 {
		t.Fatalf("failure records = %d (%+v), want exactly 1", len(recs), recs)
	}
	r := recs[0]
	if r.level != slog.LevelError {
		t.Errorf("level = %v, want Error", r.level)
	}
	want := map[string]string{
		"outcome":        "dropped",
		"reason":         "terminal",
		"handler":        "billing",
		"event_id":       "lb-1",
		"delivery_count": "1",
	}
	for k, v := range want {
		if got := r.attrs[k]; got != v {
			t.Errorf("attr %s = %q, want %q", k, got, v)
		}
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}

// TestLocalBus_NopObserverSilences: NopObserver{} produces no record, while the
// event's own OnErr method still runs.
func TestLocalBus_NopObserverSilences(t *testing.T) {
	logs := lbInstallCapture(t)
	bus := lbPermanentBus(t, event.NopObserver{})

	evt := &lbEvent{}
	_, _ = bus.EmitSync(context.Background(), evt, event.WithID("lb-1"))

	if recs := logs.withMsg(lbFailureMsg); len(recs) != 0 {
		t.Fatalf("failure records = %d (%+v), want 0", len(recs), recs)
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}
