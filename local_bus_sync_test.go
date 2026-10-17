package event_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	event "github.com/isaquesb/go-event-bus"
)

// lsEvent is the event of the EmitSync and HandlerTimeout tests.
type lsEvent struct{}

func (*lsEvent) Name() string { return "ls.evt" }

// lsHandlerErrors returns the *HandlerError members of a joined EmitSync error,
// keyed by handler name.
func lsHandlerErrors(t *testing.T, err error) map[string]*event.HandlerError {
	t.Helper()
	out := map[string]*event.HandlerError{}
	j, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("error %v (%T) is not a joined error", err, err)
	}
	for _, m := range j.Unwrap() {
		var he *event.HandlerError
		if errors.As(m, &he) {
			out[he.Handler] = he
		}
	}
	return out
}

func TestLocalBus_EmitSync_JoinsTerminalFailures(t *testing.T) {
	var onErr atomic.Int32
	bus := event.NewLocalBus(event.LocalBusOptions{
		OnErr: func(context.Context, event.Event, error, string) { onErr.Add(1) },
	})

	bad := errors.New("ls: bad")
	var h3Ran atomic.Bool
	ctx := context.Background()
	for _, h := range []struct {
		name string
		fn   event.HandleFn
	}{
		{"h1", func(context.Context, event.Event) error { panic("boom") }},
		{"h2", func(context.Context, event.Event) error { return bad }},
		{"h3", func(context.Context, event.Event) error { h3Ran.Store(true); return nil }},
	} {
		if err := bus.Subscribe(ctx, "ls.evt", h.fn, event.WithHandlerName(h.name)); err != nil {
			t.Fatalf("Subscribe %s: %v", h.name, err)
		}
	}

	evt := &lsEvent{}
	got, err := bus.EmitSync(ctx, evt)
	if got != event.Event(evt) {
		t.Errorf("EmitSync event = %v, want the emitted event", got)
	}
	if err == nil {
		t.Fatal("EmitSync err = nil, want the joined failures")
	}

	byHandler := lsHandlerErrors(t, err)
	if len(byHandler) != 2 {
		t.Fatalf("HandlerErrors = %d (%v), want 2 (h1, h2)", len(byHandler), err)
	}
	if he := byHandler["h1"]; he == nil || !errors.Is(he, event.ErrHandlerPanic) {
		t.Errorf("h1 error = %v, want a HandlerError matching ErrHandlerPanic", he)
	}
	if he := byHandler["h2"]; he == nil || !errors.Is(he, bad) {
		t.Errorf("h2 error = %v, want a HandlerError matching the handler error", he)
	}
	if byHandler["h3"] != nil {
		t.Errorf("h3 succeeded but has a HandlerError")
	}
	if !errors.Is(err, event.ErrHandlerPanic) || !errors.Is(err, bad) {
		t.Errorf("joined error %v must match both causes", err)
	}
	if !h3Ran.Load() {
		t.Error("h3 did not run after h1 panicked and h2 failed")
	}
	if got := onErr.Load(); got != 2 {
		t.Errorf("bus OnErr calls = %d, want 2", got)
	}
}

func TestLocalBus_EmitSync_AllSucceedReturnsNil(t *testing.T) {
	bus := event.NewLocalBus(event.LocalBusOptions{})
	var ran atomic.Int32
	for _, name := range []string{"a", "b"} {
		if err := bus.Subscribe(context.Background(), "ls.evt",
			func(context.Context, event.Event) error { ran.Add(1); return nil },
			event.WithHandlerName(name)); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	if _, err := bus.EmitSync(context.Background(), &lsEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil", err)
	}
	if got := ran.Load(); got != 2 {
		t.Errorf("handlers run = %d, want 2", got)
	}
}
