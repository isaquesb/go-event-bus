package event_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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

func TestLocalBus_EmitSync_StopsOnCallerCtxDone(t *testing.T) {
	t.Run("already done", func(t *testing.T) {
		bus := event.NewLocalBus(event.LocalBusOptions{})
		var ran atomic.Int32
		if err := bus.Subscribe(context.Background(), "ls.evt",
			func(context.Context, event.Event) error { ran.Add(1); return nil },
			event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := bus.EmitSync(ctx, &lsEvent{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("EmitSync = %v, want context.Canceled", err)
		}
		if got := ran.Load(); got != 0 {
			t.Errorf("handlers run = %d, want 0", got)
		}
	})

	t.Run("done mid dispatch", func(t *testing.T) {
		bus := event.NewLocalBus(event.LocalBusOptions{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var second atomic.Bool
		if err := bus.Subscribe(context.Background(), "ls.evt",
			func(context.Context, event.Event) error { cancel(); return nil },
			event.WithHandlerName("h1")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Subscribe(context.Background(), "ls.evt",
			func(context.Context, event.Event) error { second.Store(true); return nil },
			event.WithHandlerName("h2")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		_, err := bus.EmitSync(ctx, &lsEvent{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("EmitSync = %v, want context.Canceled", err)
		}
		if second.Load() {
			t.Error("h2 ran after the caller ctx was cancelled")
		}
	})
}

func TestLocalBus_EmitSync_HandledOutcomesNotErrors(t *testing.T) {
	t.Run("dead-lettered", func(t *testing.T) {
		dlq := event.NewMemoryDLQ()
		bus := event.NewLocalBus(event.LocalBusOptions{DLQPublisher: dlq})
		if err := bus.Subscribe(context.Background(), "ls.evt",
			func(context.Context, event.Event) error { return lfPermanent{} },
			event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if _, err := bus.EmitSync(context.Background(), &lsEvent{}); err != nil {
			t.Fatalf("EmitSync = %v, want nil for a dead-lettered failure", err)
		}
		if got := dlq.Len(); got != 1 {
			t.Errorf("DLQ records = %d, want 1", got)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		var onErr atomic.Int32
		bus := event.NewLocalBus(event.LocalBusOptions{
			Invoker: &lfInvoker{err: event.ErrDuplicate},
			OnErr:   func(context.Context, event.Event, error, string) { onErr.Add(1) },
		})
		lfSubscribeNoop(t, bus, "ls.evt")
		if _, err := bus.EmitSync(context.Background(), &lsEvent{}); err != nil {
			t.Fatalf("EmitSync = %v, want nil for a duplicate", err)
		}
		if got := onErr.Load(); got != 0 {
			t.Errorf("OnErr calls = %d, want 0", got)
		}
	})
}

func TestLocalBus_EmitSync_DeadLetterFailedIsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := &lfFailingPublisher{}
		bus := event.NewLocalBus(event.LocalBusOptions{
			Invoker:      &lfInvoker{err: lfPermanent{}},
			DLQPublisher: pub,
		})
		lfSubscribeNoop(t, bus, "ls.evt")

		_, err := bus.EmitSync(context.Background(), &lsEvent{})
		byHandler := lsHandlerErrors(t, err)
		he := byHandler["h"]
		if he == nil || !errors.Is(he, event.ErrDeadLetterFailed) {
			t.Fatalf("EmitSync err = %v, want a HandlerError for h matching ErrDeadLetterFailed", err)
		}
	})
}

func TestLocalBus_EmitSync_PanicDeadLetteredIsHandled(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	bus := event.NewLocalBus(event.LocalBusOptions{DLQPublisher: dlq})
	if err := bus.Subscribe(context.Background(), "ls.evt",
		func(context.Context, event.Event) error { panic("ls: boom") },
		event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := bus.EmitSync(context.Background(), &lsEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil for a dead-lettered panic", err)
	}
	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(recs))
	}
	if recs[0].Reason != event.ReasonPanic {
		t.Errorf("Reason = %q, want %q", recs[0].Reason, event.ReasonPanic)
	}
	if stack := recs[0].FailureHeaders()[event.HeaderDLQPanicStack]; stack == "" {
		t.Errorf("%s header is empty, want the panic stack", event.HeaderDLQPanicStack)
	}
}

// lsCapture records the bus OnErr calls and the DLQ-bound errors.
type lsCapture struct {
	mu   sync.Mutex
	errs []error
}

func (c *lsCapture) fn(_ context.Context, _ event.Event, err error, _ string) {
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.mu.Unlock()
}

func (c *lsCapture) all() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

func TestLocalBus_HandlerTimeoutIsTransient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dlq := event.NewMemoryDLQ()
		cap := &lsCapture{}
		bus := event.NewLocalBus(event.LocalBusOptions{
			HandlerTimeout: time.Second,
			DLQPublisher:   dlq,
			OnErr:          cap.fn,
		})

		var (
			cause    error
			hasDL    bool
			finished time.Time
		)
		if err := bus.Subscribe(context.Background(), "ls.evt", func(ctx context.Context, _ event.Event) error {
			_, hasDL = ctx.Deadline()
			<-ctx.Done()
			cause = context.Cause(ctx)
			finished = time.Now()
			return ctx.Err()
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		start := time.Now()
		bus.Emit(context.Background(), &lsEvent{})
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil (the timeout is not an interruption)", err)
		}

		if !hasDL {
			t.Error("async handler ctx has no deadline under HandlerTimeout")
		}
		if !errors.Is(cause, event.ErrHandlerTimeout) {
			t.Errorf("handler ctx cause = %v, want ErrHandlerTimeout", cause)
		}
		if got := finished.Sub(start); got != time.Second {
			t.Errorf("handler stopped after %v of fake time, want 1s", got)
		}

		recs := dlq.Records()
		if len(recs) != 1 {
			t.Fatalf("DLQ records = %d, want 1", len(recs))
		}
		if !errors.Is(recs[0].Cause, event.ErrHandlerTimeout) || !errors.Is(recs[0].Cause, context.DeadlineExceeded) {
			t.Errorf("record cause = %v, want ErrHandlerTimeout and DeadlineExceeded", recs[0].Cause)
		}

		errs := cap.all()
		if len(errs) != 1 {
			t.Fatalf("OnErr calls = %d, want 1", len(errs))
		}
		if !errors.Is(errs[0], event.ErrDeadLettered) || !errors.Is(errs[0], event.ErrHandlerTimeout) {
			t.Errorf("OnErr err = %v, want ErrDeadLettered and ErrHandlerTimeout", errs[0])
		}
		if errors.Is(errs[0], event.ErrShutdownInterrupted) {
			t.Errorf("OnErr err = %v, a timeout must not read as a shutdown interruption", errs[0])
		}
	})
}

func TestLocalBus_HandlerTimeoutScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var asyncDL, syncDL atomic.Bool
		hasDeadline := func(flag *atomic.Bool) event.HandleFn {
			return func(ctx context.Context, _ event.Event) error {
				_, ok := ctx.Deadline()
				flag.Store(ok)
				return nil
			}
		}

		// Zero timeout: an async handler has no deadline.
		zero := event.NewLocalBus(event.LocalBusOptions{})
		if err := zero.Subscribe(context.Background(), "ls.evt", hasDeadline(&asyncDL), event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		asyncDL.Store(true)
		zero.Emit(context.Background(), &lsEvent{})
		if err := zero.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if asyncDL.Load() {
			t.Error("HandlerTimeout 0: async handler ctx has a deadline, want none")
		}

		// A timeout never bounds an EmitSync handler.
		timed := event.NewLocalBus(event.LocalBusOptions{HandlerTimeout: time.Second})
		if err := timed.Subscribe(context.Background(), "ls.evt", hasDeadline(&syncDL), event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		syncDL.Store(true)
		if _, err := timed.EmitSync(context.Background(), &lsEvent{}); err != nil {
			t.Fatalf("EmitSync: %v", err)
		}
		if syncDL.Load() {
			t.Error("HandlerTimeout 1s: EmitSync handler ctx has a deadline, want none")
		}
		if err := timed.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	})
}

// lgCtxDLQ is a DLQPublisher that, like a broker-backed one, honors its ctx: it
// fails with ctx.Err() when the ctx is done and otherwise stores the record plus
// what the publish ctx carried.
type lgCtxDLQ struct {
	mu         sync.Mutex
	recs       []event.DLQRecord
	deliveryOK []bool
	metaIDs    []string
}

func (p *lgCtxDLQ) Publish(ctx context.Context, rec event.DLQRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, dok := event.DeliveryFromContext(ctx)
	md, _ := event.MetadataFromContext(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recs = append(p.recs, rec)
	p.deliveryOK = append(p.deliveryOK, dok)
	p.metaIDs = append(p.metaIDs, md.ID)
	return nil
}

// lgBlockingDLQ is a DLQPublisher that blocks until its ctx ends.
type lgBlockingDLQ struct{ calls atomic.Int32 }

func (p *lgBlockingDLQ) Publish(ctx context.Context, _ event.DLQRecord) error {
	p.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

// TestLocalBus_HandlerTimeoutDeadLettersWithCtxHonoringDLQ pins CR-02 (D-11,
// Phase 8 D-07): a handler that ran past HandlerTimeout is dead-lettered through
// a publisher that honors its ctx, because the settle ctx is detached from the
// expired timeout.
func TestLocalBus_HandlerTimeoutDeadLettersWithCtxHonoringDLQ(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := &lgCtxDLQ{}
		cap := &lsCapture{}
		bus := event.NewLocalBus(event.LocalBusOptions{
			HandlerTimeout: time.Second,
			DLQPublisher:   pub,
			OnErr:          cap.fn,
		})
		if err := bus.Subscribe(context.Background(), "ls.evt", func(ctx context.Context, _ event.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		bus.Emit(context.Background(), &lsEvent{}, event.WithID("lg-t1"))
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}

		pub.mu.Lock()
		defer pub.mu.Unlock()
		if len(pub.recs) != 1 {
			t.Fatalf("DLQ records = %d, want 1 (the timed-out event must not be lost)", len(pub.recs))
		}
		cause := pub.recs[0].Cause
		if !errors.Is(cause, event.ErrHandlerTimeout) || !errors.Is(cause, context.DeadlineExceeded) {
			t.Errorf("record cause = %v, want ErrHandlerTimeout and DeadlineExceeded", cause)
		}
		if !pub.deliveryOK[0] {
			t.Error("the publish ctx carries no Delivery")
		}
		if pub.metaIDs[0] != "lg-t1" {
			t.Errorf("publish ctx Metadata ID = %q, want lg-t1", pub.metaIDs[0])
		}

		errs := cap.all()
		if len(errs) != 1 {
			t.Fatalf("OnErr calls = %d (%v), want 1", len(errs), errs)
		}
		if !errors.Is(errs[0], event.ErrDeadLettered) || !errors.Is(errs[0], event.ErrHandlerTimeout) {
			t.Errorf("OnErr err = %v, want ErrDeadLettered and ErrHandlerTimeout", errs[0])
		}
		if errors.Is(errs[0], event.ErrDeadLetterFailed) {
			t.Errorf("OnErr err = %v, must not be ErrDeadLetterFailed", errs[0])
		}
	})
}

// TestLocalBus_EmitSync_CallerDeadlineDeadLetters pins CR-02 (D-03): an EmitSync
// whose caller deadline expired while its handler ran, and whose handler then
// failed with a non-ctx error, is dead-lettered once, and the handled failure is
// not an EmitSync error.
func TestLocalBus_EmitSync_CallerDeadlineDeadLetters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := &lgCtxDLQ{}
		cap := &lsCapture{}
		bus := event.NewLocalBus(event.LocalBusOptions{
			DLQPublisher: pub,
			OnErr:        cap.fn,
		})
		if err := bus.Subscribe(context.Background(), "ls.evt", func(context.Context, event.Event) error {
			time.Sleep(2 * time.Second) // ignores its ctx
			return errors.New("lg: late failure")
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		evt := &lsEvent{}
		got, err := bus.EmitSync(ctx, evt)
		if got != event.Event(evt) {
			t.Errorf("EmitSync event = %v, want the emitted event", got)
		}
		if err != nil {
			t.Fatalf("EmitSync = %v, want nil (the only handler's failure was dead-lettered)", err)
		}

		pub.mu.Lock()
		n := len(pub.recs)
		pub.mu.Unlock()
		if n != 1 {
			t.Fatalf("DLQ records = %d, want 1", n)
		}
		errs := cap.all()
		if len(errs) != 1 {
			t.Fatalf("OnErr calls = %d (%v), want 1", len(errs), errs)
		}
		if !errors.Is(errs[0], event.ErrDeadLettered) || errors.Is(errs[0], event.ErrDeadLetterFailed) {
			t.Errorf("OnErr err = %v, want ErrDeadLettered and not ErrDeadLetterFailed", errs[0])
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	})
}

// TestLocalBus_SettleBoundedByShutdownDeadline pins T-09-23 (D-05): a DLQ
// publisher that blocks until its ctx ends cannot hold a timed-out task past the
// Shutdown deadline, and the lost dead-letter is reported, not silent.
func TestLocalBus_SettleBoundedByShutdownDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pub := &lgBlockingDLQ{}
		cap := &lsCapture{}
		bus := event.NewLocalBus(event.LocalBusOptions{
			HandlerTimeout: time.Second,
			DLQPublisher:   pub,
			OnErr:          cap.fn,
		})
		if err := bus.Subscribe(context.Background(), "ls.evt", func(ctx context.Context, _ event.Event) error {
			<-ctx.Done()
			return ctx.Err()
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		bus.Emit(context.Background(), &lsEvent{})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := time.Now()
		if err := bus.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 5*time.Second {
			t.Errorf("Shutdown took %v of fake time, want 5s", got)
		}
		synctest.Wait() // the cancelled publish reports asynchronously

		if got := pub.calls.Load(); got < 1 {
			t.Errorf("DLQ publisher calls = %d, want at least 1", got)
		}
		errs := cap.all()
		if len(errs) != 1 {
			t.Fatalf("OnErr calls = %d (%v), want 1", len(errs), errs)
		}
		if !errors.Is(errs[0], event.ErrDeadLetterFailed) || !errors.Is(errs[0], event.ErrHandlerTimeout) {
			t.Errorf("OnErr err = %v, want ErrDeadLetterFailed and ErrHandlerTimeout", errs[0])
		}
	})
}
