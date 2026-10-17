package event_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

// lrNode is a tree event: each handler run emits two children one level deeper.
type lrNode struct{ depth int }

func (e *lrNode) Name() string { return "lr.tree" }

// lrEvent counts how many times its own OnErr was called.
type lrEvent struct {
	onErr atomic.Int32
}

func (e *lrEvent) Name() string                         { return "lr.evt" }
func (e *lrEvent) OnErr(context.Context, error, string) { e.onErr.Add(1) }

// TestLocalBus_AsyncHandlerSurvivesEmitterCtx pins CORE-03 (D-09): a handler
// emitted from a request that already ended keeps the emitter's Metadata, is not
// cancelled by the emitter ctx and is drained by Shutdown.
func TestLocalBus_AsyncHandlerSurvivesEmitterCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := event.NewLocalBus(event.LocalBusOptions{})

		var ran atomic.Int32
		if err := bus.Subscribe(context.Background(), "lr.evt", func(ctx context.Context, _ event.Event) error {
			time.Sleep(time.Second)
			if ctx.Err() != nil {
				t.Errorf("handler ctx cancelled by the emitter ctx: %v", ctx.Err())
			}
			md, ok := event.MetadataFromContext(ctx)
			if !ok || md.ID != "evt-a" {
				t.Errorf("Metadata = %+v ok=%v, want ID evt-a", md, ok)
			}
			ran.Add(1)
			return nil
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		emitCtx, cancel := context.WithCancel(context.Background())
		cancel() // the emitter request is already over

		bus.Emit(emitCtx, &lrEvent{}, event.WithID("evt-a"))

		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if got := ran.Load(); got != 1 {
			t.Fatalf("handler runs = %d, want 1", got)
		}
	})
}

// TestLocalBus_SelfEmitNoDeadlock proves D-10 and D-29: handlers that emit
// recursively under a single execution slot never wait for a slot, in every
// overflow mode.
func TestLocalBus_SelfEmitNoDeadlock(t *testing.T) {
	for _, mode := range []event.OverflowPolicy{
		event.OverflowBlock, event.OverflowDrop, event.OverflowUnbounded,
	} {
		synctest.Test(t, func(t *testing.T) {
			var onErr atomic.Int32
			bus := event.NewLocalBus(event.LocalBusOptions{
				MaxConcurrent: 1,
				QueueSize:     1,
				Overflow:      mode,
				OnErr: func(context.Context, event.Event, error, string) {
					onErr.Add(1)
				},
			})

			var runs atomic.Int32
			if err := bus.Subscribe(context.Background(), "lr.tree", func(ctx context.Context, e event.Event) error {
				runs.Add(1)
				n := e.(*lrNode)
				if n.depth < 4 {
					bus.Emit(ctx, &lrNode{depth: n.depth + 1})
					bus.Emit(ctx, &lrNode{depth: n.depth + 1})
				}
				return nil
			}, event.WithHandlerName("h")); err != nil {
				t.Fatalf("%s: Subscribe: %v", mode, err)
			}

			bus.Emit(context.Background(), &lrNode{})

			if err := bus.Shutdown(context.Background()); err != nil {
				t.Fatalf("%s: Shutdown = %v, want nil", mode, err)
			}
			if got := runs.Load(); got != 31 {
				t.Errorf("%s: handler runs = %d, want 31", mode, got)
			}
			if got := onErr.Load(); got != 0 {
				t.Errorf("%s: OnErr calls = %d, want 0", mode, got)
			}
		})
	}
}

// lrLogRec is one captured slog record with its attributes as strings.
type lrLogRec struct {
	level slog.Level
	attrs map[string]string
}

// lrLogs is a slog handler that captures every record.
type lrLogs struct {
	mu   sync.Mutex
	recs []lrLogRec
}

func (l *lrLogs) Enabled(context.Context, slog.Level) bool { return true }
func (l *lrLogs) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *lrLogs) WithGroup(string) slog.Handler            { return l }
func (l *lrLogs) Handle(_ context.Context, r slog.Record) error {
	rec := lrLogRec{level: r.Level, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	l.mu.Lock()
	l.recs = append(l.recs, rec)
	l.mu.Unlock()
	return nil
}

// withOutcome returns the records that carry an outcome attribute.
func (l *lrLogs) withOutcome() []lrLogRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []lrLogRec
	for _, r := range l.recs {
		if _, ok := r.attrs["outcome"]; ok {
			out = append(out, r)
		}
	}
	return out
}

// lrInstallLogs makes the capturing handler the slog default until t ends.
func lrInstallLogs(t *testing.T) *lrLogs {
	t.Helper()
	prev := slog.Default()
	l := &lrLogs{}
	slog.SetDefault(slog.New(l))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return l
}

// lrErrs records the bus OnErr calls.
type lrErrs struct {
	mu       sync.Mutex
	errs     []error
	handlers []string
}

func (c *lrErrs) fn(_ context.Context, _ event.Event, err error, handler string) {
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.handlers = append(c.handlers, handler)
	c.mu.Unlock()
}

func (c *lrErrs) snapshot() ([]error, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...), append([]string(nil), c.handlers...)
}

// lrRan is a set of the event ids whose handler started.
type lrRan struct {
	mu  sync.Mutex
	ids map[string]bool
}

func (r *lrRan) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ids == nil {
		r.ids = map[string]bool{}
	}
	r.ids[id] = true
}

func (r *lrRan) has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ids[id]
}

func (r *lrRan) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

// lrBlockedBus builds a bus with one execution slot and one queue place whose
// handler "h" records the event id and then blocks until gate is closed. It
// must be called inside a synctest bubble.
func lrBlockedBus(t *testing.T, policy event.OverflowPolicy, errs *lrErrs) (*event.LocalBus, chan struct{}, *lrRan) {
	t.Helper()
	gate := make(chan struct{})
	ran := &lrRan{}
	bus := event.NewLocalBus(event.LocalBusOptions{
		MaxConcurrent: 1,
		QueueSize:     1,
		Overflow:      policy,
		OnErr:         errs.fn,
	})
	if err := bus.Subscribe(context.Background(), "lr.evt", func(ctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		ran.add(md.ID)
		<-gate
		return nil
	}, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return bus, gate, ran
}

func TestLocalBus_Overflow_DropReportsQueueFull(t *testing.T) {
	logs := lrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus, gate, ran := lrBlockedBus(t, event.OverflowDrop, errs)

		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e1"))
		synctest.Wait() // e1 holds the only slot
		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e2"))
		synctest.Wait() // e2 is parked
		e3 := &lrEvent{}
		bus.Emit(context.Background(), e3, event.WithID("e3"))

		got, handlers := errs.snapshot()
		if len(got) != 1 || !errors.Is(got[0], event.ErrQueueFull) || handlers[0] != "h" {
			t.Fatalf("OnErr = %v handlers %v, want one ErrQueueFull for handler h", got, handlers)
		}
		if n := e3.onErr.Load(); n != 1 {
			t.Errorf("event OnErr calls = %d, want 1", n)
		}
		recs := logs.withOutcome()
		if len(recs) != 1 {
			t.Fatalf("outcome records = %d, want 1", len(recs))
		}
		if r := recs[0]; r.level != slog.LevelError || r.attrs["outcome"] != "dropped" || r.attrs["reason"] != "queue_full" {
			t.Errorf("record = %+v, want Error level, outcome dropped, reason queue_full", r)
		}

		close(gate)
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if !ran.has("e1") || !ran.has("e2") || ran.has("e3") {
			t.Errorf("ran e1=%v e2=%v e3=%v, want true true false", ran.has("e1"), ran.has("e2"), ran.has("e3"))
		}
	})
}

func TestLocalBus_Overflow_BlockExpiresWithQueueFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus, gate, ran := lrBlockedBus(t, event.OverflowBlock, errs)

		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e1"))
		synctest.Wait()
		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e2"))
		synctest.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		bus.Emit(ctx, &lrEvent{}, event.WithID("e3"))
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Emit blocked for %v, want exactly 1s of fake time", elapsed)
		}

		got, _ := errs.snapshot()
		if len(got) != 1 || !errors.Is(got[0], event.ErrQueueFull) {
			t.Fatalf("OnErr = %v, want one ErrQueueFull", got)
		}

		close(gate)
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if ran.has("e3") {
			t.Error("e3 ran, want it dropped")
		}
	})
}

func TestLocalBus_Overflow_BlockProceedsWhenSlotFrees(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus, gate, ran := lrBlockedBus(t, event.OverflowBlock, errs)

		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e1"))
		synctest.Wait()
		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e2"))
		synctest.Wait()

		returned := make(chan struct{})
		go func() {
			bus.Emit(context.Background(), &lrEvent{}, event.WithID("e3"))
			close(returned)
		}()
		synctest.Wait()
		select {
		case <-returned:
			t.Fatal("Emit returned while the queue was full and the ctx alive")
		default:
		}

		close(gate) // a slot frees, queue room follows
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Fatal("Emit did not return after a slot freed")
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if n := ran.count(); n != 3 {
			t.Errorf("handlers started = %d, want 3", n)
		}
		if got, _ := errs.snapshot(); len(got) != 0 {
			t.Errorf("OnErr = %v, want none", got)
		}
	})
}

func TestLocalBus_Overflow_BlockCancelledEmitterWithRoom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus, gate, ran := lrBlockedBus(t, event.OverflowBlock, errs)

		bus.Emit(context.Background(), &lrEvent{}, event.WithID("e1"))
		synctest.Wait()

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		bus.Emit(cancelled, &lrEvent{}, event.WithID("e2")) // queue has room

		close(gate)
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if !ran.has("e2") {
			t.Error("e2 did not run, want it parked and run despite the cancelled emitter ctx")
		}
		if got, _ := errs.snapshot(); len(got) != 0 {
			t.Errorf("OnErr = %v, want none (no ErrQueueFull)", got)
		}
	})
}

func TestLocalBus_Overflow_UnboundedNeverBlocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus, gate, ran := lrBlockedBus(t, event.OverflowUnbounded, errs)

		start := time.Now()
		for i := 0; i < 51; i++ {
			bus.Emit(context.Background(), &lrEvent{}, event.WithID(fmt.Sprintf("e%d", i)))
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("51 emits advanced fake time by %v, want 0", elapsed)
		}

		close(gate)
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if n := ran.count(); n != 51 {
			t.Errorf("handlers started = %d, want 51", n)
		}
		if got, _ := errs.snapshot(); len(got) != 0 {
			t.Errorf("OnErr = %v, want none", got)
		}
	})
}

func TestLocalBus_EmitAfterShutdownRejected(t *testing.T) {
	logs := lrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		var ran atomic.Int32
		bus := event.NewLocalBus(event.LocalBusOptions{OnErr: errs.fn})
		if err := bus.Subscribe(context.Background(), "lr.evt", func(context.Context, event.Event) error {
			ran.Add(1)
			return nil
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}

		evt := &lrEvent{}
		bus.Emit(context.Background(), evt)

		got, handlers := errs.snapshot()
		if len(got) != 1 || !errors.Is(got[0], event.ErrBusClosed) || handlers[0] != "" {
			t.Fatalf("OnErr = %v handlers %q, want one ErrBusClosed with handler \"\"", got, handlers)
		}
		if n := evt.onErr.Load(); n != 1 {
			t.Errorf("event OnErr calls = %d, want 1", n)
		}
		recs := logs.withOutcome()
		if len(recs) != 1 {
			t.Fatalf("outcome records = %d, want 1", len(recs))
		}
		if r := recs[0]; r.level != slog.LevelWarn || r.attrs["outcome"] != "rejected" || r.attrs["reason"] != "bus_closed" {
			t.Errorf("record = %+v, want Warn level, outcome rejected, reason bus_closed", r)
		}
		if n := ran.Load(); n != 0 {
			t.Errorf("handler ran %d times after Shutdown, want 0", n)
		}

		if _, err := bus.EmitSync(context.Background(), &lrEvent{}); !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("EmitSync = %v, want ErrBusClosed", err)
		}
		err := bus.Subscribe(context.Background(), "lr.other", func(context.Context, event.Event) error { return nil })
		if !errors.Is(err, event.ErrBusClosed) {
			t.Errorf("Subscribe = %v, want ErrBusClosed", err)
		}
	})
}

func TestLocalBus_EmitAfterShutdownNoHandlersIsNoop(t *testing.T) {
	logs := lrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus := event.NewLocalBus(event.LocalBusOptions{OnErr: errs.fn})
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}

		evt := &lrEvent{}
		bus.Emit(context.Background(), evt)
		if _, err := bus.EmitSync(context.Background(), evt); err != nil {
			t.Errorf("EmitSync with no handler = %v, want nil", err)
		}

		if got, _ := errs.snapshot(); len(got) != 0 {
			t.Errorf("OnErr = %v, want none", got)
		}
		if n := evt.onErr.Load(); n != 0 {
			t.Errorf("event OnErr calls = %d, want 0", n)
		}
		if recs := logs.withOutcome(); len(recs) != 0 {
			t.Errorf("outcome records = %v, want none", recs)
		}
	})
}

func TestLocalBus_OverflowPolicyDefaults(t *testing.T) {
	var zero event.LocalBusOptions
	if zero.Overflow != event.OverflowBlock {
		t.Errorf("zero Overflow = %v, want OverflowBlock", zero.Overflow)
	}
	for policy, want := range map[event.OverflowPolicy]string{
		event.OverflowBlock:     "block",
		event.OverflowUnbounded: "unbounded",
		event.OverflowDrop:      "drop",
	} {
		if got := policy.String(); got != want {
			t.Errorf("OverflowPolicy(%d).String() = %q, want %q", int(policy), got, want)
		}
	}
	if event.DefaultQueueSize != 1024 {
		t.Errorf("DefaultQueueSize = %d, want 1024", event.DefaultQueueSize)
	}
}

func TestReasonOf_LocalBusSentinels(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"bus closed", event.ErrBusClosed, "bus_closed"},
		{"wrapped bus closed", fmt.Errorf("emit: %w", event.ErrBusClosed), "bus_closed"},
		{"queue full", event.ErrQueueFull, "queue_full"},
		{"shutdown interrupted", event.ErrShutdownInterrupted, "interrupted"},
		{"wrapped shutdown interrupted", fmt.Errorf("%w: %w", event.ErrShutdownInterrupted, context.Canceled), "interrupted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := event.ReasonOf(tc.err); got != tc.want {
				t.Errorf("ReasonOf(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestLocalBus_AsyncPanicRecovered proves CORE-01 on the async path: a panicking
// handler is recovered and reported once while the other handlers of the same
// event still run, and the process survives.
func TestLocalBus_AsyncPanicRecovered(t *testing.T) {
	lrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		errs := &lrErrs{}
		bus := event.NewLocalBus(event.LocalBusOptions{OnErr: errs.fn})

		var normal atomic.Int32
		if err := bus.Subscribe(context.Background(), "lr.evt",
			func(context.Context, event.Event) error { panic("lr: async boom") },
			event.WithHandlerName("panicker")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Subscribe(context.Background(), "lr.evt",
			func(context.Context, event.Event) error { normal.Add(1); return nil },
			event.WithHandlerName("normal")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		bus.Emit(context.Background(), &lrEvent{})
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}

		if got := normal.Load(); got != 1 {
			t.Errorf("normal handler runs = %d, want 1", got)
		}
		got, handlers := errs.snapshot()
		if len(got) != 1 || !errors.Is(got[0], event.ErrHandlerPanic) || handlers[0] != "panicker" {
			t.Fatalf("OnErr = %v handlers %q, want one ErrHandlerPanic from panicker", got, handlers)
		}
	})
}

// lrPanickyEvent panics from its own OnErr hook.
type lrPanickyEvent struct{ lrEvent }

func (*lrPanickyEvent) OnErr(context.Context, error, string) { panic("lr: OnErr boom") }

// TestLocalBus_PanicInOnErrRecovered proves the task-level recover: a panic in
// user reporting code (the bus OnErr callback or the event's OnErr hook) on an
// async task is logged, does not crash the process and does not leak the task.
func TestLocalBus_PanicInOnErrRecovered(t *testing.T) {
	logs := lrInstallLogs(t)
	synctest.Test(t, func(t *testing.T) {
		var busOnErr atomic.Int32
		bus := event.NewLocalBus(event.LocalBusOptions{
			OnErr: func(context.Context, event.Event, error, string) {
				busOnErr.Add(1)
				panic("lr: bus OnErr boom")
			},
		})
		if err := bus.Subscribe(context.Background(), "lr.evt",
			func(context.Context, event.Event) error { return errors.New("lr: handler failed") },
			event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		bus.Emit(context.Background(), &lrEvent{})
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if got := busOnErr.Load(); got != 1 {
			t.Errorf("bus OnErr calls = %d, want 1", got)
		}

		var logged bool
		logs.mu.Lock()
		for _, r := range logs.recs {
			if r.attrs["panic"] == "lr: bus OnErr boom" {
				logged = true
			}
		}
		logs.mu.Unlock()
		if !logged {
			t.Error("the OnErr panic was not logged by the task-level recover")
		}

		// The same holds when the panic comes from the event's own OnErr hook.
		hook := event.NewLocalBus(event.LocalBusOptions{})
		if err := hook.Subscribe(context.Background(), "lr.evt",
			func(context.Context, event.Event) error { return errors.New("lr: handler failed") },
			event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		hook.Emit(context.Background(), &lrPanickyEvent{})
		if err := hook.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown after event OnErr panic = %v, want nil", err)
		}
	})
}
