package event_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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

// lbInvoker is a fake chain Invoker: it returns err without calling next, or
// calls next when err is nil.
type lbInvoker struct{ err error }

func (i *lbInvoker) Invoke(ctx context.Context, _ event.Event, _ string, next func(context.Context) error) error {
	if i.err != nil {
		return i.err
	}
	return next(ctx)
}

// lbFailingPublisher is a DLQPublisher that always fails.
type lbFailingPublisher struct{}

func (lbFailingPublisher) Publish(context.Context, event.DLQRecord) error {
	return errors.New("lb: dlq down")
}

// lbRow is one row of the LocalBus failure matrix. The handler fails with
// handlerErr (or panics), unless invErr is set, in which case the fake chain
// invoker returns it without running the handler.
type lbRow struct {
	name       string
	invErr     error
	handlerErr error
	panics     bool
	publisher  string // "", "memory" or "failing"

	failures     int
	outcome      event.Outcome
	reason       string
	invoker      event.FailingInvoker
	deadLetters  int
	publishFails int
	records      int // MemoryDLQ records expected
	causeIs      []error
}

// lbMatrixRows is the LocalBus half of research DR-1 that needs no fake time.
var lbMatrixRows = []lbRow{
	{name: "success"},
	{name: "duplicate", invErr: event.ErrDuplicate, publisher: "memory"},
	{
		name:     "chain_dead_lettered",
		invErr:   &event.DeadLetteredError{Cause: lbPermanent{}, Reason: event.ReasonTerminal},
		failures: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonTerminal,
		invoker: event.InvokerDLQ, publisher: "memory", records: 0,
		causeIs: []error{event.ErrDeadLettered, lbPermanent{}},
	},
	{
		name: "backstop_dead_lettered", handlerErr: lbPermanent{}, publisher: "memory",
		failures: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonTerminal,
		invoker: event.InvokerTransport, deadLetters: 1, records: 1,
		causeIs: []error{event.ErrDeadLettered, lbPermanent{}},
	},
	{
		name: "dropped_without_publisher", handlerErr: lbPermanent{},
		failures: 1, outcome: event.OutcomeDropped, reason: event.ReasonTerminal,
		invoker: event.InvokerTransport,
		causeIs: []error{lbPermanent{}},
	},
	{
		name: "dlq_publish_failed", handlerErr: lbPermanent{}, publisher: "failing",
		failures: 1, outcome: event.OutcomeDLQPublishFailed, reason: event.ReasonTerminal,
		invoker: event.InvokerTransport, publishFails: 1,
		causeIs: []error{event.ErrDeadLetterFailed, lbPermanent{}},
	},
	{
		name: "rate_limited", invErr: event.ErrRateLimited, publisher: "memory",
		failures: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonRateLimited,
		invoker: event.InvokerTransport, deadLetters: 1, records: 1,
		causeIs: []error{event.ErrDeadLettered, event.ErrRateLimited},
	},
	{
		name: "in_progress", invErr: event.ErrInProgress, publisher: "memory",
		failures: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonInProgress,
		invoker: event.InvokerTransport, deadLetters: 1, records: 1,
		causeIs: []error{event.ErrDeadLettered, event.ErrInProgress},
	},
	{
		name: "circuit_open", invErr: event.ErrCircuitOpen, publisher: "memory",
		failures: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonCircuitOpen,
		invoker: event.InvokerCircuitBreaker, deadLetters: 1, records: 1,
		causeIs: []error{event.ErrDeadLettered, event.ErrCircuitOpen},
	},
	{
		name: "panic", panics: true, publisher: "memory",
		failures: 1, outcome: event.OutcomeDeadLettered, reason: event.ReasonPanic,
		invoker: event.InvokerTransport, deadLetters: 1, records: 1,
		causeIs: []error{event.ErrDeadLettered, event.ErrHandlerPanic},
	},
}

// lbRunRow builds a bus for row, emits one event in the given mode and returns
// the recorder, the event and the MemoryDLQ (nil unless row.publisher is memory).
func lbRunRow(t *testing.T, row lbRow, async bool) (*obsRecorder, *lbEvent, *event.MemoryDLQ) {
	t.Helper()
	rec := &obsRecorder{}
	opts := event.LocalBusOptions{Invoker: &lbInvoker{err: row.invErr}, Observer: rec}
	var mem *event.MemoryDLQ
	switch row.publisher {
	case "memory":
		mem = event.NewMemoryDLQ()
		opts.DLQPublisher = mem
	case "failing":
		opts.DLQPublisher = lbFailingPublisher{}
	}
	bus := event.NewLocalBus(opts)
	if err := bus.Subscribe(context.Background(), "lb.order", func(context.Context, event.Event) error {
		if row.panics {
			panic("lb: handler boom")
		}
		return row.handlerErr
	}, event.WithHandlerName("billing")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	evt := &lbEvent{}
	if async {
		bus.Emit(context.Background(), evt, event.WithID("lb-1"))
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	} else {
		_, _ = bus.EmitSync(context.Background(), evt, event.WithID("lb-1"))
	}
	return rec, evt, mem
}

// lbCheckRow asserts the exact counts and the FailureContext of one row.
func lbCheckRow(t *testing.T, row lbRow, rec *obsRecorder, evt *lbEvent, mem *event.MemoryDLQ) {
	t.Helper()
	failures := rec.Failures()
	if len(failures) != row.failures {
		t.Fatalf("Failure calls = %d (%+v), want %d", len(failures), failures, row.failures)
	}
	if got := int(evt.onErr.Load()); got != row.failures {
		t.Errorf("event OnErr calls = %d, want %d (the Failure count)", got, row.failures)
	}
	if got := len(rec.DeadLetters()); got != row.deadLetters {
		t.Errorf("DeadLettered signals = %d, want %d", got, row.deadLetters)
	}
	pf := rec.DLQPublishFailures()
	if len(pf) != row.publishFails {
		t.Errorf("DLQPublishFailed signals = %d, want %d", len(pf), row.publishFails)
	}
	for _, p := range pf {
		if !p.Final {
			t.Errorf("DLQPublishFailed.Final = false, want true (LocalBus deliveries are final)")
		}
		if p.Delivery.Raw != nil {
			t.Errorf("DLQPublishFailed.Delivery.Raw = %q, want nil", p.Delivery.Raw)
		}
	}
	if mem != nil && mem.Len() != row.records {
		t.Errorf("MemoryDLQ records = %d, want %d", mem.Len(), row.records)
	}
	if row.failures == 0 {
		return
	}
	f := failures[0]
	if f.Outcome != row.outcome {
		t.Errorf("Outcome = %q, want %q", f.Outcome, row.outcome)
	}
	if f.Reason != row.reason {
		t.Errorf("Reason = %q, want %q", f.Reason, row.reason)
	}
	if f.Invoker != row.invoker {
		t.Errorf("Invoker = %q, want %q", f.Invoker, row.invoker)
	}
	if f.Handler != "billing" {
		t.Errorf("Handler = %q, want billing", f.Handler)
	}
	if f.Event != event.Event(evt) {
		t.Errorf("Event = %v, want the emitted lbEvent", f.Event)
	}
	if f.Metadata.ID != "lb-1" {
		t.Errorf("Metadata.ID = %q, want lb-1", f.Metadata.ID)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Delivery.Raw = %q, want nil", f.Delivery.Raw)
	}
	if f.At.IsZero() {
		t.Error("At is zero")
	}
	for _, want := range row.causeIs {
		if !errors.Is(f.Cause, want) {
			t.Errorf("Cause = %v, want errors.Is %v", f.Cause, want)
		}
	}
	if row.outcome != event.OutcomeDeadLettered && errors.Is(f.Cause, event.ErrDeadLettered) {
		t.Errorf("Cause = %v, must not match ErrDeadLettered for outcome %q", f.Cause, row.outcome)
	}
	if row.panics {
		var pe *event.PanicError
		if !errors.As(f.Cause, &pe) {
			t.Errorf("Cause = %v, want a *event.PanicError", f.Cause)
		}
	}
}

// TestLocalBus_FailureMatrix proves OBS-03 on LocalBus: Observer.Failure fires
// exactly once per terminal outcome, in sync and async mode, and never for
// success or a duplicate (research DR-1, D-14).
func TestLocalBus_FailureMatrix(t *testing.T) {
	for _, row := range lbMatrixRows {
		for _, mode := range []struct {
			name  string
			async bool
		}{{"sync", false}, {"async", true}} {
			t.Run(row.name+"_"+mode.name, func(t *testing.T) {
				rec, evt, mem := lbRunRow(t, row, mode.async)
				lbCheckRow(t, row, rec, evt, mem)
			})
		}
	}

	t.Run("interrupted_async", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			rec := &obsRecorder{}
			mem := event.NewMemoryDLQ()
			bus := event.NewLocalBus(event.LocalBusOptions{
				Invoker:       &passthroughInvoker{},
				Observer:      rec,
				DLQPublisher:  mem,
				MaxConcurrent: 1,
				QueueSize:     4,
			})
			if err := bus.Subscribe(context.Background(), "lb.order", func(ctx context.Context, _ event.Event) error {
				<-ctx.Done()
				return ctx.Err()
			}, event.WithHandlerName("billing")); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			// e1 runs, e2 and e3 park for the only slot.
			evts := []*lbEvent{{}, {}, {}}
			for i, e := range evts {
				bus.Emit(context.Background(), e, event.WithID([]string{"e1", "e2", "e3"}[i]))
			}
			synctest.Wait()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := bus.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
			}
			synctest.Wait() // the interrupted tasks report asynchronously

			failures := rec.Failures()
			if len(failures) != 3 {
				t.Fatalf("Failure calls = %d, want 3 (one per running or parked task)", len(failures))
			}
			for i, f := range failures {
				if f.Outcome != event.OutcomeInterrupted || f.Reason != event.ReasonInterrupted {
					t.Errorf("Failure[%d] = %q/%q, want interrupted/interrupted", i, f.Outcome, f.Reason)
				}
				if !errors.Is(f.Cause, event.ErrShutdownInterrupted) {
					t.Errorf("Failure[%d].Cause = %v, want ErrShutdownInterrupted", i, f.Cause)
				}
				if f.Handler != "billing" {
					t.Errorf("Failure[%d].Handler = %q, want billing", i, f.Handler)
				}
			}
			total := 0
			for _, e := range evts {
				total += int(e.onErr.Load())
			}
			if total != 3 {
				t.Errorf("event OnErr calls = %d, want 3", total)
			}
			if got := len(rec.DeadLetters()); got != 0 {
				t.Errorf("DeadLettered signals = %d, want 0 (an interruption is never dead-lettered)", got)
			}
			if mem.Len() != 0 {
				t.Errorf("MemoryDLQ records = %d, want 0", mem.Len())
			}
		})
	})

	t.Run("queue_full_async", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			rec := &obsRecorder{}
			gate := make(chan struct{})
			bus := event.NewLocalBus(event.LocalBusOptions{
				Invoker:       &passthroughInvoker{},
				Observer:      rec,
				MaxConcurrent: 1,
				QueueSize:     1,
				Overflow:      event.OverflowDrop,
			})
			if err := bus.Subscribe(context.Background(), "lb.order", func(context.Context, event.Event) error {
				<-gate
				return nil
			}, event.WithHandlerName("billing")); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			bus.Emit(context.Background(), &lbEvent{}, event.WithID("e1"))
			synctest.Wait() // e1 holds the only slot
			bus.Emit(context.Background(), &lbEvent{}, event.WithID("e2"))
			synctest.Wait() // e2 is parked
			e3 := &lbEvent{}
			bus.Emit(context.Background(), e3, event.WithID("e3"))

			failures := rec.Failures()
			if len(failures) != 1 {
				t.Fatalf("Failure calls = %d, want exactly 1 (the dropped e3)", len(failures))
			}
			f := failures[0]
			if f.Outcome != event.OutcomeDropped || f.Reason != event.ReasonQueueFull {
				t.Errorf("Failure = %q/%q, want dropped/queue_full", f.Outcome, f.Reason)
			}
			if f.Handler != "billing" {
				t.Errorf("Handler = %q, want billing", f.Handler)
			}
			if !errors.Is(f.Cause, event.ErrQueueFull) {
				t.Errorf("Cause = %v, want ErrQueueFull", f.Cause)
			}
			if got := e3.onErr.Load(); got != 1 {
				t.Errorf("event OnErr calls = %d, want 1", got)
			}

			close(gate)
			if err := bus.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown = %v, want nil", err)
			}
			if got := len(rec.Failures()); got != 1 {
				t.Errorf("Failure calls after drain = %d, want still 1", got)
			}
		})
	})

	t.Run("rejected_bus_closed_async", func(t *testing.T) {
		rec := &obsRecorder{}
		bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}, Observer: rec})
		if err := bus.Subscribe(context.Background(), "lb.order",
			func(context.Context, event.Event) error { return nil },
			event.WithHandlerName("billing")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}

		evt := &lbEvent{}
		bus.Emit(context.Background(), evt, event.WithID("lb-1"))

		failures := rec.Failures()
		if len(failures) != 1 {
			t.Fatalf("Failure calls = %d, want exactly 1", len(failures))
		}
		f := failures[0]
		if f.Outcome != event.OutcomeRejected || f.Reason != event.ReasonBusClosed {
			t.Errorf("Failure = %q/%q, want rejected/bus_closed", f.Outcome, f.Reason)
		}
		if f.Handler != "" {
			t.Errorf("Handler = %q, want empty (no handler ran)", f.Handler)
		}
		if !errors.Is(f.Cause, event.ErrBusClosed) {
			t.Errorf("Cause = %v, want ErrBusClosed", f.Cause)
		}
		if got := evt.onErr.Load(); got != 1 {
			t.Errorf("event OnErr calls = %d, want 1", got)
		}
	})
}

// lbBreakerInvoker writes a breaker state into the FailureScope like the
// circuit breaker invoker does, then fails with err.
type lbBreakerInvoker struct {
	state event.BreakerState
	err   error
}

func (i *lbBreakerInvoker) Invoke(ctx context.Context, _ event.Event, _ string, _ func(context.Context) error) error {
	if scope, ok := event.FailureScopeFromContext(ctx); ok && i.state != "" {
		scope.SetBreaker(i.state)
	}
	return i.err
}

// lbPanicEvent is an event whose own OnErr method panics.
type lbPanicEvent struct{}

func (lbPanicEvent) Name() string                         { return "lb.order" }
func (lbPanicEvent) OnErr(context.Context, error, string) { panic("lb: OnErr boom") }

// lbEmitFailing emits one event through a bus whose chain invoker is inv and
// whose observer is rec, and returns the first Failure.
func lbEmitFailing(t *testing.T, inv event.Invoker) event.FailureContext {
	t.Helper()
	rec := &obsRecorder{}
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: inv, Observer: rec})
	if err := bus.Subscribe(context.Background(), "lb.order",
		func(context.Context, event.Event) error { return nil },
		event.WithHandlerName("billing")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_, _ = bus.EmitSync(context.Background(), &lbEvent{}, event.WithID("lb-1"))
	failures := rec.Failures()
	if len(failures) != 1 {
		t.Fatalf("Failure calls = %d, want exactly 1", len(failures))
	}
	return failures[0]
}

// TestFailureContext_AttemptsAndHistory: Attempts, History and the retry
// invoker name come from the *FailureError the chain returned (D-16).
func TestFailureContext_AttemptsAndHistory(t *testing.T) {
	now := time.Now()
	history := []event.Attempt{
		event.NewAttempt(3, errors.New("x"), event.AttemptTransient, now, time.Millisecond, 10*time.Millisecond),
		event.NewAttempt(4, errors.New("x"), event.AttemptTransient, now, time.Millisecond, 0),
	}
	f := lbEmitFailing(t, &lbInvoker{err: &event.FailureError{
		Cause:    errors.New("x"),
		Reason:   event.ReasonRetriesExhausted,
		Attempts: 4,
		History:  history,
	}})

	if f.Attempts != 4 {
		t.Errorf("Attempts = %d, want 4", f.Attempts)
	}
	if len(f.History) != 2 || f.History[0] != history[0] || f.History[1] != history[1] {
		t.Errorf("History = %+v, want %+v", f.History, history)
	}
	if &f.History[0] == &history[0] {
		t.Error("History shares its backing array with the FailureError, want a copy")
	}
	if f.Invoker != event.InvokerRetry {
		t.Errorf("Invoker = %q, want retry", f.Invoker)
	}
	if f.Reason != event.ReasonRetriesExhausted {
		t.Errorf("Reason = %q, want retries_exhausted", f.Reason)
	}
}

// TestFailureContext_BreakerFromScope: the breaker state an invoker wrote into
// the FailureScope reaches Failure.Breaker, and is empty without one (D-16).
func TestFailureContext_BreakerFromScope(t *testing.T) {
	t.Run("written_by_invoker", func(t *testing.T) {
		f := lbEmitFailing(t, &lbBreakerInvoker{state: event.BreakerOpen, err: lbPermanent{}})
		if f.Breaker != event.BreakerOpen {
			t.Errorf("Breaker = %q, want open", f.Breaker)
		}
	})
	t.Run("absent_without_breaker", func(t *testing.T) {
		f := lbEmitFailing(t, &lbBreakerInvoker{err: lbPermanent{}})
		if f.Breaker != "" {
			t.Errorf("Breaker = %q, want empty when no breaker ran", f.Breaker)
		}
	})
}

// lbSettleCtx builds the context a transport hands to Settle: the observer, a
// FailureScope, metadata and a delivery that carries raw payload bytes.
func lbSettleCtx(rec *obsRecorder, d event.Delivery) context.Context {
	ctx := event.WithObserver(context.Background(), rec)
	ctx, _ = event.WithFailureScope(ctx)
	ctx = event.WithMetadata(ctx, event.Metadata{ID: "lb-id", CorrelationID: "lb-corr"})
	return event.WithDelivery(ctx, d)
}

// TestFailureContext_MetadataAndRedactedDelivery: the Failure carries the
// metadata ids, the delivery without its payload bytes and a current At; the
// DeadLettered signal carries no payload either (D-14, D-20).
func TestFailureContext_MetadataAndRedactedDelivery(t *testing.T) {
	rec := &obsRecorder{}
	mem := event.NewMemoryDLQ()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	ctx := lbSettleCtx(rec, event.Delivery{Count: 2, Max: 2, Raw: raw, Subject: "lb.subject"})

	before := time.Now()
	event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             &lbEvent{},
		Handler:         "billing",
		Err:             lbPermanent{},
		Publisher:       mem,
	})
	after := time.Now()

	failures := rec.Failures()
	if len(failures) != 1 {
		t.Fatalf("Failure calls = %d, want 1", len(failures))
	}
	f := failures[0]
	if f.Metadata.ID != "lb-id" || f.Metadata.CorrelationID != "lb-corr" {
		t.Errorf("Metadata = %+v, want ids lb-id / lb-corr", f.Metadata)
	}
	if f.Delivery.Count != 2 || f.Delivery.Max != 2 || f.Delivery.Subject != "lb.subject" {
		t.Errorf("Delivery = %+v, want Count 2 Max 2 Subject lb.subject", f.Delivery)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Delivery.Raw = %q, want nil", f.Delivery.Raw)
	}
	if f.At.Before(before) || f.At.After(after) {
		t.Errorf("At = %v, want within [%v, %v]", f.At, before, after)
	}

	dl := rec.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLettered signals = %d, want 1", len(dl))
	}
	if dl[0].Delivery.Raw != nil {
		t.Errorf("DeadLettered.Delivery.Raw = %q, want nil", dl[0].Delivery.Raw)
	}
	// The record handed to the publisher keeps the payload: it is the DLQ copy,
	// not a signal.
	if recs := mem.Records(); len(recs) != 1 || len(recs[0].Raw) != len(raw) {
		t.Errorf("DLQ record Raw length = %d, want %d", len(recs[0].Raw), len(raw))
	}
}

// TestSettle_DoubleReportGuard: settling the same delivery ctx twice reports one
// Failure and runs the event's OnErr once (FailureScope guard, D-01).
func TestSettle_DoubleReportGuard(t *testing.T) {
	rec := &obsRecorder{}
	ctx := lbSettleCtx(rec, event.Delivery{Count: 1, Max: 1, Subject: "lb.subject"})
	evt := &lbEvent{}
	in := event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             evt,
		Handler:         "billing",
		Err:             lbPermanent{},
	}

	first := event.Settle(ctx, in)
	second := event.Settle(ctx, in)

	if first.Outcome != event.OutcomeDropped || second.Outcome != event.OutcomeDropped {
		t.Fatalf("outcomes = %q, %q, want dropped twice", first.Outcome, second.Outcome)
	}
	if got := len(rec.Failures()); got != 1 {
		t.Errorf("Failure calls = %d, want exactly 1", got)
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}

// TestLocalBus_EventOnErrPanicIsolated: a panic inside the event's own OnErr
// method is reported as an InternalError and never reaches the bus caller.
func TestLocalBus_EventOnErrPanicIsolated(t *testing.T) {
	rec := &obsRecorder{}
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}, Observer: rec})
	if err := bus.Subscribe(context.Background(), "lb.order",
		func(context.Context, event.Event) error { return lbPermanent{} },
		event.WithHandlerName("billing")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	_, _ = bus.EmitSync(context.Background(), lbPanicEvent{}, event.WithID("lb-1"))

	if got := len(rec.Failures()); got != 1 {
		t.Errorf("Failure calls = %d, want 1", got)
	}
	ie := rec.InternalErrors()
	if len(ie) != 1 {
		t.Fatalf("InternalError calls = %d (%+v), want 1", len(ie), ie)
	}
	if ie[0].Component != event.ComponentEvent || ie[0].Op != event.OpPanic {
		t.Errorf("InternalError = %s/%s, want event/panic", ie[0].Component, ie[0].Op)
	}
	if !errors.Is(ie[0].Err, event.ErrHandlerPanic) {
		t.Errorf("InternalError.Err = %v, want a recovered panic", ie[0].Err)
	}

	// The bus survives: a second failure is reported like the first.
	_, _ = bus.EmitSync(context.Background(), lbPanicEvent{}, event.WithID("lb-2"))
	if got := len(rec.Failures()); got != 2 {
		t.Errorf("Failure calls after a second emit = %d, want 2", got)
	}
}

// TestLocalBus_ObserverPanicIsolated: a panicking observer configured next to a
// recording one does not stop the recording one or the handler path.
func TestLocalBus_ObserverPanicIsolated(t *testing.T) {
	lbInstallCapture(t) // MultiObserver logs the first panic of each observer
	rec := &obsRecorder{}
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &passthroughInvoker{},
		Observer:     event.MultiObserver(obPanicObserver{}, rec),
		DLQPublisher: event.NewMemoryDLQ(),
	})
	if err := bus.Subscribe(context.Background(), "lb.order",
		func(context.Context, event.Event) error { return lbPermanent{} },
		event.WithHandlerName("billing")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	evt := &lbEvent{}
	if _, err := bus.EmitSync(context.Background(), evt, event.WithID("lb-1")); err != nil {
		t.Fatalf("EmitSync = %v, want nil for a dead-lettered failure", err)
	}

	if got := len(rec.Failures()); got != 1 {
		t.Errorf("recording observer Failure calls = %d, want 1", got)
	}
	if got := len(rec.DeadLetters()); got != 1 {
		t.Errorf("recording observer DeadLettered calls = %d, want 1", got)
	}
	if got := len(rec.HandlerDones()); got != 1 {
		t.Errorf("recording observer HandlerDone calls = %d, want 1", got)
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}

// TestSettle_DecodeFailureSignals: a decode failure (no event) with a DLQ
// publisher emits DecodeFailed (final, no payload) and one decode_failed
// Failure whose cause is dead-lettered.
func TestSettle_DecodeFailureSignals(t *testing.T) {
	rec := &obsRecorder{}
	mem := event.NewMemoryDLQ()
	ctx := lbSettleCtx(rec, event.Delivery{Count: 1, Max: 1, Subject: "lb.subject", Raw: []byte(`{"secret":true}`)})

	s := event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Err:             &event.DecodeError{Err: event.ErrUnknownEvent},
		Publisher:       mem,
	})
	if s.Disposition != event.DispositionTerminal {
		t.Fatalf("Disposition = %v, want terminal", s.Disposition)
	}

	df := rec.DecodeFailures()
	if len(df) != 1 {
		t.Fatalf("DecodeFailed calls = %d, want 1", len(df))
	}
	if !df[0].Final {
		t.Error("DecodeFailed.Final = false, want true")
	}
	if df[0].Delivery.Raw != nil {
		t.Errorf("DecodeFailed.Delivery.Raw = %q, want nil", df[0].Delivery.Raw)
	}

	failures := rec.Failures()
	if len(failures) != 1 {
		t.Fatalf("Failure calls = %d, want 1", len(failures))
	}
	f := failures[0]
	if f.Outcome != event.OutcomeDecodeFailed {
		t.Errorf("Outcome = %q, want decode_failed", f.Outcome)
	}
	if f.Event != nil {
		t.Errorf("Event = %v, want nil", f.Event)
	}
	if !errors.Is(f.Cause, event.ErrDeadLettered) {
		t.Errorf("Cause = %v, want ErrDeadLettered", f.Cause)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Failure.Delivery.Raw = %q, want nil", f.Delivery.Raw)
	}
	if mem.Len() != 1 {
		t.Errorf("MemoryDLQ records = %d, want 1", mem.Len())
	}
	for _, d := range rec.DeadLetters() {
		if d.Delivery.Raw != nil {
			t.Errorf("DeadLettered.Delivery.Raw = %q, want nil", d.Delivery.Raw)
		}
	}
}
