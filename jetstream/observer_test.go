package jetstream

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
)

// joFailure is one recorded Observer.Failure call with the ctx it was given.
type joFailure struct {
	ctx context.Context
	fc  event.FailureContext
}

// joRecorder is the recording Observer of the JetStream tests: it embeds
// event.NopObserver and records the signals the bus emits, under a mutex. It is
// declared in an untagged file so the integration-tagged tests can use it too.
type joRecorder struct {
	event.NopObserver

	// ch, when non-nil, additionally receives every FailureContext (never
	// blocks: a full channel drops the extra copy, the slices still record it).
	ch chan event.FailureContext

	mu        sync.Mutex
	fails     []joFailure
	dones     []event.HandlerResult
	deads     []event.DeadLetterInfo
	decodes   []event.DecodeFailure
	dlqFails  []event.DLQPublishFailure
	internals []event.InternalErrorInfo
}

var _ event.Observer = (*joRecorder)(nil)

func (r *joRecorder) Failure(ctx context.Context, f event.FailureContext) {
	r.mu.Lock()
	r.fails = append(r.fails, joFailure{ctx: ctx, fc: f})
	r.mu.Unlock()
	if r.ch != nil {
		select {
		case r.ch <- f:
		default:
		}
	}
}

func (r *joRecorder) HandlerDone(_ context.Context, d event.HandlerResult) {
	r.mu.Lock()
	r.dones = append(r.dones, d)
	r.mu.Unlock()
}

func (r *joRecorder) DeadLettered(_ context.Context, d event.DeadLetterInfo) {
	r.mu.Lock()
	r.deads = append(r.deads, d)
	r.mu.Unlock()
}

func (r *joRecorder) DecodeFailed(_ context.Context, d event.DecodeFailure) {
	r.mu.Lock()
	r.decodes = append(r.decodes, d)
	r.mu.Unlock()
}

func (r *joRecorder) DLQPublishFailed(_ context.Context, d event.DLQPublishFailure) {
	r.mu.Lock()
	r.dlqFails = append(r.dlqFails, d)
	r.mu.Unlock()
}

func (r *joRecorder) InternalError(_ context.Context, i event.InternalErrorInfo) {
	r.mu.Lock()
	r.internals = append(r.internals, i)
	r.mu.Unlock()
}

// failures returns the recorded FailureContext values.
func (r *joRecorder) failures() []event.FailureContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]event.FailureContext, len(r.fails))
	for i, f := range r.fails {
		out[i] = f.fc
	}
	return out
}

// failureCtx returns the ctx of the i-th Failure call.
func (r *joRecorder) failureCtx(i int) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fails[i].ctx
}

func (r *joRecorder) failureCount() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.fails))
}

// lastCause is the Cause of the most recent Failure, nil when none was recorded.
func (r *joRecorder) lastCause() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fails) == 0 {
		return nil
	}
	return r.fails[len(r.fails)-1].fc.Cause
}

// lastEvent is the Event of the most recent Failure, nil when none was recorded.
func (r *joRecorder) lastEvent() event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fails) == 0 {
		return nil
	}
	return r.fails[len(r.fails)-1].fc.Event
}

// snapshot returns the Event and Cause of every recorded Failure, in order.
func (r *joRecorder) snapshot() ([]event.Event, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	evts := make([]event.Event, len(r.fails))
	errs := make([]error, len(r.fails))
	for i, f := range r.fails {
		evts[i], errs[i] = f.fc.Event, f.fc.Cause
	}
	return evts, errs
}

func (r *joRecorder) handlerDones() []event.HandlerResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.dones)
}

func (r *joRecorder) deadLetters() []event.DeadLetterInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.deads)
}

func (r *joRecorder) decodeFailures() []event.DecodeFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.decodes)
}

func (r *joRecorder) dlqPublishFailures() []event.DLQPublishFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.dlqFails)
}

func (r *joRecorder) internalErrors() []event.InternalErrorInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.internals)
}

// joPanicReported reports whether an InternalError with Op panic carries a
// *event.PanicError whose value prints as text.
func joPanicReported(r *joRecorder, text string) bool {
	for _, in := range r.internalErrors() {
		var pe *event.PanicError
		if in.Op == event.OpPanic && errors.As(in.Err, &pe) && fmt.Sprint(pe.Value) == text {
			return true
		}
	}
	return false
}

// joTerminal is a terminal handler error.
type joTerminal struct{ msg string }

func (e *joTerminal) Error() string  { return e.msg }
func (e *joTerminal) Terminal() bool { return true }

// --- tracer ---

// TestJetStreamBus_FailureReachesObserverOnce is the tracer of plan 10-11: a
// message that dies on its last delivery reports once to the bus Observer through
// the observer and FailureScope msgHandler installs before Decode, and is
// terminated with a snake_case reason.
func TestJetStreamBus_FailureReachesObserverOnce(t *testing.T) {
	js := &mdJS{}
	rec := &joRecorder{}
	dlq := event.NewMemoryDLQ()
	bus := newMdBus(js, newMdRegistry("jo.order"), BusOptions{DLQPublisher: dlq, Observer: rec})
	if err := bus.Emit(context.Background(), &mdEvent{name: "jo.order"}, event.WithID("id-jo")); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	msg := &mdMsg{data: js.payload(t, 0).data, numDelivered: 3}
	policy := deliveryPolicy{maxDeliver: 3, backOff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		return &joTerminal{msg: "jo: terminal failure"}
	}, policy)(msg)

	got := msg.snapshot()
	if want := []string{"dead_lettered: terminal"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.plainTerms != 0 || got.acks != 0 || got.naks != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if dlq.Len() != 1 {
		t.Errorf("DLQ records = %d, want 1", dlq.Len())
	}

	fails := rec.failures()
	if len(fails) != 1 {
		t.Fatalf("Failure calls = %d, want 1", len(fails))
	}
	f := fails[0]
	if f.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want %q", f.Outcome, event.OutcomeDeadLettered)
	}
	if f.Handler != "h" {
		t.Errorf("Handler = %q, want h", f.Handler)
	}
	if f.Event == nil {
		t.Error("Event = nil, want the decoded event")
	}
	if f.Delivery.System != "nats" {
		t.Errorf("Delivery.System = %q, want nats", f.Delivery.System)
	}
	if f.Delivery.Count != 3 {
		t.Errorf("Delivery.Count = %d, want 3", f.Delivery.Count)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Delivery.Raw = %q, want nil (no payload in signals)", f.Delivery.Raw)
	}
	if !errors.Is(f.Cause, event.ErrDeadLettered) {
		t.Errorf("Cause = %v, want it to match ErrDeadLettered", f.Cause)
	}

	dones := rec.handlerDones()
	if len(dones) != 1 || dones[0].Outcome != event.OutcomeDeadLettered {
		t.Errorf("HandlerDone = %+v, want one with outcome %q", dones, event.OutcomeDeadLettered)
	}
}

// TestDLQ_OutcomesReportedThroughObserver proves D-04: NewDLQ counts nothing
// itself, a dead-letter success and a dead-letter failure are signals that
// event.PublishDeadLetter sends to the Observer in the ctx.
func TestDLQ_OutcomesReportedThroughObserver(t *testing.T) {
	rec := &joRecorder{}
	ctx := event.WithObserver(context.Background(), rec)

	ok := &dlqtJS{}
	if err := event.PublishDeadLetter(ctx, NewDLQ(ok, ""), dlqtRecord()); err != nil {
		t.Fatalf("PublishDeadLetter = %v, want nil", err)
	}
	if n := len(rec.deadLetters()); n != 1 {
		t.Errorf("DeadLettered calls = %d, want 1", n)
	}
	if n := len(rec.dlqPublishFailures()); n != 0 {
		t.Errorf("DLQPublishFailed calls = %d, want 0 after a success", n)
	}

	bad := &dlqtJS{errs: []error{errors.New("jo: broker down")}}
	nonFinal := dlqtRecord()
	nonFinal.Delivery.Count = 1 // a final delivery would retry in process three times
	if err := event.PublishDeadLetter(ctx, NewDLQ(bad, ""), nonFinal); err == nil {
		t.Fatal("PublishDeadLetter = nil, want the publish error")
	}
	fails := rec.dlqPublishFailures()
	if len(fails) != 1 {
		t.Fatalf("DLQPublishFailed calls = %d, want 1", len(fails))
	}
	if fails[0].Delivery.Raw != nil {
		t.Errorf("DLQPublishFailure.Delivery.Raw = %q, want nil", fails[0].Delivery.Raw)
	}
	if n := len(rec.deadLetters()); n != 1 {
		t.Errorf("DeadLettered calls = %d, want still 1", n)
	}
}

// --- exactly-once matrix across redeliveries ---

// joInvoker is a chain-level invoker that returns err without calling next.
type joInvoker struct{ err error }

func (i joInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	return i.err
}

// joFailingPublisher is a DLQPublisher whose Publish always fails.
type joFailingPublisher struct{}

func (joFailingPublisher) Publish(context.Context, event.DLQRecord) error {
	return errors.New("jo: broker down")
}

// joDecodeRegistry is an event.Registry whose Decode always fails with err.
type joDecodeRegistry struct{ err error }

func (joDecodeRegistry) Register(string, event.Factory, int) {}
func (joDecodeRegistry) Encode(context.Context, event.Event) ([]byte, error) {
	return []byte("{}"), nil
}
func (r joDecodeRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	return ctx, nil, &event.DecodeError{Err: r.err}
}

// joWant is what one matrix row expects from the message and the recorder.
type joWant struct {
	// Broker calls.
	acks, naks, nakDelays int
	termReasons           []string

	// Failure signals.
	fails    int
	outcome  event.Outcome        // of the Failure, when fails > 0
	reason   string               // of the Failure, when non-empty
	invoker  event.FailingInvoker // of the Failure, when non-empty
	hasEvent bool                 // the Failure carries the decoded event
	causeIs  error                // errors.Is on the Failure cause, when non-nil

	// Other signals.
	dones       int
	doneOutcome event.Outcome // of the HandlerDone, when dones > 0
	decodes     int
	decodeFinal bool // of the DecodeFailed, when decodes > 0
	dlqFails    int
	dlqFinal    bool // of the DLQPublishFailed, when dlqFails > 0
}

// joRow is one row of the matrix: run feeds one delivery to a bus whose only
// Observer is rec and returns the message it settled.
type joRow struct {
	name string
	run  func(t *testing.T, rec *joRecorder) *mdMsg
	want joWant
}

var joBackOff = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}

// joThree is the delivery policy of most rows: three deliveries.
var joThree = deliveryPolicy{maxDeliver: 3, backOff: joBackOff}

// joUnlimited is MaxDeliver 0: unlimited deliveries, never final.
var joUnlimited = deliveryPolicy{maxDeliver: 0, backOff: joBackOff}

// joDeliver builds a bus over reg, emits one jo.order event and feeds it to
// msgHandler as delivery number delivered under policy.
func joDeliver(t *testing.T, rec *joRecorder, reg event.Registry, opts BusOptions, delivered uint64, policy deliveryPolicy, fn event.HandleFn) *mdMsg {
	t.Helper()
	js := &mdJS{}
	opts.Observer = rec
	// The payload comes from a plain bus; reg only decodes (it may be a failing one).
	if err := newMdBus(js, newMdRegistry("jo.order"), BusOptions{}).Emit(
		context.Background(), &mdEvent{name: "jo.order"}, event.WithID("id-jo")); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	bus := newMdBus(js, reg, opts)
	msg := &mdMsg{data: js.payload(t, 0).data, numDelivered: delivered, subject: "jo.order"}
	bus.msgHandler(context.Background(), "h", fn, policy)(msg)
	return msg
}

func joReg() event.Registry { return newMdRegistry("jo.order") }

func joTransient(context.Context, event.Event) error { return errors.New("jo: transient") }

func joTerminalFn(context.Context, event.Event) error { return &joTerminal{msg: "jo: terminal"} }

func joMatrixRows() []joRow {
	return []joRow{
		{
			name: "transient, delivery 1 of 3",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 1, joThree, joTransient)
			},
			want: joWant{nakDelays: 1, dones: 1, doneOutcome: event.OutcomeRetry},
		},
		{
			name: "transient, delivery 2 of 3",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 2, joThree, joTransient)
			},
			want: joWant{nakDelays: 1, dones: 1, doneOutcome: event.OutcomeRetry},
		},
		{
			name: "transient, delivery 3 of 3 dead-letters",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 3, joThree, joTransient)
			},
			want: joWant{
				termReasons: []string{"dead_lettered: delivery_exhausted"},
				fails:       1, outcome: event.OutcomeDeadLettered, reason: "delivery_exhausted", hasEvent: true,
				causeIs: event.ErrDeadLettered,
				dones:   1, doneOutcome: event.OutcomeDeadLettered,
			},
		},
		{
			name: "chain dead-lettered",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				chainErr := &event.DeadLetteredError{Cause: errors.New("jo: boom"), Reason: event.ReasonRetriesExhausted}
				return joDeliver(t, rec, joReg(), BusOptions{Invoker: joInvoker{err: chainErr}, DLQPublisher: event.NewMemoryDLQ()}, 2, joThree,
					func(context.Context, event.Event) error { return nil })
			},
			want: joWant{
				acks:  1,
				fails: 1, outcome: event.OutcomeDeadLettered, invoker: event.InvokerDLQ, hasEvent: true,
				causeIs: event.ErrDeadLettered,
				dones:   1, doneOutcome: event.OutcomeDeadLettered,
			},
		},
		{
			name: "terminal without a publisher is dropped",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{}, 1, joThree, joTerminalFn)
			},
			want: joWant{
				termReasons: []string{"dropped: terminal"},
				fails:       1, outcome: event.OutcomeDropped, reason: event.ReasonTerminal, hasEvent: true,
				dones: 1, doneOutcome: event.OutcomeDropped,
			},
		},
		{
			name: "failing publisher, delivery 1 of 3",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: joFailingPublisher{}}, 1, joThree, joTerminalFn)
			},
			want: joWant{nakDelays: 1, dlqFails: 1, dlqFinal: false, dones: 1, doneOutcome: event.OutcomeRetry},
		},
		{
			name: "failing publisher, delivery 3 of 3 is reported and never termed",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: joFailingPublisher{}}, 3, joThree, joTerminalFn)
			},
			want: joWant{
				nakDelays: 1,
				fails:     1, outcome: event.OutcomeDLQPublishFailed, hasEvent: true, causeIs: event.ErrDeadLetterFailed,
				dlqFails: 1, dlqFinal: true,
				dones: 1, doneOutcome: event.OutcomeFailed,
			},
		},
		{
			name: "failing publisher, unlimited MaxDeliver is never terminal",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: joFailingPublisher{}}, 9, joUnlimited, joTerminalFn)
			},
			want: joWant{nakDelays: 1, dlqFails: 1, dlqFinal: false, dones: 1, doneOutcome: event.OutcomeRetry},
		},
		{
			name: "unknown version, delivery 1 of 3 waits",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joDecodeRegistry{err: event.ErrUnknownVersion}, BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 1, joThree, joTransient)
			},
			want: joWant{nakDelays: 1, decodes: 1, decodeFinal: false},
		},
		{
			name: "unknown version, delivery 3 of 3 dead-letters",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joDecodeRegistry{err: event.ErrUnknownVersion}, BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 3, joThree, joTransient)
			},
			want: joWant{
				termReasons: []string{"dead_lettered: unknown_version"},
				fails:       1, outcome: event.OutcomeDecodeFailed, reason: event.ReasonUnknownVersion, hasEvent: false,
				causeIs: event.ErrDeadLettered,
				decodes: 1, decodeFinal: true,
			},
		},
		{
			name: "handler panic is dead-lettered",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 1, joThree,
					func(context.Context, event.Event) error { panic("jo: handler boom") })
			},
			want: joWant{
				termReasons: []string{"dead_lettered: panic"},
				fails:       1, outcome: event.OutcomeDeadLettered, reason: event.ReasonPanic, hasEvent: true,
				causeIs: event.ErrHandlerPanic,
				dones:   1, doneOutcome: event.OutcomeDeadLettered,
			},
		},
		{
			name: "duplicate is acked",
			run: func(t *testing.T, rec *joRecorder) *mdMsg {
				return joDeliver(t, rec, joReg(), BusOptions{DLQPublisher: event.NewMemoryDLQ()}, 3, joThree,
					func(context.Context, event.Event) error { return fmt.Errorf("jo: seen: %w", event.ErrDuplicate) })
			},
			want: joWant{acks: 1, dones: 1, doneOutcome: event.OutcomeDuplicate},
		},
		{
			name: "shutdown deadline, non-final delivery",
			run:  func(t *testing.T, rec *joRecorder) *mdMsg { return joDeadlineInterrupt(t, rec, 1) },
			want: joWant{naks: 1, dones: 1, doneOutcome: event.OutcomeInterrupted},
		},
		{
			name: "shutdown deadline, final delivery",
			run:  func(t *testing.T, rec *joRecorder) *mdMsg { return joDeadlineInterrupt(t, rec, 3) },
			want: joWant{
				naks:  1,
				fails: 1, outcome: event.OutcomeInterrupted, reason: event.ReasonInterrupted, hasEvent: true,
				causeIs: event.ErrShutdownInterrupted,
				dones:   1, doneOutcome: event.OutcomeInterrupted,
			},
		},
		{
			name: "closing bus hands back, non-final delivery",
			run:  func(t *testing.T, rec *joRecorder) *mdMsg { return joHandBack(t, rec, 1) },
			want: joWant{naks: 1},
		},
		{
			name: "closing bus hands back, final delivery",
			run:  func(t *testing.T, rec *joRecorder) *mdMsg { return joHandBack(t, rec, 3) },
			want: joWant{
				naks:  1,
				fails: 1, outcome: event.OutcomeInterrupted, hasEvent: false, causeIs: event.ErrShutdownInterrupted,
			},
		},
	}
}

// joDeadlineInterrupt delivers one message to a handler that blocks until
// Shutdown gives up at its deadline, the Phase 9 forced-interrupt path.
func joDeadlineInterrupt(t *testing.T, rec *joRecorder, delivered uint64) *mdMsg {
	t.Helper()
	started := make(chan struct{}, 1)
	bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: event.NewMemoryDLQ(), Observer: rec},
		func(ctx context.Context, _ event.Event) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		})
	msg := &mdMsg{data: payload, numDelivered: delivered, subject: "jo.order"}
	done := rbDeliver(cons, msg)
	synctest.Wait()
	<-started

	sctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := bus.Shutdown(sctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
	<-done
	return msg
}

// joHandBack delivers one message to a bus that is already closing: the handler
// does not run and the message is handed back.
func joHandBack(t *testing.T, rec *joRecorder, delivered uint64) *mdMsg {
	t.Helper()
	bus, cons, _, payload := rbShutdownBus(t, BusOptions{DLQPublisher: event.NewMemoryDLQ(), Observer: rec}, noopHandler)
	res := rbShutdown(bus, context.Background())
	synctest.Wait()

	msg := &mdMsg{data: payload, numDelivered: delivered, subject: "jo.order"}
	<-rbDeliver(cons, msg)

	cons.cc.finish()
	if err := <-res; err != nil {
		t.Errorf("Shutdown = %v, want nil", err)
	}
	return msg
}

// TestJetStreamObserver_FailureMatrix is the OBS-03 conformance proof for
// JetStream: Failure fires exactly once per terminal outcome, never on an
// intermediate NakWithDelay, and the final-delivery DLQ publish failure is
// reported without being termed (research DR-10, 08 D-26).
func TestJetStreamObserver_FailureMatrix(t *testing.T) {
	for _, row := range joMatrixRows() {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := &joRecorder{}
				msg := row.run(t, rec)
				w := row.want

				got := msg.snapshot()
				if got.plainTerms != 0 {
					t.Errorf("plain Term called %d times, the bus must only use TermWithReason", got.plainTerms)
				}
				if got.acks != w.acks || got.naks != w.naks || len(got.nakDelays) != w.nakDelays {
					t.Errorf("acks/naks/nakDelays = %d/%d/%d, want %d/%d/%d", got.acks, got.naks, len(got.nakDelays), w.acks, w.naks, w.nakDelays)
				}
				if !slices.Equal(got.termReasons, w.termReasons) {
					t.Errorf("termReasons = %q, want %q", got.termReasons, w.termReasons)
				}

				fails := rec.failures()
				if len(fails) != w.fails {
					t.Fatalf("Failure calls = %d (%+v), want %d", len(fails), fails, w.fails)
				}
				if w.fails == 1 {
					f := fails[0]
					if f.Outcome != w.outcome {
						t.Errorf("Failure outcome = %q, want %q", f.Outcome, w.outcome)
					}
					if w.reason != "" && f.Reason != w.reason {
						t.Errorf("Failure reason = %q, want %q", f.Reason, w.reason)
					}
					if w.invoker != "" && f.Invoker != w.invoker {
						t.Errorf("Failure invoker = %q, want %q", f.Invoker, w.invoker)
					}
					if (f.Event != nil) != w.hasEvent {
						t.Errorf("Failure event = %v, want present=%v", f.Event, w.hasEvent)
					}
					if w.causeIs != nil && !errors.Is(f.Cause, w.causeIs) {
						t.Errorf("Failure cause = %v, want it to match %v", f.Cause, w.causeIs)
					}
					if f.Delivery.System != "nats" || f.Delivery.Raw != nil {
						t.Errorf("Failure delivery = %+v, want System nats and nil Raw", f.Delivery)
					}
				}

				dones := rec.handlerDones()
				if len(dones) != w.dones {
					t.Fatalf("HandlerDone calls = %d (%+v), want %d", len(dones), dones, w.dones)
				}
				if w.dones == 1 && dones[0].Outcome != w.doneOutcome {
					t.Errorf("HandlerDone outcome = %q, want %q", dones[0].Outcome, w.doneOutcome)
				}

				decodes := rec.decodeFailures()
				if len(decodes) != w.decodes {
					t.Fatalf("DecodeFailed calls = %d, want %d", len(decodes), w.decodes)
				}
				if w.decodes == 1 {
					if decodes[0].Final != w.decodeFinal {
						t.Errorf("DecodeFailed.Final = %v, want %v", decodes[0].Final, w.decodeFinal)
					}
					if decodes[0].Delivery.Raw != nil {
						t.Errorf("DecodeFailed delivery Raw = %q, want nil", decodes[0].Delivery.Raw)
					}
				}

				dlqFails := rec.dlqPublishFailures()
				if len(dlqFails) != w.dlqFails {
					t.Fatalf("DLQPublishFailed calls = %d, want %d", len(dlqFails), w.dlqFails)
				}
				if w.dlqFails == 1 && dlqFails[0].Final != w.dlqFinal {
					t.Errorf("DLQPublishFailed.Final = %v, want %v", dlqFails[0].Final, w.dlqFinal)
				}
			})
		})
	}
}
