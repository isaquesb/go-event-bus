package jetstream

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
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
