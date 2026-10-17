package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
)

// noRecorder is the recording Observer of the NATS tests: it embeds
// event.NopObserver and records the signals the bus emits, under a mutex.
type noRecorder struct {
	event.NopObserver

	mu        sync.Mutex
	fails     []event.FailureContext
	dones     []event.HandlerResult
	decodes   []event.DecodeFailure
	dlqFails  []event.DLQPublishFailure
	internals []event.InternalErrorInfo
}

var _ event.Observer = (*noRecorder)(nil)

func (r *noRecorder) Failure(_ context.Context, f event.FailureContext) {
	r.mu.Lock()
	r.fails = append(r.fails, f)
	r.mu.Unlock()
}

func (r *noRecorder) HandlerDone(_ context.Context, d event.HandlerResult) {
	r.mu.Lock()
	r.dones = append(r.dones, d)
	r.mu.Unlock()
}

func (r *noRecorder) DecodeFailed(_ context.Context, d event.DecodeFailure) {
	r.mu.Lock()
	r.decodes = append(r.decodes, d)
	r.mu.Unlock()
}

func (r *noRecorder) DLQPublishFailed(_ context.Context, d event.DLQPublishFailure) {
	r.mu.Lock()
	r.dlqFails = append(r.dlqFails, d)
	r.mu.Unlock()
}

func (r *noRecorder) InternalError(_ context.Context, i event.InternalErrorInfo) {
	r.mu.Lock()
	r.internals = append(r.internals, i)
	r.mu.Unlock()
}

func (r *noRecorder) failures() []event.FailureContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.FailureContext(nil), r.fails...)
}

func (r *noRecorder) handlerDones() []event.HandlerResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.HandlerResult(nil), r.dones...)
}

func (r *noRecorder) decodeFailures() []event.DecodeFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.DecodeFailure(nil), r.decodes...)
}

func (r *noRecorder) dlqPublishFailures() []event.DLQPublishFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.DLQPublishFailure(nil), r.dlqFails...)
}

func (r *noRecorder) internalErrors() []event.InternalErrorInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]event.InternalErrorInfo(nil), r.internals...)
}

// noFailureAt returns the event and the cause of the i-th Failure, failing the
// test when fewer were recorded.
func noFailureAt(t *testing.T, r *noRecorder, i int) (event.Event, error) {
	t.Helper()
	fails := r.failures()
	if i >= len(fails) {
		t.Fatalf("Failure was called %d times, want index %d", len(fails), i)
	}
	return fails[i].Event, fails[i].Cause
}

// noPanicReported reports whether an InternalError with Op panic carries a
// *event.PanicError whose value prints as text.
func noPanicReported(r *noRecorder, text string) bool {
	for _, in := range r.internalErrors() {
		var pe *event.PanicError
		if in.Op == event.OpPanic && errors.As(in.Err, &pe) && fmt.Sprint(pe.Value) == text {
			return true
		}
	}
	return false
}

// noTerminal is a terminal handler error.
type noTerminal struct{ msg string }

func (e *noTerminal) Error() string  { return e.msg }
func (e *noTerminal) Terminal() bool { return true }

// --- tracer ---

// TestNatsSubscribe_FailureReachesObserverOnce is the tracer of plan 10-10: a
// failing NATS Core subscription reports once to the bus Observer through the
// observer and FailureScope the callback installs before Decode.
func TestNatsSubscribe_FailureReachesObserverOnce(t *testing.T) {
	conn := newFsLoopConn()
	reg := newMdRegistry("no.order")
	dlq := event.NewMemoryDLQ()
	rec := &noRecorder{}
	bus := &Bus{nc: conn, reg: reg, timeout: time.Second, dlq: dlq, observer: rec}

	boom := &noTerminal{msg: "no: boom"}
	if err := bus.Subscribe(context.Background(), "no.order", func(context.Context, event.Event) error {
		return boom
	}, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	raw := fsRequestBytes(t, reg, "no.order")
	conn.deliver(t, "no.order", raw)

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
	if f.Delivery.Count != 1 {
		t.Errorf("Delivery.Count = %d, want 1", f.Delivery.Count)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Delivery.Raw = %q, want nil (no payload in signals)", f.Delivery.Raw)
	}
	if !errors.Is(f.Cause, event.ErrDeadLettered) || !errors.Is(f.Cause, boom) {
		t.Errorf("Cause = %v, want ErrDeadLettered wrapping the handler error", f.Cause)
	}

	dones := rec.handlerDones()
	if len(dones) != 1 {
		t.Fatalf("HandlerDone calls = %d, want 1", len(dones))
	}
	if dones[0].Outcome != event.OutcomeDeadLettered {
		t.Errorf("HandlerDone outcome = %q, want %q", dones[0].Outcome, event.OutcomeDeadLettered)
	}
	if n := dlq.Len(); n != 1 {
		t.Errorf("DLQ records = %d, want 1", n)
	}
}
