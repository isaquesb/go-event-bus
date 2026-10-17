package event_test

import (
	"context"
	"sync"

	event "github.com/isaquesb/go-event-bus"
)

// obCall is one recorded Observer call: the method name, the context it
// received and the payload it carried.
type obCall struct {
	Method  string
	Ctx     context.Context
	Payload any
}

// obsRecorder is the shared recording Observer for every root test file of
// phase 10. It implements every Observer method and is safe for concurrent use.
type obsRecorder struct {
	mu    sync.Mutex
	calls []obCall
}

var _ event.Observer = (*obsRecorder)(nil)

func (r *obsRecorder) record(ctx context.Context, method string, payload any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, obCall{Method: method, Ctx: ctx, Payload: payload})
}

func (r *obsRecorder) RetryAttempt(ctx context.Context, info event.RetryAttemptInfo) {
	r.record(ctx, "RetryAttempt", info)
}

func (r *obsRecorder) BreakerTransition(ctx context.Context, t event.BreakerTransition) {
	r.record(ctx, "BreakerTransition", t)
}

func (r *obsRecorder) Failure(ctx context.Context, f event.FailureContext) {
	r.record(ctx, "Failure", f)
}

func (r *obsRecorder) DeadLettered(ctx context.Context, info event.DeadLetterInfo) {
	r.record(ctx, "DeadLettered", info)
}

func (r *obsRecorder) HandlerDone(ctx context.Context, res event.HandlerResult) {
	r.record(ctx, "HandlerDone", res)
}

func (r *obsRecorder) DecodeFailed(ctx context.Context, f event.DecodeFailure) {
	r.record(ctx, "DecodeFailed", f)
}

func (r *obsRecorder) DLQPublishFailed(ctx context.Context, f event.DLQPublishFailure) {
	r.record(ctx, "DLQPublishFailed", f)
}

func (r *obsRecorder) StoreError(ctx context.Context, e event.StoreErrorInfo) {
	r.record(ctx, "StoreError", e)
}

func (r *obsRecorder) IdempotencyStale(ctx context.Context, s event.IdempotencyStaleInfo) {
	r.record(ctx, "IdempotencyStale", s)
}

func (r *obsRecorder) InternalError(ctx context.Context, e event.InternalErrorInfo) {
	r.record(ctx, "InternalError", e)
}

// Calls returns the method names in call order.
func (r *obsRecorder) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	for i, c := range r.calls {
		out[i] = c.Method
	}
	return out
}

// All returns a copy of every recorded call, in call order.
func (r *obsRecorder) All() []obCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]obCall(nil), r.calls...)
}

// payloads returns the payloads of one method, in call order.
func obsPayloads[T any](r *obsRecorder, method string) []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []T
	for _, c := range r.calls {
		if c.Method == method {
			out = append(out, c.Payload.(T))
		}
	}
	return out
}

// Retries returns the RetryAttempt payloads.
func (r *obsRecorder) Retries() []event.RetryAttemptInfo {
	return obsPayloads[event.RetryAttemptInfo](r, "RetryAttempt")
}

// Transitions returns the BreakerTransition payloads.
func (r *obsRecorder) Transitions() []event.BreakerTransition {
	return obsPayloads[event.BreakerTransition](r, "BreakerTransition")
}

// Failures returns the Failure payloads.
func (r *obsRecorder) Failures() []event.FailureContext {
	return obsPayloads[event.FailureContext](r, "Failure")
}

// DeadLettered returns the DeadLettered payloads. It shares its name with the
// Observer method, so it is called DeadLetters.
func (r *obsRecorder) DeadLetters() []event.DeadLetterInfo {
	return obsPayloads[event.DeadLetterInfo](r, "DeadLettered")
}

// HandlerDones returns the HandlerDone payloads.
func (r *obsRecorder) HandlerDones() []event.HandlerResult {
	return obsPayloads[event.HandlerResult](r, "HandlerDone")
}

// DecodeFailures returns the DecodeFailed payloads.
func (r *obsRecorder) DecodeFailures() []event.DecodeFailure {
	return obsPayloads[event.DecodeFailure](r, "DecodeFailed")
}

// DLQPublishFailures returns the DLQPublishFailed payloads.
func (r *obsRecorder) DLQPublishFailures() []event.DLQPublishFailure {
	return obsPayloads[event.DLQPublishFailure](r, "DLQPublishFailed")
}

// StoreErrors returns the StoreError payloads.
func (r *obsRecorder) StoreErrors() []event.StoreErrorInfo {
	return obsPayloads[event.StoreErrorInfo](r, "StoreError")
}

// Stales returns the IdempotencyStale payloads.
func (r *obsRecorder) Stales() []event.IdempotencyStaleInfo {
	return obsPayloads[event.IdempotencyStaleInfo](r, "IdempotencyStale")
}

// InternalErrors returns the InternalError payloads.
func (r *obsRecorder) InternalErrors() []event.InternalErrorInfo {
	return obsPayloads[event.InternalErrorInfo](r, "InternalError")
}

// obPanicObserver panics in every method.
type obPanicObserver struct{ event.NopObserver }

func (obPanicObserver) RetryAttempt(context.Context, event.RetryAttemptInfo) { panic("ob panic") }
func (obPanicObserver) BreakerTransition(context.Context, event.BreakerTransition) {
	panic("ob panic")
}
func (obPanicObserver) Failure(context.Context, event.FailureContext)      { panic("ob panic") }
func (obPanicObserver) DeadLettered(context.Context, event.DeadLetterInfo) { panic("ob panic") }
func (obPanicObserver) HandlerDone(context.Context, event.HandlerResult)   { panic("ob panic") }
func (obPanicObserver) DecodeFailed(context.Context, event.DecodeFailure)  { panic("ob panic") }
func (obPanicObserver) DLQPublishFailed(context.Context, event.DLQPublishFailure) {
	panic("ob panic")
}
func (obPanicObserver) StoreError(context.Context, event.StoreErrorInfo) { panic("ob panic") }
func (obPanicObserver) IdempotencyStale(context.Context, event.IdempotencyStaleInfo) {
	panic("ob panic")
}
func (obPanicObserver) InternalError(context.Context, event.InternalErrorInfo) { panic("ob panic") }
