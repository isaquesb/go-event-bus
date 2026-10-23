package invoker_test

import (
	"context"
	"sync"

	"github.com/isaquesb/go-event-bus"
)

// invCall is one recorded Observer call: the context it received and the
// payload it carried.
type invCall[T any] struct {
	Ctx context.Context
	Val T
}

// invRecorder is the recording Observer for the invoker tests. It embeds
// event.NopObserver and records the signals invokers emit. It is safe for
// concurrent use.
type invRecorder struct {
	event.NopObserver

	mu          sync.Mutex
	transitions []invCall[event.BreakerTransition]
	retries     []invCall[event.RetryAttemptInfo]
	storeErrs   []invCall[event.StoreErrorInfo]
	stales      []invCall[event.IdempotencyStaleInfo]
	internals   []invCall[event.InternalErrorInfo]
	dlqFailures []invCall[event.DLQPublishFailure]
}

var _ event.Observer = (*invRecorder)(nil)

func invAppend[T any](r *invRecorder, dst *[]invCall[T], ctx context.Context, v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*dst = append(*dst, invCall[T]{Ctx: ctx, Val: v})
}

func invCopy[T any](r *invRecorder, src []invCall[T]) []invCall[T] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]invCall[T](nil), src...)
}

func (r *invRecorder) BreakerTransition(ctx context.Context, t event.BreakerTransition) {
	invAppend(r, &r.transitions, ctx, t)
}

func (r *invRecorder) RetryAttempt(ctx context.Context, i event.RetryAttemptInfo) {
	invAppend(r, &r.retries, ctx, i)
}

func (r *invRecorder) StoreError(ctx context.Context, e event.StoreErrorInfo) {
	invAppend(r, &r.storeErrs, ctx, e)
}

func (r *invRecorder) IdempotencyStale(ctx context.Context, s event.IdempotencyStaleInfo) {
	invAppend(r, &r.stales, ctx, s)
}

func (r *invRecorder) InternalError(ctx context.Context, e event.InternalErrorInfo) {
	invAppend(r, &r.internals, ctx, e)
}

func (r *invRecorder) DLQPublishFailed(ctx context.Context, f event.DLQPublishFailure) {
	invAppend(r, &r.dlqFailures, ctx, f)
}

// Transitions returns a copy of the recorded breaker transitions.
func (r *invRecorder) Transitions() []invCall[event.BreakerTransition] {
	return invCopy(r, r.transitions)
}

// Retries returns a copy of the recorded retry attempts.
func (r *invRecorder) Retries() []invCall[event.RetryAttemptInfo] { return invCopy(r, r.retries) }

// StoreErrors returns a copy of the recorded store errors.
func (r *invRecorder) StoreErrors() []invCall[event.StoreErrorInfo] {
	return invCopy(r, r.storeErrs)
}

// Stales returns a copy of the recorded stale-marker takeovers.
func (r *invRecorder) Stales() []invCall[event.IdempotencyStaleInfo] {
	return invCopy(r, r.stales)
}

// InternalErrors returns a copy of the recorded internal errors.
func (r *invRecorder) InternalErrors() []invCall[event.InternalErrorInfo] {
	return invCopy(r, r.internals)
}

// DLQFailures returns a copy of the recorded failed dead-letter publishes.
func (r *invRecorder) DLQFailures() []invCall[event.DLQPublishFailure] {
	return invCopy(r, r.dlqFailures)
}
