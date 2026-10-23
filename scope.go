package event

import (
	"context"
	"errors"
	"sync"
)

// FailureScope is the per-delivery recorder of what the failure report needs
// and the returned error does not carry: the circuit breaker state, an
// exactly-once guard for the Failure signal, and the HandlerDone result that
// waits for the transport to settle the message.
//
// A transport installs one with WithFailureScope before InvokeHandler. Invokers
// write the breaker state into it and tolerate a missing scope. All methods
// are safe for concurrent use, and the exported ones are safe on a nil
// receiver.
type FailureScope struct {
	mu       sync.Mutex
	breaker  BreakerState
	reported bool
	pending  *HandlerResult
	flushed  bool
	settles  bool
}

type scopeKey struct{}

// WithFailureScope installs a fresh FailureScope in the returned context.
// A transport calls it once per delivery before InvokeHandler, on the context
// it later passes to Settle. Invokers used outside a bus need none.
func WithFailureScope(ctx context.Context) (context.Context, *FailureScope) {
	s := &FailureScope{settles: true}
	return context.WithValue(ctx, scopeKey{}, s), s
}

// FailureScopeFromContext returns the FailureScope carried by ctx. The boolean
// is false when ctx carries none.
func FailureScopeFromContext(ctx context.Context) (*FailureScope, bool) {
	s, ok := ctx.Value(scopeKey{}).(*FailureScope)
	return s, ok && s != nil
}

// SetBreaker records the circuit breaker state seen by this delivery. It does
// nothing on a nil scope.
func (s *FailureScope) SetBreaker(state BreakerState) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.breaker = state
	s.mu.Unlock()
}

// Breaker returns the recorded circuit breaker state, empty when no breaker
// ran or on a nil scope.
func (s *FailureScope) Breaker() BreakerState {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.breaker
}

// newDetachedScope installs a scope that does not defer HandlerDone.
// InvokeHandler uses it when the transport installed none, so the signal is
// emitted at the end of the chain.
func newDetachedScope(ctx context.Context) (context.Context, *FailureScope) {
	s := &FailureScope{}
	return context.WithValue(ctx, scopeKey{}, s), s
}

// markReported is the exactly-once guard: true on the first call, false after.
// A nil scope has no guard and always reports true.
func (s *FailureScope) markReported() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reported {
		return false
	}
	s.reported = true
	return true
}

// deferHandlerDone stores r until flushHandlerDone and reports whether it did.
// It refuses on a detached scope and when a result is already pending.
func (s *FailureScope) deferHandlerDone(r HandlerResult) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.settles || s.pending != nil {
		return false
	}
	s.pending = &r
	return true
}

// flushHandlerDone emits the pending HandlerDone, if any, once, with the given
// final outcome. The observer runs outside the lock.
func (s *FailureScope) flushHandlerDone(ctx context.Context, outcome Outcome) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.pending == nil || s.flushed {
		s.mu.Unlock()
		return
	}
	r := *s.pending
	r.Outcome = outcome
	s.flushed = true
	s.mu.Unlock()
	ObserverFromContext(ctx).HandlerDone(ctx, r)
}

// OutcomeOf is the only mapping from a handler chain error to a HandlerDone
// outcome. It uses errors.Is and Dispose, never string matching, and checks in
// this order:
//
//	nil                                        ok
//	ErrDeadLettered                            dead_lettered
//	ErrDuplicate                               duplicate
//	ErrShutdownInterrupted, or a done ctx
//	  that is not a handler timeout            interrupted
//	ErrInProgress                              in_progress
//	ErrRateLimited                             rate_limited
//	ErrCircuitOpen                             circuit_open
//	Dispose says retry and not final           retry
//	anything else                              failed
//
// The Delivery comes from ctx; without one the message is final. failed means
// the chain ended in a terminal failure it did not dead-letter. When a
// transport installed a FailureScope, InvokeHandler defers the HandlerDone
// signal and Settle replaces failed with the settled outcome (dead_lettered,
// dropped, retry or failed).
func OutcomeOf(ctx context.Context, err error) Outcome {
	switch {
	case err == nil:
		return OutcomeOK
	case errors.Is(err, ErrDeadLettered):
		return OutcomeDeadLettered
	case errors.Is(err, ErrDuplicate):
		return OutcomeDuplicate
	case errors.Is(err, ErrShutdownInterrupted):
		return OutcomeInterrupted
	case IsInterrupted(ctx, err) && !errors.Is(context.Cause(ctx), ErrHandlerTimeout):
		return OutcomeInterrupted
	case errors.Is(err, ErrInProgress):
		return OutcomeInProgress
	case errors.Is(err, ErrRateLimited):
		return OutcomeRateLimited
	case errors.Is(err, ErrCircuitOpen):
		return OutcomeCircuitOpen
	}
	d := Delivery{Count: 1, Max: 1}
	if got, ok := DeliveryFromContext(ctx); ok {
		d = got
	}
	if Dispose(err, d) == DispositionRetryDelayed && !d.Final() {
		return OutcomeRetry
	}
	return OutcomeFailed
}
