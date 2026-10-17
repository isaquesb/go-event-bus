package event

import (
	"context"
	"sync"
)

// RED stub: signatures only, behavior arrives in the GREEN commit.

// FailureScope is a per-delivery recorder.
type FailureScope struct {
	mu sync.Mutex
}

// WithFailureScope installs a fresh scope.
func WithFailureScope(ctx context.Context) (context.Context, *FailureScope) {
	return ctx, &FailureScope{}
}

// FailureScopeFromContext returns the scope carried by ctx.
func FailureScopeFromContext(ctx context.Context) (*FailureScope, bool) { return nil, false }

// SetBreaker records the breaker state.
func (s *FailureScope) SetBreaker(state BreakerState) {}

// Breaker returns the recorded breaker state.
func (s *FailureScope) Breaker() BreakerState { return "" }

func newDetachedScope(ctx context.Context) (context.Context, *FailureScope) {
	return ctx, &FailureScope{}
}

func (s *FailureScope) markReported() bool { return false }

func (s *FailureScope) deferHandlerDone(r HandlerResult) bool { return false }

func (s *FailureScope) flushHandlerDone(ctx context.Context, outcome Outcome) {}

// OutcomeOf maps a handler chain error to an Outcome.
func OutcomeOf(ctx context.Context, err error) Outcome { return "" }
