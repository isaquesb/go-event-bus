package event

import "context"

// Test-only re-exports of unexported helpers, used by the external event_test
// package.

// RedactDelivery re-exports redactDelivery.
var RedactDelivery = redactDelivery

// NewDetachedScope re-exports newDetachedScope.
var NewDetachedScope = newDetachedScope

// MarkReported re-exports (*FailureScope).markReported.
func MarkReported(s *FailureScope) bool { return s.markReported() }

// DeferHandlerDone re-exports (*FailureScope).deferHandlerDone.
func DeferHandlerDone(s *FailureScope, r HandlerResult) bool { return s.deferHandlerDone(r) }

// FlushHandlerDone re-exports (*FailureScope).flushHandlerDone.
func FlushHandlerDone(ctx context.Context, s *FailureScope, o Outcome) {
	s.flushHandlerDone(ctx, o)
}
