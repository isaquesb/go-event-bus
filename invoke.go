package event

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrHandlerPanic is matched by every *PanicError. It is classified
	// terminal: the failure is never retried, is neutral for the circuit
	// breaker and is dead-lettered with reason panic.
	ErrHandlerPanic = errors.New("event: handler panic")

	// ErrHandlerTimeout is the context cause a transport sets with
	// context.WithTimeoutCause when its own per-handler timeout expires, for
	// example LocalBusOptions.HandlerTimeout. context.Cause(ctx) returning it is
	// how the circuit breaker counts a handler timeout while it ignores caller
	// cancellation.
	ErrHandlerTimeout = errors.New("event: handler timeout")
)

// PanicError is a panic recovered from a handler or from an invoker. errors.Is
// matches ErrHandlerPanic, and Terminal reports true, so the failure contract
// treats it as permanent.
//
// PanicError deliberately has no Unwrap method. A panicked error value
// (context.Canceled, a permanent error, a retryable error) must never change
// how the panic is classified; callers read Value instead.
type PanicError struct {
	// Value is the value passed to panic. For panic(nil) it is a
	// *runtime.PanicNilError.
	Value any
	// Stack is the goroutine stack captured at recovery.
	Stack []byte
}

// Error implements error. It never panics, including on a nil receiver.
func (e *PanicError) Error() string {
	if e == nil {
		return "event: handler panic"
	}
	return "event: handler panic: " + fmt.Sprint(e.Value)
}

// Is reports whether target is ErrHandlerPanic.
func (e *PanicError) Is(target error) bool { return target == ErrHandlerPanic }

// Terminal reports true: a panic is never retried.
func (e *PanicError) Terminal() bool { return true }

// InvokeHandler is the single handler entry point for LocalBus, NATS Core,
// JetStream and the v0.5.0 adapters. It runs fn through inv and never lets a
// panic escape the calling goroutine.
//
// A panic inside fn is recovered innermost, so every invoker in the chain
// (Retry, CircuitBreaker, DLQ) receives it as an ordinary returned
// *PanicError. A panic inside an invoker is recovered by an outer layer. The
// value and the stack travel in the PanicError, so they reach observers through
// HandlerResult.Err and FailureContext.Cause; nothing is logged here. A nil inv
// is a passthrough: fn is called directly.
//
// HandlerDone is emitted to the Observer in ctx for every invocation, with the
// outcome from OutcomeOf, the duration and the error. A terminal failure that
// the chain did not dead-letter (outcome failed) is not final yet: when the
// transport installed a FailureScope, the signal is deferred and the
// transport's Settle emits it once with what really happened to the message
// (dead_lettered, dropped, retry or failed). Without a FailureScope the signal
// is emitted at the end of the chain with outcome failed.
//
// A flow-control outcome (circuit_open, rate_limited or in_progress) is
// deferred the same way on a final delivery, that is when ctx carries no
// Delivery or Delivery.Final reports true: the message is lost there, so Settle
// completes the signal with the settled outcome (dead_lettered, dropped, or
// failed when the DLQ publish failed) and HandlerDone agrees with the single
// Observer.Failure. On a non-final delivery the flow-control outcome is emitted
// at once and means the message will be retried.
//
// The recover does not stop runtime.Goexit, so callers release their
// accounting in a defer.
func InvokeHandler(ctx context.Context, inv Invoker, evt Event, handlerName string, fn func(context.Context) error) (err error) {
	start := time.Now()
	scope, ok := FailureScopeFromContext(ctx)
	if !ok {
		ctx, scope = newDetachedScope(ctx)
	}
	defer func() {
		r := HandlerResult{
			Handler:  handlerName,
			Event:    safeEventName(evt),
			Outcome:  OutcomeOf(ctx, err),
			Duration: time.Since(start),
			Err:      err,
			At:       time.Now().UTC(),
		}
		if (r.Outcome == OutcomeFailed || (isFlowControl(r.Outcome) && finalDelivery(ctx))) && scope.deferHandlerDone(r) {
			return
		}
		ObserverFromContext(ctx).HandlerDone(ctx, r)
	}()
	defer func() {
		if r := recover(); r != nil {
			err = recoveredPanic(r)
		}
	}()
	call := func(c context.Context) (cerr error) {
		defer func() {
			if r := recover(); r != nil {
				cerr = recoveredPanic(r)
			}
		}()
		return fn(c)
	}
	if inv == nil {
		return call(ctx)
	}
	return inv.Invoke(ctx, evt, handlerName, call)
}

// finalDelivery reports whether no redelivery follows: ctx carries no Delivery,
// or its Delivery is final. It is the rule OutcomeOf applies.
func finalDelivery(ctx context.Context) bool {
	d, ok := DeliveryFromContext(ctx)
	return !ok || d.Final()
}

// isFlowControl reports whether o is one of the outcomes of a message the chain
// declined to run: in_progress, rate_limited or circuit_open.
func isFlowControl(o Outcome) bool {
	return o == OutcomeInProgress || o == OutcomeRateLimited || o == OutcomeCircuitOpen
}

// safeEventName returns evt.Name(), or "" for a nil event or one whose Name
// panics: a recover site must not panic again.
func safeEventName(evt Event) (name string) {
	if evt == nil {
		return ""
	}
	defer func() {
		if recover() != nil {
			name = ""
		}
	}()
	return evt.Name()
}
