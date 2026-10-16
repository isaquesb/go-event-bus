package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
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
// *PanicError. A panic inside an invoker is recovered by an outer layer. Both
// are logged once with slog.ErrorContext, with the full value and stack as
// structured attributes. A nil inv is a passthrough: fn is called directly.
//
// The recover does not stop runtime.Goexit, so callers release their
// accounting in a defer.
func InvokeHandler(ctx context.Context, inv Invoker, evt Event, handlerName string, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(ctx, r, evt, handlerName, "invoker")
		}
	}()
	call := func(c context.Context) (cerr error) {
		defer func() {
			if r := recover(); r != nil {
				cerr = newPanicError(c, r, evt, handlerName, "handler")
			}
		}()
		return fn(c)
	}
	if inv == nil {
		return call(ctx)
	}
	return inv.Invoke(ctx, evt, handlerName, call)
}

// newPanicError builds the PanicError for a recovered value and logs it. It
// must be called from the deferred function that recovered r, so the captured
// stack still holds the panicking frames.
func newPanicError(ctx context.Context, r any, evt Event, handlerName, site string) *PanicError {
	pe := &PanicError{Value: r, Stack: debug.Stack()}
	msg := "event: handler panic recovered"
	if site == "invoker" {
		msg = "event: invoker panic recovered"
	}
	attrs := make([]slog.Attr, 0, 5)
	attrs = append(attrs, slog.String("handler", handlerName))
	if name := safeEventName(evt); name != "" {
		attrs = append(attrs, slog.String("event", name))
	}
	if md, ok := MetadataFromContext(ctx); ok && md.ID != "" {
		attrs = append(attrs, slog.String("event_id", md.ID))
	}
	attrs = append(attrs,
		slog.String("panic", fmt.Sprint(r)),
		slog.String("stack", string(pe.Stack)),
	)
	slog.LogAttrs(ctx, slog.LevelError, msg, attrs...)
	return pe
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
