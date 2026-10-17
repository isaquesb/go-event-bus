package event

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
)

// Observer receives the typed failure and resilience signals of the event bus.
// It is the single contract that invokers and transports report through; they
// only call it, and backends (slog, OpenTelemetry, Prometheus) implement it.
//
// An Observer is configured once per bus (BusOptions.Observer or
// LocalBusOptions.Observer), carried in the message context, and called
// synchronously on the handler goroutine with the message context.
// Implementations must be fast and must not block.
//
// Embed NopObserver to stay compatible when methods are added later, the same
// way gRPC servers embed their Unimplemented type:
//
//	type myObserver struct{ event.NopObserver }
//
//	func (myObserver) Failure(ctx context.Context, f event.FailureContext) { ... }
//
// An invoker used outside a bus reads the observer installed with
// WithObserver. There is deliberately no constructor override: the observer is
// configured in one place.
type Observer interface {
	// RetryAttempt reports a failed attempt that the retry policy will repeat.
	RetryAttempt(ctx context.Context, info RetryAttemptInfo)
	// BreakerTransition reports a circuit breaker state change.
	BreakerTransition(ctx context.Context, t BreakerTransition)
	// Failure reports that a message ended and no redelivery follows.
	Failure(ctx context.Context, f FailureContext)
	// DeadLettered reports a successful dead-letter publish.
	DeadLettered(ctx context.Context, info DeadLetterInfo)
	// HandlerDone reports the end of one handler chain invocation.
	HandlerDone(ctx context.Context, r HandlerResult)
	// DecodeFailed reports a message that could not be decoded.
	DecodeFailed(ctx context.Context, f DecodeFailure)
	// DLQPublishFailed reports a failed dead-letter publish.
	DLQPublishFailed(ctx context.Context, f DLQPublishFailure)
	// StoreError reports a failing idempotency or rate-limit store call.
	StoreError(ctx context.Context, e StoreErrorInfo)
	// IdempotencyStale reports a stale in-progress marker that was taken over.
	IdempotencyStale(ctx context.Context, s IdempotencyStaleInfo)
	// InternalError reports an error inside the library that no handler sees.
	InternalError(ctx context.Context, e InternalErrorInfo)
}

// NopObserver implements Observer with methods that do nothing. All receivers
// are values, so both NopObserver{} and &NopObserver{} satisfy Observer, and a
// struct that embeds it keeps compiling when Observer gains methods.
type NopObserver struct{}

// RetryAttempt does nothing.
func (NopObserver) RetryAttempt(context.Context, RetryAttemptInfo) {}

// BreakerTransition does nothing.
func (NopObserver) BreakerTransition(context.Context, BreakerTransition) {}

// Failure does nothing.
func (NopObserver) Failure(context.Context, FailureContext) {}

// DeadLettered does nothing.
func (NopObserver) DeadLettered(context.Context, DeadLetterInfo) {}

// HandlerDone does nothing.
func (NopObserver) HandlerDone(context.Context, HandlerResult) {}

// DecodeFailed does nothing.
func (NopObserver) DecodeFailed(context.Context, DecodeFailure) {}

// DLQPublishFailed does nothing.
func (NopObserver) DLQPublishFailed(context.Context, DLQPublishFailure) {}

// StoreError does nothing.
func (NopObserver) StoreError(context.Context, StoreErrorInfo) {}

// IdempotencyStale does nothing.
func (NopObserver) IdempotencyStale(context.Context, IdempotencyStaleInfo) {}

// InternalError does nothing.
func (NopObserver) InternalError(context.Context, InternalErrorInfo) {}

type observerKey struct{}

// WithObserver returns a context that carries o. A nil o stores NopObserver.
func WithObserver(ctx context.Context, o Observer) context.Context {
	if o == nil {
		o = NopObserver{}
	}
	return context.WithValue(ctx, observerKey{}, o)
}

// ObserverFromContext returns the Observer carried by ctx, or NopObserver when
// ctx carries none.
func ObserverFromContext(ctx context.Context) Observer {
	if o, ok := ctx.Value(observerKey{}).(Observer); ok && o != nil {
		return o
	}
	return NopObserver{}
}

// observer method indexes, used to log a recovered panic once per child and
// method.
const (
	obsRetryAttempt = iota
	obsBreakerTransition
	obsFailure
	obsDeadLettered
	obsHandlerDone
	obsDecodeFailed
	obsDLQPublishFailed
	obsStoreError
	obsIdempotencyStale
	obsInternalError
	obsMethodCount
)

var observerMethodNames = [obsMethodCount]string{
	"RetryAttempt", "BreakerTransition", "Failure", "DeadLettered", "HandlerDone",
	"DecodeFailed", "DLQPublishFailed", "StoreError", "IdempotencyStale", "InternalError",
}

// observerChild is one child of a multiObserver and the per-method flags that
// make a recovered panic log once.
type observerChild struct {
	obs    Observer
	logged [obsMethodCount]atomic.Bool
}

// multiObserver fans every signal out to its children, in order.
type multiObserver struct {
	children []*observerChild
}

// MultiObserver returns an Observer that calls every child in order for every
// signal. Nested multi observers are flattened, and nil and NopObserver
// children are skipped. With no child left it returns NopObserver{}. A single
// child is still wrapped, so a user observer that panics is isolated too.
//
// A child that panics never stops the other children and the panic never
// reaches the handler goroutine. The first panic of each child and method is
// logged once with slog.Default(). This is the only direct log call in the
// observer path, because a failing observer cannot report through itself.
func MultiObserver(obs ...Observer) Observer {
	var children []*observerChild
	for _, o := range obs {
		switch v := o.(type) {
		case nil, NopObserver, *NopObserver:
			continue
		case *multiObserver:
			if v != nil {
				children = append(children, v.children...)
			}
		default:
			children = append(children, &observerChild{obs: o})
		}
	}
	if len(children) == 0 {
		return NopObserver{}
	}
	return &multiObserver{children: children}
}

// each runs fn for every child under a per-child recover.
func (m *multiObserver) each(ctx context.Context, method int, fn func(Observer)) {
	for _, c := range m.children {
		callObserver(ctx, c, method, fn)
	}
}

func callObserver(ctx context.Context, c *observerChild, method int, fn func(Observer)) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if !c.logged[method].CompareAndSwap(false, true) {
			return
		}
		slog.Default().LogAttrs(ctx, slog.LevelError, "eventbus: observer panic recovered",
			slog.String("method", observerMethodNames[method]),
			slog.String("observer", fmt.Sprintf("%T", c.obs)),
			slog.Any("panic", r),
		)
	}()
	fn(c.obs)
}

func (m *multiObserver) RetryAttempt(ctx context.Context, info RetryAttemptInfo) {
	m.each(ctx, obsRetryAttempt, func(o Observer) { o.RetryAttempt(ctx, info) })
}

func (m *multiObserver) BreakerTransition(ctx context.Context, t BreakerTransition) {
	m.each(ctx, obsBreakerTransition, func(o Observer) { o.BreakerTransition(ctx, t) })
}

func (m *multiObserver) Failure(ctx context.Context, f FailureContext) {
	m.each(ctx, obsFailure, func(o Observer) { o.Failure(ctx, f) })
}

func (m *multiObserver) DeadLettered(ctx context.Context, info DeadLetterInfo) {
	m.each(ctx, obsDeadLettered, func(o Observer) { o.DeadLettered(ctx, info) })
}

func (m *multiObserver) HandlerDone(ctx context.Context, r HandlerResult) {
	m.each(ctx, obsHandlerDone, func(o Observer) { o.HandlerDone(ctx, r) })
}

func (m *multiObserver) DecodeFailed(ctx context.Context, f DecodeFailure) {
	m.each(ctx, obsDecodeFailed, func(o Observer) { o.DecodeFailed(ctx, f) })
}

func (m *multiObserver) DLQPublishFailed(ctx context.Context, f DLQPublishFailure) {
	m.each(ctx, obsDLQPublishFailed, func(o Observer) { o.DLQPublishFailed(ctx, f) })
}

func (m *multiObserver) StoreError(ctx context.Context, e StoreErrorInfo) {
	m.each(ctx, obsStoreError, func(o Observer) { o.StoreError(ctx, e) })
}

func (m *multiObserver) IdempotencyStale(ctx context.Context, s IdempotencyStaleInfo) {
	m.each(ctx, obsIdempotencyStale, func(o Observer) { o.IdempotencyStale(ctx, s) })
}

func (m *multiObserver) InternalError(ctx context.Context, e InternalErrorInfo) {
	m.each(ctx, obsInternalError, func(o Observer) { o.InternalError(ctx, e) })
}
