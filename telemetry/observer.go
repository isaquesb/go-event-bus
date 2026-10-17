package telemetry

import (
	"context"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

var _ event.Observer = (*Observer)(nil)

// ObserverOption configures an Observer.
type ObserverOption func(*Observer)

// Observer is an event.Observer that reports in-chain signals as span events on
// the consumer span that TracerInvoker creates. One retry attempt is one span
// event, never a child span.
//
// Failure, HandlerDone and DecodeFailed add no span event: the consumer span
// has already ended when the transport reports them, or never existed.
type Observer struct {
	mp metric.MeterProvider
}

// WithMeterProvider enables the metric set of the Observer and records it
// through mp. There is deliberately no global MeterProvider fallback: without
// this option the Observer records no metric, which avoids double counting next
// to the prometheus module.
func WithMeterProvider(mp metric.MeterProvider) ObserverOption {
	return func(o *Observer) { o.mp = mp }
}

// NewObserver returns an Observer.
func NewObserver(opts ...ObserverOption) *Observer {
	o := &Observer{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// addEvent adds one event to the span in ctx. It is a no-op without a recording
// span.
func addEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.AddEvent(name, trace.WithAttributes(attrs...))
}

// RetryAttempt adds an eventbus.retry.attempt span event.
func (o *Observer) RetryAttempt(ctx context.Context, info event.RetryAttemptInfo) {
	addEvent(ctx, EventRetryAttempt,
		attribute.String(AttrHandler, info.Handler),
		attribute.Int(AttrRetryAttempt, info.Attempt),
		attribute.String(AttrRetryClass, string(info.Class)),
		attribute.Int64(AttrRetryDelayMs, info.Delay.Milliseconds()),
		semconv.ErrorType(info.Err),
	)
}

// BreakerTransition adds an eventbus.breaker.transition span event.
func (o *Observer) BreakerTransition(ctx context.Context, t event.BreakerTransition) {
	addEvent(ctx, EventBreakerTransition,
		attribute.String(AttrHandler, t.Handler),
		attribute.String(AttrBreakerFrom, string(t.From)),
		attribute.String(AttrBreakerTo, string(t.To)),
		attribute.String(AttrBreakerReason, string(t.Reason)),
		attribute.Int(AttrBreakerFailures, t.Failures),
		attribute.Int64(AttrBreakerSeq, int64(t.Seq)),
	)
}

// Failure adds no span event: the consumer span ended before the transport
// reports the terminal failure.
func (o *Observer) Failure(context.Context, event.FailureContext) {}

// DeadLettered adds an eventbus.dead_lettered span event.
func (o *Observer) DeadLettered(ctx context.Context, info event.DeadLetterInfo) {
	addEvent(ctx, EventDeadLettered,
		attribute.String(AttrHandler, info.Handler),
		attribute.String(AttrReason, info.Reason),
		attribute.Int(AttrDeliveryCount, info.Delivery.Count),
	)
}

// HandlerDone adds no span event: the consumer span ended before the transport
// reports it.
func (o *Observer) HandlerDone(context.Context, event.HandlerResult) {}

// DecodeFailed adds no span event: no consumer span exists for a message that
// could not be decoded.
func (o *Observer) DecodeFailed(context.Context, event.DecodeFailure) {}

// DLQPublishFailed adds an eventbus.dlq.publish_failed span event.
func (o *Observer) DLQPublishFailed(ctx context.Context, f event.DLQPublishFailure) {
	addEvent(ctx, EventDLQPublishFailed,
		attribute.String(AttrHandler, f.Handler),
		attribute.String(AttrReason, f.Reason),
		semconv.ErrorType(f.Err),
	)
}

// StoreError adds an eventbus.store.error span event.
func (o *Observer) StoreError(ctx context.Context, e event.StoreErrorInfo) {
	addEvent(ctx, EventStoreError,
		attribute.String(AttrHandler, e.Handler),
		attribute.String(AttrStore, e.Store),
		attribute.String(AttrOp, e.Op),
		semconv.ErrorType(e.Err),
	)
}

// IdempotencyStale adds an eventbus.idempotency.stale span event.
func (o *Observer) IdempotencyStale(ctx context.Context, s event.IdempotencyStaleInfo) {
	addEvent(ctx, EventIdempotencyStale, attribute.String(AttrHandler, s.Handler))
}

// InternalError adds an eventbus.internal_error span event.
func (o *Observer) InternalError(ctx context.Context, e event.InternalErrorInfo) {
	addEvent(ctx, EventInternalError,
		attribute.String(AttrComponent, e.Component),
		attribute.String(AttrOp, e.Op),
	)
}
