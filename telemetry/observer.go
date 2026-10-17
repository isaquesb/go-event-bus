package telemetry

import (
	"context"
	"errors"
	"sync"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
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
//
// With WithMeterProvider the Observer also records the metric set below through
// the OpenTelemetry metrics API. Dots in the names map to underscores and the
// unit "s" to a _seconds suffix in the OpenTelemetry Prometheus translation, so
// they line up with the prometheus module:
//
//	eventbus.handler.duration  histogram, s   handler, event, outcome
//	eventbus.handler           counter        handler, event, outcome
//	eventbus.retry.attempts    counter        handler, event, reason (attempt class)
//	eventbus.breaker.transitions counter      handler, from, to, reason
//	eventbus.breaker.state     gauge          handler (0 closed, 1 open, 2 half_open)
//	eventbus.dlq.records       counter        handler, event, reason
//	eventbus.dlq.publish.errors counter       handler, event
//	eventbus.decode.failures   counter        reason
//	eventbus.store.errors      counter        handler, store, op
//	eventbus.idempotency.stale counter        handler
//	eventbus.internal.errors   counter        component, op
//
// Attribute values come only from bounded fields: no event id, subject,
// payload or error text.
type Observer struct {
	mp metric.MeterProvider

	// instruments are nil unless a MeterProvider was configured.
	instruments *instruments

	// breakerMu guards breakerSeq and orders the gauge writes, so the gauge is
	// last-writer-wins by Seq per breaker instance (handler and BreakerID).
	breakerMu  sync.Mutex
	breakerSeq map[breakerKey]uint64
}

// breakerKey scopes Seq: it increases per handler breaker of one CircuitBreaker
// instance, so the order guard is keyed by the handler and the breaker id.
type breakerKey struct {
	handler string
	breaker uint64
}

// instruments holds the metric instruments created from a MeterProvider.
type instruments struct {
	duration    metric.Float64Histogram
	handlers    metric.Int64Counter
	retries     metric.Int64Counter
	transitions metric.Int64Counter
	state       metric.Int64Gauge
	dlqRecords  metric.Int64Counter
	dlqErrors   metric.Int64Counter
	decode      metric.Int64Counter
	storeErrors metric.Int64Counter
	stale       metric.Int64Counter
	internal    metric.Int64Counter
}

// defaultBuckets are the explicit histogram bounds of eventbus.handler.duration
// in seconds, the same as the prometheus module.
var defaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Breaker gauge values of eventbus.breaker.state.
const (
	breakerGaugeClosed   = 0
	breakerGaugeOpen     = 1
	breakerGaugeHalfOpen = 2
)

// WithMeterProvider enables the metric set of the Observer and records it
// through mp. There is deliberately no global MeterProvider fallback: without
// this option the Observer records no metric, which avoids double counting next
// to the prometheus module.
func WithMeterProvider(mp metric.MeterProvider) ObserverOption {
	return func(o *Observer) { o.mp = mp }
}

// NewObserver returns an Observer.
func NewObserver(opts ...ObserverOption) *Observer {
	o := &Observer{breakerSeq: map[breakerKey]uint64{}}
	for _, opt := range opts {
		opt(o)
	}
	if o.mp != nil {
		ins, err := newInstruments(o.mp.Meter(ScopeName))
		if err != nil {
			// A broken provider never fails the bus: report once through the
			// OpenTelemetry error handler and keep whatever was created.
			otel.Handle(err)
		}
		o.instruments = ins
	}
	return o
}

// newInstruments creates every instrument. Creation errors are joined, and the
// returned value is always usable (a failed instrument is replaced by a no-op one).
func newInstruments(m metric.Meter) (*instruments, error) {
	var errs []error
	counter := func(name, unit, desc string) metric.Int64Counter {
		c, err := m.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		if c == nil {
			c, _ = noop.NewMeterProvider().Meter(ScopeName).Int64Counter(name)
		}
		errs = append(errs, err)
		return c
	}
	ins := &instruments{}
	const durationName = "eventbus.handler.duration"
	d, err := m.Float64Histogram(durationName,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one handler chain invocation."),
		metric.WithExplicitBucketBoundaries(defaultBuckets...))
	if d == nil {
		d, _ = noop.NewMeterProvider().Meter(ScopeName).Float64Histogram(durationName)
	}
	errs = append(errs, err)
	ins.duration = d
	ins.handlers = counter("eventbus.handler", "{message}", "Handler chain invocations by outcome.")
	ins.retries = counter("eventbus.retry.attempts", "{attempt}", "Failed attempts that the retry policy repeats.")
	ins.transitions = counter("eventbus.breaker.transitions", "{transition}", "Circuit breaker state changes.")
	g, err := m.Int64Gauge("eventbus.breaker.state",
		metric.WithUnit("{state}"),
		metric.WithDescription("Circuit breaker state: 0 closed, 1 open, 2 half_open."))
	if g == nil {
		g, _ = noop.NewMeterProvider().Meter(ScopeName).Int64Gauge("eventbus.breaker.state")
	}
	errs = append(errs, err)
	ins.state = g
	ins.dlqRecords = counter("eventbus.dlq.records", "{record}", "Messages published to the dead-letter queue.")
	ins.dlqErrors = counter("eventbus.dlq.publish.errors", "{error}", "Failed dead-letter publishes.")
	ins.decode = counter("eventbus.decode.failures", "{failure}", "Messages that could not be decoded.")
	ins.storeErrors = counter("eventbus.store.errors", "{error}", "Failing idempotency or rate-limit store calls.")
	ins.stale = counter("eventbus.idempotency.stale", "{lock}", "Stale in-progress markers that were taken over.")
	ins.internal = counter("eventbus.internal.errors", "{error}", "Errors inside the library that no handler sees.")
	return ins, errors.Join(errs...)
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
	if ins := o.instruments; ins != nil {
		ins.retries.Add(ctx, 1, metric.WithAttributes(
			attribute.String("handler", info.Handler),
			attribute.String("event", info.Event),
			attribute.String("reason", string(info.Class)),
		))
	}
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
	if ins := o.instruments; ins != nil {
		ins.transitions.Add(ctx, 1, metric.WithAttributes(
			attribute.String("handler", t.Handler),
			attribute.String("from", string(t.From)),
			attribute.String("to", string(t.To)),
			attribute.String("reason", string(t.Reason)),
		))
		o.recordBreakerState(ctx, t)
	}
}

// recordBreakerState writes the breaker gauge only when t is newer than the
// last transition recorded for the same breaker instance (handler and
// BreakerID). A rebuilt breaker that reuses a handler name starts a new order
// and is followed. Transitions are delivered on the goroutine of the message
// that caused them, so they can arrive out of order.
// The lock covers the write as well, so two goroutines cannot interleave the
// check and the record.
func (o *Observer) recordBreakerState(ctx context.Context, t event.BreakerTransition) {
	var v int64
	switch t.To {
	case event.BreakerClosed:
		v = breakerGaugeClosed
	case event.BreakerOpen:
		v = breakerGaugeOpen
	case event.BreakerHalfOpen:
		v = breakerGaugeHalfOpen
	default:
		return
	}
	o.breakerMu.Lock()
	defer o.breakerMu.Unlock()
	key := breakerKey{handler: t.Handler, breaker: t.BreakerID}
	if last, ok := o.breakerSeq[key]; ok && t.Seq <= last {
		return
	}
	o.breakerSeq[key] = t.Seq
	o.instruments.state.Record(ctx, v, metric.WithAttributes(attribute.String("handler", t.Handler)))
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
	if ins := o.instruments; ins != nil {
		ins.dlqRecords.Add(ctx, 1, metric.WithAttributes(
			attribute.String("handler", info.Handler),
			attribute.String("event", info.Event),
			attribute.String("reason", info.Reason),
		))
	}
}

// HandlerDone adds no span event: the consumer span ended before the transport
// reports it. It records the duration histogram and the handler counter.
func (o *Observer) HandlerDone(ctx context.Context, r event.HandlerResult) {
	if ins := o.instruments; ins != nil {
		attrs := metric.WithAttributes(
			attribute.String("handler", r.Handler),
			attribute.String("event", r.Event),
			attribute.String("outcome", string(r.Outcome)),
		)
		ins.duration.Record(ctx, r.Duration.Seconds(), attrs)
		ins.handlers.Add(ctx, 1, attrs)
	}
}

// DecodeFailed adds no span event: no consumer span exists for a message that
// could not be decoded. It counts the failure by its stable reason.
func (o *Observer) DecodeFailed(ctx context.Context, f event.DecodeFailure) {
	if ins := o.instruments; ins != nil {
		ins.decode.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", f.Reason)))
	}
}

// DLQPublishFailed adds an eventbus.dlq.publish_failed span event.
func (o *Observer) DLQPublishFailed(ctx context.Context, f event.DLQPublishFailure) {
	addEvent(ctx, EventDLQPublishFailed,
		attribute.String(AttrHandler, f.Handler),
		attribute.String(AttrReason, f.Reason),
		semconv.ErrorType(f.Err),
	)
	if ins := o.instruments; ins != nil {
		ins.dlqErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("handler", f.Handler),
			attribute.String("event", f.Event),
		))
	}
}

// StoreError adds an eventbus.store.error span event.
func (o *Observer) StoreError(ctx context.Context, e event.StoreErrorInfo) {
	addEvent(ctx, EventStoreError,
		attribute.String(AttrHandler, e.Handler),
		attribute.String(AttrStore, e.Store),
		attribute.String(AttrOp, e.Op),
		semconv.ErrorType(e.Err),
	)
	if ins := o.instruments; ins != nil {
		ins.storeErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("handler", e.Handler),
			attribute.String("store", e.Store),
			attribute.String("op", e.Op),
		))
	}
}

// IdempotencyStale adds an eventbus.idempotency.stale span event.
func (o *Observer) IdempotencyStale(ctx context.Context, s event.IdempotencyStaleInfo) {
	addEvent(ctx, EventIdempotencyStale, attribute.String(AttrHandler, s.Handler))
	if ins := o.instruments; ins != nil {
		ins.stale.Add(ctx, 1, metric.WithAttributes(attribute.String("handler", s.Handler)))
	}
}

// InternalError adds an eventbus.internal_error span event.
func (o *Observer) InternalError(ctx context.Context, e event.InternalErrorInfo) {
	addEvent(ctx, EventInternalError,
		attribute.String(AttrComponent, e.Component),
		attribute.String(AttrOp, e.Op),
	)
	if ins := o.instruments; ins != nil {
		ins.internal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("component", e.Component),
			attribute.String("op", e.Op),
		))
	}
}
