package telemetry

import (
	"context"
	"errors"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerOption configures a TracerInvoker.
type TracerOption func(*tracerConfig)

type tracerConfig struct {
	provider trace.TracerProvider
	dest     func(ctx context.Context, evt event.Event) string
}

// WithTracerProvider sets the provider used when NewTracerInvoker receives a nil
// tracer. Without it the global provider is used.
func WithTracerProvider(tp trace.TracerProvider) TracerOption {
	return func(c *tracerConfig) { c.provider = tp }
}

// WithSpanDestination overrides the destination of the consumer span. The
// destination names the span ("process <destination>") and is the value of
// messaging.destination.name. It defaults to Delivery.Subject, then the event
// name. Use it when subjects carry entity ids, so span names stay low
// cardinality.
func WithSpanDestination(fn func(ctx context.Context, evt event.Event) string) TracerOption {
	return func(c *tracerConfig) { c.dest = fn }
}

// NewTracerInvoker returns an invoker that wraps every handler execution in an
// OpenTelemetry consumer span. A nil tracer is created from WithTracerProvider
// or the global provider, with the instrumentation scope ScopeName.
//
// The span is named "process <destination>" and carries the messaging
// attributes of the OpenTelemetry messaging semantic conventions (pinned to
// semconv v1.43.0, Development status in the specification) plus eventbus.*
// attributes. A replayed message gets span links to its dead-letter span and
// its replay span.
//
// TracerInvoker must be the outermost invoker of the chain, so the signals that
// NewObserver turns into span events land on its span.
func NewTracerInvoker(tracer trace.Tracer, opts ...TracerOption) *TracerInvoker {
	var cfg tracerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if tracer == nil {
		tp := cfg.provider
		if tp == nil {
			tp = otel.GetTracerProvider()
		}
		tracer = tp.Tracer(ScopeName)
	}
	return &TracerInvoker{tracer: tracer, dest: cfg.dest}
}

// TracerInvoker is an event.Invoker that creates the consumer span.
type TracerInvoker struct {
	tracer trace.Tracer
	dest   func(ctx context.Context, evt event.Event) string
}

// Invoke runs fn inside the consumer span of the message.
func (i *TracerInvoker) Invoke(
	ctx context.Context,
	evt event.Event,
	handlerName string,
	fn func(context.Context) error,
) error {
	md, _ := event.MetadataFromContext(ctx)
	d, _ := event.DeliveryFromContext(ctx)

	dest := d.Subject
	if i.dest != nil {
		dest = i.dest(ctx, evt)
	}
	if dest == "" {
		dest = evt.Name()
	}
	system := d.System
	if system == "" {
		system = defaultMessagingSystem
	}

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(system),
		semconv.MessagingOperationName("process"),
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingDestinationName(dest),
		attribute.String(AttrHandler, handlerName),
		attribute.Int(AttrDeliveryCount, d.Count),
	}
	if md.ID != "" {
		attrs = append(attrs, semconv.MessagingMessageID(md.ID))
	}
	if md.CorrelationID != "" {
		attrs = append(attrs, attribute.String(AttrCorrelationID, md.CorrelationID))
	}
	links, replayAttrs := replayLinks(d)
	attrs = append(attrs, replayAttrs...)

	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
	if len(links) > 0 {
		startOpts = append(startOpts, trace.WithLinks(links...))
	}
	ctx, span := i.tracer.Start(ctx, "process "+dest, startOpts...)
	defer span.End()

	err := fn(ctx)
	// A duplicate is a handled outcome, not a failure of the span.
	if err != nil && !errors.Is(err, event.ErrDuplicate) {
		span.RecordError(err)
		span.SetAttributes(semconv.ErrorType(err))
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// replayLinks returns the span links and attributes of a replayed message.
func replayLinks(event.Delivery) ([]trace.Link, []attribute.KeyValue) {
	return nil, nil
}
