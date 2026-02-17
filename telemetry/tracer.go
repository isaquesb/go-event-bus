package telemetry

import (
	"context"

	"github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func NewTracerInvoker(tracer trace.Tracer) *TracerInvoker {
	return &TracerInvoker{tracer: tracer}
}

type TracerInvoker struct {
	tracer trace.Tracer
}

func (i *TracerInvoker) Invoke(
	ctx context.Context,
	evt event.Event,
	handlerName string,
	fn func(context.Context) error,
) error {
	ctx, span := i.tracer.Start(
		ctx,
		"event.handle",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("event.name", evt.Name()),
			attribute.String("handler.name", handlerName),
		),
	)
	defer span.End()

	err := fn(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
