package telemetry

import (
	"context"
	"errors"
	"testing"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type stubEvent struct{ name string }

func (s stubEvent) Name() string { return s.name }

var _ event.Event = stubEvent{}

func newTestTracer() (trace.Tracer, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return tp.Tracer("test"), sr
}

func TestSpanCreation(t *testing.T) {
	tracer, sr := newTestTracer()
	invoker := NewTracerInvoker(tracer)

	err := invoker.Invoke(context.Background(), stubEvent{"test.event"}, "myHandler", func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "event.handle" {
		t.Errorf("expected span name 'event.handle', got %q", spans[0].Name())
	}
	if spans[0].SpanKind() != trace.SpanKindConsumer {
		t.Errorf("expected SpanKindConsumer, got %v", spans[0].SpanKind())
	}
}

func TestAttributeMapping(t *testing.T) {
	tracer, sr := newTestTracer()
	invoker := NewTracerInvoker(tracer)

	_ = invoker.Invoke(context.Background(), stubEvent{"order.created"}, "sendEmail", func(ctx context.Context) error {
		return nil
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	attrs := map[string]string{}
	for _, a := range spans[0].Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}

	if attrs["event.name"] != "order.created" {
		t.Errorf("event.name: expected 'order.created', got %q", attrs["event.name"])
	}
	if attrs["handler.name"] != "sendEmail" {
		t.Errorf("handler.name: expected 'sendEmail', got %q", attrs["handler.name"])
	}
}

func TestContextPropagation(t *testing.T) {
	tracer, _ := newTestTracer()
	invoker := NewTracerInvoker(tracer)

	var spanInsideFn trace.Span
	_ = invoker.Invoke(context.Background(), stubEvent{"ctx.event"}, "handler", func(ctx context.Context) error {
		spanInsideFn = trace.SpanFromContext(ctx)
		return nil
	})

	if spanInsideFn == nil {
		t.Fatal("expected span in context passed to fn, got nil")
	}
	if !spanInsideFn.SpanContext().IsValid() {
		t.Error("span context passed to fn is not valid")
	}
}

func TestErrorRecording(t *testing.T) {
	tracer, sr := newTestTracer()
	invoker := NewTracerInvoker(tracer)
	boom := errors.New("boom")

	err := invoker.Invoke(context.Background(), stubEvent{"fail.event"}, "handler", func(ctx context.Context) error {
		return boom
	})

	if err != boom {
		t.Fatalf("expected boom error, got %v", err)
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Errorf("expected Error status code, got %v", spans[0].Status().Code)
	}
	if spans[0].Status().Description != "boom" {
		t.Errorf("expected status description 'boom', got %q", spans[0].Status().Description)
	}
}
