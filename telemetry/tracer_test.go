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

	// A Delivery subject names the span; without one the event name is used.
	ctx := event.WithDelivery(context.Background(), event.Delivery{Subject: "orders.created"})
	_ = invoker.Invoke(ctx, stubEvent{"test.event"}, "myHandler", func(context.Context) error { return nil })
	if got := sr.Ended()[0].Name(); got != "process orders.created" {
		t.Errorf("span name from Delivery subject: got %q", got)
	}

	err := invoker.Invoke(context.Background(), stubEvent{"test.event"}, "myHandler", func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := sr.Ended()[1:]
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name() != "process test.event" {
		t.Errorf("expected span name 'process test.event', got %q", spans[0].Name())
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
		attrs[string(a.Key)] = a.Value.String()
	}

	want := map[string]string{
		"messaging.system":           "eventbus",
		"messaging.operation.name":   "process",
		"messaging.operation.type":   "process",
		"messaging.destination.name": "order.created",
		"eventbus.handler":           "sendEmail",
		"eventbus.delivery_count":    "0",
	}
	for k, v := range want {
		if attrs[k] != v {
			t.Errorf("%s: expected %q, got %q", k, v, attrs[k])
		}
	}
	for _, absent := range []string{"event.name", "handler.name", "messaging.message.id", "eventbus.correlation_id"} {
		if _, ok := attrs[absent]; ok {
			t.Errorf("attribute %s must be absent, got %q", absent, attrs[absent])
		}
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
