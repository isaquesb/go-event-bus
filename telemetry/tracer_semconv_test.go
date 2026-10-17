package telemetry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// tmSpanContext starts a span on a throwaway provider and returns its context,
// so tests get valid, distinct W3C ids.
func tmSpanContext(t *testing.T) trace.SpanContext {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	_, span := tp.Tracer("tm").Start(context.Background(), "s")
	defer span.End()
	sc := span.SpanContext()
	if !sc.IsValid() {
		t.Fatal("generated span context is invalid")
	}
	return sc
}

func tmTraceparent(sc trace.SpanContext) string {
	return fmt.Sprintf("00-%s-%s-%02x", sc.TraceID(), sc.SpanID(), byte(sc.TraceFlags()))
}

// tmReplayCtx builds a context with a remote producer parent and a Delivery
// carrying the given headers.
func tmReplayCtx(parent trace.SpanContext, headers map[string][]string) context.Context {
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent)
	return event.WithDelivery(ctx, event.Delivery{Count: 1, Subject: "orders.created", Headers: headers})
}

func tmInvokeOK(t *testing.T, ctx context.Context) tracetest.SpanStub {
	t.Helper()
	tracer, sr := newTestTracer()
	inv := NewTracerInvoker(tracer)
	if err := inv.Invoke(ctx, tmEvent{}, "billing", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	return tracetest.SpanStubFromReadOnlySpan(spans[0])
}

func TestTracer_ReplayLinks(t *testing.T) {
	p, a, b := tmSpanContext(t), tmSpanContext(t), tmSpanContext(t)
	ctx := tmReplayCtx(p, map[string][]string{
		event.HeaderReplayTraceparent:       {tmTraceparent(a)},
		event.HeaderReplayFailedTraceparent: {tmTraceparent(b)},
		event.HeaderReplayDLQSequence:       {"42"},
	})
	span := tmInvokeOK(t, ctx)

	if span.Parent.SpanID() != p.SpanID() || span.Parent.TraceID() != p.TraceID() {
		t.Errorf("parent = %v, want the producer span %v", span.Parent, p)
	}
	if len(span.Links) != 2 {
		t.Fatalf("links = %d, want 2", len(span.Links))
	}
	for i, want := range []struct {
		sc  trace.SpanContext
		typ string
	}{{b, "dead_letter"}, {a, "replay"}} {
		l := span.Links[i]
		if l.SpanContext.TraceID() != want.sc.TraceID() || l.SpanContext.SpanID() != want.sc.SpanID() {
			t.Errorf("link %d span context = %v, want %v", i, l.SpanContext, want.sc)
		}
		tmExpect(t, fmt.Sprintf("link %d", i), tmAttrMap(l.Attributes), map[string]string{"eventbus.link.type": want.typ})
	}
	tmExpect(t, "span", tmAttrMap(span.Attributes), map[string]string{
		"eventbus.replayed":     "true",
		"eventbus.dlq.sequence": "42",
	})
}

func TestTracer_ReplayHeadersCaseInsensitiveAndInvalid(t *testing.T) {
	p, a := tmSpanContext(t), tmSpanContext(t)

	// Mixed-case keys still match; the invalid failed traceparent is ignored and
	// the non-numeric sequence keeps eventbus.replayed without the sequence.
	span := tmInvokeOK(t, tmReplayCtx(p, map[string][]string{
		"eventbus-replay-traceparent":        {tmTraceparent(a)},
		"EVENTBUS-REPLAY-FAILED-TRACEPARENT": {"not-a-traceparent"},
		"Eventbus-Replay-Dlq-Sequence":       {"forty-two"},
	}))
	if len(span.Links) != 1 {
		t.Fatalf("links = %d, want 1 (only the valid replay traceparent)", len(span.Links))
	}
	tmExpect(t, "link", tmAttrMap(span.Links[0].Attributes), map[string]string{"eventbus.link.type": "replay"})
	attrs := tmAttrMap(span.Attributes)
	if attrs["eventbus.replayed"] != "true" {
		t.Errorf("eventbus.replayed = %q, want true", attrs["eventbus.replayed"])
	}
	if _, ok := attrs["eventbus.dlq.sequence"]; ok {
		t.Errorf("eventbus.dlq.sequence must be omitted for a non-numeric value, got %q", attrs["eventbus.dlq.sequence"])
	}
	if span.Parent.SpanID() != p.SpanID() {
		t.Errorf("an invalid header changed the parent: %v", span.Parent)
	}

	// All headers invalid: no link, no panic.
	span = tmInvokeOK(t, tmReplayCtx(p, map[string][]string{
		event.HeaderReplayTraceparent:       {"00-00000000000000000000000000000000-0000000000000000-01"},
		event.HeaderReplayFailedTraceparent: {""},
	}))
	if len(span.Links) != 0 {
		t.Errorf("links = %d, want 0 for invalid traceparents", len(span.Links))
	}
}

func TestTracer_NoReplayNoLinks(t *testing.T) {
	p := tmSpanContext(t)
	span := tmInvokeOK(t, tmReplayCtx(p, map[string][]string{"X-Other": {"v"}}))
	if len(span.Links) != 0 {
		t.Errorf("links = %d, want 0", len(span.Links))
	}
	attrs := tmAttrMap(span.Attributes)
	for _, k := range []string{"eventbus.replayed", "eventbus.dlq.sequence"} {
		if _, ok := attrs[k]; ok {
			t.Errorf("attribute %s must be absent without replay headers", k)
		}
	}
}

func TestTracer_SpanDestinationOption(t *testing.T) {
	tracer, sr := newTestTracer()
	inv := NewTracerInvoker(tracer, WithSpanDestination(func(_ context.Context, evt event.Event) string {
		return "orders.{id}"
	}))
	ctx := event.WithDelivery(context.Background(), event.Delivery{Subject: "orders.12345"})
	if err := inv.Invoke(ctx, tmEvent{}, "billing", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	span := sr.Ended()[0]
	if span.Name() != "process orders.{id}" {
		t.Errorf("span name = %q, want %q", span.Name(), "process orders.{id}")
	}
	tmExpect(t, "span", tmAttrMap(span.Attributes()), map[string]string{"messaging.destination.name": "orders.{id}"})
}

func TestTracer_TracerProviderOption(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	inv := NewTracerInvoker(nil, WithTracerProvider(tp))
	if err := inv.Invoke(context.Background(), tmEvent{}, "billing", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans in the configured provider = %d, want 1", len(spans))
	}
	if got := spans[0].InstrumentationScope().Name; got != ScopeName {
		t.Errorf("instrumentation scope = %q, want %q", got, ScopeName)
	}

	// A nil tracer without options uses the global provider.
	gsr := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(gsr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	if err := NewTracerInvoker(nil).Invoke(context.Background(), tmEvent{}, "billing", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if n := len(gsr.Ended()); n != 1 {
		t.Errorf("spans in the global provider = %d, want 1", n)
	}
}

func TestTracer_DuplicateNotAnError(t *testing.T) {
	tracer, sr := newTestTracer()
	inv := NewTracerInvoker(tracer)
	dup := fmt.Errorf("wrapped: %w", event.ErrDuplicate)
	err := inv.Invoke(context.Background(), tmEvent{}, "billing", func(context.Context) error { return dup })
	if !errors.Is(err, event.ErrDuplicate) {
		t.Fatalf("err = %v, want the duplicate error returned unchanged", err)
	}
	span := sr.Ended()[0]
	if span.Status().Code != codes.Unset {
		t.Errorf("status = %v, want Unset", span.Status().Code)
	}
	if len(span.Events()) != 0 {
		t.Errorf("a duplicate recorded %d span events, want none", len(span.Events()))
	}
	if _, ok := tmAttrMap(span.Attributes())["error.type"]; ok {
		t.Error("a duplicate must not set error.type")
	}
}
