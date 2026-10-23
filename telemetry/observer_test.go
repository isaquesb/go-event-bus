package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type tmEvent struct{}

func (tmEvent) Name() string { return "order.created" }

// tmAttrMap flattens attributes to their string form for assertions.
func tmAttrMap(kvs []attribute.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value.String()
	}
	return m
}

func tmExpect(t *testing.T, what string, got map[string]string, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %s = %q, want %q", what, k, got[k], v)
		}
	}
}

func TestObserver_SpanEventsOnProcessSpan(t *testing.T) {
	tracer, sr := newTestTracer()
	inv := NewTracerInvoker(tracer)
	obs := NewObserver()

	ctx := event.WithObserver(context.Background(), obs)
	ctx = event.WithDelivery(ctx, event.Delivery{Count: 2, Max: 5, Subject: "orders.created", System: "nats"})
	ctx = event.WithMetadata(ctx, event.Metadata{ID: "m-1", CorrelationID: "c-1"})

	boom := errors.New("boom")
	err := inv.Invoke(ctx, tmEvent{}, "billing", func(c context.Context) error {
		o := event.ObserverFromContext(c)
		o.RetryAttempt(c, event.RetryAttemptInfo{
			Handler: "billing", Event: "order.created", Attempt: 1,
			Class: event.AttemptTransient, Err: boom, Delay: 50 * time.Millisecond,
		})
		o.BreakerTransition(c, event.BreakerTransition{
			Handler: "billing", From: event.BreakerClosed, To: event.BreakerOpen,
			Reason: event.BreakerReasonFailureThreshold, Failures: 5, Seq: 1,
		})
		o.DeadLettered(c, event.DeadLetterInfo{
			Handler: "billing", Event: "order.created", Reason: "retries_exhausted",
			Delivery: event.Delivery{Count: 2},
		})
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}

	// The transport reports these after the span ended: no event may appear.
	obs.Failure(ctx, event.FailureContext{Handler: "billing", Outcome: event.OutcomeDeadLettered})
	obs.HandlerDone(ctx, event.HandlerResult{Handler: "billing", Outcome: event.OutcomeFailed})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "process orders.created" {
		t.Errorf("span name = %q", span.Name())
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("span kind = %v", span.SpanKind())
	}
	attrs := tmAttrMap(span.Attributes())
	tmExpect(t, "span", attrs, map[string]string{
		"messaging.system":           "nats",
		"messaging.operation.name":   "process",
		"messaging.operation.type":   "process",
		"messaging.destination.name": "orders.created",
		"messaging.message.id":       "m-1",
		"eventbus.handler":           "billing",
		"eventbus.correlation_id":    "c-1",
		"eventbus.delivery_count":    "2",
	})
	if attrs["error.type"] == "" {
		t.Errorf("error.type attribute missing on a failed span: %v", attrs)
	}
	if span.Status().Code.String() != "Error" {
		t.Errorf("status = %v, want Error", span.Status().Code)
	}

	// RecordError adds an "exception" event first; the observer events follow.
	var names []string
	var evs []sdktrace.Event
	for _, e := range span.Events() {
		if e.Name == "exception" {
			continue
		}
		names = append(names, e.Name)
		evs = append(evs, e)
	}
	wantNames := []string{EventRetryAttempt, EventBreakerTransition, EventDeadLettered}
	if len(names) != len(wantNames) {
		t.Fatalf("span events = %v, want %v", names, wantNames)
	}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("span events = %v, want %v", names, wantNames)
		}
	}
	tmExpect(t, "retry event", tmAttrMap(evs[0].Attributes), map[string]string{
		"eventbus.handler":        "billing",
		"eventbus.retry.attempt":  "1",
		"eventbus.retry.class":    "transient",
		"eventbus.retry.delay_ms": "50",
	})
	if tmAttrMap(evs[0].Attributes)["error.type"] == "" {
		t.Error("retry event: error.type missing")
	}
	tmExpect(t, "breaker event", tmAttrMap(evs[1].Attributes), map[string]string{
		"eventbus.handler":          "billing",
		"eventbus.breaker.from":     "closed",
		"eventbus.breaker.to":       "open",
		"eventbus.breaker.reason":   "failure_threshold",
		"eventbus.breaker.failures": "5",
		"eventbus.breaker.seq":      "1",
	})
	tmExpect(t, "dead-letter event", tmAttrMap(evs[2].Attributes), map[string]string{
		"eventbus.reason":         "retries_exhausted",
		"eventbus.delivery_count": "2",
	})
	for _, e := range evs {
		for _, a := range e.Attributes {
			if a.Value.String() == boom.Error() {
				t.Errorf("event %s leaks the error text in %s", e.Name, a.Key)
			}
		}
	}
}

func TestObserver_NoSpanNoPanic(t *testing.T) {
	obs := NewObserver()
	ctx := context.Background()
	err := errors.New("x")

	obs.RetryAttempt(ctx, event.RetryAttemptInfo{Err: err})
	obs.BreakerTransition(ctx, event.BreakerTransition{})
	obs.Failure(ctx, event.FailureContext{})
	obs.DeadLettered(ctx, event.DeadLetterInfo{})
	obs.HandlerDone(ctx, event.HandlerResult{})
	obs.DecodeFailed(ctx, event.DecodeFailure{})
	obs.DLQPublishFailed(ctx, event.DLQPublishFailure{Err: err})
	obs.StoreError(ctx, event.StoreErrorInfo{Err: err})
	obs.IdempotencyStale(ctx, event.IdempotencyStaleInfo{})
	obs.InternalError(ctx, event.InternalErrorInfo{Err: err})
}
