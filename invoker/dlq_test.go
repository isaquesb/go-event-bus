package invoker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

func TestDLQ_SuccessNoPublish(t *testing.T) {
	publisher := &mockDLQPublisher{}
	dlq := invoker.NewDLQ(publisher, nil)

	err := dlq.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if publisher.published {
		t.Error("DLQ should not publish on success")
	}
}

func TestDLQ_NonDLQErrorPassesThrough(t *testing.T) {
	publisher := &mockDLQPublisher{}
	dlq := invoker.NewDLQ(publisher, nil)

	handlerErr := errors.New("handler failed")

	err := dlq.Invoke(context.Background(), &testEvent{name: "test.event"}, "handler", func(ctx context.Context) error {
		return handlerErr
	})

	// Non-DLQ errors should pass through without publishing
	if !errors.Is(err, handlerErr) {
		t.Errorf("expected handler error to pass through, got '%v'", err)
	}
	if publisher.published {
		t.Error("DLQ should NOT publish for non-DLQ errors")
	}
}

func TestDLQ_ErrSendToDLQPublishes(t *testing.T) {
	publisher := &mockDLQPublisher{}
	dlq := invoker.NewDLQ(publisher, nil)

	testEvt := &testEvent{name: "test.event"}
	dlqErr := fmt.Errorf("terminal failure: %w", invoker.ErrSendToDLQ)

	err := dlq.Invoke(context.Background(), testEvt, "handler", func(ctx context.Context) error {
		return dlqErr
	})

	// Error should be swallowed after successful DLQ publish
	if err != nil {
		t.Errorf("expected nil error after DLQ publish, got '%v'", err)
	}
	if !publisher.published {
		t.Error("DLQ should publish on ErrSendToDLQ")
	}
	if publisher.evt != testEvt {
		t.Error("DLQ should receive the event")
	}
}

func TestDLQ_PublishErrorReturned(t *testing.T) {
	publishErr := errors.New("publish failed")
	publisher := &mockDLQPublisher{err: publishErr}
	metrics := &testMetricProvider{}
	dlq := invoker.NewDLQ(publisher, metrics)

	err := dlq.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return fmt.Errorf("terminal: %w", invoker.ErrSendToDLQ)
	})

	if !errors.Is(err, publishErr) {
		t.Errorf("expected publish error, got '%v'", err)
	}

	if metrics.counters["eventbus_dlq_publish_error_total"] != 1 {
		t.Error("expected DLQ error metric to be tracked")
	}
}

func TestDLQ_MetricsOnSuccess(t *testing.T) {
	publisher := &mockDLQPublisher{}
	metrics := &testMetricProvider{}
	dlq := invoker.NewDLQ(publisher, metrics)

	_ = dlq.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return fmt.Errorf("terminal: %w", invoker.ErrSendToDLQ)
	})

	if metrics.counters["eventbus_dlq_published_total"] != 1 {
		t.Errorf("expected 1 dlq_published_total, got %d", metrics.counters["eventbus_dlq_published_total"])
	}
}

// mockDLQPublisher implements invoker.DLQPublisher for testing
type mockDLQPublisher struct {
	published bool
	evt       event.Event
	cause     error
	err       error
}

func (m *mockDLQPublisher) Publish(ctx context.Context, evt event.Event, cause error) error {
	m.published = true
	m.evt = evt
	m.cause = cause
	return m.err
}
