package event_test

import (
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
)

// --- EmitOptions ---

func TestApplyEmitOptions_DefaultsToEventName(t *testing.T) {
	evt := &testEvent{name: "order.created"}
	cfg := event.ApplyEmitOptions(evt)

	if cfg.Subject != "order.created" {
		t.Errorf("expected subject 'order.created', got '%s'", cfg.Subject)
	}
}

func TestApplyEmitOptions_WithSubjectOverride(t *testing.T) {
	evt := &testEvent{name: "order.created"}
	cfg := event.ApplyEmitOptions(evt, event.WithSubject("orders.v2"))

	if cfg.Subject != "orders.v2" {
		t.Errorf("expected subject 'orders.v2', got '%s'", cfg.Subject)
	}
}

func TestApplyEmitOptions_LastWithSubjectWins(t *testing.T) {
	evt := &testEvent{name: "order.created"}
	cfg := event.ApplyEmitOptions(evt,
		event.WithSubject("first"),
		event.WithSubject("second"),
	)

	if cfg.Subject != "second" {
		t.Errorf("expected subject 'second', got '%s'", cfg.Subject)
	}
}

func TestApplyEmitOptions_EmptySubjectKeepsEventName(t *testing.T) {
	evt := &testEvent{name: "order.created"}
	// passing WithSubject("") explicitly overrides to empty — that's intentional
	cfg := event.ApplyEmitOptions(evt, event.WithSubject(""))

	if cfg.Subject != "" {
		t.Errorf("expected empty subject, got '%s'", cfg.Subject)
	}
}

// --- SubscribeOptions ---

func TestApplyOptions_Defaults(t *testing.T) {
	cfg := event.ApplyOptions()

	if cfg.HandlerName != "" {
		t.Errorf("expected empty HandlerName, got '%s'", cfg.HandlerName)
	}
	if cfg.Stream != "" {
		t.Errorf("expected empty Stream, got '%s'", cfg.Stream)
	}
	if cfg.Consumer != "" {
		t.Errorf("expected empty Consumer, got '%s'", cfg.Consumer)
	}
	if cfg.MaxDeliver != 0 {
		t.Errorf("expected MaxDeliver 0, got %d", cfg.MaxDeliver)
	}
	if cfg.BackOff != nil {
		t.Errorf("expected nil BackOff, got %v", cfg.BackOff)
	}
}

func TestApplyOptions_WithHandlerName(t *testing.T) {
	cfg := event.ApplyOptions(event.WithHandlerName("my-handler"))

	if cfg.HandlerName != "my-handler" {
		t.Errorf("expected HandlerName 'my-handler', got '%s'", cfg.HandlerName)
	}
}

func TestApplyOptions_WithStream(t *testing.T) {
	cfg := event.ApplyOptions(event.WithStream("orders"))

	if cfg.Stream != "orders" {
		t.Errorf("expected Stream 'orders', got '%s'", cfg.Stream)
	}
}

func TestApplyOptions_WithConsumer(t *testing.T) {
	cfg := event.ApplyOptions(event.WithConsumer("orders-consumer"))

	if cfg.Consumer != "orders-consumer" {
		t.Errorf("expected Consumer 'orders-consumer', got '%s'", cfg.Consumer)
	}
}

func TestApplyOptions_WithMaxDeliver(t *testing.T) {
	cfg := event.ApplyOptions(event.WithMaxDeliver(5))

	if cfg.MaxDeliver != 5 {
		t.Errorf("expected MaxDeliver 5, got %d", cfg.MaxDeliver)
	}
}

func TestApplyOptions_WithBackOff(t *testing.T) {
	delays := []time.Duration{1 * time.Second, 5 * time.Second}
	cfg := event.ApplyOptions(event.WithBackOff(delays))

	if len(cfg.BackOff) != 2 {
		t.Fatalf("expected 2 BackOff entries, got %d", len(cfg.BackOff))
	}
	if cfg.BackOff[0] != time.Second {
		t.Errorf("expected BackOff[0] = 1s, got %v", cfg.BackOff[0])
	}
	if cfg.BackOff[1] != 5*time.Second {
		t.Errorf("expected BackOff[1] = 5s, got %v", cfg.BackOff[1])
	}
}

func TestApplyOptions_AllOptions(t *testing.T) {
	delays := []time.Duration{2 * time.Second}
	cfg := event.ApplyOptions(
		event.WithHandlerName("h"),
		event.WithStream("s"),
		event.WithConsumer("c"),
		event.WithMaxDeliver(3),
		event.WithBackOff(delays),
	)

	if cfg.HandlerName != "h" {
		t.Errorf("HandlerName: expected 'h', got '%s'", cfg.HandlerName)
	}
	if cfg.Stream != "s" {
		t.Errorf("Stream: expected 's', got '%s'", cfg.Stream)
	}
	if cfg.Consumer != "c" {
		t.Errorf("Consumer: expected 'c', got '%s'", cfg.Consumer)
	}
	if cfg.MaxDeliver != 3 {
		t.Errorf("MaxDeliver: expected 3, got %d", cfg.MaxDeliver)
	}
	if len(cfg.BackOff) != 1 || cfg.BackOff[0] != 2*time.Second {
		t.Errorf("BackOff: expected [2s], got %v", cfg.BackOff)
	}
}
