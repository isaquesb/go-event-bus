// Example: JetStream Bus for Distributed Events
//
// This example demonstrates how to use JetStream for durable,
// distributed event processing with replay support.
package examples

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/isaquesb/go-event-bus/jetstream"
	eventjson "github.com/isaquesb/go-event-bus/json"

	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Event Definitions
// =============================================================================

// ChatMessage represents a chat message event
type ChatMessage struct {
	MessageID string    `json:"message_id"`
	RoomID    string    `json:"room_id"`
	UserID    string    `json:"user_id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *ChatMessage) Name() string { return "chat.message" }
func (e *ChatMessage) Version() int { return 1 }

// Id returns unique identifier for idempotency
func (e *ChatMessage) Id() string { return e.MessageID }

// =============================================================================
// Stream Configuration
// =============================================================================

const (
	StreamName     = "CHAT_EVENTS"
	ConsumerName   = "chat-processor"
	SubjectPattern = "chat.>"
)

// =============================================================================
// Example Usage
// =============================================================================

func ExampleJetStreamBus() {
	ctx := context.Background()

	// Connect to NATS
	nc, err := nats.Connect(
		nats.DefaultURL,
		nats.Name("chat-service"),
		nats.ReconnectWait(time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		slog.Error("failed to connect to NATS", "error", err)
		return
	}
	defer nc.Close()

	// Get JetStream context
	js, err := natsjs.New(nc)
	if err != nil {
		slog.Error("failed to get JetStream", "error", err)
		return
	}

	// Create or update stream
	_, err = js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
		Name:        StreamName,
		Description: "Chat events stream",
		Subjects:    []string{SubjectPattern},
		Retention:   natsjs.LimitsPolicy,
		MaxAge:      7 * 24 * time.Hour, // Keep 7 days
		MaxBytes:    1 << 30,            // 1 GB
		Replicas:    1,                  // Use 3 for production
		Storage:     natsjs.FileStorage,
	})
	if err != nil {
		slog.Error("failed to create stream", "error", err)
		return
	}

	// Setup registry
	// Note: json.Registry.Register signature is (name, factory, version)
	registry := eventjson.NewRegistry()
	registry.Register("chat.message", func() event.Event { return &ChatMessage{} }, 1)

	// Setup invoker chain
	chain := invoker.NewChain(
		invoker.NewMetrics(nil),
		invoker.NewIdempotency(
			invoker.NewMemoryIdempotencyStore(),
			invoker.IdempotencyConfig{ProcessingTTL: 5 * time.Minute},
			nil,
		),
		invoker.NewRetry(
			invoker.RetryPolicy{
				MaxAttempts: 3,
				BaseDelay:   100 * time.Millisecond,
				MaxDelay:    5 * time.Second,
			},
			nil,
		),
	)

	// Create JetStream bus
	bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
		Invoker: chain,
		OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
			slog.Error("handler failed",
				"event", evt.Name(),
				"handler", handler,
				"error", err,
			)
		},
	})

	// Subscribe to events using base event options
	err = bus.Subscribe(
		ctx,
		"chat.message.*", // Filter by room
		func(ctx context.Context, evt event.Event) error {
			msg := evt.(*ChatMessage)
			slog.Info("persisting message",
				"message_id", msg.MessageID,
				"room_id", msg.RoomID,
				"user_id", msg.UserID,
			)
			// ... persist to database
			return nil
		},
		event.WithStream(StreamName),
		event.WithHandlerName("persist-message"),
		event.WithConsumer(ConsumerName),
	)
	if err != nil {
		slog.Error("failed to subscribe", "error", err)
		return
	}

	// Publish events
	for i := 0; i < 5; i++ {
		msg := &ChatMessage{
			MessageID: fmt.Sprintf("msg-%d", i),
			RoomID:    "room-general",
			UserID:    "user-123",
			Content:   fmt.Sprintf("Hello world #%d", i),
			Timestamp: time.Now(),
		}

		subject := fmt.Sprintf("chat.message.%s", msg.RoomID)
		if err := bus.Emit(ctx, msg, event.WithSubject(subject)); err != nil {
			slog.Error("failed to publish", "error", err)
		}
	}

	slog.Info("published 5 messages to JetStream")
}

// =============================================================================
// Replay Example
// =============================================================================

func ExampleJetStreamReplay() {
	ctx := context.Background()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return
	}
	defer nc.Close()

	js, err := natsjs.New(nc)
	if err != nil {
		return
	}

	// Create replayer
	replayer := jetstream.NewReplayer(js)

	// Replay failed events from DLQ to original topic
	err = replayer.Replay(ctx, jetstream.ReplayOptions{
		FromStream: "CHAT_EVENTS_DLQ", // Source: DLQ stream
		ToSubject:  "chat.message",    // Destination: retry processing
		Limit:      100,               // Replay up to 100 events
	})
	if err != nil {
		slog.Error("replay failed", "error", err)
		return
	}

	slog.Info("replay completed")
}

// =============================================================================
// DLQ Publisher Example
// =============================================================================

func ExampleJetStreamDLQ() {
	ctx := context.Background()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	js, _ := natsjs.New(nc)

	// Create DLQ stream first
	_, _ = js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
		Name:     "CHAT_EVENTS_DLQ",
		Subjects: []string{"dlq.>"},
		MaxAge:   30 * 24 * time.Hour, // Keep 30 days for investigation
	})

	// Create DLQ publisher
	dlq := jetstream.NewDQL(js, nil, "")

	// In your invoker chain, use:
	chain := invoker.NewChain(
		invoker.NewRetry(
			invoker.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second},
			nil,
		),
		invoker.NewDLQ(dlq, nil), // Terminal errors go to DLQ
	)

	_ = chain // Use in your bus
}
