//go:build integration

package jetstream

import (
	"context"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/nats-io/nats.go/jetstream"
)

type smokeEvent struct {
	ID string `json:"id"`
}

func (e *smokeEvent) Name() string { return "smoke.created" }

// TestIntegration_Smoke_EmitSubscribeAck drives the library's own Bus against
// a real nats-server -js: Emit, Subscribe with WithStream/WithConsumer, handler
// receives the event, and the consumer reports it acked.
func TestIntegration_Smoke_EmitSubscribeAck(t *testing.T) {
	_, js := connect(t)
	stream := newStream(t, js, "smoke.>")
	consumer := uniqueName(t, "C")

	reg := newTestRegistry()
	reg.Register("smoke.created", func() event.Event { return &smokeEvent{} }, 1)

	// The explicit Invoker is kept for clarity although NewBus defaults a nil one (CORE-05).
	// Explicit delays silence the default-value warnings.
	bus := NewBus(js, reg, BusOptions{
		Invoker:          invoker.NewChain(),
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	got := make(chan string, 1)
	handler := func(_ context.Context, evt event.Event) error {
		got <- evt.(*smokeEvent).ID
		return nil
	}

	// WithConsumer is mandatory: the default consumer name equals the dotted
	// subject, which is not a valid consumer name. MaxDeliver must exceed
	// len(defaultBackOff) == 4.
	if err := bus.Subscribe(ctx, "smoke.created", handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(5),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Emit(ctx, &smokeEvent{ID: "42"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	select {
	case id := <-got:
		if id != "42" {
			t.Fatalf("handler received id %q, want 42", id)
		}
	case <-ctx.Done():
		t.Fatalf("handler did not receive the event: %v", ctx.Err())
	}

	info := waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.NumAckPending == 0 &&
			ci.NumPending == 0 &&
			ci.AckFloor.Consumer == 1 &&
			ci.Delivered.Consumer == 1
	})
	t.Logf("consumer acked: delivered=%d ack_floor=%d", info.Delivered.Consumer, info.AckFloor.Consumer)
}
