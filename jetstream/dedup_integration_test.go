//go:build integration

package jetstream

import (
	"context"
	"sync"
	"testing"
	"time"

	event "github.com/isaquesb/go-event-bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ddOrder carries its own id: Event.Id() is the broker dedup key.
type ddOrder struct {
	ID string `json:"id"`
}

func (e *ddOrder) Name() string { return "dd.order" }
func (e *ddOrder) Id() string   { return e.ID }

// ddAnon has no Id method, so every Emit gets a fresh generated id and the
// stream never sees a repeated Nats-Msg-Id.
type ddAnon struct {
	Note string `json:"note"`
}

func (e *ddAnon) Name() string { return "dd.anon" }

// TestIntegration_EmitDedupByEventID proves on a real nats-server -js that the
// Nats-Msg-Id Emit sets from the resolved event id makes the stream keep one
// copy of a re-emitted id inside its duplicate window, that a raw re-publish
// with that id is acked as Duplicate, and that events without an id are not
// deduplicated. The bus is built without an Invoker (CORE-05) and still
// consumes the messages.
func TestIntegration_EmitDedupByEventID(t *testing.T) {
	_, js := connect(t)
	base := uniqueName(t, "dd")
	// The server default Duplicates window (2 minutes) applies.
	stream := newStream(t, js, base+".>")

	reg := newTestRegistry()
	reg.Register("dd.order", func() event.Event { return &ddOrder{} }, 1)
	reg.Register("dd.anon", func() event.Event { return &ddAnon{} }, 1)

	bus := NewBus(js, reg, BusOptions{
		CircuitOpenDelay: time.Second,
		RateLimitDelay:   time.Second,
	})
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Handler calls are counted per message id (the envelope Metadata.ID).
	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	handler := func(ctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		mu.Lock()
		seen[md.ID]++
		mu.Unlock()
		return nil
	}

	consumer := uniqueName(t, "C")
	orderSubject := base + ".order"
	if err := bus.Subscribe(ctx, orderSubject, handler,
		event.WithStream(stream),
		event.WithConsumer(consumer),
		event.WithMaxDeliver(5),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The same Event.Id() twice, then the same WithID twice: one message each.
	for range 2 {
		if err := bus.Emit(ctx, &ddOrder{ID: "o-1"}, event.WithSubject(orderSubject)); err != nil {
			t.Fatalf("Emit o-1: %v", err)
		}
	}
	for range 2 {
		if err := bus.Emit(ctx, &ddOrder{ID: "x"}, event.WithSubject(orderSubject), event.WithID("w-1")); err != nil {
			t.Fatalf("Emit w-1: %v", err)
		}
	}
	if got := itStreamMsgs(t, js, stream); got != 2 {
		t.Fatalf("stream holds %d messages after the re-emits, want 2", got)
	}

	// A raw re-publish with the same Nats-Msg-Id is acked as Duplicate and not
	// stored.
	raw := &nats.Msg{Subject: orderSubject, Data: []byte("raw"), Header: nats.Header{}}
	raw.Header.Set(jetstream.MsgIDHeader, "o-1")
	ack, err := js.PublishMsg(ctx, raw)
	if err != nil {
		t.Fatalf("raw PublishMsg: %v", err)
	}
	if !ack.Duplicate {
		t.Errorf("raw re-publish of o-1: PubAck.Duplicate = false, want true")
	}
	if got := itStreamMsgs(t, js, stream); got != 2 {
		t.Fatalf("stream holds %d messages after the raw duplicate, want 2", got)
	}

	// Negative control: without an id every Emit is a new message.
	for range 2 {
		if err := bus.Emit(ctx, &ddAnon{Note: "a"}, event.WithSubject(base+".anon")); err != nil {
			t.Fatalf("Emit anon: %v", err)
		}
	}
	if got := itStreamMsgs(t, js, stream); got != 4 {
		t.Fatalf("stream holds %d messages after two id-less emits, want 4", got)
	}

	// The nil-Invoker bus consumed exactly the two deduplicated messages.
	waitConsumerInfo(t, js, stream, consumer, func(ci *jetstream.ConsumerInfo) bool {
		return ci.AckFloor.Consumer == 2 && ci.NumAckPending == 0
	})
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen["o-1"] != 1 || seen["w-1"] != 1 {
		t.Errorf("handler calls by id = %v, want o-1 and w-1 once each", seen)
	}
}
