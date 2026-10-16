package jetstream

import (
	"context"
	"testing"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// pbEvent is the event used by the publish tests. Id() reports ID, so an event
// with an ID is deduplicated by the broker and one without is not.
type pbEvent struct {
	ID   string `json:"id"`
	Note string `json:"note,omitempty"`
}

func (e *pbEvent) Name() string { return "pb.order" }
func (e *pbEvent) Id() string   { return e.ID }

// pbRegistry is an mdRegistry that knows pbEvent.
func pbRegistry() *mdRegistry {
	reg := newMdRegistry()
	reg.Register("pb.order", func() event.Event { return &pbEvent{} }, 1)
	return reg
}

// pbMsgID is the Nats-Msg-Id header of a recorded publish.
func pbMsgID(h natsgo.Header) string { return h.Get(jetstream.MsgIDHeader) }

func TestJetStreamBus_MsgID_FromEventID(t *testing.T) {
	js := &mdJS{}
	reg := pbRegistry()
	bus := NewBus(js, reg, BusOptions{CircuitOpenDelay: 1, RateLimitDelay: 1})

	if err := bus.Emit(context.Background(), &pbEvent{ID: "o-1"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	pub := js.payload(t, 0)
	if got := pbMsgID(pub.header); got != "o-1" {
		t.Errorf("Nats-Msg-Id = %q, want o-1", got)
	}
	if env := mdDecodeMetadata(t, reg, pub.data); env.ID != pbMsgID(pub.header) {
		t.Errorf("envelope id = %q, header id = %q, want equal", env.ID, pbMsgID(pub.header))
	}
	if pub.opts != 0 {
		t.Errorf("PublishMsg got %d options, want 0", pub.opts)
	}
}

func TestJetStreamBus_NilInvokerDefaultHandlesMessage(t *testing.T) {
	js := &mdJS{}
	bus := NewBus(js, pbRegistry(), BusOptions{CircuitOpenDelay: 1, RateLimitDelay: 1})

	if err := bus.Emit(context.Background(), &pbEvent{ID: "o-2"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	runs := 0
	msg := &mdMsg{data: js.payload(t, 0).data, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		runs++
		return nil
	}, deliveryPolicy{})(msg)

	if runs != 1 {
		t.Errorf("handler ran %d times, want 1", runs)
	}
	if acks, naks, terms := msg.counts(); acks != 1 || naks != 0 || terms != 0 {
		t.Errorf("acks=%d naks=%d terms=%d, want 1/0/0", acks, naks, terms)
	}
}
