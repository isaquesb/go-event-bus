package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
)

// --- in-test envelope registry (no json module import) ---

// mdEvent is the event used by the metadata tests. Id() reports EventID, so a
// decoded event still reports the id it was emitted with.
type mdEvent struct {
	name    string
	EventID string `json:"event_id"`
	Payload string `json:"payload"`
}

func (e *mdEvent) Name() string { return e.name }
func (e *mdEvent) Id() string   { return e.EventID }

// mdRegistry implements event.Registry and event.EnvelopeEncoder on
// event.Envelope.
type mdRegistry struct {
	mu        sync.Mutex
	factories map[string]event.Factory
	encoded   []event.Metadata
}

func newMdRegistry(names ...string) *mdRegistry {
	r := &mdRegistry{factories: map[string]event.Factory{}}
	for _, n := range names {
		name := n
		r.Register(name, func() event.Event { return &mdEvent{name: name} }, 1)
	}
	return r
}

func (r *mdRegistry) Register(name string, factory event.Factory, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

func (r *mdRegistry) Encode(ctx context.Context, evt event.Event) ([]byte, error) {
	return r.EncodeWith(ctx, evt, event.ResolveOutbound(ctx, evt, event.EmitOptions{}))
}

func (r *mdRegistry) EncodeWith(_ context.Context, evt event.Event, md event.Metadata) ([]byte, error) {
	r.mu.Lock()
	r.encoded = append(r.encoded, md)
	r.mu.Unlock()

	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	return json.Marshal(event.Envelope{
		Name:          evt.Name(),
		Version:       1,
		Timestamp:     md.Timestamp,
		ID:            md.ID,
		CorrelationID: md.CorrelationID,
		CausationID:   md.CausationID,
		Headers:       md.Headers,
		Payload:       payload,
	})
}

func (r *mdRegistry) Decode(ctx context.Context, data []byte) (context.Context, event.Event, error) {
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ctx, nil, err
	}
	r.mu.Lock()
	factory, ok := r.factories[env.Name]
	r.mu.Unlock()
	if !ok {
		return ctx, nil, fmt.Errorf("mdRegistry: unknown event %q", env.Name)
	}
	evt := factory()
	if err := json.Unmarshal(env.Payload, evt); err != nil {
		return ctx, nil, err
	}
	md := event.Metadata{
		ID:            env.ID,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Name:          env.Name,
		Version:       env.Version,
		Timestamp:     env.Timestamp,
		Headers:       maps.Clone(env.Headers),
	}
	return event.WithMetadata(ctx, md), evt, nil
}

func (r *mdRegistry) lastEncoded(t *testing.T) event.Metadata {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.encoded) == 0 {
		t.Fatal("mdRegistry: nothing was encoded")
	}
	return r.encoded[len(r.encoded)-1]
}

func (r *mdRegistry) resetEncoded() {
	r.mu.Lock()
	r.encoded = nil
	r.mu.Unlock()
}

// --- loopback conn ---

type mdPublished struct {
	subject string
	data    []byte
}

type mdRequested struct {
	subject string
	data    []byte
}

// mdConn is a loopback natsConn: Publish calls the handler subscribed to the
// subject synchronously.
type mdConn struct {
	mu        sync.Mutex
	handlers  map[string]natsgo.MsgHandler
	published []mdPublished
	requests  []mdRequested
	reply     []byte
}

func newMdConn() *mdConn { return &mdConn{handlers: map[string]natsgo.MsgHandler{}} }

func (c *mdConn) Subscribe(subj string, cb natsgo.MsgHandler) (*natsgo.Subscription, error) {
	c.mu.Lock()
	c.handlers[subj] = cb
	c.mu.Unlock()
	return nil, nil
}

func (c *mdConn) Publish(subj string, data []byte) error {
	c.mu.Lock()
	c.published = append(c.published, mdPublished{subject: subj, data: data})
	cb := c.handlers[subj]
	c.mu.Unlock()
	if cb != nil {
		cb(&natsgo.Msg{Subject: subj, Data: data})
	}
	return nil
}

func (c *mdConn) Request(subj string, data []byte, _ time.Duration) (*natsgo.Msg, error) {
	c.mu.Lock()
	c.requests = append(c.requests, mdRequested{subject: subj, data: data})
	reply := c.reply
	c.mu.Unlock()
	return &natsgo.Msg{Subject: subj, Data: reply}, nil
}

func (c *mdConn) Close() {}

func (c *mdConn) handler(t *testing.T, subj string) natsgo.MsgHandler {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	cb := c.handlers[subj]
	if cb == nil {
		t.Fatalf("no handler subscribed to %q", subj)
	}
	return cb
}

func newMdBus(conn natsConn, reg event.Registry) *Bus {
	return &Bus{nc: conn, reg: reg, timeout: 5 * time.Second}
}

// --- tests ---

func TestNatsBus_Metadata_EmitToHandler(t *testing.T) {
	conn := newMdConn()
	bus := newMdBus(conn, newMdRegistry("order.created"))

	var (
		got  event.Metadata
		ok   bool
		seen int
	)
	err := bus.Subscribe(context.Background(), "order.created", func(ctx context.Context, _ event.Event) error {
		got, ok = event.MetadataFromContext(ctx)
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	err = bus.Emit(context.Background(), &mdEvent{name: "order.created", Payload: "p"},
		event.WithID("id-1"),
		event.WithCorrelationID("corr-1"),
		event.WithHeader("tenant", "acme"),
	)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if seen != 1 || !ok {
		t.Fatalf("handler runs=%d, metadata ok=%v; want 1 and true", seen, ok)
	}
	if got.ID != "id-1" {
		t.Errorf("ID = %q, want id-1", got.ID)
	}
	if got.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want corr-1", got.CorrelationID)
	}
	if got.CausationID != "" {
		t.Errorf("CausationID = %q, want empty", got.CausationID)
	}
	if got.Headers["tenant"] != "acme" {
		t.Errorf("Headers = %v, want tenant=acme", got.Headers)
	}
	if got.Name != "order.created" {
		t.Errorf("Name = %q, want order.created", got.Name)
	}
}

func TestNatsBus_Metadata_NestedEmitLineage(t *testing.T) {
	conn := newMdConn()
	bus := newMdBus(conn, newMdRegistry("order.created", "invoice.created"))

	var order, invoice event.Metadata
	var invoiceOK bool

	err := bus.Subscribe(context.Background(), "order.created", func(ctx context.Context, _ event.Event) error {
		order, _ = event.MetadataFromContext(ctx)
		return bus.Emit(ctx, &mdEvent{name: "invoice.created"})
	})
	if err != nil {
		t.Fatalf("Subscribe order: %v", err)
	}
	err = bus.Subscribe(context.Background(), "invoice.created", func(ctx context.Context, _ event.Event) error {
		invoice, invoiceOK = event.MetadataFromContext(ctx)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe invoice: %v", err)
	}

	if err := bus.Emit(context.Background(), &mdEvent{name: "order.created"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if order.ID == "" {
		t.Fatal("order handler saw no id")
	}
	if !invoiceOK {
		t.Fatal("invoice handler did not run with Metadata")
	}
	if invoice.CausationID != order.ID {
		t.Errorf("invoice CausationID = %q, want order ID %q", invoice.CausationID, order.ID)
	}
	if invoice.CorrelationID != order.ID {
		t.Errorf("invoice CorrelationID = %q, want order ID %q (root correlation)", invoice.CorrelationID, order.ID)
	}
	if invoice.ID == "" || invoice.ID == order.ID {
		t.Errorf("invoice ID = %q, want a new id different from %q", invoice.ID, order.ID)
	}
}

func TestNatsBus_Emit_RegistryWithoutEnvelopeEncoder(t *testing.T) {
	conn := newMdConn()
	bus := newMdBus(conn, &contextCapturingRegistry{})

	if err := bus.Emit(context.Background(), &fakeEvent{name: "plain.event"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(conn.published))
	}
	if conn.published[0].subject != "plain.event" {
		t.Errorf("subject = %q, want plain.event", conn.published[0].subject)
	}
	if string(conn.published[0].data) != "{}" {
		t.Errorf("data = %q, want {} (Registry.Encode fallback)", conn.published[0].data)
	}
}
