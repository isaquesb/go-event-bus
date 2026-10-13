package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// --- capturing jsBackend ---

type mdPublished struct {
	subject string
	data    []byte
	opts    int
}

// mdJS is a jsBackend that records every Publish.
type mdJS struct {
	mu        sync.Mutex
	published []mdPublished
}

func (m *mdJS) Publish(_ context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	m.mu.Lock()
	m.published = append(m.published, mdPublished{subject: subject, data: payload, opts: len(opts)})
	m.mu.Unlock()
	return &jetstream.PubAck{}, nil
}

func (m *mdJS) CreateOrUpdateConsumer(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return &mockConsumer{}, nil
}

func (m *mdJS) payload(t *testing.T, i int) mdPublished {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if i >= len(m.published) {
		t.Fatalf("published %d messages, want at least %d", len(m.published), i+1)
	}
	return m.published[i]
}

func (m *mdJS) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.published)
}

// --- fake jetstream.Msg ---

// mdMsg is a jetstream.Msg over a data slice that counts ack dispositions.
type mdMsg struct {
	data []byte

	mu            sync.Mutex
	acks          int
	naks          int
	nakWithDelays int
	terms         int
}

func (m *mdMsg) Metadata() (*jetstream.MsgMetadata, error) { return &jetstream.MsgMetadata{}, nil }
func (m *mdMsg) Data() []byte                              { return m.data }
func (m *mdMsg) Headers() natsgo.Header                    { return nil }
func (m *mdMsg) Subject() string                           { return "" }
func (m *mdMsg) Reply() string                             { return "" }
func (m *mdMsg) DoubleAck(context.Context) error           { return nil }
func (m *mdMsg) InProgress() error                         { return nil }

func (m *mdMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks++
	return nil
}

func (m *mdMsg) Nak() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.naks++
	return nil
}

func (m *mdMsg) NakWithDelay(time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakWithDelays++
	return nil
}

func (m *mdMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terms++
	return nil
}

func (m *mdMsg) TermWithReason(string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terms++
	return nil
}

func (m *mdMsg) counts() (acks, naks, terms int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acks, m.naks, m.terms
}

func newMdBus(js jsBackend, reg event.Registry, opts BusOptions) *Bus {
	if opts.Invoker == nil {
		opts.Invoker = invoker.NewChain()
	}
	opts.CircuitOpenDelay = 1
	opts.RateLimitDelay = 1
	return NewBus(js, reg, opts)
}

// --- tests ---

func TestJetStreamBus_Metadata_EmitToHandler(t *testing.T) {
	js := &mdJS{}
	bus := newMdBus(js, newMdRegistry("order.created"), BusOptions{})

	err := bus.Emit(context.Background(), &mdEvent{name: "order.created", Payload: "p"},
		event.WithID("id-1"),
		event.WithCorrelationID("corr-1"),
		event.WithHeader("tenant", "acme"),
	)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	pub := js.payload(t, 0)
	if pub.subject != "order.created" {
		t.Errorf("subject = %q, want order.created", pub.subject)
	}
	if pub.opts != 0 {
		t.Errorf("Publish got %d options, want 0 (publish dedup is Phase 9)", pub.opts)
	}

	var (
		got  event.Metadata
		ok   bool
		seen int
	)
	fn := func(ctx context.Context, _ event.Event) error {
		got, ok = event.MetadataFromContext(ctx)
		seen++
		return nil
	}
	msg := &mdMsg{data: pub.data}
	bus.msgHandler(context.Background(), "h", fn)(msg)

	if seen != 1 || !ok {
		t.Fatalf("handler runs=%d, metadata ok=%v; want 1 and true", seen, ok)
	}
	if got.ID != "id-1" {
		t.Errorf("ID = %q, want id-1", got.ID)
	}
	if got.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want corr-1", got.CorrelationID)
	}
	if got.Headers["tenant"] != "acme" {
		t.Errorf("Headers = %v, want tenant=acme", got.Headers)
	}
	if acks, naks, terms := msg.counts(); acks != 1 || naks != 0 || terms != 0 {
		t.Errorf("acks=%d naks=%d terms=%d, want 1/0/0", acks, naks, terms)
	}
}

func TestJetStreamBus_Emit_RegistryWithoutEnvelopeEncoder(t *testing.T) {
	js := &mdJS{}
	bus := newMdBus(js, &fakeRegistry{}, BusOptions{})

	if err := bus.Emit(context.Background(), &fakeEvent{name: "plain.event"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	pub := js.payload(t, 0)
	if pub.subject != "plain.event" {
		t.Errorf("subject = %q, want plain.event", pub.subject)
	}
	if string(pub.data) != "{}" {
		t.Errorf("data = %q, want {} (Registry.Encode fallback)", pub.data)
	}
}
