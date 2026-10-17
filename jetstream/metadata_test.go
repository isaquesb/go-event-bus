package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	header  natsgo.Header
	opts    int
}

// mdJS is a jsBackend that records every PublishMsg.
type mdJS struct {
	mu        sync.Mutex
	published []mdPublished
}

func (m *mdJS) PublishMsg(_ context.Context, msg *natsgo.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	m.mu.Lock()
	m.published = append(m.published, mdPublished{subject: msg.Subject, data: msg.Data, header: maps.Clone(msg.Header), opts: len(opts)})
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

// mdMsg is a jetstream.Msg over a data slice that counts ack dispositions and
// records the delays and reasons it was given.
type mdMsg struct {
	data []byte

	// numDelivered is reported by Metadata; subject and headers by the
	// accessors of the same name; metaErr, when set, fails Metadata.
	numDelivered uint64
	subject      string
	headers      natsgo.Header
	metaErr      error

	mu            sync.Mutex
	acks          int
	naks          int
	nakWithDelays int
	terms         int
	plainTerms    int
	nakDelays     []time.Duration
	termReasons   []string
}

func (m *mdMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{
		NumDelivered: m.numDelivered,
		Stream:       "S",
		Consumer:     "C",
		Sequence:     jetstream.SequencePair{Stream: 7},
	}, nil
}
func (m *mdMsg) Data() []byte                    { return m.data }
func (m *mdMsg) Headers() natsgo.Header          { return m.headers }
func (m *mdMsg) Subject() string                 { return m.subject }
func (m *mdMsg) Reply() string                   { return "" }
func (m *mdMsg) DoubleAck(context.Context) error { return nil }
func (m *mdMsg) InProgress() error               { return nil }

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

func (m *mdMsg) NakWithDelay(d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakWithDelays++
	m.nakDelays = append(m.nakDelays, d)
	return nil
}

// Term is the reason-less term; the bus must never call it.
func (m *mdMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terms++
	m.plainTerms++
	return nil
}

func (m *mdMsg) TermWithReason(reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terms++
	m.termReasons = append(m.termReasons, reason)
	return nil
}

func (m *mdMsg) counts() (acks, naks, terms int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acks, m.naks, m.terms
}

// mdDispositions is a locked snapshot of everything mdMsg recorded.
type mdDispositions struct {
	acks, naks, plainTerms int
	nakDelays              []time.Duration
	termReasons            []string
}

func (m *mdMsg) snapshot() mdDispositions {
	m.mu.Lock()
	defer m.mu.Unlock()
	return mdDispositions{
		acks:        m.acks,
		naks:        m.naks,
		plainTerms:  m.plainTerms,
		nakDelays:   append([]time.Duration(nil), m.nakDelays...),
		termReasons: append([]string(nil), m.termReasons...),
	}
}

func newMdBus(js jsBackend, reg event.Registry, opts BusOptions) *Bus {
	if opts.Invoker == nil {
		opts.Invoker = invoker.NewChain()
	}
	if opts.Observer == nil {
		// Keep the default SlogObserver out of the test output.
		opts.Observer = event.NopObserver{}
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
		t.Errorf("PublishMsg got %d options, want 0 (the Nats-Msg-Id travels as a header)", pub.opts)
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
	bus.msgHandler(context.Background(), "h", fn, deliveryPolicy{})(msg)

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

func TestJetStreamBus_FailureReceivesMessageContext(t *testing.T) {
	rec := &joRecorder{}
	js := &mdJS{}
	bus := newMdBus(js, newMdRegistry("order.created"), BusOptions{Observer: rec})

	if err := bus.Emit(context.Background(), &mdEvent{name: "order.created"}, event.WithID("id-err")); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// The final delivery: only a terminal outcome is reported (D-16).
	msg := &mdMsg{data: js.payload(t, 0).data, numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		return errors.New("boom")
	}, deliveryPolicy{maxDeliver: 1})(msg)

	if rec.failureCount() != 1 {
		t.Fatalf("Failure calls = %d, want 1", rec.failureCount())
	}
	md, ok := event.MetadataFromContext(rec.failureCtx(0))
	if !ok || md.ID != "id-err" {
		t.Errorf("Failure ctx metadata = %+v ok=%v, want ID id-err", md, ok)
	}
	if f := rec.failures()[0]; f.Metadata.ID != "id-err" {
		t.Errorf("FailureContext.Metadata.ID = %q, want id-err", f.Metadata.ID)
	}
	got := msg.snapshot()
	if got.naks != 0 || got.plainTerms != 0 || got.acks != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if want := []string{"dropped: delivery_exhausted"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
}

func TestJetStreamBus_DecodeFailureDroppedWithoutDLQ(t *testing.T) {
	var fnCalled bool
	rec := &joRecorder{}
	bus := newMdBus(&mdJS{}, newMdRegistry("order.created"), BusOptions{Observer: rec})

	msg := &mdMsg{data: []byte("not json"), numDelivered: 1}
	bus.msgHandler(context.Background(), "h", func(context.Context, event.Event) error {
		fnCalled = true
		return nil
	}, deliveryPolicy{maxDeliver: 3})(msg)

	got := msg.snapshot()
	if want := []string{"dropped: decode_failed"}; !slices.Equal(got.termReasons, want) {
		t.Errorf("termReasons = %q, want %q", got.termReasons, want)
	}
	if got.acks != 0 || got.naks != 0 || got.plainTerms != 0 || len(got.nakDelays) != 0 {
		t.Errorf("snapshot = %+v, want only a TermWithReason", got)
	}
	if rec.failureCount() != 1 || rec.lastEvent() != nil {
		t.Errorf("Failure calls=%d evt=%v, want one call with a nil event (the silent Term is gone)", rec.failureCount(), rec.lastEvent())
	}
	if f := rec.failures()[0]; f.Outcome != event.OutcomeDecodeFailed {
		t.Errorf("Failure outcome = %q, want %q", f.Outcome, event.OutcomeDecodeFailed)
	}
	if fnCalled {
		t.Error("handler was called for an undecodable message")
	}
}

func TestJetStreamBus_Metadata_NestedEmitLineage(t *testing.T) {
	js := &mdJS{}
	reg := newMdRegistry("order.created", "invoice.created")
	bus := newMdBus(js, reg, BusOptions{})

	if err := bus.Emit(context.Background(), &mdEvent{name: "order.created"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	msg := &mdMsg{data: js.payload(t, 0).data}
	bus.msgHandler(context.Background(), "h", func(ctx context.Context, _ event.Event) error {
		return bus.Emit(ctx, &mdEvent{name: "invoice.created"})
	}, deliveryPolicy{})(msg)
	if acks, _, _ := msg.counts(); acks != 1 {
		t.Errorf("acks = %d, want 1", acks)
	}

	if js.count() != 2 {
		t.Fatalf("published %d messages, want 2", js.count())
	}
	parent := mdDecodeMetadata(t, reg, js.payload(t, 0).data)
	child := mdDecodeMetadata(t, reg, js.payload(t, 1).data)

	if parent.ID == "" {
		t.Fatal("parent has no id")
	}
	if child.CausationID != parent.ID {
		t.Errorf("child CausationID = %q, want parent ID %q", child.CausationID, parent.ID)
	}
	if child.CorrelationID != parent.CorrelationID {
		t.Errorf("child CorrelationID = %q, want %q", child.CorrelationID, parent.CorrelationID)
	}
	if child.ID == "" || child.ID == parent.ID {
		t.Errorf("child ID = %q, want a new id different from %q", child.ID, parent.ID)
	}
}

func TestJetStreamBus_MetadataOptions(t *testing.T) {
	var (
		mu sync.Mutex
		n  int
	)
	gen := func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return fmt.Sprintf("gen-%d", n)
	}
	js := &mdJS{}
	reg := newMdRegistry("root.event", "child.event")
	bus := newMdBus(js, reg, BusOptions{
		IDGenerator:    gen,
		InheritHeaders: []string{"tenant"},
	})

	err := bus.Emit(context.Background(), &mdEvent{name: "root.event"},
		event.WithHeader("tenant", "acme"),
		event.WithHeader("secret", "s"),
	)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	msg := &mdMsg{data: js.payload(t, 0).data}
	bus.msgHandler(context.Background(), "h", func(ctx context.Context, _ event.Event) error {
		return bus.Emit(ctx, &mdEvent{name: "child.event"})
	}, deliveryPolicy{})(msg)

	root := mdDecodeMetadata(t, reg, js.payload(t, 0).data)
	child := mdDecodeMetadata(t, reg, js.payload(t, 1).data)

	if root.ID != "gen-1" {
		t.Errorf("root ID = %q, want gen-1", root.ID)
	}
	if root.Headers["secret"] != "s" {
		t.Errorf("root Headers = %v, want the explicit secret header on the root", root.Headers)
	}
	if child.ID != "gen-2" {
		t.Errorf("child ID = %q, want gen-2", child.ID)
	}
	if child.CausationID != "gen-1" {
		t.Errorf("child CausationID = %q, want gen-1", child.CausationID)
	}
	if child.Headers["tenant"] != "acme" {
		t.Errorf("child Headers = %v, want tenant=acme", child.Headers)
	}
	if _, leaked := child.Headers["secret"]; leaked {
		t.Errorf("child Headers = %v, non-allowlisted key secret must not be inherited", child.Headers)
	}
}

// mdDecodeMetadata decodes data with reg and returns the Metadata it carries.
func mdDecodeMetadata(t *testing.T, reg *mdRegistry, data []byte) event.Metadata {
	t.Helper()
	ctx, _, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	md, ok := event.MetadataFromContext(ctx)
	if !ok {
		t.Fatal("decoded ctx carries no Metadata")
	}
	return md
}
