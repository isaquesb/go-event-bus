package jetstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// --- minimal fakes ---

type fakeEvent struct{ name string }

func (e *fakeEvent) Name() string { return e.name }

type fakeRegistry struct{}

func (r *fakeRegistry) Register(_ string, _ event.Factory, _ int) {}
func (r *fakeRegistry) Encode(_ context.Context, _ event.Event) ([]byte, error) {
	return []byte("{}"), nil
}
func (r *fakeRegistry) Decode(_ context.Context, _ []byte) (context.Context, event.Event, error) {
	return context.Background(), &fakeEvent{name: "test"}, nil
}

// mockJS implements jsBackend and records CreateOrUpdateConsumer calls.
type mockJS struct {
	mu              sync.Mutex
	consumers       []string // stream names passed to CreateOrUpdateConsumer
	createErr       error
	consumerFactory func() jetstream.Consumer
}

func (m *mockJS) PublishMsg(_ context.Context, _ *natsgo.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return &jetstream.PubAck{}, nil
}

func (m *mockJS) CreateOrUpdateConsumer(
	_ context.Context,
	stream string,
	_ jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	m.mu.Lock()
	m.consumers = append(m.consumers, stream)
	m.mu.Unlock()

	if m.createErr != nil {
		return nil, m.createErr
	}
	if m.consumerFactory != nil {
		return m.consumerFactory(), nil
	}
	return &mockConsumer{}, nil
}

// mockConsumer implements jetstream.Consumer – only Consume is used by the bus.
type mockConsumer struct{}

func (c *mockConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	panic("not implemented")
}
func (c *mockConsumer) FetchBytes(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	panic("not implemented")
}
func (c *mockConsumer) FetchNoWait(int) (jetstream.MessageBatch, error) { panic("not implemented") }
func (c *mockConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	panic("not implemented")
}
func (c *mockConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) { panic("not implemented") }
func (c *mockConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	return &jetstream.ConsumerInfo{}, nil
}
func (c *mockConsumer) CachedInfo() *jetstream.ConsumerInfo { return nil }
func (c *mockConsumer) Consume(jetstream.MessageHandler, ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	return &mockConsumeCtx{}, nil
}

// mockConsumeCtx implements jetstream.ConsumeContext.
type mockConsumeCtx struct{}

func (c *mockConsumeCtx) Stop()                   {}
func (c *mockConsumeCtx) Drain()                  {}
func (c *mockConsumeCtx) Closed() <-chan struct{} { return make(chan struct{}) }

// subscriberWithOpts implements event.SubscribeOptionsProvider.
type subscriberWithOpts struct {
	name   string
	events map[string]event.HandleFn
	opts   []event.SubscribeOption
}

func (s *subscriberWithOpts) Name() string                              { return s.name }
func (s *subscriberWithOpts) Events() map[string]event.HandleFn         { return s.events }
func (s *subscriberWithOpts) SubscribeOptions() []event.SubscribeOption { return s.opts }

func noopHandler(_ context.Context, _ event.Event) error { return nil }

func newTestBus(js jsBackend) *Bus {
	return NewBus(js, &fakeRegistry{}, BusOptions{
		Invoker:          invoker.NewChain(),
		CircuitOpenDelay: 1,
		RateLimitDelay:   1,
	})
}

// --- tests ---

func TestJetStreamBus_RegisterSubscribers_ErrStreamRequired(t *testing.T) {
	bus := newTestBus(&mockJS{})
	ctx := context.Background()

	// Subscriber does NOT implement SubscribeOptionsProvider — no stream provided.
	sub := &struct {
		name   string
		events map[string]event.HandleFn
	}{name: "svc", events: map[string]event.HandleFn{"order.placed": noopHandler}}

	err := bus.RegisterSubscribers(ctx, &plainSubscriber{
		name:   "svc",
		events: map[string]event.HandleFn{"order.placed": noopHandler},
	})

	if !errors.Is(err, ErrStreamRequired) {
		t.Errorf("expected ErrStreamRequired, got %v", err)
	}

	_ = sub
}

func TestJetStreamBus_RegisterSubscribers_WithStream(t *testing.T) {
	mock := &mockJS{}
	bus := newTestBus(mock)
	ctx := context.Background()

	err := bus.RegisterSubscribers(ctx, &subscriberWithOpts{
		name: "order-svc",
		events: map[string]event.HandleFn{
			"order.placed":  noopHandler,
			"order.shipped": noopHandler,
		},
		opts: []event.SubscribeOption{event.WithStream("ORDERS")},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock.mu.Lock()
	got := len(mock.consumers)
	mock.mu.Unlock()

	if got != 2 {
		t.Errorf("expected 2 consumers created, got %d", got)
	}
}

func TestJetStreamBus_RegisterSubscribers_MultipleSubscribers(t *testing.T) {
	mock := &mockJS{}
	bus := newTestBus(mock)
	ctx := context.Background()

	err := bus.RegisterSubscribers(ctx,
		&subscriberWithOpts{
			name:   "svc-a",
			events: map[string]event.HandleFn{"evt.a": noopHandler},
			opts:   []event.SubscribeOption{event.WithStream("STREAM_A")},
		},
		&subscriberWithOpts{
			name:   "svc-b",
			events: map[string]event.HandleFn{"evt.b": noopHandler},
			opts:   []event.SubscribeOption{event.WithStream("STREAM_B")},
		},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock.mu.Lock()
	got := len(mock.consumers)
	mock.mu.Unlock()

	if got != 2 {
		t.Errorf("expected 2 consumers, got %d", got)
	}
}

func TestJetStreamBus_RegisterSubscribers_ErrorPropagation(t *testing.T) {
	wantErr := errors.New("nats unavailable")
	mock := &mockJS{createErr: wantErr}
	bus := newTestBus(mock)

	err := bus.RegisterSubscribers(context.Background(), &subscriberWithOpts{
		name:   "svc",
		events: map[string]event.HandleFn{"evt": noopHandler},
		opts:   []event.SubscribeOption{event.WithStream("S")},
	})

	if !errors.Is(err, wantErr) {
		t.Errorf("expected %v, got %v", wantErr, err)
	}
}

func TestJetStreamBus_RegisterSubscribers_NoSubscribers(t *testing.T) {
	bus := newTestBus(&mockJS{})

	if err := bus.RegisterSubscribers(context.Background()); err != nil {
		t.Errorf("expected nil for empty subscribers, got %v", err)
	}
}

// plainSubscriber only implements event.Subscriber (no SubscribeOptionsProvider).
type plainSubscriber struct {
	name   string
	events map[string]event.HandleFn
}

func (s *plainSubscriber) Name() string                      { return s.name }
func (s *plainSubscriber) Events() map[string]event.HandleFn { return s.events }

func TestJetStreamBus_Subscribe_MaxDeliverValidation_TooLow(t *testing.T) {
	bus := newTestBus(&mockJS{})
	ctx := context.Background()

	err := bus.Subscribe(ctx, "test.subject", noopHandler,
		event.WithStream("S"),
		event.WithMaxDeliver(3),
		event.WithBackOff([]time.Duration{time.Second, 2 * time.Second, 3 * time.Second}),
	)

	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig (MaxDeliver=3 <= len(BackOff)=3), got %v", err)
	}
}

func TestJetStreamBus_Subscribe_MaxDeliverValidation_Valid(t *testing.T) {
	bus := newTestBus(&mockJS{})
	ctx := context.Background()

	err := bus.Subscribe(ctx, "test.subject", noopHandler,
		event.WithStream("S"),
		event.WithMaxDeliver(5),
		event.WithBackOff([]time.Duration{time.Second, 2 * time.Second, 3 * time.Second}),
	)

	if err != nil {
		t.Errorf("expected no error (MaxDeliver=5 > len(BackOff)=3), got %v", err)
	}
}

func TestJetStreamBus_Subscribe_MaxDeliverValidation_ZeroSkips(t *testing.T) {
	bus := newTestBus(&mockJS{})
	ctx := context.Background()

	err := bus.Subscribe(ctx, "test.subject", noopHandler,
		event.WithStream("S"),
		event.WithMaxDeliver(0),
		event.WithBackOff([]time.Duration{time.Second, 2 * time.Second, 3 * time.Second}),
	)

	if err != nil {
		t.Errorf("expected no error (MaxDeliver=0 means unlimited, skip validation), got %v", err)
	}
}
