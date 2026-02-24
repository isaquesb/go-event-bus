package nats

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	natsgo "github.com/nats-io/nats.go"
)

// --- minimal fakes ---

type fakeEvent struct{ name string }

func (e *fakeEvent) Name() string { return e.name }

type fakeInvoker struct{}

func (f *fakeInvoker) Invoke(ctx context.Context, _ event.Event, _ string, fn func(context.Context) error) error {
	return fn(ctx)
}

// mockNatsConn records calls to Subscribe.
type mockNatsConn struct {
	mu          sync.Mutex
	subscribed  []string // subjects passed to Subscribe
	subscribeErr error
}

func (m *mockNatsConn) Subscribe(subj string, _ natsgo.MsgHandler) (*natsgo.Subscription, error) {
	m.mu.Lock()
	m.subscribed = append(m.subscribed, subj)
	m.mu.Unlock()
	return nil, m.subscribeErr
}

func (m *mockNatsConn) Publish(_ string, _ []byte) error           { return nil }
func (m *mockNatsConn) Request(_ string, _ []byte, _ time.Duration) (*natsgo.Msg, error) {
	return nil, nil
}
func (m *mockNatsConn) Close() {}

// subscriberWithOpts also implements event.SubscribeOptionsProvider.
type subscriberWithOpts struct {
	name   string
	events map[string]event.HandleFn
	opts   []event.SubscribeOption
}

func (s *subscriberWithOpts) Name() string                      { return s.name }
func (s *subscriberWithOpts) Events() map[string]event.HandleFn { return s.events }
func (s *subscriberWithOpts) SubscribeOptions() []event.SubscribeOption { return s.opts }

// --- helpers ---

func noopHandler(_ context.Context, _ event.Event) error { return nil }

func newTestBus(nc natsConn) *Bus {
	return &Bus{
		nc:      nc,
		timeout: 5 * time.Second,
	}
}

// --- tests ---

func TestNatsBus_RegisterSubscribers_CallsSubscribeForEachEvent(t *testing.T) {
	mock := &mockNatsConn{}
	bus := newTestBus(mock)
	ctx := context.Background()

	sub := &subscriberWithOpts{
		name: "svc",
		events: map[string]event.HandleFn{
			"user.created": noopHandler,
			"user.deleted": noopHandler,
		},
	}

	if err := bus.RegisterSubscribers(ctx, sub); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock.mu.Lock()
	got := len(mock.subscribed)
	mock.mu.Unlock()

	if got != 2 {
		t.Errorf("expected 2 Subscribe calls, got %d", got)
	}
}

func TestNatsBus_RegisterSubscribers_MultipleSubscribers(t *testing.T) {
	mock := &mockNatsConn{}
	bus := newTestBus(mock)
	ctx := context.Background()

	err := bus.RegisterSubscribers(ctx,
		&subscriberWithOpts{name: "a", events: map[string]event.HandleFn{"evt.a": noopHandler}},
		&subscriberWithOpts{name: "b", events: map[string]event.HandleFn{"evt.b": noopHandler}},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock.mu.Lock()
	got := len(mock.subscribed)
	mock.mu.Unlock()

	if got != 2 {
		t.Errorf("expected 2 Subscribe calls (one per subscriber), got %d", got)
	}
}

func TestNatsBus_RegisterSubscribers_ErrorPropagation(t *testing.T) {
	wantErr := errors.New("subscribe failed")
	mock := &mockNatsConn{subscribeErr: wantErr}
	bus := newTestBus(mock)

	err := bus.RegisterSubscribers(context.Background(),
		&subscriberWithOpts{name: "svc", events: map[string]event.HandleFn{"evt": noopHandler}},
	)

	if !errors.Is(err, wantErr) {
		t.Errorf("expected %v, got %v", wantErr, err)
	}
}

func TestNatsBus_RegisterSubscribers_NoSubscribers(t *testing.T) {
	mock := &mockNatsConn{}
	bus := newTestBus(mock)

	if err := bus.RegisterSubscribers(context.Background()); err != nil {
		t.Errorf("expected nil for empty subscribers, got %v", err)
	}
}

func TestNatsBus_RegisterSubscribers_WithSubscribeOptionsProvider(t *testing.T) {
	mock := &mockNatsConn{}
	bus := newTestBus(mock)
	ctx := context.Background()

	// Subscriber supplies its own handler-name option via SubscribeOptionsProvider
	sub := &subscriberWithOpts{
		name: "base-name",
		events: map[string]event.HandleFn{
			"item.updated": noopHandler,
		},
		opts: []event.SubscribeOption{event.WithHandlerName("custom-name")},
	}

	if err := bus.RegisterSubscribers(ctx, sub); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock.mu.Lock()
	subjects := mock.subscribed
	mock.mu.Unlock()

	if len(subjects) != 1 || subjects[0] != "item.updated" {
		t.Errorf("expected subscription on 'item.updated', got %v", subjects)
	}
}
