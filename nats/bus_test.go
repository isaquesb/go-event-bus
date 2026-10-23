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

// mockNatsConn records calls to Subscribe and optionally captures the callback.
type mockNatsConn struct {
	mu           sync.Mutex
	subscribed   []string // subjects passed to Subscribe
	subscribeErr error
	lastCb       natsgo.MsgHandler // capture the most recent callback
	publishedMsg []*natsgo.Msg     // messages passed to PublishMsg
}

func (m *mockNatsConn) Subscribe(subj string, cb natsgo.MsgHandler) (*natsgo.Subscription, error) {
	m.mu.Lock()
	m.subscribed = append(m.subscribed, subj)
	m.lastCb = cb
	m.mu.Unlock()
	return nil, m.subscribeErr
}

// contextCapturingRegistry records the context passed to Decode.
type contextCapturingRegistry struct {
	decodeCalled bool
	decodeCtx    context.Context
}

func (r *contextCapturingRegistry) Encode(_ context.Context, _ event.Event) ([]byte, error) {
	return []byte("{}"), nil
}
func (r *contextCapturingRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	r.decodeCalled = true
	r.decodeCtx = ctx
	return ctx, &fakeEvent{name: "test"}, nil
}
func (r *contextCapturingRegistry) Register(_ string, _ event.Factory, _ int) {}

func (m *mockNatsConn) Publish(_ string, _ []byte) error { return nil }
func (m *mockNatsConn) Request(_ string, _ []byte, _ time.Duration) (*natsgo.Msg, error) {
	return nil, nil
}
func (m *mockNatsConn) PublishMsg(msg *natsgo.Msg) error {
	m.mu.Lock()
	m.publishedMsg = append(m.publishedMsg, msg)
	m.mu.Unlock()
	return nil
}
func (m *mockNatsConn) Close() {}

// subscriberWithOpts also implements event.SubscribeOptionsProvider.
type subscriberWithOpts struct {
	name   string
	events map[string]event.HandleFn
	opts   []event.SubscribeOption
}

func (s *subscriberWithOpts) Name() string                              { return s.name }
func (s *subscriberWithOpts) Events() map[string]event.HandleFn         { return s.events }
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

type ctxKey string

func TestNatsBus_Subscribe_PropagatesContext(t *testing.T) {
	mock := &mockNatsConn{}
	reg := &contextCapturingRegistry{}
	bus := &Bus{
		nc:      mock,
		reg:     reg,
		timeout: 5 * time.Second,
	}

	const key ctxKey = "test-key"
	parentCtx := context.WithValue(context.Background(), key, "test-value")

	err := bus.Subscribe(parentCtx, "test.subject", noopHandler)
	if err != nil {
		t.Fatalf("unexpected Subscribe error: %v", err)
	}

	// Trigger the callback synchronously
	mock.mu.Lock()
	cb := mock.lastCb
	mock.mu.Unlock()

	if cb == nil {
		t.Fatal("expected callback to be captured, got nil")
	}

	cb(&natsgo.Msg{Data: []byte("{}")})

	if !reg.decodeCalled {
		t.Fatal("expected Decode to be called")
	}

	if reg.decodeCtx == nil {
		t.Fatal("expected Decode to receive a non-nil context")
	}

	if reg.decodeCtx.Value(key) != "test-value" {
		t.Errorf("expected parent context to be propagated (key=%q), got context without the value", key)
	}
}

func TestNatsBus_SubscribeSync_PropagatesContext(t *testing.T) {
	mock := &mockNatsConn{}
	reg := &contextCapturingRegistry{}
	bus := &Bus{
		nc:      mock,
		reg:     reg,
		timeout: 5 * time.Second,
	}

	const key ctxKey = "sync-test-key"
	parentCtx := context.WithValue(context.Background(), key, "sync-test-value")

	err := bus.SubscribeSync(parentCtx, "test.subject", noopHandler)
	if err != nil {
		t.Fatalf("unexpected SubscribeSync error: %v", err)
	}

	// Trigger the callback synchronously
	mock.mu.Lock()
	cb := mock.lastCb
	mock.mu.Unlock()

	if cb == nil {
		t.Fatal("expected callback to be captured, got nil")
	}

	cb(&natsgo.Msg{Data: []byte("{}")})

	if !reg.decodeCalled {
		t.Fatal("expected Decode to be called")
	}

	if reg.decodeCtx == nil {
		t.Fatal("expected Decode to receive a non-nil context")
	}

	if reg.decodeCtx.Value(key) != "sync-test-value" {
		t.Errorf("expected parent context to be propagated (key=%q), got context without the value", key)
	}
}
