package event_test

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
)

// testEvent is a simple event for testing
type testEvent struct {
	name    string
	id      string
	message string
}

func (e *testEvent) Name() string { return e.name }
func (e *testEvent) Id() string   { return e.id }

// testEventWithOnErr implements event.OnErr
type testEventWithOnErr struct {
	name      string
	onErrChan chan error
}

func (e *testEventWithOnErr) Name() string { return e.name }
func (e *testEventWithOnErr) OnErr(ctx context.Context, err error, listenerName string) {
	if e.onErrChan != nil {
		e.onErrChan <- err
	}
}

// passthroughInvoker just calls the handler directly
type passthroughInvoker struct{}

func (p *passthroughInvoker) Invoke(
	ctx context.Context,
	_ event.Event,
	_ string,
	fn func(context.Context) error,
) error {
	return fn(ctx)
}

func TestLocalBus_SubscribeAndEmitSync(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	var received string
	err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		received = e.(*testEvent).message
		return nil
	}, event.WithHandlerName("handler1"))
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	evt := &testEvent{name: "user.created", message: "hello"}
	_, _ = bus.EmitSync(ctx, evt)

	if received != "hello" {
		t.Errorf("expected message 'hello', got '%s'", received)
	}
}

func TestLocalBus_SubscribeDefaultHandlerName(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	// Subscribe without WithHandlerName - should use subject as handler name
	var called bool
	err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	evt := &testEvent{name: "user.created"}
	_, _ = bus.EmitSync(ctx, evt)

	if !called {
		t.Error("expected handler to be called")
	}
}

func TestLocalBus_Emit(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	var wg sync.WaitGroup
	wg.Add(1)

	var received string
	if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		defer wg.Done()
		received = e.(*testEvent).message
		return nil
	}, event.WithHandlerName("handler1")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	evt := &testEvent{name: "user.created", message: "async-hello"}
	bus.Emit(ctx, evt)

	wg.Wait()

	if received != "async-hello" {
		t.Errorf("expected message 'async-hello', got '%s'", received)
	}
}

func TestLocalBus_MultipleHandlers(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	var count atomic.Int32

	if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		count.Add(1)
		return nil
	}, event.WithHandlerName("handler1")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		count.Add(1)
		return nil
	}, event.WithHandlerName("handler2")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	evt := &testEvent{name: "user.created"}
	_, _ = bus.EmitSync(ctx, evt)

	if count.Load() != 2 {
		t.Errorf("expected 2 handlers to be called, got %d", count.Load())
	}
}

func TestLocalBus_OnErrCallback(t *testing.T) {
	ctx := context.Background()
	var capturedErr error
	var capturedHandler string

	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
		OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
			capturedErr = err
			capturedHandler = handler
		},
	})

	handlerErr := errors.New("handler failed")
	if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		return handlerErr
	}, event.WithHandlerName("failing-handler")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	evt := &testEvent{name: "user.created"}
	_, _ = bus.EmitSync(ctx, evt)

	if capturedErr != handlerErr {
		t.Errorf("expected error '%v', got '%v'", handlerErr, capturedErr)
	}
	if capturedHandler != "failing-handler" {
		t.Errorf("expected handler 'failing-handler', got '%s'", capturedHandler)
	}
}

func TestLocalBus_EventOnErrInterface(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	errChan := make(chan error, 1)
	handlerErr := errors.New("handler failed")

	if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		return handlerErr
	}, event.WithHandlerName("handler")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	evt := &testEventWithOnErr{name: "user.created", onErrChan: errChan}
	_, _ = bus.EmitSync(ctx, evt)

	select {
	case err := <-errChan:
		if err != handlerErr {
			t.Errorf("expected error '%v', got '%v'", handlerErr, err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected OnErr to be called")
	}
}

func TestLocalBus_NoHandlers(t *testing.T) {
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	// Should not panic when no handlers registered
	evt := &testEvent{name: "unknown.event"}
	_, _ = bus.EmitSync(context.Background(), evt)
}

// TestLocalBus_ContextCancellation pins CORE-03 (D-09): async handlers are
// detached from the emitter ctx, so a ctx that is already cancelled when Emit is
// called neither skips nor cancels any handler. Shutdown waits for all of them.
func TestLocalBus_ContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		bus := event.NewLocalBus(event.LocalBusOptions{
			Invoker:       &passthroughInvoker{},
			MaxConcurrent: 1,
		})

		var count atomic.Int32

		// Register multiple handlers
		for i := 0; i < 5; i++ {
			if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
				time.Sleep(50 * time.Millisecond)
				count.Add(1)
				return nil
			}, event.WithHandlerName("handler")); err != nil {
				t.Fatalf("subscribe: %v", err)
			}
		}

		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		evt := &testEvent{name: "user.created"}
		bus.Emit(cancelCtx, evt)

		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}

		// The emitter ctx no longer bounds the handlers: all of them ran.
		if got := count.Load(); got != 5 {
			t.Errorf("expected all 5 handlers to run with a cancelled emitter ctx, got %d", got)
		}
	})
}

func TestLocalBus_ConcurrencyLimit(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       &passthroughInvoker{},
		MaxConcurrent: 2,
	})

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		if err := bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
			defer wg.Done()
			current := concurrent.Add(1)
			defer concurrent.Add(-1)

			// Track max concurrent
			for {
				old := maxConcurrent.Load()
				if current <= old || maxConcurrent.CompareAndSwap(old, current) {
					break
				}
			}

			time.Sleep(20 * time.Millisecond)
			return nil
		}, event.WithHandlerName("handler")); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}

	evt := &testEvent{name: "user.created"}
	bus.Emit(ctx, evt)

	wg.Wait()

	if maxConcurrent.Load() > 2 {
		t.Errorf("expected max concurrent <= 2, got %d", maxConcurrent.Load())
	}
}

func TestLocalBus_RegisterSubscribers(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	var received []string
	var mu sync.Mutex

	subscriber := &testSubscriber{
		name: "test-subscriber",
		events: map[string]event.HandleFn{
			"user.created": func(ctx context.Context, e event.Event) error {
				mu.Lock()
				received = append(received, "user.created")
				mu.Unlock()
				return nil
			},
			"user.deleted": func(ctx context.Context, e event.Event) error {
				mu.Lock()
				received = append(received, "user.deleted")
				mu.Unlock()
				return nil
			},
		},
	}

	err := bus.RegisterSubscribers(ctx, subscriber)
	if err != nil {
		t.Fatalf("RegisterSubscribers failed: %v", err)
	}

	_, _ = bus.EmitSync(ctx, &testEvent{name: "user.created"})
	_, _ = bus.EmitSync(ctx, &testEvent{name: "user.deleted"})

	if len(received) != 2 {
		t.Errorf("expected 2 events received, got %d", len(received))
	}
}

func TestLocalBus_DefaultMaxConcurrent(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
		// MaxConcurrent not set, should use default
	})

	var wg sync.WaitGroup
	wg.Add(1)

	if err := bus.Subscribe(ctx, "test", func(ctx context.Context, e event.Event) error {
		defer wg.Done()
		return nil
	}, event.WithHandlerName("handler")); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	bus.Emit(ctx, &testEvent{name: "test"})
	wg.Wait()
	// Test passes if no deadlock or panic
}

func TestLocalBus_Close(t *testing.T) {
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
	})

	err := bus.Close()
	if err != nil {
		t.Errorf("expected Close to return nil, got %v", err)
	}
}

type testSubscriber struct {
	name   string
	events map[string]event.HandleFn
}

func (s *testSubscriber) Name() string                      { return s.name }
func (s *testSubscriber) Events() map[string]event.HandleFn { return s.events }

// testSubscriberWithOpts also implements SubscribeOptionsProvider
type testSubscriberWithOpts struct {
	testSubscriber
	opts []event.SubscribeOption
}

func (s *testSubscriberWithOpts) SubscribeOptions() []event.SubscribeOption { return s.opts }

func TestLocalBus_RegisterSubscribers_MultipleSubscribers(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}})

	var mu sync.Mutex
	received := map[string]int{}

	err := bus.RegisterSubscribers(ctx,
		&testSubscriber{
			name: "subscriber-a",
			events: map[string]event.HandleFn{
				"order.placed": func(_ context.Context, _ event.Event) error {
					mu.Lock()
					received["order.placed"]++
					mu.Unlock()
					return nil
				},
			},
		},
		&testSubscriber{
			name: "subscriber-b",
			events: map[string]event.HandleFn{
				"order.shipped": func(_ context.Context, _ event.Event) error {
					mu.Lock()
					received["order.shipped"]++
					mu.Unlock()
					return nil
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("RegisterSubscribers failed: %v", err)
	}

	_, _ = bus.EmitSync(ctx, &testEvent{name: "order.placed"})
	_, _ = bus.EmitSync(ctx, &testEvent{name: "order.shipped"})

	if received["order.placed"] != 1 {
		t.Errorf("expected order.placed count 1, got %d", received["order.placed"])
	}
	if received["order.shipped"] != 1 {
		t.Errorf("expected order.shipped count 1, got %d", received["order.shipped"])
	}
}

func TestLocalBus_RegisterSubscribers_HandlerNameFromSubscriberName(t *testing.T) {
	ctx := context.Background()

	var capturedHandler string
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
		OnErr: func(_ context.Context, _ event.Event, _ error, handler string) {
			capturedHandler = handler
		},
	})

	err := bus.RegisterSubscribers(ctx, &testSubscriber{
		name: "my-service",
		events: map[string]event.HandleFn{
			"user.created": func(_ context.Context, _ event.Event) error {
				return errors.New("boom")
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterSubscribers failed: %v", err)
	}

	_, _ = bus.EmitSync(ctx, &testEvent{name: "user.created"})

	if capturedHandler != "my-service" {
		t.Errorf("expected handler name 'my-service', got '%s'", capturedHandler)
	}
}

func TestLocalBus_RegisterSubscribers_WithSubscribeOptionsProvider(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}})

	var called bool
	err := bus.RegisterSubscribers(ctx, &testSubscriberWithOpts{
		testSubscriber: testSubscriber{
			name: "svc",
			events: map[string]event.HandleFn{
				"item.updated": func(_ context.Context, _ event.Event) error {
					called = true
					return nil
				},
			},
		},
		// HandlerName from WithHandlerName inside SubscribeOptions should override
		opts: []event.SubscribeOption{event.WithHandlerName("override-name")},
	})
	if err != nil {
		t.Fatalf("RegisterSubscribers failed: %v", err)
	}

	_, _ = bus.EmitSync(ctx, &testEvent{name: "item.updated"})
	if !called {
		t.Error("expected handler to be called")
	}
}

func TestLocalBus_RegisterSubscribers_NoSubscribers(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}})

	if err := bus.RegisterSubscribers(ctx); err != nil {
		t.Errorf("expected nil error for empty subscribers, got %v", err)
	}
}

func TestLocalBus_NilInvoker_NoPanic(t *testing.T) {
	ctx := context.Background()
	// NewLocalBus with zero-value options (nil Invoker) must not panic.
	// The bus should default to a passthrough invoker defined in the root module.
	bus := event.NewLocalBus(event.LocalBusOptions{})

	var called bool
	err := bus.Subscribe(ctx, "test.event", func(ctx context.Context, e event.Event) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	_, _ = bus.EmitSync(ctx, &testEvent{name: "test.event"})

	if !called {
		t.Error("expected handler to be called with nil invoker, but it was not")
	}
}

func TestLocalBus_EmitSync_ReturnsEventAndNil(t *testing.T) {
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}})
	ctx := context.Background()
	evt := &testEvent{name: "test.sync"}

	// Subscribe so dispatch has something to call
	_ = bus.Subscribe(ctx, "test.sync", func(_ context.Context, _ event.Event) error {
		return nil
	})

	got, err := bus.EmitSync(ctx, evt)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got != evt {
		t.Errorf("expected same event back, got %v", got)
	}
}

func TestLocalBus_Metadata_EmitSyncHandlerReadsMetadata(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{})

	var (
		got event.Metadata
		ok  bool
	)
	_ = bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		got, ok = event.MetadataFromContext(ctx)
		return nil
	})

	_, _ = bus.EmitSync(ctx, &testEvent{name: "user.created"},
		event.WithID("id-1"), event.WithCorrelationID("corr-1"))

	if !ok {
		t.Fatal("expected metadata in handler ctx")
	}
	if got.ID != "id-1" || got.CorrelationID != "corr-1" {
		t.Errorf("got ID %q CorrelationID %q", got.ID, got.CorrelationID)
	}
	if got.CausationID != "" {
		t.Errorf("expected empty CausationID, got %q", got.CausationID)
	}
	if got.Name != "user.created" || got.Version != 1 {
		t.Errorf("got Name %q Version %d", got.Name, got.Version)
	}
	if got.Timestamp.IsZero() {
		t.Error("expected non-zero Timestamp")
	}
}

var uuidV7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestLocalBus_Metadata_AsyncEmit(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{})

	ch := make(chan event.Metadata, 1)
	_ = bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		ch <- md
		return nil
	})

	bus.Emit(ctx, &testEvent{name: "user.created"})

	select {
	case md := <-ch:
		if !uuidV7Re.MatchString(md.ID) {
			t.Errorf("ID %q is not a UUIDv7", md.ID)
		}
		if md.CorrelationID != md.ID {
			t.Errorf("root CorrelationID %q should equal ID %q", md.CorrelationID, md.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler")
	}
}

func TestLocalBus_Metadata_NestedEmitLineage(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{})

	var order, invoice event.Metadata
	_ = bus.Subscribe(ctx, "order.created", func(ctx context.Context, e event.Event) error {
		order, _ = event.MetadataFromContext(ctx)
		_, err := bus.EmitSync(ctx, &testEvent{name: "invoice.created"})
		return err
	})
	_ = bus.Subscribe(ctx, "invoice.created", func(ctx context.Context, e event.Event) error {
		invoice, _ = event.MetadataFromContext(ctx)
		return nil
	})

	_, _ = bus.EmitSync(ctx, &testEvent{name: "order.created"})

	if order.ID == "" {
		t.Fatal("order metadata missing")
	}
	if invoice.CausationID != order.ID {
		t.Errorf("CausationID %q, want order ID %q", invoice.CausationID, order.ID)
	}
	if invoice.CorrelationID != order.CorrelationID || order.CorrelationID != order.ID {
		t.Errorf("CorrelationID invoice %q order %q order ID %q",
			invoice.CorrelationID, order.CorrelationID, order.ID)
	}
	if invoice.ID == order.ID {
		t.Error("child must have its own id")
	}
}

func TestLocalBus_Metadata_WithIdEventIsMessageID(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{})

	var got event.Metadata
	_ = bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		got, _ = event.MetadataFromContext(ctx)
		return nil
	})

	_, _ = bus.EmitSync(ctx, &testEvent{name: "user.created", id: "evt-7"})

	if got.ID != "evt-7" {
		t.Errorf("ID %q, want evt-7", got.ID)
	}
}

func TestLocalBus_Metadata_IDGeneratorOption(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		IDGenerator: func() string { return "gen-1" },
	})

	var got event.Metadata
	_ = bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		got, _ = event.MetadataFromContext(ctx)
		return nil
	})

	_, _ = bus.EmitSync(ctx, &testEvent{name: "user.created"})

	if got.ID != "gen-1" {
		t.Errorf("ID %q, want gen-1", got.ID)
	}
}

func TestLocalBus_Metadata_InheritHeadersAllowlist(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{InheritHeaders: []string{"tenant"}})

	var child event.Metadata
	_ = bus.Subscribe(ctx, "order.created", func(ctx context.Context, e event.Event) error {
		_, err := bus.EmitSync(ctx, &testEvent{name: "invoice.created"})
		return err
	})
	_ = bus.Subscribe(ctx, "invoice.created", func(ctx context.Context, e event.Event) error {
		child, _ = event.MetadataFromContext(ctx)
		return nil
	})

	_, _ = bus.EmitSync(ctx, &testEvent{name: "order.created"},
		event.WithHeader("tenant", "acme"), event.WithHeader("secret", "s"))

	if len(child.Headers) != 1 || child.Headers["tenant"] != "acme" {
		t.Errorf("child headers %v, want only tenant=acme", child.Headers)
	}
}
