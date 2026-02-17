package event_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
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
	bus.EmitSync(ctx, evt)

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
	bus.EmitSync(ctx, evt)

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
	bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		defer wg.Done()
		received = e.(*testEvent).message
		return nil
	}, event.WithHandlerName("handler1"))

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

	bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		count.Add(1)
		return nil
	}, event.WithHandlerName("handler1"))

	bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		count.Add(1)
		return nil
	}, event.WithHandlerName("handler2"))

	evt := &testEvent{name: "user.created"}
	bus.EmitSync(ctx, evt)

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
	bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		return handlerErr
	}, event.WithHandlerName("failing-handler"))

	evt := &testEvent{name: "user.created"}
	bus.EmitSync(ctx, evt)

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

	bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
		return handlerErr
	}, event.WithHandlerName("handler"))

	evt := &testEventWithOnErr{name: "user.created", onErrChan: errChan}
	bus.EmitSync(ctx, evt)

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
	bus.EmitSync(context.Background(), evt)
}

func TestLocalBus_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       &passthroughInvoker{},
		MaxConcurrent: 1,
	})

	var count atomic.Int32

	// Register multiple handlers
	for i := 0; i < 5; i++ {
		bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
			time.Sleep(50 * time.Millisecond)
			count.Add(1)
			return nil
		}, event.WithHandlerName("handler"))
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	evt := &testEvent{name: "user.created"}
	bus.Emit(cancelCtx, evt)

	time.Sleep(100 * time.Millisecond)

	// With context already cancelled, handlers should not execute
	if count.Load() > 0 {
		t.Errorf("expected no handlers to be called with cancelled context, got %d", count.Load())
	}
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
		bus.Subscribe(ctx, "user.created", func(ctx context.Context, e event.Event) error {
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
		}, event.WithHandlerName("handler"))
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

	bus.EmitSync(ctx, &testEvent{name: "user.created"})
	bus.EmitSync(ctx, &testEvent{name: "user.deleted"})

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

	bus.Subscribe(ctx, "test", func(ctx context.Context, e event.Event) error {
		defer wg.Done()
		return nil
	}, event.WithHandlerName("handler"))

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
