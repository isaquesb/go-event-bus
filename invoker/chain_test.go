package invoker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

type testEvent struct {
	name string
}

func (e *testEvent) Name() string { return e.name }

func TestChain_ExecutesInOrder(t *testing.T) {
	var order []int

	inv1 := &orderTrackingInvoker{order: &order, id: 1}
	inv2 := &orderTrackingInvoker{order: &order, id: 2}
	inv3 := &orderTrackingInvoker{order: &order, id: 3}

	chain := invoker.NewChain(inv1, inv2, inv3)

	err := chain.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		order = append(order, 0) // handler executes last
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []int{1, 2, 3, 0}
	if len(order) != len(expected) {
		t.Errorf("expected order %v, got %v", expected, order)
		return
	}

	for i, v := range expected {
		if order[i] != v {
			t.Errorf("expected order %v, got %v", expected, order)
			break
		}
	}
}

func TestChain_Empty(t *testing.T) {
	chain := invoker.NewChain()

	called := false
	err := chain.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !called {
		t.Error("expected handler to be called")
	}
}

func TestChain_PropagatesError(t *testing.T) {
	handlerErr := errors.New("handler failed")

	chain := invoker.NewChain(&passthroughInvoker{})

	err := chain.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return handlerErr
	})

	if !errors.Is(err, handlerErr) {
		t.Errorf("expected error '%v', got '%v'", handlerErr, err)
	}
}

func TestChain_InvokerCanShortCircuit(t *testing.T) {
	shortCircuitErr := errors.New("short circuit")

	shortCircuitInvoker := &errorInvoker{err: shortCircuitErr}
	neverCalledInvoker := &callCountingInvoker{}

	chain := invoker.NewChain(shortCircuitInvoker, neverCalledInvoker)

	err := chain.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		t.Error("handler should not be called")
		return nil
	})

	if !errors.Is(err, shortCircuitErr) {
		t.Errorf("expected error '%v', got '%v'", shortCircuitErr, err)
	}

	if neverCalledInvoker.count != 0 {
		t.Error("second invoker should not be called")
	}
}

func TestChain_PassesContextThrough(t *testing.T) {
	type ctxKey string
	key := ctxKey("test")

	contextModifyingInvoker := &contextInvoker{
		modify: func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key, "modified")
		},
	}

	chain := invoker.NewChain(contextModifyingInvoker)

	var receivedValue any
	err := chain.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		receivedValue = ctx.Value(key)
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if receivedValue != "modified" {
		t.Errorf("expected context value 'modified', got '%v'", receivedValue)
	}
}

// Helper invokers

type orderTrackingInvoker struct {
	order *[]int
	id    int
}

func (o *orderTrackingInvoker) Invoke(
	ctx context.Context,
	_ event.Event,
	_ string,
	fn func(context.Context) error,
) error {
	*o.order = append(*o.order, o.id)
	return fn(ctx)
}

type passthroughInvoker struct{}

func (p *passthroughInvoker) Invoke(
	ctx context.Context,
	_ event.Event,
	_ string,
	fn func(context.Context) error,
) error {
	return fn(ctx)
}

type errorInvoker struct {
	err error
}

func (e *errorInvoker) Invoke(
	ctx context.Context,
	_ event.Event,
	_ string,
	_ func(context.Context) error,
) error {
	return e.err
}

type callCountingInvoker struct {
	count int
}

func (c *callCountingInvoker) Invoke(
	ctx context.Context,
	_ event.Event,
	_ string,
	fn func(context.Context) error,
) error {
	c.count++
	return fn(ctx)
}

type contextInvoker struct {
	modify func(ctx context.Context) context.Context
}

func (c *contextInvoker) Invoke(
	ctx context.Context,
	_ event.Event,
	_ string,
	fn func(context.Context) error,
) error {
	return fn(c.modify(ctx))
}
