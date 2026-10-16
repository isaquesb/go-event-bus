package invoker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

type pcEvent struct{}

func (pcEvent) Name() string { return "pc.order" }

// pcChain builds the documented order: DLQ outside Retry outside CircuitBreaker.
func pcChain(pub event.DLQPublisher) *invoker.Chain {
	return invoker.NewChain(
		invoker.NewDLQ(pub, nil),
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    2 * time.Millisecond,
		}, nil),
		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Minute,
		}, nil),
	)
}

func pcCtx(parent context.Context, count, max int) context.Context {
	return event.WithDelivery(parent, event.Delivery{Count: count, Max: max})
}

func pcInvoke(ctx context.Context, chain *invoker.Chain, fn func(context.Context) error) error {
	return event.InvokeHandler(ctx, chain, pcEvent{}, "h", fn)
}

// pcFollowUp runs a succeeding handler through the chain and reports whether
// the handler was reached.
func pcFollowUp(chain *invoker.Chain) (reached bool, err error) {
	err = pcInvoke(pcCtx(context.Background(), 1, 3), chain, func(context.Context) error {
		reached = true
		return nil
	})
	return reached, err
}

func TestChain_PanicNotRetriedDeadLetteredBreakerNeutral(t *testing.T) {
	pub := event.NewMemoryDLQ()
	chain := pcChain(pub)

	var calls atomic.Int64
	err := pcInvoke(pcCtx(context.Background(), 1, 1), chain, func(context.Context) error {
		calls.Add(1)
		panic("pc: boom")
	})

	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1 (a panic is never retried)", got)
	}
	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("want ErrDeadLettered, got %v", err)
	}
	if !errors.Is(err, event.ErrHandlerPanic) {
		t.Errorf("want ErrHandlerPanic preserved, got %v", err)
	}

	recs := pub.Records()
	if len(recs) != 1 {
		t.Fatalf("want exactly 1 DLQ record, got %d", len(recs))
	}
	if recs[0].Reason != event.ReasonPanic {
		t.Errorf("record reason = %q, want %q", recs[0].Reason, event.ReasonPanic)
	}
	if recs[0].FailureHeaders()[event.HeaderDLQPanicStack] == "" {
		t.Errorf("record is missing the %s header", event.HeaderDLQPanicStack)
	}

	// The panic was neutral: the breaker (threshold 1) is still closed.
	if reached, err := pcFollowUp(chain); !reached || err != nil {
		t.Errorf("breaker opened on a panic: reached=%v err=%v", reached, err)
	}
}

func TestChain_HandlerTimeoutCountsForBreaker(t *testing.T) {
	pub := event.NewMemoryDLQ()
	chain := pcChain(pub)

	ctx, cancel := context.WithTimeoutCause(
		pcCtx(context.Background(), 1, 3), 10*time.Millisecond, event.ErrHandlerTimeout)
	defer cancel()

	var calls atomic.Int64
	_ = pcInvoke(ctx, chain, func(c context.Context) error {
		calls.Add(1)
		<-c.Done()
		return c.Err()
	})
	// Retry does not retry context errors: one run, one counted failure.
	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}

	reached, err := pcFollowUp(chain)
	if reached {
		t.Error("handler must not run while the breaker is open")
	}
	if !errors.Is(err, event.ErrCircuitOpen) {
		t.Errorf("want ErrCircuitOpen after a handler timeout, got %v", err)
	}
}

func TestChain_CallerCancellationNeutralForBreaker(t *testing.T) {
	pub := event.NewMemoryDLQ()
	chain := pcChain(pub)

	ctx, cancel := context.WithCancel(pcCtx(context.Background(), 1, 3))
	defer cancel()

	entered := make(chan struct{})
	go func() {
		<-entered
		cancel()
	}()
	_ = pcInvoke(ctx, chain, func(c context.Context) error {
		close(entered)
		<-c.Done()
		return c.Err()
	})

	if reached, err := pcFollowUp(chain); !reached || err != nil {
		t.Errorf("caller cancellation opened the breaker: reached=%v err=%v", reached, err)
	}
}
