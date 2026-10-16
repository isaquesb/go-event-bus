package invoker

import (
	"context"
	"github.com/isaquesb/go-event-bus"
)

/*
Example usage, in the documented order:

	chain := invoker.NewChain(
		// Observability
		telemetry.NewTracerInvoker(tracer),
		invoker.NewMetrics(metrics),

		// Execution control
		invoker.NewRateLimiter(limitConfig),
		invoker.NewIdempotency(store, cfg, metrics),

		// Failure handling: DLQ outside Retry
		invoker.NewDLQ(dlqPublisher, metrics),

		// Resilience
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 5,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    5 * time.Second,
		}, metrics),

		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			OpenTimeout:      30 * time.Second,
		}, metrics),
	)

	Tracer
	 └─ Metrics
	     └─ RateLimiter (may abort early)
	         └─ Idempotency (may skip execution)
	             └─ DLQ (dead-letters the final outcome)
	                 └─ Retry (re-runs the handler)
	                     └─ CircuitBreaker (is the system healthy?)
	                         └─ Handler

DLQ sits outside Retry so retries run first and only the final outcome is
dead-lettered. Put inside Retry, it would dead-letter a recoverable transient
error on its first failure.

Flow-control errors from RateLimit and Idempotency never pass through DLQ, so
the transport dead-letters them on the final delivery with its own publisher.
*/

type Chain struct {
	invokers []event.Invoker
}

func NewChain(invokers ...event.Invoker) *Chain {
	return &Chain{invokers: invokers}
}

func (c *Chain) Invoke(
	ctx context.Context,
	evt event.Event,
	handlerName string,
	handle func(context.Context) error,
) error {
	var next func(i int, ctx context.Context) error

	next = func(i int, ctx context.Context) error {
		if i == len(c.invokers) {
			return handle(ctx)
		}

		return c.invokers[i].Invoke(
			ctx,
			evt,
			handlerName,
			func(ctx context.Context) error {
				return next(i+1, ctx)
			},
		)
	}

	return next(0, ctx)
}
