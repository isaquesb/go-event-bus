package invoker

import (
	"context"
	"github.com/isaquesb/go-event-bus"
)

/**
// Example Usage
invoker := NewChain(
	// Observabilidade
	telemetry.NewTracerInvoker(tracer),
	NewMetrics(metrics),

	// Controle de execução
	NewRateLimiter(limitConfig),
	NewIdempotency(store, cfg),

	// Resiliência
	NewRetry(RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}, metrics),

	NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
	}, metrics),

	// Terminal
	NewDLQ(dlqPublisher),
)

Tracer
 └─ Metrics
     └─ RateLimiter (pode abortar cedo)
         └─ Idempotency (pode pular execução)
             └─ Retry (reexecuta handler)
                 └─ CircuitBreaker (decide se o sistema está saudável)
                     └─ DLQ (último recurso)
                         └─ Handler
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
