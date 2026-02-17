// Example: Redis Stores for Production
//
// This example demonstrates how to use Redis-backed stores for
// distributed idempotency and rate limiting in production.
package examples

import (
	"context"
	"log/slog"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/isaquesb/go-event-bus/redis"

	redislib "github.com/redis/go-redis/v9"
)

// =============================================================================
// Event Definitions
// =============================================================================

// OrderCreated represents an order creation event
type OrderCreated struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Total      float64 `json:"total"`
}

func (e OrderCreated) Name() string { return "order.created" }

// IdempotencyKey prevents duplicate order processing
func (e OrderCreated) IdempotencyKey() string {
	return "order:" + e.OrderID
}

// RateLimitKey limits order creation per customer
func (e OrderCreated) RateLimitKey() string {
	return "customer:" + e.CustomerID
}

// =============================================================================
// Production Setup with Redis
// =============================================================================

func ExampleRedisStores() {
	ctx := context.Background()

	// Connect to Redis
	rdb := redislib.NewClient(&redislib.Options{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     100,
		MinIdleConns: 10,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Verify connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("failed to connect to Redis", "error", err)
		return
	}
	defer rdb.Close()

	// Create Redis-backed stores
	idempotencyStore := redis.NewIdempotencyStore(rdb)
	rateLimitStore := redis.NewRedisRateLimitStore(rdb)

	// Build production-ready invoker chain
	chain := invoker.NewChain(
		// Metrics first (always wrap)
		invoker.NewMetrics(nil),

		// Rate limiting - prevent abuse
		invoker.NewRateLimiter(
			rateLimitStore,
			invoker.RateLimitConfig{
				Rate:      100,             // 100 orders
				Period:    time.Hour,       // per hour
				Burst:     10,              // burst up to 10
				KeyPrefix: "orders:limit:", // Redis key prefix
				OnLimit: func(ctx context.Context, evt event.Event, key string) {
					slog.Warn("customer rate limited",
						"key", key,
						"event", evt.Name(),
					)
				},
			},
			nil,
		),

		// Idempotency - prevent duplicate processing
		invoker.NewIdempotency(
			idempotencyStore,
			invoker.IdempotencyConfig{
				ProcessingTTL: 5 * time.Minute, // Lock expires after 5 min
			},
			nil,
		),

		// Retry with backoff
		invoker.NewRetry(
			invoker.RetryPolicy{
				MaxAttempts: 3,
				BaseDelay:   100 * time.Millisecond,
				MaxDelay:    10 * time.Second,
			},
			nil,
		),

		// Circuit breaker for downstream protection
		invoker.NewCircuitBreaker(
			invoker.CircuitBreakerConfig{
				FailureThreshold: 5,
				SuccessThreshold: 2,
				OpenTimeout:      30 * time.Second,
			},
			nil,
		),
	)

	// Create bus
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       chain,
		MaxConcurrent: 32,
		OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
			slog.Error("handler error",
				"event", evt.Name(),
				"handler", handler,
				"error", err,
			)
		},
	})

	// Register handler
	bus.Subscribe(ctx, "order.created", func(ctx context.Context, evt event.Event) error {
		order := evt.(OrderCreated)
		slog.Info("processing order",
			"order_id", order.OrderID,
			"customer_id", order.CustomerID,
			"total", order.Total,
		)
		// ... process order
		return nil
	}, event.WithHandlerName("process-order"))

	// Test scenarios
	order := OrderCreated{
		OrderID:    "order-12345",
		CustomerID: "cust-789",
		Total:      99.99,
	}

	// First attempt - succeeds
	slog.Info("=== First attempt ===")
	bus.EmitSync(ctx, order)

	// Second attempt - blocked by idempotency
	slog.Info("=== Second attempt (should be blocked) ===")
	bus.EmitSync(ctx, order)

	// Different order - succeeds
	slog.Info("=== Different order ===")
	bus.EmitSync(ctx, OrderCreated{
		OrderID:    "order-12346",
		CustomerID: "cust-789",
		Total:      149.99,
	})
}

// =============================================================================
// Redis Idempotency Store Behavior
// =============================================================================

/*
Redis Key Structure:
  - Key: "order:order-12345:process-order"
  - Value: JSON { status, started_at, ended_at }
  - TTL: Based on status
    - processing: 5 minutes (lock recovery)
    - completed: 24 hours (duplicate prevention)
    - failed: 1 hour (allow retry)

Example Redis commands:
  SETNX order:order-12345:process-order '{"status":"processing","started_at":"..."}'
  EXPIRE order:order-12345:process-order 300

After completion:
  SET order:order-12345:process-order '{"status":"completed","started_at":"...","ended_at":"..."}'
  EXPIRE order:order-12345:process-order 86400
*/

// =============================================================================
// Redis Rate Limiter Behavior
// =============================================================================

/*
Token Bucket Algorithm (Lua script):

1. Get current tokens and timestamp
2. Calculate refill since last request
3. If tokens available, decrement and allow
4. If no tokens, reject

Redis Key Structure:
  - Key: "orders:limit:customer:cust-789"
  - Value: Hash { tokens, ts }
  - TTL: Period duration

Example flow:
  Request 1: tokens=100 -> allowed, tokens=99
  Request 2: tokens=99  -> allowed, tokens=98
  ...
  Request 101: tokens=0 -> REJECTED

After period:
  Tokens refill based on elapsed time
*/

// =============================================================================
// Monitoring Redis Keys
// =============================================================================

func ExampleMonitorRedisKeys() {
	rdb := redislib.NewClient(&redislib.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	// List all idempotency keys
	keys, _ := rdb.Keys(ctx, "order:*").Result()
	for _, key := range keys {
		val, _ := rdb.Get(ctx, key).Result()
		ttl, _ := rdb.TTL(ctx, key).Result()
		slog.Info("idempotency key",
			"key", key,
			"value", val,
			"ttl", ttl,
		)
	}

	// List all rate limit keys
	keys, _ = rdb.Keys(ctx, "orders:limit:*").Result()
	for _, key := range keys {
		tokens, _ := rdb.HGet(ctx, key, "tokens").Result()
		slog.Info("rate limit key",
			"key", key,
			"tokens", tokens,
		)
	}
}
