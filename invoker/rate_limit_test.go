package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// eventWithRateLimitKey implements WithRateLimitKey
type eventWithRateLimitKey struct {
	key  string
	name string
}

func (e *eventWithRateLimitKey) Name() string         { return e.name }
func (e *eventWithRateLimitKey) RateLimitKey() string { return e.key }

// eventWithEmptyRateLimitKey has empty rate limit key
type eventWithEmptyRateLimitKey struct {
	name string
}

func (e *eventWithEmptyRateLimitKey) Name() string         { return e.name }
func (e *eventWithEmptyRateLimitKey) RateLimitKey() string { return "" }

// memoryRateLimitStore is a simple in-memory rate limiter for testing
type memoryRateLimitStore struct {
	counts map[string]int
	burst  int
}

func newMemoryRateLimitStore(burst int) *memoryRateLimitStore {
	return &memoryRateLimitStore{
		counts: make(map[string]int),
		burst:  burst,
	}
}

func (s *memoryRateLimitStore) Allow(
	_ context.Context,
	key string,
	rate int,
	period time.Duration,
	burst int,
) (bool, error) {
	s.counts[key]++
	return s.counts[key] <= s.burst, nil
}

func (s *memoryRateLimitStore) Reset() {
	s.counts = make(map[string]int)
}

func TestRateLimiter_AllowsWithinLimit(t *testing.T) {
	store := newMemoryRateLimitStore(3)
	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  3,
	}, nil)

	evt := &eventWithRateLimitKey{key: "user-1", name: "test"}

	// Should allow 3 requests
	for i := 0; i < 3; i++ {
		called := false
		err := limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
			called = true
			return nil
		})

		if err != nil {
			t.Errorf("unexpected error on request %d: %v", i+1, err)
		}
		if !called {
			t.Errorf("handler should be called on request %d", i+1)
		}
	}
}

func TestRateLimiter_BlocksOverLimit(t *testing.T) {
	store := newMemoryRateLimitStore(2)
	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  2,
	}, nil)

	evt := &eventWithRateLimitKey{key: "user-1", name: "test"}

	// Exhaust the limit
	for i := 0; i < 2; i++ {
		_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
			return nil
		})
	}

	// Next request should be blocked
	called := false
	err := limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, invoker.ErrRateLimited) {
		t.Errorf("expected ErrRateLimited, got '%v'", err)
	}
	if called {
		t.Error("handler should not be called when rate limited")
	}
}

func TestRateLimiter_NoKeySkipsCheck(t *testing.T) {
	store := newMemoryRateLimitStore(1)
	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  1,
	}, nil)

	// Event without rate limit key
	evt := &testEvent{name: "test"}

	// Should allow all requests (no rate limiting)
	for i := 0; i < 10; i++ {
		called := false
		err := limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
			called = true
			return nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("handler should be called without rate limit key")
		}
	}
}

func TestRateLimiter_EmptyKeySkipsCheck(t *testing.T) {
	store := newMemoryRateLimitStore(1)
	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  1,
	}, nil)

	evt := &eventWithEmptyRateLimitKey{name: "test"}

	// Should allow all requests (empty key)
	for i := 0; i < 10; i++ {
		called := false
		err := limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
			called = true
			return nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("handler should be called with empty rate limit key")
		}
	}
}

func TestRateLimiter_KeyPrefix(t *testing.T) {
	store := &keyTrackingStore{
		keys:  make(map[string]bool),
		allow: true,
	}

	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:      1,
		Period:    time.Second,
		Burst:     10,
		KeyPrefix: "myapp:",
	}, nil)

	evt := &eventWithRateLimitKey{key: "user-1", name: "test"}

	_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	if !store.keys["myapp:user-1"] {
		t.Error("expected key with prefix 'myapp:user-1'")
	}
}

func TestRateLimiter_OnLimitCallback(t *testing.T) {
	store := newMemoryRateLimitStore(1)

	var capturedKey string
	var capturedEvt event.Event

	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:      1,
		Period:    time.Second,
		Burst:     1,
		KeyPrefix: "test:",
		OnLimit: func(ctx context.Context, evt event.Event, key string) {
			capturedEvt = evt
			capturedKey = key
		},
	}, nil)

	evt := &eventWithRateLimitKey{key: "user-1", name: "test"}

	// Exhaust limit
	_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	// Trigger rate limit
	_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	if capturedEvt != evt {
		t.Error("expected OnLimit callback to receive the event")
	}
	if capturedKey != "test:user-1" {
		t.Errorf("expected key 'test:user-1', got '%s'", capturedKey)
	}
}

func TestRateLimiter_StoreError(t *testing.T) {
	storeErr := errors.New("store error")
	store := &errorRateLimitStore{err: storeErr}
	metrics := &testMetricProvider{}

	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  10,
	}, metrics)

	evt := &eventWithRateLimitKey{key: "user-1", name: "test"}

	err := limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	if !errors.Is(err, storeErr) {
		t.Errorf("expected store error, got '%v'", err)
	}

	if metrics.counters["eventbus_ratelimit_error_total"] != 1 {
		t.Error("expected error metric to be tracked")
	}
}

func TestRateLimiter_MetricsTracked(t *testing.T) {
	store := newMemoryRateLimitStore(2)
	metrics := &testMetricProvider{}

	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  2,
	}, metrics)

	evt := &eventWithRateLimitKey{key: "user-1", name: "test"}

	// Two allowed
	_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})
	_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	// One blocked
	_ = limiter.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	if metrics.counters["eventbus_ratelimit_allowed_total"] != 2 {
		t.Errorf("expected 2 allowed, got %d", metrics.counters["ratelimit_allowed_total"])
	}

	if metrics.counters["eventbus_ratelimit_blocked_total"] != 1 {
		t.Errorf("expected 1 blocked, got %d", metrics.counters["ratelimit_blocked_total"])
	}
}

func TestRateLimiter_PerKeyLimit(t *testing.T) {
	store := newMemoryRateLimitStore(1)
	limiter := invoker.NewRateLimiter(store, invoker.RateLimitConfig{
		Rate:   1,
		Period: time.Second,
		Burst:  1,
	}, nil)

	// User 1 exhausts limit
	evt1 := &eventWithRateLimitKey{key: "user-1", name: "test"}
	_ = limiter.Invoke(context.Background(), evt1, "handler", func(ctx context.Context) error {
		return nil
	})

	// User 1 is blocked
	err := limiter.Invoke(context.Background(), evt1, "handler", func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, invoker.ErrRateLimited) {
		t.Error("user-1 should be rate limited")
	}

	// User 2 should still be allowed
	evt2 := &eventWithRateLimitKey{key: "user-2", name: "test"}
	called := false
	err = limiter.Invoke(context.Background(), evt2, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error for user-2: %v", err)
	}
	if !called {
		t.Error("user-2 should not be rate limited")
	}
}

// Helper stores

type keyTrackingStore struct {
	keys  map[string]bool
	allow bool
}

func (s *keyTrackingStore) Allow(
	_ context.Context,
	key string,
	rate int,
	period time.Duration,
	burst int,
) (bool, error) {
	s.keys[key] = true
	return s.allow, nil
}

type errorRateLimitStore struct {
	err error
}

func (s *errorRateLimitStore) Allow(
	_ context.Context,
	key string,
	rate int,
	period time.Duration,
	burst int,
) (bool, error) {
	return false, s.err
}
