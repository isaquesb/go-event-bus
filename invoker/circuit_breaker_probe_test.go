package invoker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus/invoker"
)

// cbpFail is the transient failure the probe tests feed to the breaker.
var cbpFail = errors.New("cbp: transient failure")

func cbpOK(context.Context) error   { return nil }
func cbpBoom(context.Context) error { return cbpFail }

func cbpInvoke(cb *invoker.CircuitBreaker, fn func(context.Context) error) error {
	return cb.Invoke(context.Background(), &testEvent{}, "handler", fn)
}

// TestCircuitBreaker_HalfOpenOneProbe: 200 concurrent callers hit a half-open
// breaker; exactly one runs next, 199 get ErrCircuitOpen.
func TestCircuitBreaker_HalfOpenOneProbe(t *testing.T) {
	// The lease equals OpenTimeout, so it is generous: every arrival must land
	// well inside it even under -race on a loaded machine.
	const openTimeout = 150 * time.Millisecond
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      openTimeout,
	}, nil)

	if err := cbpInvoke(cb, cbpBoom); !errors.Is(err, cbpFail) {
		t.Fatalf("opening call: got %v", err)
	}
	time.Sleep(openTimeout + 20*time.Millisecond)

	const callers = 200
	var (
		start    = make(chan struct{})
		release  = make(chan struct{})
		inflight atomic.Int64
		maxSeen  atomic.Int64
		admitted atomic.Int64
		blocked  atomic.Int64
		wg       sync.WaitGroup
	)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := cbpInvoke(cb, func(context.Context) error {
				admitted.Add(1)
				cur := inflight.Add(1)
				for {
					prev := maxSeen.Load()
					if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
						break
					}
				}
				<-release
				inflight.Add(-1)
				return nil
			})
			if errors.Is(err, invoker.ErrCircuitOpen) {
				blocked.Add(1)
			}
		}()
	}
	close(start)

	deadline := time.Now().Add(5 * time.Second)
	for blocked.Load() < callers-1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()

	if got := admitted.Load(); got != 1 {
		t.Errorf("admitted: got %d, want 1", got)
	}
	if got := blocked.Load(); got != callers-1 {
		t.Errorf("blocked: got %d, want %d", got, callers-1)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Errorf("max concurrency inside next: got %d, want 1", got)
	}
	if err := cbpInvoke(cb, cbpOK); err != nil {
		t.Errorf("breaker should be closed after the successful probe, got %v", err)
	}
}

// TestCircuitBreaker_StaleGenerationIgnored: a completion that started in an
// earlier generation never changes the breaker.
func TestCircuitBreaker_StaleGenerationIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
		}, nil)

		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			_ = cbpInvoke(cb, func(context.Context) error {
				close(started)
				<-release
				return cbpFail
			})
		}()
		<-started

		// B fails and opens the breaker.
		_ = cbpInvoke(cb, cbpBoom)
		if err := cbpInvoke(cb, cbpOK); !errors.Is(err, invoker.ErrCircuitOpen) {
			t.Fatalf("breaker should be open, got %v", err)
		}

		// Probe C succeeds and closes it.
		time.Sleep(2 * time.Second)
		if err := cbpInvoke(cb, cbpOK); err != nil {
			t.Fatalf("probe: %v", err)
		}

		// A finally returns its failure: stale, ignored.
		close(release)
		<-finished

		called := false
		err := cbpInvoke(cb, func(context.Context) error {
			called = true
			return nil
		})
		if err != nil || !called {
			t.Errorf("stale failure reopened the breaker: err=%v called=%v", err, called)
		}
	})
}
