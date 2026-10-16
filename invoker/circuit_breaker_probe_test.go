package invoker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/isaquesb/go-event-bus"
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

// cbpNoRetry has only Retryable() false: it is not terminal, so it still
// counts toward opening (D-12).
type cbpNoRetry struct{}

func (cbpNoRetry) Error() string   { return "cbp: not retryable" }
func (cbpNoRetry) Retryable() bool { return false }

// cbpRow is one classification input. ctx builds the context the call runs
// under (nil means context.Background) and err builds the returned error.
type cbpRow struct {
	name    string
	counted bool
	ctx     func() (context.Context, context.CancelFunc)
	err     func(ctx context.Context) error
}

func cbpFixedErr(err error) func(context.Context) error {
	return func(context.Context) error { return err }
}

func cbpRows() []cbpRow {
	timeoutCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeoutCause(context.Background(), 10*time.Millisecond, event.ErrHandlerTimeout)
	}
	cancelledCtx := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	}
	ctxErr := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	return []cbpRow{
		{name: "context.Canceled", err: cbpFixedErr(context.Canceled)},
		{name: "bare DeadlineExceeded on a live ctx", err: cbpFixedErr(context.DeadlineExceeded)},
		{name: "caller cancellation", ctx: cancelledCtx, err: ctxErr},
		{name: "PermanentError", err: cbpFixedErr(invoker.PermanentError{Err: errors.New("bad")})},
		{name: "ErrSendToDLQ", err: cbpFixedErr(event.ErrSendToDLQ)},
		{name: "PanicError wrapping Canceled", err: cbpFixedErr(&event.PanicError{Value: context.Canceled})},
		{name: "ErrDuplicate", err: cbpFixedErr(event.ErrDuplicate)},
		{name: "DeadLetteredError", err: cbpFixedErr(&event.DeadLetteredError{Cause: errors.New("x"), Reason: "terminal"})},
		{name: "ErrInProgress", err: cbpFixedErr(event.ErrInProgress)},
		{name: "ErrRateLimited", err: cbpFixedErr(event.ErrRateLimited)},
		{name: "ErrCircuitOpen", err: cbpFixedErr(event.ErrCircuitOpen)},
		{name: "DeadLetterFailedError", err: cbpFixedErr(&event.DeadLetterFailedError{Cause: errors.New("x")})},

		{name: "unclassified error", counted: true, err: cbpFixedErr(errors.New("transient"))},
		{name: "RetryableError", counted: true, err: cbpFixedErr(invoker.RetryableError{Err: errors.New("again")})},
		{name: "Retryable false only", counted: true, err: cbpFixedErr(cbpNoRetry{})},
		{name: "handler timeout through ctx cause", counted: true, ctx: timeoutCtx, err: ctxErr},
		{name: "error wrapping ErrHandlerTimeout", counted: true, err: cbpFixedErr(fmt.Errorf("wrapped: %w", event.ErrHandlerTimeout))},
	}
}

// cbpRun calls the breaker once with the row's context and error.
func cbpRun(cb *invoker.CircuitBreaker, row cbpRow) {
	ctx := context.Background()
	if row.ctx != nil {
		var cancel context.CancelFunc
		ctx, cancel = row.ctx()
		defer cancel()
	}
	_ = cb.Invoke(ctx, &testEvent{}, "handler", func(c context.Context) error {
		return row.err(c)
	})
}

// cbpFollowUp reports whether a plain successful call is admitted.
func cbpFollowUp(cb *invoker.CircuitBreaker) (admitted bool, err error) {
	err = cbpInvoke(cb, func(context.Context) error {
		admitted = true
		return nil
	})
	return admitted, err
}

func TestCircuitBreaker_ClassificationClosed(t *testing.T) {
	for _, row := range cbpRows() {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
					FailureThreshold: 1,
					SuccessThreshold: 1,
					OpenTimeout:      time.Second,
				}, nil)

				cbpRun(cb, row)

				admitted, err := cbpFollowUp(cb)
				if row.counted {
					if admitted || !errors.Is(err, invoker.ErrCircuitOpen) {
						t.Errorf("counted failure should open the breaker: admitted=%v err=%v", admitted, err)
					}
					return
				}
				if !admitted || err != nil {
					t.Errorf("neutral outcome must not open the breaker: admitted=%v err=%v", admitted, err)
				}
			})
		})
	}
}

func TestCircuitBreaker_ClassificationHalfOpen(t *testing.T) {
	for _, row := range cbpRows() {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
					FailureThreshold: 1,
					SuccessThreshold: 1,
					OpenTimeout:      time.Second,
				}, nil)

				_ = cbpInvoke(cb, cbpBoom)
				time.Sleep(2 * time.Second)

				// The row is the half-open probe.
				cbpRun(cb, row)

				admitted, err := cbpFollowUp(cb)
				if row.counted {
					if admitted || !errors.Is(err, invoker.ErrCircuitOpen) {
						t.Errorf("counted probe failure should reopen: admitted=%v err=%v", admitted, err)
					}
					return
				}
				if !admitted || err != nil {
					t.Errorf("neutral probe should release the slot for the next arrival: admitted=%v err=%v", admitted, err)
				}
			})
		})
	}
}

// cbpHold runs a call that blocks inside next until release is closed, then
// returns result. started closes once next is entered.
func cbpHold(cb *invoker.CircuitBreaker, result error) (started <-chan struct{}, release chan<- struct{}, done <-chan struct{}) {
	st := make(chan struct{})
	rel := make(chan struct{})
	dn := make(chan struct{})
	go func() {
		defer close(dn)
		_ = cbpInvoke(cb, func(context.Context) error {
			close(st)
			<-rel
			return result
		})
	}()
	return st, rel, dn
}

func TestCircuitBreaker_SequentialProbesUntilSuccessThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 3,
			OpenTimeout:      time.Second,
		}, nil)

		_ = cbpInvoke(cb, cbpBoom)
		time.Sleep(2 * time.Second)

		for i := 1; i <= 3; i++ {
			started, release, done := cbpHold(cb, nil)
			<-started
			if err := cbpInvoke(cb, cbpOK); !errors.Is(err, invoker.ErrCircuitOpen) {
				t.Fatalf("probe %d in flight: concurrent arrival got %v, want ErrCircuitOpen", i, err)
			}
			close(release)
			<-done
		}

		// Closed again: concurrent calls are both admitted.
		started, release, done := cbpHold(cb, nil)
		<-started
		if err := cbpInvoke(cb, cbpOK); err != nil {
			t.Errorf("breaker should be closed after %d probes, got %v", 3, err)
		}
		close(release)
		<-done
	})
}

func TestCircuitBreaker_ProbeLeaseSupersedes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
		}, nil)

		_ = cbpInvoke(cb, cbpBoom)
		time.Sleep(2 * time.Second)

		started, release, done := cbpHold(cb, cbpFail)
		<-started

		// Inside the lease the slot stays taken.
		time.Sleep(500 * time.Millisecond)
		if err := cbpInvoke(cb, cbpOK); !errors.Is(err, invoker.ErrCircuitOpen) {
			t.Fatalf("inside the lease: got %v, want ErrCircuitOpen", err)
		}

		// Past the lease the next arrival becomes the probe and closes it.
		time.Sleep(500 * time.Millisecond)
		if err := cbpInvoke(cb, cbpOK); err != nil {
			t.Fatalf("after the lease the arrival should be the new probe: %v", err)
		}

		// The superseded probe fails late: stale, the breaker stays closed.
		close(release)
		<-done
		if admitted, err := cbpFollowUp(cb); !admitted || err != nil {
			t.Errorf("stale probe failure changed the breaker: admitted=%v err=%v", admitted, err)
		}
	})
}

func TestCircuitBreaker_ZeroOpenTimeoutNeverSupersedes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      0,
		}, nil)

		_ = cbpInvoke(cb, cbpBoom)

		// With OpenTimeout 0 the very next call is the half-open probe.
		started, release, done := cbpHold(cb, nil)
		<-started

		time.Sleep(24 * time.Hour)
		if err := cbpInvoke(cb, cbpOK); !errors.Is(err, invoker.ErrCircuitOpen) {
			t.Fatalf("a zero OpenTimeout never expires the lease: got %v", err)
		}

		close(release)
		<-done
		if admitted, err := cbpFollowUp(cb); !admitted || err != nil {
			t.Errorf("probe success should close the breaker: admitted=%v err=%v", admitted, err)
		}
	})
}

func TestCircuitBreaker_PanicInNextReleasesProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
		}, nil)

		_ = cbpInvoke(cb, cbpBoom)
		time.Sleep(2 * time.Second)

		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Error("the panic from next must keep propagating")
				}
			}()
			_ = cbpInvoke(cb, func(context.Context) error { panic("cbp: boom") })
		}()

		if admitted, err := cbpFollowUp(cb); !admitted || err != nil {
			t.Errorf("a panicking probe must release the slot: admitted=%v err=%v", admitted, err)
		}
	})
}
