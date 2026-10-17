package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
	"time"
)

// Retryable false never retry
type Retryable interface {
	Retryable() bool
}

// Terminal true never retry
type Terminal interface {
	Terminal() bool
}
type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }
func (e PermanentError) Retryable() bool {
	return false
}
func (e PermanentError) Terminal() bool {
	return true
}

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string { return e.Err.Error() }
func (e RetryableError) Unwrap() error { return e.Err }
func (e RetryableError) Retryable() bool {
	return true
}

// FailureError is the root type event.FailureError: the failure of a handler
// after the retry policy finished, with the cause, a stable reason and the
// number of attempts.
type FailureError = event.FailureError

// DefaultHistorySize is the retry history bound used when
// RetryPolicy.HistorySize is zero.
const DefaultHistorySize = 5

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration

	// HistorySize bounds the retry history Retry puts in the returned
	// *event.FailureError (and through it in the DLQ record and the failure
	// hook). Zero means DefaultHistorySize; a negative value disables the
	// history, which then allocates nothing. The history holds the last
	// HistorySize failed in-process attempts of this delivery, oldest first, with
	// the error text truncated. Earlier broker deliveries only appear as
	// Delivery.Count, because the in-flight envelope is immutable and the history
	// is never written into it.
	HistorySize int
}

// attemptRing keeps the last size attempts, oldest first. A size of zero or
// less keeps nothing and never allocates.
type attemptRing struct {
	size  int
	items []event.Attempt
}

// newAttemptRing resolves the configured HistorySize: 0 means the default and
// a negative value disables the history.
func newAttemptRing(configured int) attemptRing {
	switch {
	case configured == 0:
		configured = DefaultHistorySize
	case configured < 0:
		configured = 0
	}
	return attemptRing{size: configured}
}

func (g *attemptRing) enabled() bool { return g.size > 0 }

// add appends a, dropping the oldest entry when the ring is full.
func (g *attemptRing) add(a event.Attempt) {
	if g.size <= 0 {
		return
	}
	if len(g.items) == g.size {
		copy(g.items, g.items[1:])
		g.items[len(g.items)-1] = a
		return
	}
	g.items = append(g.items, a)
}

func classify(err error) (retry bool, terminal bool) {
	if err == nil {
		return false, false
	}

	if errors.Is(err, ErrDuplicate) {
		return false, false
	}

	if errors.Is(err, ErrRateLimited) {
		return false, false
	}

	if errors.Is(err, ErrCircuitOpen) {
		return false, false
	}

	// Another execution is still running: the transport redelivers later.
	if errors.Is(err, event.ErrInProgress) {
		return false, false
	}

	// A dead-lettered result is final and is never retried.
	if errors.Is(err, event.ErrDeadLettered) ||
		errors.Is(err, event.ErrDeadLetterFailed) {
		return false, false
	}

	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false, false
	}

	var t Terminal
	if errors.As(err, &t) && t.Terminal() {
		return false, true
	}

	var r Retryable
	if errors.As(err, &r) {
		return r.Retryable(), false
	}

	// default: retryable
	return true, false
}

func backoff(attempt int, base, max time.Duration) time.Duration {
	d := time.Duration(1<<attempt) * base
	if d > max {
		return max
	}
	return d
}

func NewRetry(p RetryPolicy) *Retry {
	return &Retry{policy: p}
}

type Retry struct {
	policy RetryPolicy
}

func (r *Retry) Invoke(
	ctx context.Context,
	evt event.Event,
	handler string,
	next func(context.Context) error,
) error {
	var lastErr error

	// A policy below 1 still runs the handler once, so a FailureError never
	// carries a nil cause.
	maxAttempts := max(r.policy.MaxAttempts, 1)
	hist := newAttemptRing(r.policy.HistorySize)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		started := time.Now()
		err := next(ctx)
		if err == nil {
			return nil
		}
		took := time.Since(started)

		retry, terminal := classify(err)

		if terminal {
			if hist.enabled() {
				hist.add(event.NewAttempt(attempt, err, event.AttemptClassOf(ctx, err), started, took, 0))
			}
			return &event.FailureError{Cause: err, Reason: event.ReasonTerminal, Attempts: attempt, History: hist.items}
		}

		// Flow-control, dead-lettered and context errors are returned unchanged
		// and are not attempts of the retry policy. The one exception is a
		// circuit that opens after real attempts ran: the rejected call is not an
		// attempt and stays out of the history, but the attempts that did run
		// are kept as a FailureError{circuit_open}, which still matches
		// ErrCircuitOpen, so the DLQ record tells how many handler runs preceded
		// the open circuit. On the first call nothing ran, and the error is
		// returned unchanged.
		if !retry {
			if attempt > 1 && errors.Is(err, event.ErrCircuitOpen) {
				return &event.FailureError{Cause: err, Reason: event.ReasonCircuitOpen, Attempts: attempt - 1, History: hist.items}
			}
			return err
		}

		lastErr = err
		class := event.AttemptClassOf(ctx, err)

		if attempt == maxAttempts {
			if hist.enabled() {
				hist.add(event.NewAttempt(attempt, err, class, started, took, 0))
			}
			break
		}

		delay := backoff(attempt-1, r.policy.BaseDelay, r.policy.MaxDelay)

		event.ObserverFromContext(ctx).RetryAttempt(ctx, event.RetryAttemptInfo{
			Handler: handler,
			Event:   eventName(evt),
			Attempt: attempt,
			Class:   class,
			Err:     err,
			Delay:   delay,
			At:      started.Add(took),
		})
		if hist.enabled() {
			hist.add(event.NewAttempt(attempt, err, class, started, took, delay))
		}

		if err := wait(ctx, delay); err != nil {
			return err
		}
	}

	return &event.FailureError{
		Cause:    lastErr,
		Reason:   event.ReasonRetriesExhausted,
		Attempts: maxAttempts,
		History:  hist.items,
	}
}

// wait sleeps for d, or returns the context error when ctx ends first. A zero
// delay does not allocate a timer; it only honors cancellation.
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func eventName(evt event.Event) string {
	if evt == nil {
		return ""
	}
	return evt.Name()
}

/*
// Example Usage
func (h *CreateUserHandler) Handle(
	ctx context.Context,
	evt event.Event,
) error {
	e := evt.(*UserCreated)

	if e.Email == "" {
		return event.PermanentError{
			Err: errors.New("email is required"),
		}
	}

	if err := h.repo.Save(ctx, e); err != nil {
		return event.RetryableError{Err: err}
	}

	return nil
}
*/
