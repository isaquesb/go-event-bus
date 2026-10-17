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

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
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
	_ event.Event,
	handler string,
	next func(context.Context) error,
) error {
	var lastErr error

	// A policy below 1 still runs the handler once, so a FailureError never
	// carries a nil cause.
	maxAttempts := max(r.policy.MaxAttempts, 1)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := next(ctx)
		if err == nil {
			return nil
		}

		retry, terminal := classify(err)

		if terminal {
			return &event.FailureError{Cause: err, Reason: event.ReasonTerminal, Attempts: attempt}
		}

		if !retry {
			return err
		}

		lastErr = err

		if attempt == maxAttempts {
			break
		}

		delay := backoff(attempt-1, r.policy.BaseDelay, r.policy.MaxDelay)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return &event.FailureError{
		Cause:    lastErr,
		Reason:   event.ReasonRetriesExhausted,
		Attempts: maxAttempts,
	}
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
