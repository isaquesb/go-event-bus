package event_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	event "github.com/isaquesb/go-event-bus"
)

// obPermanentErr is a permanent error (Terminal() true), shaped like
// invoker.PermanentError, which root tests cannot import.
type obPermanentErr struct{}

func (obPermanentErr) Error() string  { return "ob permanent" }
func (obPermanentErr) Terminal() bool { return true }

func obCancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// obHandlerTimedOut returns a context whose deadline passed with the
// ErrHandlerTimeout cause, the way a transport builds the handler context.
func obHandlerTimedOut() context.Context {
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Nanosecond, event.ErrHandlerTimeout)
	<-ctx.Done()
	cancel()
	return ctx
}

func TestOutcomeOf_TruthTable(t *testing.T) {
	plain := errors.New("boom")
	rows := []struct {
		name      string
		ctx       func() context.Context
		err       error
		nonFinal  event.Outcome
		finalWant event.Outcome
	}{
		{"nil", context.Background, nil, event.OutcomeOK, event.OutcomeOK},
		{"dead_lettered", context.Background, fmt.Errorf("w: %w", &event.DeadLetteredError{Cause: plain, Reason: event.ReasonTerminal}),
			event.OutcomeDeadLettered, event.OutcomeDeadLettered},
		{"dead_lettered_circuit_open", context.Background,
			&event.DeadLetteredError{Cause: event.ErrCircuitOpen, Reason: event.ReasonCircuitOpen},
			event.OutcomeDeadLettered, event.OutcomeDeadLettered},
		{"duplicate", context.Background, fmt.Errorf("w: %w", event.ErrDuplicate), event.OutcomeDuplicate, event.OutcomeDuplicate},
		{"in_progress", context.Background, event.ErrInProgress, event.OutcomeInProgress, event.OutcomeInProgress},
		{"rate_limited", context.Background, fmt.Errorf("w: %w", event.ErrRateLimited), event.OutcomeRateLimited, event.OutcomeRateLimited},
		{"circuit_open", context.Background, event.ErrCircuitOpen, event.OutcomeCircuitOpen, event.OutcomeCircuitOpen},
		{"cancelled_ctx", obCancelled, context.Canceled, event.OutcomeInterrupted, event.OutcomeInterrupted},
		{"shutdown_interrupted", context.Background, fmt.Errorf("w: %w", event.ErrShutdownInterrupted),
			event.OutcomeInterrupted, event.OutcomeInterrupted},
		{"handler_timeout", obHandlerTimedOut, context.DeadlineExceeded, event.OutcomeRetry, event.OutcomeFailed},
		{"handler_timeout_wrapped", obHandlerTimedOut, fmt.Errorf("%w: %w", event.ErrHandlerTimeout, context.DeadlineExceeded),
			event.OutcomeRetry, event.OutcomeFailed},
		{"plain", context.Background, plain, event.OutcomeRetry, event.OutcomeFailed},
		{"terminal_failure_error", context.Background,
			&event.FailureError{Cause: plain, Reason: event.ReasonTerminal, Attempts: 1}, event.OutcomeFailed, event.OutcomeFailed},
		{"permanent", context.Background, obPermanentErr{}, event.OutcomeFailed, event.OutcomeFailed},
		{"dead_letter_failed", context.Background,
			&event.DeadLetterFailedError{Cause: plain, Reason: event.ReasonRetriesExhausted, PublishErr: errors.New("pub")},
			event.OutcomeRetry, event.OutcomeFailed},
		{"panic", context.Background, &event.PanicError{Value: "x"}, event.OutcomeFailed, event.OutcomeFailed},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			nonFinal := event.WithDelivery(row.ctx(), event.Delivery{Count: 1, Max: 3})
			final := event.WithDelivery(row.ctx(), event.Delivery{Count: 3, Max: 3})
			if got := event.OutcomeOf(nonFinal, row.err); got != row.nonFinal {
				t.Errorf("non-final: OutcomeOf = %q, want %q", got, row.nonFinal)
			}
			if got := event.OutcomeOf(final, row.err); got != row.finalWant {
				t.Errorf("final: OutcomeOf = %q, want %q", got, row.finalWant)
			}
		})
	}

	t.Run("no_delivery_in_ctx_is_final", func(t *testing.T) {
		if got := event.OutcomeOf(context.Background(), errors.New("boom")); got != event.OutcomeFailed {
			t.Fatalf("OutcomeOf without a Delivery = %q, want %q", got, event.OutcomeFailed)
		}
	})
}

func TestFailingInvokerOf(t *testing.T) {
	plain := errors.New("boom")
	rows := []struct {
		name string
		err  error
		want event.FailingInvoker
	}{
		{"circuit_open", event.ErrCircuitOpen, event.InvokerCircuitBreaker},
		{"circuit_open_wrapped", fmt.Errorf("w: %w", event.ErrCircuitOpen), event.InvokerCircuitBreaker},
		{"circuit_open_dead_lettered", &event.DeadLetteredError{Cause: event.ErrCircuitOpen, Reason: event.ReasonCircuitOpen}, event.InvokerCircuitBreaker},
		{"dead_lettered", &event.DeadLetteredError{Cause: plain, Reason: event.ReasonTerminal}, event.InvokerDLQ},
		{"dead_letter_failed", &event.DeadLetterFailedError{Cause: plain, PublishErr: plain}, event.InvokerDLQ},
		{"failure_error", &event.FailureError{Cause: plain, Reason: event.ReasonRetriesExhausted, Attempts: 3}, event.InvokerRetry},
		{"failure_error_wrapped", fmt.Errorf("w: %w", &event.FailureError{Cause: plain}), event.InvokerRetry},
		{"plain", plain, event.InvokerTransport},
		{"nil", nil, event.InvokerTransport},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if got := event.FailingInvokerOf(row.err); got != row.want {
				t.Fatalf("FailingInvokerOf = %q, want %q", got, row.want)
			}
		})
	}
}

func TestAttemptClassOf(t *testing.T) {
	plain := errors.New("boom")
	rows := []struct {
		name string
		ctx  context.Context
		err  error
		want event.AttemptClass
	}{
		{"panic", context.Background(), &event.PanicError{Value: "x"}, event.AttemptPanic},
		{"handler_timeout", context.Background(), event.ErrHandlerTimeout, event.AttemptTimeout},
		{"deadline_exceeded", context.Background(), context.DeadlineExceeded, event.AttemptTimeout},
		{"canceled_with_timeout_cause", obHandlerTimedOut(), context.Canceled, event.AttemptTimeout},
		{"canceled_plain_ctx", obCancelled(), context.Canceled, event.AttemptTransient},
		{"send_to_dlq", context.Background(), fmt.Errorf("w: %w", event.ErrSendToDLQ), event.AttemptTerminal},
		{"permanent", context.Background(), obPermanentErr{}, event.AttemptTerminal},
		{"plain", context.Background(), plain, event.AttemptTransient},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if got := event.AttemptClassOf(row.ctx, row.err); got != row.want {
				t.Fatalf("AttemptClassOf = %q, want %q", got, row.want)
			}
		})
	}
}

func TestNewAttempt_TruncatesRuneSafe(t *testing.T) {
	started := time.Unix(1700000000, 0).UTC()

	long := errors.New(strings.Repeat("x", 1000))
	a := event.NewAttempt(2, long, event.AttemptTransient, started, 3*time.Second, time.Second)
	if len(a.Err) > event.AttemptErrLimit {
		t.Fatalf("len(Err) = %d, want at most %d", len(a.Err), event.AttemptErrLimit)
	}
	if !strings.HasSuffix(a.Err, event.TruncationMarker) {
		t.Fatalf("Err %q does not end with the truncation marker", a.Err)
	}
	if a.N != 2 || a.Class != event.AttemptTransient || !a.Started.Equal(started) ||
		a.Duration != 3*time.Second || a.Delay != time.Second {
		t.Fatalf("Attempt fields not copied: %+v", a)
	}

	wide := errors.New(strings.Repeat("€", 400)) // 1200 bytes of 3-byte runes
	a = event.NewAttempt(1, wide, event.AttemptTerminal, started, 0, 0)
	if len(a.Err) > event.AttemptErrLimit {
		t.Fatalf("len(Err) = %d, want at most %d", len(a.Err), event.AttemptErrLimit)
	}
	if !utf8.ValidString(a.Err) {
		t.Fatalf("Err is not valid UTF-8: %q", a.Err)
	}
	if !strings.HasSuffix(a.Err, event.TruncationMarker) {
		t.Fatalf("Err %q does not end with the truncation marker", a.Err)
	}

	if a := event.NewAttempt(1, nil, event.AttemptTransient, started, 0, 0); a.Err != "" {
		t.Fatalf("nil error gave Err %q, want empty", a.Err)
	}
	if a := event.NewAttempt(1, errors.New("short"), event.AttemptTransient, started, 0, 0); a.Err != "short" {
		t.Fatalf("short error gave Err %q, want unchanged", a.Err)
	}
	if a := event.NewAttempt(1, errors.New("a\nb"), event.AttemptTransient, started, 0, 0); a.Err != "a b" {
		t.Fatalf("control character not replaced: %q", a.Err)
	}
}

func TestFailureScope_Lifecycle(t *testing.T) {
	ctx, scope := event.WithFailureScope(context.Background())
	if got, ok := event.FailureScopeFromContext(ctx); !ok || got != scope {
		t.Fatalf("FailureScopeFromContext = %v, %v, want the installed scope", got, ok)
	}
	if _, ok := event.FailureScopeFromContext(context.Background()); ok {
		t.Fatal("FailureScopeFromContext found a scope in a bare context")
	}

	// breaker round trip
	if scope.Breaker() != "" {
		t.Fatalf("fresh scope Breaker = %q, want empty", scope.Breaker())
	}
	scope.SetBreaker(event.BreakerHalfOpen)
	if scope.Breaker() != event.BreakerHalfOpen {
		t.Fatalf("Breaker = %q, want %q", scope.Breaker(), event.BreakerHalfOpen)
	}

	// nil receiver never panics
	var nilScope *event.FailureScope
	nilScope.SetBreaker(event.BreakerOpen)
	if nilScope.Breaker() != "" {
		t.Fatalf("nil scope Breaker = %q, want empty", nilScope.Breaker())
	}
	if !event.MarkReported(nilScope) {
		t.Fatal("nil scope MarkReported = false, want true")
	}
	event.FlushHandlerDone(ctx, nilScope, event.OutcomeOK)
	if event.DeferHandlerDone(nilScope, event.HandlerResult{}) {
		t.Fatal("nil scope DeferHandlerDone = true, want false")
	}

	// exactly-once guard
	if !event.MarkReported(scope) {
		t.Fatal("first MarkReported = false, want true")
	}
	if event.MarkReported(scope) {
		t.Fatal("second MarkReported = true, want false")
	}

	// defer and flush once, through the observer found in ctx
	rec := &obsRecorder{}
	octx := event.WithObserver(ctx, rec)
	event.FlushHandlerDone(octx, scope, event.OutcomeOK) // nothing pending yet
	if len(rec.HandlerDones()) != 0 {
		t.Fatal("flush without a pending result emitted a signal")
	}
	pending := event.HandlerResult{Handler: "h", Event: "e", Outcome: event.OutcomeFailed, Duration: time.Second}
	if !event.DeferHandlerDone(scope, pending) {
		t.Fatal("DeferHandlerDone on a transport scope = false, want true")
	}
	if event.DeferHandlerDone(scope, pending) {
		t.Fatal("second DeferHandlerDone = true, want false")
	}
	if len(rec.HandlerDones()) != 0 {
		t.Fatal("deferred result was emitted before the flush")
	}
	event.FlushHandlerDone(octx, scope, event.OutcomeDeadLettered)
	got := rec.HandlerDones()
	if len(got) != 1 {
		t.Fatalf("HandlerDone signals = %d, want 1", len(got))
	}
	if got[0].Outcome != event.OutcomeDeadLettered || got[0].Handler != "h" || got[0].Duration != time.Second {
		t.Fatalf("flushed result = %+v", got[0])
	}
	event.FlushHandlerDone(octx, scope, event.OutcomeFailed)
	if n := len(rec.HandlerDones()); n != 1 {
		t.Fatalf("HandlerDone signals after a second flush = %d, want 1", n)
	}

	// a detached scope refuses to defer
	dctx, detached := event.NewDetachedScope(context.Background())
	if got, ok := event.FailureScopeFromContext(dctx); !ok || got != detached {
		t.Fatal("detached scope not installed in the returned context")
	}
	if event.DeferHandlerDone(detached, pending) {
		t.Fatal("detached scope accepted a deferred result")
	}
	detached.SetBreaker(event.BreakerClosed)
	if detached.Breaker() != event.BreakerClosed {
		t.Fatal("detached scope lost the breaker state")
	}
}

func TestFailureError_HistoryKeepsMatching(t *testing.T) {
	cause := errors.New("boom")
	history := []event.Attempt{{N: 2, Err: "boom", Class: event.AttemptTransient}, {N: 3, Err: "boom", Class: event.AttemptTerminal}}
	for _, row := range []struct {
		reason   string
		sentinel error
	}{
		{event.ReasonTerminal, event.ErrSendToDLQ},
		{event.ReasonRetriesExhausted, event.ErrRetriesExhausted},
	} {
		fe := &event.FailureError{Cause: cause, Reason: row.reason, Attempts: 3, History: history}
		wrapped := fmt.Errorf("w: %w", fe)

		var found *event.FailureError
		if !errors.As(wrapped, &found) || found.Attempts != 3 || len(found.History) != 2 {
			t.Fatalf("reason %s: errors.As did not recover the FailureError: %+v", row.reason, found)
		}
		if !errors.Is(wrapped, cause) {
			t.Errorf("reason %s: errors.Is lost the cause", row.reason)
		}
		if !errors.Is(wrapped, row.sentinel) {
			t.Errorf("reason %s: errors.Is lost the reason sentinel", row.reason)
		}
		if got := event.AttemptsOf(wrapped); got != 3 {
			t.Errorf("reason %s: AttemptsOf = %d, want 3", row.reason, got)
		}
		if got, want := fe.Error(), "event: "+row.reason+": boom"; got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	}
}

func TestRedactDelivery_DropsRaw(t *testing.T) {
	in := event.Delivery{
		Count:   2,
		Max:     5,
		Subject: "orders.created",
		Raw:     []byte("payload"),
		Headers: map[string][]string{"k": {"v"}},
		System:  "nats",
	}
	out := event.RedactDelivery(in)
	if out.Raw != nil {
		t.Fatalf("Raw = %q, want nil", out.Raw)
	}
	if out.Subject != "orders.created" || out.Count != 2 || out.Max != 5 || out.System != "nats" {
		t.Fatalf("scalar fields changed: %+v", out)
	}
	if got := out.Headers["k"]; len(got) != 1 || got[0] != "v" {
		t.Fatalf("Headers = %v, want kept", out.Headers)
	}
	if string(in.Raw) != "payload" {
		t.Fatal("RedactDelivery mutated the input")
	}
}

func TestOutcome_NewValues(t *testing.T) {
	for want, got := range map[string]event.Outcome{
		"ok":            event.OutcomeOK,
		"retry":         event.OutcomeRetry,
		"duplicate":     event.OutcomeDuplicate,
		"in_progress":   event.OutcomeInProgress,
		"rate_limited":  event.OutcomeRateLimited,
		"circuit_open":  event.OutcomeCircuitOpen,
		"failed":        event.OutcomeFailed,
		"decode_failed": event.OutcomeDecodeFailed,
	} {
		if string(got) != want {
			t.Errorf("outcome = %q, want %q", got, want)
		}
	}
	// the Phase 8/9 constants keep their snake_case values
	for want, got := range map[string]event.Outcome{
		"dead_lettered":      event.OutcomeDeadLettered,
		"dlq_publish_failed": event.OutcomeDLQPublishFailed,
		"dropped":            event.OutcomeDropped,
		"interrupted":        event.OutcomeInterrupted,
		"rejected":           event.OutcomeRejected,
	} {
		if string(got) != want {
			t.Errorf("outcome = %q, want %q", got, want)
		}
	}
}

func TestTraceHeaderNames(t *testing.T) {
	for want, got := range map[string]string{
		"Eventbus-Dlq-Traceparent":           event.HeaderDLQTraceparent,
		"Eventbus-Replay-Traceparent":        event.HeaderReplayTraceparent,
		"Eventbus-Replay-Failed-Traceparent": event.HeaderReplayFailedTraceparent,
		"Eventbus-Replay-Dlq-Sequence":       event.HeaderReplayDLQSequence,
	} {
		if got != want {
			t.Errorf("header = %q, want %q", got, want)
		}
	}
}
