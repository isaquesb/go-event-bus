package invoker_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

type rhEvent struct{}

func (rhEvent) Name() string { return "rh.order" }

// rhFailing returns a handler that fails with err on every call and counts
// the calls.
func rhFailing(calls *int, err error) func(context.Context) error {
	return func(context.Context) error {
		*calls++
		return err
	}
}

// rhFailureOf extracts the *event.FailureError carried by err.
func rhFailureOf(t *testing.T, err error) *event.FailureError {
	t.Helper()
	var fe *event.FailureError
	if !errors.As(err, &fe) {
		t.Fatalf("expected a *event.FailureError, got %T: %v", err, err)
	}
	return fe
}

func TestRetryHistory_ChainToDLQHeader(t *testing.T) {
	rec := &invRecorder{}
	pub := event.NewMemoryDLQ()
	chain := invoker.NewChain(
		invoker.NewDLQ(pub),
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    2 * time.Millisecond,
		}),
	)
	ctx := event.WithObserver(
		event.WithDelivery(context.Background(), event.Delivery{Count: 1, Max: 1, Subject: "rh.subject"}),
		rec,
	)

	calls := 0
	err := event.InvokeHandler(ctx, chain, rhEvent{}, "h", rhFailing(&calls, errors.New("db down")))

	if !errors.Is(err, event.ErrDeadLettered) {
		t.Fatalf("expected ErrDeadLettered, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("handler calls = %d, want 3", calls)
	}

	fe := rhFailureOf(t, err)
	if fe.Attempts != 3 || len(fe.History) != 3 {
		t.Fatalf("Attempts=%d len(History)=%d, want 3 and 3", fe.Attempts, len(fe.History))
	}
	for i, a := range fe.History {
		if a.N != i+1 {
			t.Errorf("History[%d].N = %d, want %d", i, a.N, i+1)
		}
		if a.Class != event.AttemptTransient || a.Err != "db down" {
			t.Errorf("History[%d] class=%q err=%q, want transient and %q", i, a.Class, a.Err, "db down")
		}
		wantDelay := i < 2
		if (a.Delay != 0) != wantDelay {
			t.Errorf("History[%d].Delay = %v, want non-zero=%v", i, a.Delay, wantDelay)
		}
	}

	recs := pub.Records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 DLQ record, got %d", len(recs))
	}
	if len(recs[0].History) != 3 {
		t.Fatalf("record History length = %d, want 3", len(recs[0].History))
	}

	hdr := recs[0].FailureHeaders()[event.HeaderDLQHistory]
	var entries []struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal([]byte(hdr), &entries); err != nil {
		t.Fatalf("history header %q is not JSON: %v", hdr, err)
	}
	if len(entries) != 3 || entries[0].N != 1 || entries[1].N != 2 || entries[2].N != 3 {
		t.Errorf("history header entries = %+v, want n 1, 2, 3", entries)
	}

	retries := rec.Retries()
	if len(retries) != 2 {
		t.Fatalf("RetryAttempt signals = %d, want 2", len(retries))
	}
	for i, c := range retries {
		if c.Val.Attempt != i+1 || c.Val.Delay <= 0 || c.Val.Class != event.AttemptTransient {
			t.Errorf("retry[%d] = %+v, want attempt %d, positive delay, transient", i, c.Val, i+1)
		}
		if c.Val.Handler != "h" || c.Val.Event != "rh.order" {
			t.Errorf("retry[%d] handler=%q event=%q, want h and rh.order", i, c.Val.Handler, c.Val.Event)
		}
	}
}

// rhFast is a policy whose backoff is negligible, so the suite stays fast.
func rhFast(maxAttempts, historySize int) invoker.RetryPolicy {
	return invoker.RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Microsecond,
		MaxDelay:    time.Microsecond,
		HistorySize: historySize,
	}
}

// rhNs lists the attempt numbers of a history.
func rhNs(h []event.Attempt) []int {
	out := make([]int, len(h))
	for i, a := range h {
		out[i] = a.N
	}
	return out
}

func rhEqualInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRetryHistory_DefaultSizeFive(t *testing.T) {
	calls := 0
	err := invoker.NewRetry(rhFast(8, 0)).Invoke(context.Background(), rhEvent{}, "h",
		rhFailing(&calls, errors.New("boom")))

	fe := rhFailureOf(t, err)
	if fe.Attempts != 8 {
		t.Errorf("Attempts = %d, want the true total 8", fe.Attempts)
	}
	if got, want := rhNs(fe.History), []int{4, 5, 6, 7, 8}; !rhEqualInts(got, want) {
		t.Errorf("History N = %v, want %v", got, want)
	}
	if invoker.DefaultHistorySize != 5 {
		t.Errorf("DefaultHistorySize = %d, want 5", invoker.DefaultHistorySize)
	}
}

func TestRetryHistory_ExplicitSize(t *testing.T) {
	calls := 0
	err := invoker.NewRetry(rhFast(4, 2)).Invoke(context.Background(), rhEvent{}, "h",
		rhFailing(&calls, errors.New("boom")))

	fe := rhFailureOf(t, err)
	if fe.Attempts != 4 {
		t.Errorf("Attempts = %d, want 4", fe.Attempts)
	}
	if got, want := rhNs(fe.History), []int{3, 4}; !rhEqualInts(got, want) {
		t.Errorf("History N = %v, want %v", got, want)
	}
	if last := fe.History[len(fe.History)-1]; last.Delay != 0 {
		t.Errorf("last attempt Delay = %v, want 0", last.Delay)
	}
}

func TestRetryHistory_DisabledNoAlloc(t *testing.T) {
	failure := errors.New("boom")
	fn := func(context.Context) error { return failure }
	ctx := context.Background()

	// BaseDelay 0 waits without a timer. What remains per attempt is the error
	// classification (errors.As targets), a constant that does not depend on the
	// history; the history itself must add nothing on top of it.
	allocs := func(maxAttempts, historySize int) float64 {
		r := invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: maxAttempts, HistorySize: historySize})
		return testing.AllocsPerRun(50, func() { _ = r.Invoke(ctx, rhEvent{}, "h", fn) })
	}

	err := invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 8, HistorySize: -1}).Invoke(ctx, rhEvent{}, "h", fn)
	if fe := rhFailureOf(t, err); fe.History != nil || fe.Attempts != 8 {
		t.Errorf("disabled history = %v attempts = %d, want nil and 8", fe.History, fe.Attempts)
	}

	a1, a2, a8 := allocs(1, -1), allocs(2, -1), allocs(8, -1)
	perAttempt := a2 - a1
	if a8-a1 != 7*perAttempt {
		t.Errorf("disabled history allocations are not a constant per attempt: 1 attempt %v, 2 attempts %v, 8 attempts %v", a1, a2, a8)
	}
	if enabled := allocs(8, 0); enabled <= a8 {
		t.Errorf("enabled history should allocate more than disabled: %v vs %v", enabled, a8)
	}
}

func TestRetryHistory_TruncatesRuneSafe(t *testing.T) {
	long := errors.New(strings.Repeat("€", 334)) // 1002 bytes of 3-byte runes
	calls := 0

	err := invoker.NewRetry(rhFast(3, 0)).Invoke(context.Background(), rhEvent{}, "h", rhFailing(&calls, long))

	fe := rhFailureOf(t, err)
	if len(fe.History) != 3 {
		t.Fatalf("History length = %d, want 3", len(fe.History))
	}
	for i, a := range fe.History {
		if len(a.Err) > event.AttemptErrLimit {
			t.Errorf("History[%d].Err is %d bytes, want <= %d", i, len(a.Err), event.AttemptErrLimit)
		}
		if !utf8.ValidString(a.Err) {
			t.Errorf("History[%d].Err is not valid UTF-8", i)
		}
		if !strings.HasSuffix(a.Err, event.TruncationMarker) {
			t.Errorf("History[%d].Err does not end with the truncation marker", i)
		}
	}
	// The cause itself is never truncated.
	if fe.Cause.Error() != long.Error() {
		t.Errorf("Cause was altered")
	}
}

func TestRetryHistory_PanicClass(t *testing.T) {
	rec := &invRecorder{}
	chain := invoker.NewChain(invoker.NewRetry(rhFast(3, 0)))
	ctx := event.WithObserver(context.Background(), rec)

	calls := 0
	err := event.InvokeHandler(ctx, chain, rhEvent{}, "h", func(context.Context) error {
		calls++
		panic("kaboom")
	})

	if calls != 1 {
		t.Errorf("handler calls = %d, want 1 (a panic is terminal)", calls)
	}
	fe := rhFailureOf(t, err)
	if fe.Reason != event.ReasonTerminal || fe.Attempts != 1 || len(fe.History) != 1 {
		t.Fatalf("reason=%q attempts=%d history=%d, want terminal, 1, 1", fe.Reason, fe.Attempts, len(fe.History))
	}
	if fe.History[0].Class != event.AttemptPanic {
		t.Errorf("History[0].Class = %q, want panic", fe.History[0].Class)
	}
	if n := len(rec.Retries()); n != 0 {
		t.Errorf("RetryAttempt signals = %d, want 0 for a terminal failure", n)
	}
}

func TestRetryHistory_TimeoutNotRecorded(t *testing.T) {
	rec := &invRecorder{}
	base, cancel := context.WithTimeoutCause(context.Background(), time.Nanosecond, event.ErrHandlerTimeout)
	defer cancel()
	<-base.Done()
	ctx := event.WithObserver(base, rec)

	calls := 0
	err := invoker.NewRetry(rhFast(3, 0)).Invoke(ctx, rhEvent{}, "h", func(c context.Context) error {
		calls++
		return c.Err()
	})

	var fe *event.FailureError
	if errors.As(err, &fe) {
		t.Fatalf("a context error must be returned unchanged, got a FailureError: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected the context error, got %v", err)
	}
	if calls != 1 {
		t.Errorf("handler calls = %d, want 1", calls)
	}
	if n := len(rec.Retries()); n != 0 {
		t.Errorf("RetryAttempt signals = %d, want 0", n)
	}
}

func TestRetryHistory_TerminalAfterTransient(t *testing.T) {
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	calls := 0

	err := invoker.NewRetry(rhFast(5, 0)).Invoke(ctx, rhEvent{}, "h", func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("flaky")
		}
		return invoker.PermanentError{Err: errors.New("bad input")}
	})

	fe := rhFailureOf(t, err)
	if fe.Reason != event.ReasonTerminal || fe.Attempts != 2 {
		t.Fatalf("reason=%q attempts=%d, want terminal and 2", fe.Reason, fe.Attempts)
	}
	if len(fe.History) != 2 ||
		fe.History[0].Class != event.AttemptTransient || fe.History[1].Class != event.AttemptTerminal {
		t.Fatalf("History = %+v, want transient then terminal", fe.History)
	}
	if fe.History[0].Delay == 0 || fe.History[1].Delay != 0 {
		t.Errorf("delays = %v, %v, want non-zero then zero", fe.History[0].Delay, fe.History[1].Delay)
	}
	if n := len(rec.Retries()); n != 1 {
		t.Errorf("RetryAttempt signals = %d, want 1", n)
	}
}

// First call, no attempt ran: ErrCircuitOpen is returned unchanged.
func TestRetryHistory_FlowControlNoHistory(t *testing.T) {
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	calls := 0

	err := invoker.NewRetry(rhFast(5, 0)).Invoke(ctx, rhEvent{}, "h", rhFailing(&calls, event.ErrCircuitOpen))

	if err != event.ErrCircuitOpen {
		t.Errorf("expected ErrCircuitOpen unchanged, got %v", err)
	}
	var fe *event.FailureError
	if errors.As(err, &fe) {
		t.Errorf("flow-control errors must not become a FailureError")
	}
	if calls != 1 {
		t.Errorf("handler calls = %d, want 1", calls)
	}
	if n := len(rec.Retries()); n != 0 {
		t.Errorf("RetryAttempt signals = %d, want 0", n)
	}
}

// The breaker opens after two real attempts: the error keeps the attempts that
// ran and their history, and the rejected call is neither counted nor recorded.
func TestRetryHistory_CircuitOpensMidRetry(t *testing.T) {
	transient := errors.New("db down")

	for _, tc := range []struct {
		name        string
		historySize int
		wantNs      []int
	}{
		{"history on", 5, []int{1, 2}},
		{"history off", -1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			next := func(context.Context) error {
				calls++
				if calls <= 2 {
					return transient
				}
				return event.ErrCircuitOpen
			}

			err := invoker.NewRetry(rhFast(5, tc.historySize)).Invoke(context.Background(), rhEvent{}, "h", next)

			fe := rhFailureOf(t, err)
			if fe.Attempts != 2 {
				t.Errorf("Attempts = %d, want 2 (the rejected call is not an attempt)", fe.Attempts)
			}
			if got := rhNs(fe.History); !rhEqualInts(got, tc.wantNs) {
				t.Errorf("History numbers = %v, want %v", got, tc.wantNs)
			}
			if tc.wantNs == nil && fe.History != nil {
				t.Errorf("History = %+v, want nil when disabled", fe.History)
			}
			if calls != 3 {
				t.Errorf("handler calls = %d, want 3", calls)
			}
			if !errors.Is(err, event.ErrCircuitOpen) {
				t.Errorf("errors.Is(ErrCircuitOpen) = false for %v", err)
			}
			if errors.Is(err, event.ErrRetriesExhausted) {
				t.Errorf("a mid-retry open circuit must not match ErrRetriesExhausted: %v", err)
			}
			if r := event.ReasonOf(err); r != event.ReasonCircuitOpen {
				t.Errorf("ReasonOf = %q, want circuit_open", r)
			}
			if o := event.OutcomeOf(context.Background(), err); o != event.OutcomeCircuitOpen {
				t.Errorf("OutcomeOf = %q, want circuit_open", o)
			}
		})
	}
}
