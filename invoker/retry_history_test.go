package invoker_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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
