package event_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

// soRecord is one captured slog record.
type soRecord struct {
	Level slog.Level
	Msg   string
	Attrs map[string]any
}

// soCapture is a slog.Handler that stores every record it receives.
type soCapture struct {
	mu   *sync.Mutex
	recs *[]soRecord
}

func newSoCapture() soCapture {
	return soCapture{mu: &sync.Mutex{}, recs: &[]soRecord{}}
}

func (c soCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c soCapture) Handle(_ context.Context, r slog.Record) error {
	rec := soRecord{Level: r.Level, Msg: r.Message, Attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[a.Key] = a.Value.Any()
		return true
	})
	c.mu.Lock()
	*c.recs = append(*c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c soCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c soCapture) WithGroup(string) slog.Handler      { return c }

// records returns a copy of the captured records.
func (c soCapture) records() []soRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]soRecord(nil), *c.recs...)
}

// soInstallDefault installs a capturing default logger until the test ends.
func soInstallDefault(t *testing.T) soCapture {
	t.Helper()
	c := newSoCapture()
	prev := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

func TestSlogObserver_ZeroConfigLogsFailureOnce(t *testing.T) {
	capt := soInstallDefault(t)
	md := event.Metadata{ID: "e-1", CorrelationID: "c-1"}
	ctx := event.WithObserver(event.WithMetadata(context.Background(), md), event.MultiObserver(event.NewSlogObserver(nil)))

	event.ObserverFromContext(ctx).Failure(ctx, event.FailureContext{
		Handler:  "billing",
		Outcome:  event.OutcomeDropped,
		Reason:   "terminal",
		Cause:    errors.New("boom"),
		Invoker:  event.InvokerRetry,
		Attempts: 3,
		Delivery: event.Delivery{Count: 2, Max: 5, Subject: "orders.created"},
		Metadata: md,
	})

	recs := capt.records()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1: %+v", len(recs), recs)
	}
	r := recs[0]
	if r.Level != slog.LevelError || r.Msg != "eventbus: message failed" {
		t.Fatalf("level/msg = %v %q", r.Level, r.Msg)
	}
	want := map[string]any{
		"outcome": "dropped", "reason": "terminal", "handler": "billing",
		"event_id": "e-1", "correlation_id": "c-1", "invoker": "retry",
		"err": "boom", "subject": "orders.created",
		"attempts": int64(3), "delivery_count": int64(2), "max_deliver": int64(5),
	}
	for k, v := range want {
		if r.Attrs[k] != v {
			t.Errorf("attr %s = %#v, want %#v", k, r.Attrs[k], v)
		}
	}
	if _, ok := r.Attrs["stack"]; ok {
		t.Errorf("stack attribute present without a panic")
	}
}

func TestSlogObserver_LevelTable(t *testing.T) {
	deadLettered := &event.DeadLetteredError{Cause: errors.New("x"), Reason: event.ReasonTerminal}
	if !errors.Is(deadLettered, event.ErrDeadLettered) {
		t.Fatalf("DeadLetteredError does not match ErrDeadLettered")
	}

	rows := []struct {
		name string
		call func(context.Context, event.Observer)
		want []slog.Level // nil: no record
		msg  string
	}{
		{"failure dead-lettered cause", func(ctx context.Context, o event.Observer) {
			o.Failure(ctx, event.FailureContext{Outcome: event.OutcomeDeadLettered, Cause: deadLettered})
		}, []slog.Level{slog.LevelWarn}, "eventbus: message failed"},
		{"failure rejected", func(ctx context.Context, o event.Observer) {
			o.Failure(ctx, event.FailureContext{Outcome: event.OutcomeRejected, Cause: errors.New("r")})
		}, []slog.Level{slog.LevelWarn}, "eventbus: message failed"},
		{"failure dlq publish failed", func(ctx context.Context, o event.Observer) {
			o.Failure(ctx, event.FailureContext{Outcome: event.OutcomeDLQPublishFailed, Cause: errors.New("d")})
		}, []slog.Level{slog.LevelError}, "eventbus: message failed"},
		{"dead lettered", func(ctx context.Context, o event.Observer) {
			o.DeadLettered(ctx, event.DeadLetterInfo{Handler: "h"})
		}, []slog.Level{slog.LevelInfo}, "eventbus: message dead-lettered"},
		{"decode failed", func(ctx context.Context, o event.Observer) {
			o.DecodeFailed(ctx, event.DecodeFailure{Handler: "h", Err: errors.New("e")})
		}, []slog.Level{slog.LevelWarn}, "eventbus: decode failed"},
		{"dlq publish failed", func(ctx context.Context, o event.Observer) {
			o.DLQPublishFailed(ctx, event.DLQPublishFailure{Handler: "h", Err: errors.New("e")})
		}, []slog.Level{slog.LevelError}, "eventbus: dead-letter publish failed"},
		{"store error", func(ctx context.Context, o event.Observer) {
			o.StoreError(ctx, event.StoreErrorInfo{Handler: "h", Store: "idempotency", Op: "begin", Err: errors.New("e")})
		}, []slog.Level{slog.LevelError}, "eventbus: store error"},
		{"idempotency stale", func(ctx context.Context, o event.Observer) {
			o.IdempotencyStale(ctx, event.IdempotencyStaleInfo{Handler: "h"})
		}, []slog.Level{slog.LevelWarn}, "eventbus: stale idempotency lock"},
		{"internal error", func(ctx context.Context, o event.Observer) {
			o.InternalError(ctx, event.InternalErrorInfo{Component: event.ComponentTransport, Op: event.OpAck, Err: errors.New("e")})
		}, []slog.Level{slog.LevelError}, "eventbus: internal error"},
		{"transition to open", func(ctx context.Context, o event.Observer) {
			o.BreakerTransition(ctx, event.BreakerTransition{Handler: "h", From: event.BreakerClosed, To: event.BreakerOpen})
		}, []slog.Level{slog.LevelWarn}, "eventbus: circuit breaker transition"},
		{"transition to closed", func(ctx context.Context, o event.Observer) {
			o.BreakerTransition(ctx, event.BreakerTransition{Handler: "h", From: event.BreakerHalfOpen, To: event.BreakerClosed})
		}, []slog.Level{slog.LevelInfo}, "eventbus: circuit breaker transition"},
		{"retry attempt", func(ctx context.Context, o event.Observer) {
			o.RetryAttempt(ctx, event.RetryAttemptInfo{Handler: "h", Err: errors.New("e")})
		}, []slog.Level{slog.LevelDebug}, "eventbus: retry scheduled"},
		{"handler done", func(ctx context.Context, o event.Observer) {
			o.HandlerDone(ctx, event.HandlerResult{Handler: "h", Outcome: event.OutcomeOK})
		}, nil, ""},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			capt := newSoCapture()
			obs := event.NewSlogObserver(slog.New(capt))
			row.call(context.Background(), obs)
			recs := capt.records()
			if len(recs) != len(row.want) {
				t.Fatalf("got %d records, want %d: %+v", len(recs), len(row.want), recs)
			}
			for i, lvl := range row.want {
				if recs[i].Level != lvl {
					t.Errorf("level = %v, want %v", recs[i].Level, lvl)
				}
				if recs[i].Msg != row.msg {
					t.Errorf("msg = %q, want %q", recs[i].Msg, row.msg)
				}
			}
		})
	}
}

func TestSlogObserver_PanicStackAttribute(t *testing.T) {
	capt := newSoCapture()
	obs := event.NewSlogObserver(slog.New(capt))
	cause := &event.FailureError{
		Cause:  &event.PanicError{Value: "x", Stack: []byte("goroutine 1 [running]")},
		Reason: event.ReasonTerminal,
	}
	obs.Failure(context.Background(), event.FailureContext{Handler: "h", Outcome: event.OutcomeDropped, Cause: cause})

	recs := capt.records()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	stack, _ := recs[0].Attrs["stack"].(string)
	if !strings.Contains(stack, "goroutine 1") {
		t.Fatalf("stack attribute = %q, want it to contain the panic stack", stack)
	}
}

func TestSlogObserver_ExplicitLogger(t *testing.T) {
	def := soInstallDefault(t)
	capt := newSoCapture()
	obs := event.NewSlogObserver(slog.New(capt))
	obs.DecodeFailed(context.Background(), event.DecodeFailure{Handler: "h", Reason: "bad", Err: errors.New("e")})

	if n := len(capt.records()); n != 1 {
		t.Fatalf("explicit logger got %d records, want 1", n)
	}
	if n := len(def.records()); n != 0 {
		t.Fatalf("default logger got %d records, want 0", n)
	}
}

// soSummaries returns the summary records of recs.
func soSummaries(recs []soRecord) []soRecord {
	var out []soRecord
	for _, r := range recs {
		if r.Msg == "eventbus: repeated log records suppressed" {
			out = append(out, r)
		}
	}
	return out
}

func soCheckSummary(t *testing.T, r soRecord, want map[string]any) {
	t.Helper()
	for k, v := range want {
		if r.Attrs[k] != v {
			t.Errorf("summary attr %s = %#v, want %#v", k, r.Attrs[k], v)
		}
	}
}

func TestSlogObserver_DedupWindowSummary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		f := event.FailureContext{Handler: "billing", Outcome: event.OutcomeDeadLettered, Reason: "circuit_open", Cause: errors.New("x")}

		for i := 0; i < 5; i++ {
			obs.Failure(ctx, f)
			time.Sleep(200 * time.Millisecond)
		}
		if n := len(capt.records()); n != 1 {
			t.Fatalf("inside the window got %d records, want 1", n)
		}

		time.Sleep(10 * time.Second)
		synctest.Wait()
		recs := capt.records()
		if len(recs) != 2 {
			t.Fatalf("after the window got %d records, want 2: %+v", len(recs), recs)
		}
		sums := soSummaries(recs)
		if len(sums) != 1 {
			t.Fatalf("got %d summaries, want 1", len(sums))
		}
		soCheckSummary(t, sums[0], map[string]any{
			"handler": "billing", "signal": "failure", "outcome": "dead_lettered",
			"reason": "circuit_open", "suppressed": int64(4), "window": 10 * time.Second,
		})

		obs.Failure(ctx, f)
		if n := len(capt.records()); n != 3 {
			t.Fatalf("sixth failure after the window: got %d records, want 3", n)
		}
	})
}

func TestSlogObserver_DedupKeysByReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		obs.Failure(ctx, event.FailureContext{Handler: "h", Outcome: event.OutcomeDropped, Reason: "terminal"})
		obs.Failure(ctx, event.FailureContext{Handler: "h", Outcome: event.OutcomeDropped, Reason: "exhausted"})
		if n := len(capt.records()); n != 2 {
			t.Fatalf("got %d records, want 2 (distinct reasons are distinct keys)", n)
		}
		if n := event.SlogDedupKeys(obs); n != 2 {
			t.Fatalf("tracked keys = %d, want 2", n)
		}
		obs.Flush()
	})
}

func TestSlogObserver_TransitionsNeverDeduplicated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		for i := 0; i < 5; i++ {
			obs.BreakerTransition(ctx, event.BreakerTransition{Handler: "h", From: event.BreakerClosed, To: event.BreakerOpen, Seq: uint64(i + 1)})
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		recs := capt.records()
		if len(recs) != 5 {
			t.Fatalf("got %d records, want 5", len(recs))
		}
		if n := len(soSummaries(recs)); n != 0 {
			t.Fatalf("got %d summaries, want 0", n)
		}
		if n := event.SlogDedupKeys(obs); n != 0 {
			t.Fatalf("tracked keys = %d, want 0", n)
		}
	})
}

func TestSlogObserver_LogEveryZeroDisables(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt), event.WithLogEvery(0))
		for i := 0; i < 5; i++ {
			obs.Failure(ctx, event.FailureContext{Handler: "h", Outcome: event.OutcomeDropped, Reason: "terminal"})
		}
		if n := len(capt.records()); n != 5 {
			t.Fatalf("got %d records, want 5", n)
		}
		if n := event.SlogDedupKeys(obs); n != 0 {
			t.Fatalf("tracked keys = %d, want 0 (no deduplication state)", n)
		}
	})
}

func TestSlogObserver_SingleOccurrenceNoSummary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		obs.Failure(context.Background(), event.FailureContext{Handler: "h", Outcome: event.OutcomeDropped, Reason: "terminal"})
		time.Sleep(30 * time.Second)
		synctest.Wait()
		recs := capt.records()
		if len(recs) != 1 || len(soSummaries(recs)) != 0 {
			t.Fatalf("got %d records (%d summaries), want 1 and 0", len(recs), len(soSummaries(recs)))
		}
	})
}

func TestSlogObserver_KeyCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		for i := 0; i < 1100; i++ {
			obs.Failure(ctx, event.FailureContext{Handler: fmt.Sprintf("h-%d", i), Outcome: event.OutcomeDropped, Reason: "terminal"})
		}
		if n := len(capt.records()); n != 1100 {
			t.Fatalf("got %d records, want 1100", n)
		}
		if n := event.SlogDedupKeys(obs); n > 1024 {
			t.Fatalf("tracked keys = %d, want at most 1024", n)
		}

		// Elapsed, unsuppressed entries are swept to make room for new keys.
		time.Sleep(11 * time.Second)
		obs.Failure(ctx, event.FailureContext{Handler: "late", Outcome: event.OutcomeDropped, Reason: "terminal"})
		if n := event.SlogDedupKeys(obs); n < 1 || n > 1024 {
			t.Fatalf("tracked keys after sweep = %d, want 1..1024", n)
		}
		obs.Flush()
	})
}

func TestSlogObserver_FlushEmitsPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		f := event.FailureContext{Handler: "billing", Outcome: event.OutcomeDropped, Reason: "terminal"}
		for i := 0; i < 3; i++ {
			obs.Failure(ctx, f)
		}
		if n := len(capt.records()); n != 1 {
			t.Fatalf("before Flush got %d records, want 1", n)
		}
		obs.Flush()
		recs := capt.records()
		sums := soSummaries(recs)
		if len(recs) != 2 || len(sums) != 1 {
			t.Fatalf("after Flush got %d records (%d summaries), want 2 and 1", len(recs), len(sums))
		}
		soCheckSummary(t, sums[0], map[string]any{"handler": "billing", "suppressed": int64(2)})
		if n := event.SlogDedupKeys(obs); n != 0 {
			t.Fatalf("tracked keys after Flush = %d, want 0", n)
		}
		// The bubble ending without a pending timer goroutine is the leak check.
	})
}

func TestSlogObserver_DedupPerSignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		capt := newSoCapture()
		obs := event.NewSlogObserver(slog.New(capt))
		for i := 0; i < 3; i++ {
			obs.DecodeFailed(ctx, event.DecodeFailure{Handler: "h", Reason: "bad", Err: errors.New("e")})
			obs.DLQPublishFailed(ctx, event.DLQPublishFailure{Handler: "h", Reason: "terminal", Err: errors.New("e")})
		}
		if n := len(capt.records()); n != 2 {
			t.Fatalf("got %d immediate records, want 2", n)
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		sums := soSummaries(capt.records())
		if len(sums) != 2 {
			t.Fatalf("got %d summaries, want 2", len(sums))
		}
		signals := map[any]bool{}
		for _, s := range sums {
			signals[s.Attrs["signal"]] = true
			soCheckSummary(t, s, map[string]any{"suppressed": int64(2)})
		}
		if !signals["decode_failed"] || !signals["dlq_publish_failed"] {
			t.Fatalf("summary signals = %v, want decode_failed and dlq_publish_failed", signals)
		}
	})
}
