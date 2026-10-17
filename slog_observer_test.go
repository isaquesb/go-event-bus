package event_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

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
