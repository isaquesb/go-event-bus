package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// dsEvent is an event with an id and a rate-limit key.
type dsEvent struct{}

func (dsEvent) Name() string         { return "ds.order" }
func (dsEvent) Id() string           { return "ds-1" }
func (dsEvent) RateLimitKey() string { return "tenant-1" }

// dsFailingStore is an IdempotencyStore scripted per call: Get returns rec,
// exists and getErr; Put fails with putErrs[status] when set.
type dsFailingStore struct {
	rec     invoker.IdempotencyRecord
	exists  bool
	getErr  error
	putErrs map[invoker.IdempotencyStatus]error
}

func (s *dsFailingStore) Get(context.Context, string) (invoker.IdempotencyRecord, bool, error) {
	return s.rec, s.exists, s.getErr
}

func (s *dsFailingStore) Put(_ context.Context, _ string, rec invoker.IdempotencyRecord) error {
	return s.putErrs[rec.Status]
}

func (s *dsFailingStore) Delete(context.Context, string) error { return nil }

// dsFailingLimiter is a RateLimitStore that fails with err, or answers allowed.
type dsFailingLimiter struct {
	allowed bool
	err     error
}

func (l *dsFailingLimiter) Allow(context.Context, string, int, time.Duration, int) (bool, error) {
	return l.allowed, l.err
}

// dsRun runs fn through an Idempotency with store under an Observer recorder.
func dsRun(store invoker.IdempotencyStore, ttl time.Duration, fn func(context.Context) error) (*invRecorder, error) {
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	err := invoker.NewIdempotency(store, invoker.IdempotencyConfig{ProcessingTTL: ttl}).
		Invoke(ctx, dsEvent{}, "h", fn)
	return rec, err
}

func TestIdempotency_StoreErrorSignals(t *testing.T) {
	boom := errors.New("redis down")
	handlerErr := errors.New("handler failed")
	deadLettered := &event.DeadLetteredError{Cause: errors.New("exhausted"), Reason: event.ReasonRetriesExhausted}

	cases := []struct {
		name       string
		store      *dsFailingStore
		handlerErr error
		wantOp     string
		wantRan    bool
		wantErr    error
	}{
		{
			name:    "get",
			store:   &dsFailingStore{getErr: boom},
			wantOp:  "get",
			wantErr: boom,
		},
		{
			name:    "put_processing",
			store:   &dsFailingStore{putErrs: map[invoker.IdempotencyStatus]error{invoker.StatusProcessing: boom}},
			wantOp:  "put_processing",
			wantErr: boom,
		},
		{
			name:       "put_failed",
			store:      &dsFailingStore{putErrs: map[invoker.IdempotencyStatus]error{invoker.StatusFailed: boom}},
			handlerErr: handlerErr,
			wantOp:     "put_failed",
			wantRan:    true,
			wantErr:    handlerErr,
		},
		{
			name:    "put_completed",
			store:   &dsFailingStore{putErrs: map[invoker.IdempotencyStatus]error{invoker.StatusCompleted: boom}},
			wantOp:  "put_completed",
			wantRan: true,
		},
		{
			name:       "put_dead_lettered",
			store:      &dsFailingStore{putErrs: map[invoker.IdempotencyStatus]error{invoker.StatusDeadLettered: boom}},
			handlerErr: deadLettered,
			wantOp:     "put_dead_lettered",
			wantRan:    true,
			wantErr:    deadLettered,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ran := false
			rec, err := dsRun(tc.store, time.Minute, func(context.Context) error {
				ran = true
				return tc.handlerErr
			})

			if ran != tc.wantRan {
				t.Errorf("handler ran = %v, want %v", ran, tc.wantRan)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("error = %v, want %v", err, tc.wantErr)
			}

			got := rec.StoreErrors()
			if len(got) != 1 {
				t.Fatalf("StoreError signals = %d, want 1", len(got))
			}
			v := got[0].Val
			if v.Handler != "h" || v.Event != "ds.order" || v.Store != "idempotency" || v.Op != tc.wantOp {
				t.Errorf("StoreError = %+v, want handler h, event ds.order, store idempotency, op %s", v, tc.wantOp)
			}
			if !errors.Is(v.Err, boom) {
				t.Errorf("StoreError.Err = %v, want the store error", v.Err)
			}
			if n := len(rec.Stales()); n != 0 {
				t.Errorf("IdempotencyStale signals = %d, want 0", n)
			}
		})
	}
}

func TestIdempotency_StaleSignal(t *testing.T) {
	const ttl = time.Minute
	store := &dsFailingStore{
		exists: true,
		rec: invoker.IdempotencyRecord{
			Status:    invoker.StatusProcessing,
			StartedAt: time.Now().Add(-3 * ttl),
		},
	}

	ran := false
	rec, err := dsRun(store, ttl, func(context.Context) error {
		ran = true
		return nil
	})

	if err != nil || !ran {
		t.Fatalf("a stale lock must be taken over: err=%v ran=%v", err, ran)
	}
	stales := rec.Stales()
	if len(stales) != 1 {
		t.Fatalf("IdempotencyStale signals = %d, want 1", len(stales))
	}
	v := stales[0].Val
	if v.Handler != "h" || v.Event != "ds.order" || v.Age < ttl {
		t.Errorf("IdempotencyStale = %+v, want handler h, event ds.order, age >= %v", v, ttl)
	}
	if n := len(rec.StoreErrors()); n != 0 {
		t.Errorf("StoreError signals = %d, want 0", n)
	}
}

func TestIdempotency_DuplicateAndInProgressNoSignal(t *testing.T) {
	cases := []struct {
		name string
		rec  invoker.IdempotencyRecord
		want error
	}{
		{"duplicate", invoker.IdempotencyRecord{Status: invoker.StatusCompleted, StartedAt: time.Now()}, event.ErrDuplicate},
		{"in_progress", invoker.IdempotencyRecord{Status: invoker.StatusProcessing, StartedAt: time.Now()}, event.ErrInProgress},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ran := false
			rec, err := dsRun(&dsFailingStore{exists: true, rec: tc.rec}, time.Minute, func(context.Context) error {
				ran = true
				return nil
			})

			if !errors.Is(err, tc.want) || ran {
				t.Errorf("err=%v ran=%v, want %v and not run", err, ran, tc.want)
			}
			if n := len(rec.StoreErrors()) + len(rec.Stales()); n != 0 {
				t.Errorf("diagnostic signals = %d, want 0 (the outcome is a HandlerDone)", n)
			}
		})
	}
}

func TestRateLimiter_StoreErrorSignal(t *testing.T) {
	boom := errors.New("redis down")
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	ran := false

	err := invoker.NewRateLimiter(&dsFailingLimiter{err: boom}, invoker.RateLimitConfig{Rate: 1, Period: time.Second, Burst: 1}).
		Invoke(ctx, dsEvent{}, "h", func(context.Context) error {
			ran = true
			return nil
		})

	if !errors.Is(err, boom) || ran {
		t.Errorf("err=%v ran=%v, want the store error and not run", err, ran)
	}
	got := rec.StoreErrors()
	if len(got) != 1 {
		t.Fatalf("StoreError signals = %d, want 1", len(got))
	}
	v := got[0].Val
	if v.Handler != "h" || v.Event != "ds.order" || v.Store != "rate_limit" || v.Op != "allow" || !errors.Is(v.Err, boom) {
		t.Errorf("StoreError = %+v, want handler h, event ds.order, store rate_limit, op allow", v)
	}
}

func TestRateLimiter_BlockedNoSignal(t *testing.T) {
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	limited := 0

	err := invoker.NewRateLimiter(&dsFailingLimiter{allowed: false}, invoker.RateLimitConfig{
		Rate: 1, Period: time.Second, Burst: 1,
		OnLimit: func(context.Context, event.Event, string) { limited++ },
	}).Invoke(ctx, dsEvent{}, "h", func(context.Context) error { return nil })

	if !errors.Is(err, event.ErrRateLimited) {
		t.Errorf("expected ErrRateLimited, got %v", err)
	}
	if limited != 1 {
		t.Errorf("OnLimit calls = %d, want 1", limited)
	}
	if n := len(rec.StoreErrors()) + len(rec.Stales()) + len(rec.InternalErrors()); n != 0 {
		t.Errorf("diagnostic signals = %d, want 0 (the outcome is a HandlerDone)", n)
	}
}
