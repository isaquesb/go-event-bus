package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// eventWithId implements event.WithId
type eventWithId struct {
	id   string
	name string
}

func (e *eventWithId) Name() string { return e.name }
func (e *eventWithId) Id() string   { return e.id }

// eventWithIdempotencyKey implements WithIdempotencyKey
type eventWithIdempotencyKey struct {
	key  string
	name string
}

func (e *eventWithIdempotencyKey) Name() string           { return e.name }
func (e *eventWithIdempotencyKey) IdempotencyKey() string { return e.key }

// eventWithoutKey has no idempotency key
type eventWithoutKey struct {
	name string
}

func (e *eventWithoutKey) Name() string { return e.name }

func TestIdempotency_FirstExecutionAllowed(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	called := false
	err := idempotency.Invoke(context.Background(), &eventWithId{id: "evt-1", name: "test"}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected handler to be called on first execution")
	}
}

func TestIdempotency_DuplicateBlocked(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	evt := &eventWithId{id: "evt-1", name: "test"}

	// First execution
	_ = idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	// Second execution should be blocked
	called := false
	err := idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, invoker.ErrDuplicate) {
		t.Errorf("expected ErrDuplicate, got '%v'", err)
	}
	if called {
		t.Error("handler should not be called for duplicate")
	}
}

func TestIdempotency_DifferentEventsAllowed(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	// First event
	called1 := false
	_ = idempotency.Invoke(context.Background(), &eventWithId{id: "evt-1", name: "test"}, "handler", func(ctx context.Context) error {
		called1 = true
		return nil
	})

	// Different event
	called2 := false
	_ = idempotency.Invoke(context.Background(), &eventWithId{id: "evt-2", name: "test"}, "handler", func(ctx context.Context) error {
		called2 = true
		return nil
	})

	if !called1 || !called2 {
		t.Error("both events should be processed")
	}
}

func TestIdempotency_WithIdempotencyKeyInterface(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	evt := &eventWithIdempotencyKey{key: "custom-key-1", name: "test"}

	// First execution
	_ = idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	// Second execution should be blocked by custom key
	called := false
	err := idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, invoker.ErrDuplicate) {
		t.Errorf("expected ErrDuplicate, got '%v'", err)
	}
	if called {
		t.Error("handler should not be called for duplicate custom key")
	}
}

func TestIdempotency_NoKeySkipsCheck(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	evt := &eventWithoutKey{name: "test"}

	// Both executions should be allowed (no idempotency check)
	called1 := false
	_ = idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called1 = true
		return nil
	})

	called2 := false
	_ = idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called2 = true
		return nil
	})

	if !called1 || !called2 {
		t.Error("both executions should be allowed without idempotency key")
	}
}

func TestIdempotency_HandlerNameInKey(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	evt := &eventWithId{id: "evt-1", name: "test"}

	// Process with handler1
	_ = idempotency.Invoke(context.Background(), evt, "handler1", func(ctx context.Context) error {
		return nil
	})

	// Same event with handler2 should still work
	called := false
	err := idempotency.Invoke(context.Background(), evt, "handler2", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("different handlers should have separate idempotency")
	}
}

func TestIdempotency_StaleLockAllowsReprocessing(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{
		ProcessingTTL: 10 * time.Millisecond,
	})

	evt := &eventWithId{id: "evt-1", name: "test"}

	// Manually put a stale processing record
	_ = store.Put(context.Background(), "evt-1:handler", invoker.IdempotencyRecord{
		Status:    invoker.StatusProcessing,
		StartedAt: time.Now().Add(-time.Second), // Started 1 second ago
	})

	// Should be allowed to reprocess (stale lock)
	called := false
	err := idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("stale processing lock should allow reprocessing")
	}
}

func TestIdempotency_ActiveLockBlocks(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{
		ProcessingTTL: time.Hour,
	})

	evt := &eventWithId{id: "evt-1", name: "test"}

	// Manually put an active processing record
	_ = store.Put(context.Background(), "evt-1:handler", invoker.IdempotencyRecord{
		Status:    invoker.StatusProcessing,
		StartedAt: time.Now(),
	})

	// Should be blocked (active lock)
	called := false
	err := idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, event.ErrInProgress) {
		t.Errorf("expected ErrInProgress for active lock, got '%v'", err)
	}
	if called {
		t.Error("active processing lock should block execution")
	}
}

func TestIdempotency_InProgressIsNotDuplicate(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{ProcessingTTL: time.Hour})

	_ = store.Put(context.Background(), "evt-1:handler", invoker.IdempotencyRecord{
		Status:    invoker.StatusProcessing,
		StartedAt: time.Now(),
	})

	called := false
	err := idempotency.Invoke(context.Background(), &eventWithId{id: "evt-1", name: "test"}, "handler", func(context.Context) error {
		called = true
		return nil
	})

	if called {
		t.Error("the handler must not run while another execution is in progress")
	}
	if !errors.Is(err, event.ErrInProgress) {
		t.Errorf("expected ErrInProgress, got '%v'", err)
	}
	if errors.Is(err, event.ErrDuplicate) {
		t.Errorf("an in-progress record must not look like a duplicate (the transport would Ack it): %v", err)
	}
}

func TestIdempotency_DeadLetteredRecorded(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	dead := &event.DeadLetteredError{Cause: errors.New("bad"), Reason: event.ReasonTerminal}
	err := idempotency.Invoke(context.Background(), &eventWithId{id: "evt-1", name: "test"}, "handler", func(context.Context) error {
		return dead
	})

	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("expected the dead-lettered error to pass through, got '%v'", err)
	}
	rec, ok, _ := store.Get(context.Background(), "evt-1:handler")
	if !ok || rec.Status != invoker.StatusDeadLettered {
		t.Errorf("stored status = %q (found %v), want %q", rec.Status, ok, invoker.StatusDeadLettered)
	}
}

func TestIdempotency_DeadLetteredReplayExecutes(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	_ = store.Put(context.Background(), "evt-1:handler", invoker.IdempotencyRecord{
		Status:    invoker.StatusDeadLettered,
		StartedAt: time.Now(),
	})

	called := false
	err := idempotency.Invoke(context.Background(), &eventWithId{id: "evt-1", name: "test"}, "handler", func(context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("a dead_lettered record must re-execute on replay")
	}
	rec, _, _ := store.Get(context.Background(), "evt-1:handler")
	if rec.Status != invoker.StatusCompleted {
		t.Errorf("stored status = %q, want completed", rec.Status)
	}
}

func TestIdempotency_CompletedIsDuplicate(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	_ = store.Put(context.Background(), "evt-1:handler", invoker.IdempotencyRecord{
		Status:    invoker.StatusCompleted,
		StartedAt: time.Now(),
	})

	called := false
	err := idempotency.Invoke(context.Background(), &eventWithId{id: "evt-1", name: "test"}, "handler", func(context.Context) error {
		called = true
		return nil
	})

	if called {
		t.Error("a completed record must not re-execute")
	}
	if !errors.Is(err, event.ErrDuplicate) {
		t.Errorf("expected ErrDuplicate, got '%v'", err)
	}
}

func TestIdempotency_FailedExecutionAllowsRetry(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	evt := &eventWithId{id: "evt-1", name: "test"}

	// First execution fails
	handlerErr := errors.New("handler failed")
	_ = idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return handlerErr
	})

	// Check the record status (should be failed, not completed)
	rec, exists, _ := store.Get(context.Background(), "evt-1:handler")
	if !exists {
		t.Fatal("record should exist")
	}
	if rec.Status != invoker.StatusFailed {
		t.Errorf("expected status Failed, got %v", rec.Status)
	}
}

// Memory store tests

func TestMemoryIdempotencyStore_PutAndGet(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()

	rec := invoker.IdempotencyRecord{
		Status:    invoker.StatusCompleted,
		StartedAt: time.Now(),
	}

	err := store.Put(context.Background(), "key1", rec)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	got, exists, err := store.Get(context.Background(), "key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected record to exist")
	}
	if got.Status != invoker.StatusCompleted {
		t.Errorf("expected status Completed, got %v", got.Status)
	}
}

func TestMemoryIdempotencyStore_GetNonExistent(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()

	_, exists, err := store.Get(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected record to not exist")
	}
}

func TestMemoryIdempotencyStore_Delete(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()

	_ = store.Put(context.Background(), "key1", invoker.IdempotencyRecord{
		Status: invoker.StatusCompleted,
	})

	err := store.Delete(context.Background(), "key1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_, exists, _ := store.Get(context.Background(), "key1")
	if exists {
		t.Error("expected record to be deleted")
	}
}

func TestIdempotency_StoreError(t *testing.T) {
	storeErr := errors.New("store error")
	store := &failingStore{err: storeErr}
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	evt := &eventWithId{id: "evt-1", name: "test"}

	err := idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	if !errors.Is(err, storeErr) {
		t.Errorf("expected store error, got '%v'", err)
	}
}

type failingStore struct {
	err error
}

func (s *failingStore) Get(_ context.Context, _ string) (invoker.IdempotencyRecord, bool, error) {
	return invoker.IdempotencyRecord{}, false, s.err
}

func (s *failingStore) Put(_ context.Context, _ string, _ invoker.IdempotencyRecord) error {
	return s.err
}

func (s *failingStore) Delete(_ context.Context, _ string) error {
	return s.err
}

// Verify idempotency key extraction precedence
func TestIdempotency_IdempotencyKeyTakesPrecedence(t *testing.T) {
	store := invoker.NewMemoryIdempotencyStore()
	idempotency := invoker.NewIdempotency(store, invoker.IdempotencyConfig{})

	// Event that implements both WithId and WithIdempotencyKey
	evt := &eventWithBothKeys{id: "id-1", idempotencyKey: "custom-key"}

	_ = idempotency.Invoke(context.Background(), evt, "handler", func(ctx context.Context) error {
		return nil
	})

	// Check that custom key was used
	_, existsCustom, _ := store.Get(context.Background(), "custom-key:handler")
	_, existsId, _ := store.Get(context.Background(), "id-1:handler")

	if !existsCustom {
		t.Error("expected record with custom key to exist")
	}
	if existsId {
		t.Error("expected record with id key to NOT exist (custom key takes precedence)")
	}
}

type eventWithBothKeys struct {
	id             string
	idempotencyKey string
}

func (e *eventWithBothKeys) Name() string           { return "test" }
func (e *eventWithBothKeys) Id() string             { return e.id }
func (e *eventWithBothKeys) IdempotencyKey() string { return e.idempotencyKey }

// Verify that WithIdempotencyKey is checked before WithId
var _ event.WithId = (*eventWithBothKeys)(nil)
var _ invoker.WithIdempotencyKey = (*eventWithBothKeys)(nil)
