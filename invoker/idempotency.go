package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
	"sync"
	"time"
)

type IdempotencyStatus string

const (
	StatusProcessing IdempotencyStatus = "processing"
	StatusCompleted  IdempotencyStatus = "completed"
	StatusFailed     IdempotencyStatus = "failed"
	// StatusDeadLettered records that the message was published to the DLQ. It
	// is not a duplicate: a replay with the same key executes again.
	StatusDeadLettered IdempotencyStatus = "dead_lettered"
)

// ErrDuplicate is the root sentinel event.ErrDuplicate, kept here for source
// compatibility.
var ErrDuplicate = event.ErrDuplicate

type IdempotencyStore interface {
	Get(ctx context.Context, key string) (IdempotencyRecord, bool, error)
	Put(ctx context.Context, key string, rec IdempotencyRecord) error
	Delete(ctx context.Context, key string) error
}

type WithIdempotencyKey interface {
	IdempotencyKey() string
}

type IdempotencyRecord struct {
	Status    IdempotencyStatus
	StartedAt time.Time
	EndedAt   *time.Time
}

type IdempotencyConfig struct {
	KeyFunc       func(evt event.Event, handler string) string
	ProcessingTTL time.Duration
}

func NewIdempotency(
	store IdempotencyStore,
	cfg IdempotencyConfig,
) *Idempotency {
	if cfg.ProcessingTTL == 0 {
		cfg.ProcessingTTL = 5 * time.Minute
	}

	return &Idempotency{
		store: store,
		cfg:   cfg,
	}
}

// Idempotency skips a handler that already completed for the same key.
//
// Diagnostics reach the Observer in the context: a store failure is a
// StoreError{Store: "idempotency"} with Op get, put_processing, put_completed,
// put_failed or put_dead_lettered, and taking over a processing record older
// than ProcessingTTL is an IdempotencyStale. A duplicate (ErrDuplicate) and an
// execution in progress (ErrInProgress) emit no diagnostic signal: they are
// HandlerDone outcomes.
type Idempotency struct {
	store IdempotencyStore
	cfg   IdempotencyConfig
}

// idempotencyStore is the Store label of the idempotency StoreError signals.
const idempotencyStore = "idempotency"

func (i *Idempotency) storeError(ctx context.Context, evt event.Event, handler, op string, err error) {
	event.ObserverFromContext(ctx).StoreError(ctx, event.StoreErrorInfo{
		Handler: handler,
		Event:   evt.Name(),
		Store:   idempotencyStore,
		Op:      op,
		Err:     err,
	})
}

func (i *Idempotency) Invoke(
	ctx context.Context,
	evt event.Event,
	handlerName string,
	handle func(context.Context) error,
) error {
	key, ok := extractKey(evt)
	if !ok {
		return handle(ctx)
	}

	if handlerName != "" {
		key = key + ":" + handlerName
	}

	rec, exists, err := i.store.Get(ctx, key)
	if err != nil {
		i.storeError(ctx, evt, handlerName, "get", err)
		return err
	}

	if exists {
		switch rec.Status {
		case StatusCompleted:
			return ErrDuplicate

		case StatusProcessing:
			if time.Since(rec.StartedAt) < i.cfg.ProcessingTTL {
				// Not a duplicate: another instance may have crashed mid-run.
				// ErrInProgress is retried later, never Acked as done.
				return event.ErrInProgress
			}

			// Stale lock: take it over and run.
			event.ObserverFromContext(ctx).IdempotencyStale(ctx, event.IdempotencyStaleInfo{
				Handler: handlerName,
				Event:   evt.Name(),
				Age:     time.Since(rec.StartedAt),
			})

		case StatusDeadLettered, StatusFailed:
			// dead_lettered and failed records are not completions: fall
			// through and execute again.
		}
	}

	// started is kept by the processing marker and by the final record.
	started := time.Now()
	if err := i.store.Put(ctx, key, IdempotencyRecord{
		Status:    StatusProcessing,
		StartedAt: started,
	}); err != nil {
		i.storeError(ctx, evt, handlerName, "put_processing", err)
		return err
	}

	err = handle(ctx)

	now := time.Now()
	status, op := StatusCompleted, "put_completed"

	switch {
	case err == nil:
	case errors.Is(err, event.ErrDeadLettered):
		status, op = StatusDeadLettered, "put_dead_lettered"
	default:
		status, op = StatusFailed, "put_failed"
	}

	// A failing final Put does not change the handler result; it only leaves
	// the processing record to expire, and is reported as a StoreError.
	if perr := i.store.Put(ctx, key, IdempotencyRecord{
		Status:    status,
		StartedAt: started,
		EndedAt:   &now,
	}); perr != nil {
		i.storeError(ctx, evt, handlerName, op, perr)
	}

	return err
}

type MemoryIdempotencyStore struct {
	mu    sync.Mutex
	store map[string]IdempotencyRecord
}

func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{
		store: make(map[string]IdempotencyRecord),
	}
}

func (m *MemoryIdempotencyStore) Get(
	_ context.Context,
	key string,
) (IdempotencyRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.store[key]
	return rec, ok, nil
}

func (m *MemoryIdempotencyStore) Put(
	_ context.Context,
	key string,
	rec IdempotencyRecord,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[key] = rec
	return nil
}

func (m *MemoryIdempotencyStore) Delete(
	_ context.Context,
	key string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.store, key)
	return nil
}

func extractKey(evt event.Event) (string, bool) {
	if k, ok := evt.(WithIdempotencyKey); ok {
		return k.IdempotencyKey(), true
	}
	if k, ok := evt.(event.WithId); ok {
		return k.Id(), true
	}
	return "", false
}

/**
// Example Usage
type PaymentConfirmed struct {
	PaymentID string
	Amount    int64
}

func (PaymentConfirmed) Name() string { return "payment.confirmed" }

func (e PaymentConfirmed) IdempotencyKey() string {
	return "payment:" + e.PaymentID
}
*/
