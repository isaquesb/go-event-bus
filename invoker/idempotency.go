package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
	"sync"
	"time"
)

/**
Expose:
idempotency_hits_total{event,handler}
idempotency_inflight_total
idempotency_completed_total
idempotency_expired_locks_total
*/

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
	metrics MetricProvider,
) *Idempotency {

	if cfg.ProcessingTTL == 0 {
		cfg.ProcessingTTL = 5 * time.Minute
	}

	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &Idempotency{
		store:   store,
		cfg:     cfg,
		metrics: metrics,
	}
}

type Idempotency struct {
	store   IdempotencyStore
	cfg     IdempotencyConfig
	metrics MetricProvider
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
		i.metrics.IncCounter(
			"eventbus_idempotency_error_total",
			1,
			Labels{"handler": handlerName, "stage": "get"},
		)
		return err
	}

	if exists {
		switch rec.Status {
		case StatusCompleted:
			i.metrics.IncCounter(
				"eventbus_idempotency_duplicate_total",
				1,
				Labels{"handler": handlerName, "reason": "completed"},
			)
			return ErrDuplicate

		case StatusProcessing:
			if time.Since(rec.StartedAt) < i.cfg.ProcessingTTL {
				// Not a duplicate: another instance may have crashed mid-run.
				// ErrInProgress is retried later, never Acked as done.
				i.metrics.IncCounter(
					"eventbus_idempotency_in_progress_total",
					1,
					Labels{"handler": handlerName},
				)
				return event.ErrInProgress
			}

			i.metrics.IncCounter(
				"eventbus_idempotency_processing_stale_total",
				1,
				Labels{"handler": handlerName},
			)
			// stale lock → continuar

		case StatusDeadLettered, StatusFailed:
			// dead_lettered and failed records are not completions: fall
			// through and execute again.
		}
	}

	if err := i.store.Put(ctx, key, IdempotencyRecord{
		Status:    StatusProcessing,
		StartedAt: time.Now(),
	}); err != nil {
		i.metrics.IncCounter(
			"eventbus_idempotency_error_total",
			1,
			Labels{"handler": handlerName, "stage": "put_processing"},
		)
		return err
	}

	i.metrics.IncCounter(
		"eventbus_idempotency_lock_acquired_total",
		1,
		Labels{"handler": handlerName},
	)

	i.metrics.IncCounter(
		"eventbus_idempotency_executed_total",
		1,
		Labels{"handler": handlerName},
	)

	err = handle(ctx)

	now := time.Now()
	status := StatusCompleted
	stage := "put_completed"

	switch {
	case err == nil:
	case errors.Is(err, event.ErrDeadLettered):
		status = StatusDeadLettered
		stage = "put_dead_lettered"
	default:
		status = StatusFailed
		stage = "put_failed"
	}

	if putErr := i.store.Put(ctx, key, IdempotencyRecord{
		Status:    status,
		StartedAt: rec.StartedAt,
		EndedAt:   &now,
	}); putErr != nil {
		i.metrics.IncCounter(
			"eventbus_idempotency_error_total",
			1,
			Labels{"handler": handlerName, "stage": stage},
		)
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
