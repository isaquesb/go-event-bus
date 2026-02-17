package redis

import (
	"context"
	"encoding/json"
	"github.com/isaquesb/go-event-bus/invoker"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewIdempotencyStore(rdb *redis.Client) *IdempotencyStore {
	return &IdempotencyStore{rdb: rdb}
}

type IdempotencyStore struct {
	rdb *redis.Client
}

func (s *IdempotencyStore) Put(
	ctx context.Context,
	key string,
	rec invoker.IdempotencyRecord,
) error {

	data, _ := json.Marshal(rec)

	var ttl time.Duration
	switch rec.Status {
	case invoker.StatusProcessing:
		ttl = 5 * time.Minute
	case invoker.StatusCompleted:
		ttl = 24 * time.Hour
	default:
		ttl = 1 * time.Hour
	}

	// Use SET for updates (when status changes from processing to completed/failed)
	// For initial lock acquisition, the idempotency invoker checks Get first
	return s.rdb.Set(ctx, key, data, ttl).Err()
}

func (s *IdempotencyStore) Get(
	ctx context.Context,
	key string,
) (invoker.IdempotencyRecord, bool, error) {

	val, err := s.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return invoker.IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return invoker.IdempotencyRecord{}, false, err
	}

	var rec invoker.IdempotencyRecord
	if err := json.Unmarshal([]byte(val), &rec); err != nil {
		return invoker.IdempotencyRecord{}, false, err
	}

	return rec, true, nil
}

func (s *IdempotencyStore) Delete(
	ctx context.Context,
	key string,
) error {
	return s.rdb.Del(ctx, key).Err()
}
