package invoker

import (
	"context"
	"github.com/isaquesb/go-event-bus"
	"time"
)

// ErrRateLimited is the root sentinel event.ErrRateLimited, kept here for
// source compatibility.
var ErrRateLimited = event.ErrRateLimited

type RateLimitStore interface {
	Allow(
		ctx context.Context,
		key string,
		rate int,
		period time.Duration,
		burst int,
	) (bool, error)
}

type WithRateLimitKey interface {
	RateLimitKey() string
}

type RateLimitConfig struct {
	Rate      int           // tokens por período
	Period    time.Duration // janela
	Burst     int           // capacidade máxima
	KeyPrefix string
	OnLimit   func(ctx context.Context, evt event.Event, key string)
}

func NewRateLimiter(
	store RateLimitStore,
	cfg RateLimitConfig,
) *RateLimiter {
	return &RateLimiter{
		store: store,
		cfg:   cfg,
	}
}

type RateLimiter struct {
	store RateLimitStore
	cfg   RateLimitConfig
}

func (i *RateLimiter) Invoke(
	ctx context.Context,
	evt event.Event,
	handler string,
	next func(context.Context) error,
) error {
	keyed, ok := evt.(WithRateLimitKey)
	if !ok {
		return next(ctx)
	}

	key := keyed.RateLimitKey()
	if key == "" {
		return next(ctx)
	}

	key = i.cfg.KeyPrefix + key

	allowed, err := i.store.Allow(
		ctx,
		key,
		i.cfg.Rate,
		i.cfg.Period,
		i.cfg.Burst,
	)
	if err != nil {
		return err
	}

	if !allowed {
		if i.cfg.OnLimit != nil {
			i.cfg.OnLimit(ctx, evt, key)
		}

		return ErrRateLimited
	}

	return next(ctx)
}

/**
//Example usage
type MessageSent struct {
	UserID string
	ChatID string
}

func (MessageSent) Name() string { return "chat.message.sent" }

func (e MessageSent) RateLimitKey() string {
	return "user:" + e.UserID
}
*/
