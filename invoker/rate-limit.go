package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
	"time"
)

var ErrRateLimited = errors.New("rate limit exceeded")

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
	metrics MetricProvider,
) *RateLimiter {
	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &RateLimiter{
		store:   store,
		cfg:     cfg,
		metrics: metrics,
	}
}

type RateLimiter struct {
	store   RateLimitStore
	cfg     RateLimitConfig
	metrics MetricProvider
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
		i.metrics.IncCounter(
			"eventbus_ratelimit_error_total",
			1,
			Labels{"handler": handler},
		)
		return err
	}

	if !allowed {
		i.metrics.IncCounter(
			"eventbus_ratelimit_blocked_total",
			1,
			Labels{"handler": handler},
		)

		if i.cfg.OnLimit != nil {
			i.cfg.OnLimit(ctx, evt, key)
		}

		return ErrRateLimited
	}

	i.metrics.IncCounter(
		"eventbus_ratelimit_allowed_total",
		1,
		Labels{"handler": handler},
	)

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
