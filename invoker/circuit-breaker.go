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
circuit_state{handler=}
circuit_open_total
circuit_half_open_total
circuit_blocked_total
*/

type circuitState int

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

var ErrCircuitOpen = errors.New("circuit breaker open")

type handlerBreaker struct {
	state     circuitState
	failures  int
	successes int
	openedAt  time.Time
	mu        sync.Mutex
}

type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	OpenTimeout      time.Duration
}

func NewCircuitBreaker(cfg CircuitBreakerConfig, metrics MetricProvider) *CircuitBreaker {
	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &CircuitBreaker{
		cfg:      cfg,
		metrics:  metrics,
		breakers: make(map[string]*handlerBreaker),
	}
}

type CircuitBreaker struct {
	cfg     CircuitBreakerConfig
	metrics MetricProvider

	mu       sync.Mutex
	breakers map[string]*handlerBreaker
}

func (c *CircuitBreaker) Invoke(
	ctx context.Context,
	_ event.Event,
	handler string,
	next func(context.Context) error,
) error {
	b := c.breaker(handler)

	b.mu.Lock()
	switch b.state {
	case stateOpen:
		if time.Since(b.openedAt) < c.cfg.OpenTimeout {
			c.metrics.IncCounter(
				"eventbus_circuit_blocked_total",
				1,
				Labels{"handler": handler},
			)
			b.mu.Unlock()
			return ErrCircuitOpen
		}
		b.state = stateHalfOpen
		b.successes = 0

		c.metrics.IncCounter(
			"eventbus_circuit_half_open_total",
			1,
			Labels{"handler": handler},
		)
		c.emitState(handler, b.state)

	case stateHalfOpen, stateClosed:
	}
	b.mu.Unlock()

	err := next(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case stateClosed:
		if err != nil {
			if !errors.Is(err, ErrSendToDLQ) { // não conta como falha
				b.failures++
			}
			if b.failures >= c.cfg.FailureThreshold {
				b.state = stateOpen
				b.openedAt = time.Now()

				c.metrics.IncCounter(
					"eventbus_circuit_open_total",
					1,
					Labels{"handler": handler},
				)
			}
		} else {
			b.failures = 0
		}

	case stateHalfOpen:
		if err != nil {
			b.state = stateOpen
			b.openedAt = time.Now()

			c.metrics.IncCounter(
				"eventbus_circuit_open_total",
				1,
				Labels{"handler": handler},
			)
		} else {
			b.successes++
			if b.successes >= c.cfg.SuccessThreshold {
				b.state = stateClosed
				b.failures = 0
			}
		}
	}

	c.emitState(handler, b.state)
	return err
}

func (c *CircuitBreaker) emitState(handler string, state circuitState) {
	c.metrics.SetGauge(
		"eventbus_circuit_state",
		float64(state),
		Labels{"handler": handler},
	)
}

func (c *CircuitBreaker) breaker(handler string) *handlerBreaker {
	c.mu.Lock()
	defer c.mu.Unlock()

	if b, ok := c.breakers[handler]; ok {
		return b
	}

	b := &handlerBreaker{state: stateClosed}
	c.breakers[handler] = b

	c.metrics.SetGauge(
		"eventbus_circuit_state",
		float64(stateClosed),
		Labels{"handler": handler},
	)

	return b
}
