// Example: Failure hooks
//
// Three hooks report a failure, at three levels:
//
//   - Observer.Failure: called once per terminal outcome of a message, on the
//     bus-wide Observer. It carries the handler, outcome, reason, attempts,
//     delivery (count/max), failing invoker and circuit breaker state. This
//     replaces the bus-level OnErr of earlier versions.
//   - CircuitBreakerConfig.OnStateChange: called for every breaker state
//     transition, after the Observer received the same transition.
//   - The event's own OnErr(ctx, err, listenerName) method, which still
//     exists: it runs after Observer.Failure, once per terminal failure.
//
// Every hook runs synchronously on the handler goroutine. They must be fast
// and must never block.
//
// The Observer.Failure part runs under go test: see Example_failureObserver in
// runnable_examples_test.go.
package examples

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// =============================================================================
// Event Definitions
// =============================================================================

// ReportRequested has its own OnErr method.
type ReportRequested struct {
	ReportID string
}

func (e ReportRequested) Name() string { return "report.requested" }

// OnErr runs after Observer.Failure, once per terminal failure of this event.
// It is optional and independent of the bus Observer.
func (e ReportRequested) OnErr(ctx context.Context, err error, listenerName string) {
	slog.Error("report failed", "report_id", e.ReportID, "handler", listenerName, "error", err)
}

// =============================================================================
// A custom Observer
// =============================================================================

// billingAlerts embeds event.NopObserver, so it implements every Observer
// method and keeps compiling when the interface gains methods. It overrides
// only Failure.
type billingAlerts struct {
	event.NopObserver
}

// Failure is called once per terminal outcome, never on an intermediate retry.
func (a *billingAlerts) Failure(ctx context.Context, f event.FailureContext) {
	slog.Warn("message ended",
		"handler", f.Handler,
		"outcome", f.Outcome,
		"reason", f.Reason,
		"attempts", f.Attempts,
		"delivery", f.Delivery.Count,
		"max_delivery", f.Delivery.Max,
		"breaker", f.Breaker,
	)
}

// =============================================================================
// Example Usage
// =============================================================================

func ExampleFailureHooks() {
	// Transitions are handed to a buffered channel with a non-blocking send and
	// processed elsewhere.
	transitions := make(chan event.BreakerTransition, 16)

	breaker := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
		// OnStateChange runs on the goroutine of the message that caused the
		// transition. NEVER block in it: no network call, no unbuffered send, no
		// lock held elsewhere. Drop the signal when the consumer is slow.
		OnStateChange: func(ctx context.Context, t event.BreakerTransition) {
			select {
			case transitions <- t:
			default:
				// consumer is behind: drop rather than block the handler
			}
		},
	})

	chain := invoker.NewChain(
		invoker.NewDLQ(event.NewMemoryDLQ()),
		invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}),
		breaker,
	)

	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: chain,
		// MultiObserver fans one signal out to several observers and isolates
		// the panic of any of them. The slog observer deduplicates repeated
		// records: within a 30s window the first is logged and the rest are
		// counted into one summary record.
		Observer: event.MultiObserver(
			event.NewSlogObserver(nil, event.WithLogEvery(30*time.Second)),
			&billingAlerts{},
		),
	})
	defer func() { _ = bus.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case t := <-transitions:
				slog.Info("breaker transition", "handler", t.Handler, "from", t.From, "to", t.To, "reason", t.Reason)
			case <-ctx.Done():
				return
			}
		}
	}()

	if err := bus.Subscribe(ctx, "report.requested", func(ctx context.Context, evt event.Event) error {
		return errors.New("report backend unavailable")
	}, event.WithHandlerName("build-report")); err != nil {
		slog.Error("subscribe failed", "error", err)
		return
	}

	if _, err := bus.EmitSync(ctx, ReportRequested{ReportID: "rep-1"}); err != nil {
		slog.Error("emit sync failed", "error", err)
	}
}
