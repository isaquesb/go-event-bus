---
layout: default
title: Failure Hooks
parent: Examples
nav_order: 10
---

# Failure Hooks

Demonstrates the three hooks that report a failure: `Observer.Failure`, the circuit breaker `OnStateChange`, and the event's own `OnErr`.

Source: [`examples/10_failure_hooks.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/10_failure_hooks.go)

## The three hooks

- `Observer.Failure`: called once per terminal outcome of a message, on the bus-wide Observer. It carries the handler, outcome, reason, attempts, delivery (count/max), failing invoker and circuit breaker state. It replaces the bus-level `OnErr` of earlier versions.
- `CircuitBreakerConfig.OnStateChange`: called for every breaker state transition, after the Observer received the same transition.
- The event's own `OnErr(ctx, err, listenerName)` method, which still exists: it runs after `Observer.Failure`, once per terminal failure.

Every hook runs synchronously on the handler goroutine. They must be fast and must never block. See [Observability]({{ site.baseurl }}/observability/) for the full Observer reference.

## An event with its own OnErr

```go
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
```

## A custom Observer

Embed `event.NopObserver` so the type implements every Observer method and keeps compiling when the interface gains methods. Override only what you need.

```go
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
```

## Wiring: MultiObserver, SlogObserver and OnStateChange

```go
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
```

The consumer of `transitions` runs in its own goroutine, scoped to the context:

```go
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
```

## Runnable output

The `Observer.Failure` part runs under `go test` as `Example_failureObserver` in [`examples/runnable_examples_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/runnable_examples_test.go). Here a message is dropped after Retry gave up and no DLQ publisher was configured. The `// Output:` block is checked by `go test` in the examples module.

```go
// printingObserver prints the one Failure it receives. Embedding
// event.NopObserver implements the other Observer methods.
type printingObserver struct {
	event.NopObserver
}

func (printingObserver) Failure(_ context.Context, f event.FailureContext) {
	fmt.Printf("failure handler=%s outcome=%s reason=%s attempts=%d delivery=%d/%d\n",
		f.Handler, f.Outcome, f.Reason, f.Attempts, f.Delivery.Count, f.Delivery.Max)
}

func Example_failureObserver() {
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: invoker.NewChain(
			invoker.NewRetry(invoker.RetryPolicy{
				MaxAttempts: 2,
				BaseDelay:   time.Millisecond,
				MaxDelay:    2 * time.Millisecond,
			}),
		),
		Observer: printingObserver{},
	})
	defer func() { _ = bus.Close() }()

	ctx := context.Background()
	_ = bus.Subscribe(ctx, "invoice.issued", func(context.Context, event.Event) error {
		return errors.New("billing backend unavailable")
	}, event.WithHandlerName("billing"))

	_, _ = bus.EmitSync(ctx, &InvoiceIssued{}, event.WithID("inv-9"))

	// Output:
	// failure handler=billing outcome=dropped reason=retries_exhausted attempts=2 delivery=1/1
}
```

Output:

```
failure handler=billing outcome=dropped reason=retries_exhausted attempts=2 delivery=1/1
```
