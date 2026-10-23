---
layout: default
title: Causation & Correlation
parent: Examples
nav_order: 8
---

# Causation & Correlation

Demonstrates how event identity and causal lineage propagate from a handler to the events it emits.

Source: [`examples/08_causation_correlation.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/08_causation_correlation.go)

## What propagates

Every event carries an `ID`, a `CorrelationID` that groups everything caused by one root event, and a `CausationID` that points at the event that directly caused it. See [Event Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) for the full field list and resolution order.

- A handler that emits with **the ctx it received** makes the new event a child: its `CausationID` is the handler event's `ID` and its `CorrelationID` is the root `ID`. Do not pass `context.Background()` there, or the new event becomes a root.
- A root event (emitted with a ctx that carries no Metadata) has an empty `CausationID` and a `CorrelationID` equal to its own `ID`.
- `event.WithID` and `event.WithCorrelationID` set the ids at the call site. There is no option for the causation id: it always comes from the parent.
- `event.WithHeader` attaches a user header. Headers are **not** inherited by default: a child only receives the parent headers whose keys are on the bus `InheritHeaders` allowlist.
- Headers travel in the clear on the wire and into dead-letter records. **Never put a secret, token or personal data in an event header.**

## Event definitions

```go
// CheckoutStarted is the root event of a checkout.
type CheckoutStarted struct {
	CartID string
}

func (e CheckoutStarted) Name() string { return "checkout.started" }

// Id lets the event provide its own id. WithID at the Emit call wins over it.
func (e CheckoutStarted) Id() string { return e.CartID }

// PaymentRequested is caused by CheckoutStarted.
type PaymentRequested struct {
	PaymentID string
}

func (e PaymentRequested) Name() string { return "payment.requested" }

func (e PaymentRequested) Id() string { return e.PaymentID }
```

## The bus and the parent handler

```go
bus := event.NewLocalBus(event.LocalBusOptions{
	// Headers are copied from the handled event to the events it emits only
	// when their key is on this allowlist. Nothing is inherited by default.
	InheritHeaders: []string{"tenant"},
	Observer:       event.NewSlogObserver(nil),
})
defer func() { _ = bus.Close() }()

ctx := context.Background()

// The checkout handler reads the Metadata of the event it is handling and
// emits the next event with the SAME ctx. Passing the handler ctx is what
// links the two events: do not pass context.Background() here.
if err := bus.Subscribe(ctx, "checkout.started", func(ctx context.Context, evt event.Event) error {
	md, _ := event.MetadataFromContext(ctx)
	slog.Info("checkout started",
		"id", md.ID,
		"correlation_id", md.CorrelationID,
		"causation_id", md.CausationID, // empty: this is the root event
	)

	cart := evt.(CheckoutStarted)
	_, err := bus.EmitSync(ctx, PaymentRequested{PaymentID: "pay-" + cart.CartID})
	return err
}, event.WithHandlerName("start-checkout")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}
```

## The child handler and the root emit

```go
// The payment handler sees the lineage: its CausationID is the checkout
// event ID and its CorrelationID is the root ID.
if err := bus.Subscribe(ctx, "payment.requested", func(ctx context.Context, evt event.Event) error {
	md, _ := event.MetadataFromContext(ctx)
	slog.Info("payment requested",
		"id", md.ID,
		"correlation_id", md.CorrelationID,
		"causation_id", md.CausationID,
		"tenant", md.Headers["tenant"], // inherited through the allowlist
	)
	return nil
}, event.WithHandlerName("request-payment")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}

// The root emit chooses its id and attaches a user header. WithID wins over
// Id() and over the generated id.
if _, err := bus.EmitSync(ctx, CheckoutStarted{CartID: "cart-7"},
	event.WithID("checkout-1"),
	event.WithHeader("tenant", "acme"),
); err != nil {
	slog.Error("emit sync failed", "error", err)
}

// Headers travel in the clear on the wire and into dead-letter records:
// they are an allowlist of routing and tenancy hints. NEVER put a secret,
// token or personal data in an event header.
```

## Runnable output

The same flow runs under `go test` as `Example_causationCorrelation` in [`examples/runnable_examples_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/runnable_examples_test.go). The `// Output:` block is checked, so the output below is verified by `go test` in the examples module.

```go
func Example_causationCorrelation() {
	// NopObserver keeps the default slog observer from writing to stderr.
	bus := event.NewLocalBus(event.LocalBusOptions{Observer: event.NopObserver{}})
	defer func() { _ = bus.Close() }()

	ctx := context.Background()

	_ = bus.Subscribe(ctx, "checkout.started", func(ctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		fmt.Printf("checkout id=%s correlation=%s causation=%q\n", md.ID, md.CorrelationID, md.CausationID)
		// The handler ctx links the new event to this one.
		_, err := bus.EmitSync(ctx, PaymentRequested{}, event.WithID("pay-1"))
		return err
	}, event.WithHandlerName("start-checkout"))

	_ = bus.Subscribe(ctx, "payment.requested", func(ctx context.Context, _ event.Event) error {
		md, _ := event.MetadataFromContext(ctx)
		fmt.Printf("payment  id=%s correlation=%s causation=%q\n", md.ID, md.CorrelationID, md.CausationID)
		return nil
	}, event.WithHandlerName("request-payment"))

	_, _ = bus.EmitSync(ctx, CheckoutStarted{}, event.WithID("checkout-1"))

	// Output:
	// checkout id=checkout-1 correlation=checkout-1 causation=""
	// payment  id=pay-1 correlation=checkout-1 causation="checkout-1"
}
```

Output:

```
checkout id=checkout-1 correlation=checkout-1 causation=""
payment  id=pay-1 correlation=checkout-1 causation="checkout-1"
```

The child event has `correlation=checkout-1` (the root id) and `causation="checkout-1"` (its parent), while the root event has an empty causation.
