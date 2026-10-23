// Example: Causation and Correlation
//
// Every event carries an identity (event.Metadata): its own ID, a
// CorrelationID that groups everything caused by one root event, and a
// CausationID that points at the event that directly caused it.
//
// The rule is applied on Emit and needs no code in the handler beyond passing
// its own ctx: a handler that emits with the ctx it received makes the new
// event's CausationID the handler event's ID, and its CorrelationID the root
// ID. A root event (emitted with a ctx that carries no Metadata) has an empty
// CausationID and a CorrelationID equal to its own ID.
//
// The same behavior runs under go test: see Example_causationCorrelation in
// runnable_examples_test.go.
package examples

import (
	"context"
	"log/slog"

	"github.com/isaquesb/go-event-bus"
)

// =============================================================================
// Event Definitions
// =============================================================================

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

// =============================================================================
// Example Usage
// =============================================================================

func ExampleCausationCorrelation() {
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
}
