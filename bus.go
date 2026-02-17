package event

import "context"

// Bus defines the common interface for event buses
type Bus interface {
	// Subscribe registers a handler for events on the given subject
	Subscribe(ctx context.Context, subject string, handler HandleFn, opts ...SubscribeOption) error

	// Close gracefully shuts down the bus
	Close() error
}
