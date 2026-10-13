package event

import (
	"errors"
	"strconv"
)

var (
	// ErrUnknownEvent is returned by a registry when the envelope names an event
	// that is not registered, or whose registered factory is nil.
	ErrUnknownEvent = errors.New("event: unknown event")

	// ErrMalformedEnvelope is returned by a registry when the bytes are not a
	// decodable envelope, the name is missing, or the version is negative.
	ErrMalformedEnvelope = errors.New("event: malformed envelope")

	// ErrMissingUpcaster is returned by a registry when no upcaster is registered
	// for an intermediate version on the path to the latest registered version.
	ErrMissingUpcaster = errors.New("event: missing upcaster")

	// ErrUnknownVersion is returned by a registry when the envelope version is
	// newer than the latest registered version. This is the rolling-deploy case:
	// a newer producer is ahead of this consumer, so a retry may succeed later.
	ErrUnknownVersion = errors.New("event: unknown event version")
)

// DecodeError describes a failure to decode an envelope. Err is usually one of
// the Err* sentinels, possibly joined with the underlying cause through
// multiple %w verbs, so errors.Is matches both. Transports classify failures
// with errors.Is and errors.As without importing a registry implementation.
type DecodeError struct {
	// Name is the event name from the envelope, empty when it was not readable.
	Name string
	// Version is the envelope version, 0 when it was not readable.
	Version int
	// Err is the underlying error.
	Err error
}

// Error implements error. It never panics, including when Err is nil.
func (e *DecodeError) Error() string {
	msg := "unknown error"
	if e.Err != nil {
		msg = e.Err.Error()
	}
	if e.Name != "" {
		return "event: decode " + e.Name + " v" + strconv.Itoa(e.Version) + ": " + msg
	}
	return "event: decode: " + msg
}

// Unwrap returns the underlying error so errors.Is and errors.As traverse it.
func (e *DecodeError) Unwrap() error { return e.Err }
