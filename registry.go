package event

import (
	"context"
	"encoding/json"
	"time"
)

// Envelope is the JSON wire format shared by the registry-backed transports.
//
// ID, CorrelationID, CausationID and Headers are optional (omitempty), so
// bytes produced by v0.3.0 decode unchanged and bytes without these fields are
// identical to v0.3.0 output; v0.3.0 consumers ignore the new keys. TraceID and
// SpanID are dual-written with the W3C headers for one minor release and read as
// a fallback. Headers carry W3C traceparent, tracestate and baggage plus user
// headers, and they count toward the broker max payload.
type Envelope struct {
	Name          string            `json:"name"`
	Version       int               `json:"version"`
	Timestamp     time.Time         `json:"ts"`
	ID            string            `json:"id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	SpanID        string            `json:"span_id,omitempty"`
	Payload       json.RawMessage   `json:"payload"`
}

type Upcaster interface {
	EventName() string
	FromVersion() int
	ToVersion() int
	Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error)
}

type Factory func() Event

type Registry interface {
	Register(name string, factory Factory, version int)
	Decode(ctx context.Context, data []byte) (context.Context, Event, error)
	Encode(ctx context.Context, evt Event) ([]byte, error)
}
