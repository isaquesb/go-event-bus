package event

import (
	"context"
	"encoding/json"
	"time"
)

type Envelope struct {
	Name      string          `json:"name"`
	Version   int             `json:"version"`
	Timestamp time.Time       `json:"ts"`
	TraceID   string          `json:"trace_id,omitempty"`
	SpanID    string          `json:"span_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
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
