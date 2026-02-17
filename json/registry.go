package json

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/isaquesb/go-event-bus"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type Registry struct {
	mu        sync.RWMutex
	factories map[eventKey]event.Factory
	upcasters map[eventKey]event.Upcaster
	latest    map[string]int
}

func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[eventKey]event.Factory),
		upcasters: make(map[eventKey]event.Upcaster),
		latest:    make(map[string]int),
	}
}

func (r *Registry) Register(
	name string,
	factory event.Factory,
	version int,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := eventKey{name, version}
	r.factories[key] = factory

	if r.latest[name] < version {
		r.latest[name] = version
	}
}

func (r *Registry) RegisterUpcaster(u event.Upcaster) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := eventKey{u.EventName(), u.FromVersion()}
	r.upcasters[key] = u
}

func (r *Registry) Encode(ctx context.Context, evt event.Event) ([]byte, error) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}

	v := 1
	if withVersion, hasVersion := evt.(event.WithVersion); hasVersion {
		v = withVersion.Version()
	}

	env := event.Envelope{
		Name:      evt.Name(),
		Version:   v,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if sc.IsValid() {
		env.TraceID = sc.TraceID().String()
		env.SpanID = sc.SpanID().String()
	}

	return json.Marshal(env)
}

func (r *Registry) Decode(ctx context.Context, data []byte) (context.Context, event.Event, error) {
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ctx, nil, err
	}

	if env.TraceID != "" {
		traceId, tErr := trace.TraceIDFromHex(env.TraceID)
		spanId, sErr := trace.SpanIDFromHex(env.SpanID)

		if tErr == nil && sErr == nil {
			ctx = trace.ContextWithSpanContext(
				ctx,
				trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    traceId,
					SpanID:     spanId,
					TraceFlags: trace.FlagsSampled,
					Remote:     true,
				}),
			)
		}
	}

	current := env.Version
	payload := env.Payload

	for {
		latest := r.latest[env.Name]
		if current == latest {
			break
		}

		key := eventKey{env.Name, current}
		up, ok := r.upcasters[key]
		if !ok {
			return ctx, nil, fmt.Errorf(
				"no upcaster for %s v%d → v%d",
				env.Name, current, latest,
			)
		}

		var err error
		payload, err = up.Upcast(ctx, payload)
		if err != nil {
			return ctx, nil, err
		}

		current = up.ToVersion()
	}

	factory := r.factories[eventKey{env.Name, current}]
	evt := factory()

	if err := json.Unmarshal(payload, evt); err != nil {
		return ctx, nil, err
	}

	return ctx, evt, nil
}

type eventKey struct {
	name    string
	version int
}
