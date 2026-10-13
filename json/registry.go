package json

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/trace"
)

var (
	_ event.Registry        = (*Registry)(nil)
	_ event.EnvelopeEncoder = (*Registry)(nil)
)

type Registry struct {
	mu        sync.RWMutex
	factories map[eventKey]event.Factory
	upcasters map[eventKey]event.Upcaster
	latest    map[string]int
	idGen     func() string
}

// Option configures a Registry created by NewRegistry.
type Option func(*Registry)

// WithIDGenerator sets the function that mints the id of events that carry
// neither an explicit id nor an Id() method. A nil gen keeps the default
// UUIDv7 generator.
func WithIDGenerator(gen func() string) Option {
	return func(r *Registry) {
		if gen != nil {
			r.idGen = gen
		}
	}
}

// NewRegistry returns an empty Registry. Options are optional, so existing
// NewRegistry() callers compile unchanged.
func NewRegistry(opts ...Option) *Registry {
	r := &Registry{
		factories: make(map[eventKey]event.Factory),
		upcasters: make(map[eventKey]event.Upcaster),
		latest:    make(map[string]int),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
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

// Encode encodes evt, resolving its Metadata from ctx: a parent Metadata in
// ctx makes evt its child (causation and correlation), so a registry used
// directly still applies the causal rules.
func (r *Registry) Encode(ctx context.Context, evt event.Event) ([]byte, error) {
	return r.EncodeWith(ctx, evt, event.ResolveOutbound(ctx, evt, event.EmitOptions{IDGenerator: r.idGen}))
}

// EncodeWith encodes evt with md already resolved by the caller. When md.ID is
// empty it resolves md itself from ctx, keeping md.CorrelationID and
// md.Headers, so every envelope carries an id and the causal rules stay in
// event.ResolveOutbound. The envelope timestamp is md.Timestamp, or the
// current time when it is zero.
func (r *Registry) EncodeWith(ctx context.Context, evt event.Event, md event.Metadata) ([]byte, error) {
	if md.ID == "" {
		md = event.ResolveOutbound(ctx, evt, event.EmitOptions{
			IDGenerator:   r.idGen,
			CorrelationID: md.CorrelationID,
			Headers:       md.Headers,
		})
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}

	v := 1
	if withVersion, hasVersion := evt.(event.WithVersion); hasVersion {
		v = withVersion.Version()
	}

	ts := md.Timestamp.UTC()
	if md.Timestamp.IsZero() {
		ts = time.Now().UTC()
	}

	env := event.Envelope{
		Name:          evt.Name(),
		Version:       v,
		Timestamp:     ts,
		ID:            md.ID,
		CorrelationID: md.CorrelationID,
		CausationID:   md.CausationID,
		Payload:       payload,
	}
	if len(md.Headers) > 0 {
		env.Headers = maps.Clone(md.Headers)
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

	version := env.Version
	if version == 0 {
		version = 1
	}
	ctx = event.WithMetadata(ctx, event.Metadata{
		ID:            env.ID,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Name:          env.Name,
		Version:       version,
		Timestamp:     env.Timestamp,
		Headers:       maps.Clone(env.Headers),
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

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
