package json

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/isaquesb/go-event-bus"
	"go.opentelemetry.io/otel/propagation"
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
	prop      propagation.TextMapPropagator
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

// WithPropagator replaces the default W3C trace context and baggage
// propagator used to write and read trace headers in the envelope. A nil p
// keeps the default. The process-global propagator is never used.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(r *Registry) {
		if p != nil {
			r.prop = p
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
		prop: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
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

	// System wins (D-12): drop any user header the propagator owns, then let
	// it write. The map is always allocated, because MapCarrier.Set panics on
	// a nil map when the span context is valid.
	headers := make(map[string]string, len(md.Headers)+3)
	maps.Copy(headers, md.Headers)
	for _, k := range r.prop.Fields() {
		delete(headers, k)
	}
	r.prop.Inject(ctx, propagation.MapCarrier(headers))
	if len(headers) > 0 {
		env.Headers = headers
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

	ctx = r.extractTrace(ctx, &env)

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

// extractTrace restores the producer's trace context. A valid remote span
// context extracted from the W3C headers wins, so the producer's sampled flag
// and tracestate are honored (D-15). Otherwise, an envelope that carries
// legacy trace_id and span_id is read as sampled and remote (D-16), which also
// covers a malformed traceparent.
//
// Extract has no error result, so success is detected on the result: a
// successful extraction always yields a remote span context, while a local
// span already in ctx comes back unchanged and not remote.
func (r *Registry) extractTrace(ctx context.Context, env *event.Envelope) context.Context {
	base := ctx
	if len(env.Headers) > 0 {
		out := r.prop.Extract(ctx, propagation.MapCarrier(env.Headers))
		if sc := trace.SpanContextFromContext(out); sc.IsValid() && sc.IsRemote() {
			return out
		}
		base = out // keeps baggage extracted from the headers
	}

	if env.TraceID != "" {
		traceId, tErr := trace.TraceIDFromHex(env.TraceID)
		spanId, sErr := trace.SpanIDFromHex(env.SpanID)

		if tErr == nil && sErr == nil {
			return trace.ContextWithSpanContext(
				base,
				trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    traceId,
					SpanID:     spanId,
					TraceFlags: trace.FlagsSampled,
					Remote:     true,
				}),
			)
		}
	}

	return base
}

type eventKey struct {
	name    string
	version int
}
