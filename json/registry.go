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

// Register stores factory for name at version. A version <= 0 is registered as
// version 1, the same normalization Decode applies to the wire version (D-18),
// so latest is never 0 for a registered name. Versions 0 and 1 of one name are
// therefore one registration, and the last call wins.
func (r *Registry) Register(
	name string,
	factory event.Factory,
	version int,
) {
	if version <= 0 {
		version = 1
	}

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

	// The wire carries Version() unchanged, so consumers still on v0.3.0 keep
	// reading it; Decode reads a version 0 as 1.
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

// Decode decodes untrusted envelope bytes into an event. It never panics on
// malformed input: every failure is a *event.DecodeError carrying the envelope
// Name and the version reached, and classified by one of the event sentinels
// (ErrMalformedEnvelope, ErrUnknownEvent, ErrUnknownVersion, ErrMissingUpcaster)
// so callers use errors.Is, or errors.As for the catch-all. An upcaster's own
// error comes back as a *event.DecodeError without a sentinel.
//
// An envelope with version 0, or without a version key, decodes as version 1.
// Once the envelope has parsed, the returned ctx carries its Metadata even
// when decoding fails. No registry lock is held while user upcaster code runs,
// so an upcaster may call Register or Decode on the same registry.
func (r *Registry) Decode(ctx context.Context, data []byte) (context.Context, event.Event, error) {
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ctx, nil, &event.DecodeError{
			Name:    env.Name,
			Version: env.Version,
			Err:     fmt.Errorf("%w: %w", event.ErrMalformedEnvelope, err),
		}
	}

	// {} , null and a DLQ record {"event":...,"error":...} land here.
	if env.Name == "" || env.Version < 0 {
		return ctx, nil, &event.DecodeError{
			Name:    env.Name,
			Version: env.Version,
			Err:     fmt.Errorf("%w: missing name or negative version", event.ErrMalformedEnvelope),
		}
	}

	version := env.Version
	if version == 0 {
		version = 1
	}

	ctx = r.extractTrace(ctx, &env)
	ctx = event.WithMetadata(ctx, event.Metadata{
		ID:            env.ID,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Name:          env.Name,
		Version:       version,
		Timestamp:     env.Timestamp,
		Headers:       maps.Clone(env.Headers),
	})

	// latest is snapshotted once, so a concurrent Register cannot move the
	// target of the upcast loop below.
	r.mu.RLock()
	latest := r.latest[env.Name]
	r.mu.RUnlock()

	// Register normalizes a version <= 0 to 1, so latest is 0 only for a name
	// that was never registered.
	if latest == 0 {
		return ctx, nil, &event.DecodeError{Name: env.Name, Version: version, Err: event.ErrUnknownEvent}
	}
	if version > latest {
		return ctx, nil, &event.DecodeError{Name: env.Name, Version: version, Err: event.ErrUnknownVersion}
	}

	payload := env.Payload
	for version != latest {
		r.mu.RLock()
		up, ok := r.upcasters[eventKey{env.Name, version}]
		r.mu.RUnlock()

		if !ok {
			return ctx, nil, &event.DecodeError{
				Name:    env.Name,
				Version: version,
				Err:     fmt.Errorf("%w: v%d to v%d", event.ErrMissingUpcaster, version, latest),
			}
		}

		next := up.ToVersion()
		if next <= version || next > latest {
			return ctx, nil, &event.DecodeError{
				Name:    env.Name,
				Version: version,
				Err: fmt.Errorf(
					"upcaster for %s v%d does not advance to a valid version (to v%d, latest v%d)",
					env.Name, version, next, latest,
				),
			}
		}

		// User code: no registry lock is held here.
		var err error
		payload, err = up.Upcast(ctx, payload)
		if err != nil {
			return ctx, nil, &event.DecodeError{Name: env.Name, Version: version, Err: err}
		}

		version = next
	}

	r.mu.RLock()
	factory := r.factories[eventKey{env.Name, version}]
	r.mu.RUnlock()

	if factory == nil {
		return ctx, nil, &event.DecodeError{Name: env.Name, Version: version, Err: event.ErrUnknownEvent}
	}

	evt := factory()
	if evt == nil {
		return ctx, nil, &event.DecodeError{Name: env.Name, Version: version, Err: event.ErrUnknownEvent}
	}

	if err := json.Unmarshal(payload, evt); err != nil {
		return ctx, nil, &event.DecodeError{
			Name:    env.Name,
			Version: version,
			Err:     fmt.Errorf("%w: %w", event.ErrMalformedEnvelope, err),
		}
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
