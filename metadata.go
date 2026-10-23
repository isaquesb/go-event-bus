package event

import (
	"context"
	"time"
)

// Metadata is the identity and causal lineage of one event, plus its headers.
// Handlers read it from their context with MetadataFromContext.
//
// Metadata is read-only by contract: do not mutate Headers.
//
// On the consumer side Version is the wire version, where 0 is read as 1, and
// Headers include the system keys traceparent, tracestate and baggage when the
// producer sent them.
type Metadata struct {
	ID            string
	CorrelationID string
	CausationID   string
	Name          string
	Version       int
	Timestamp     time.Time
	Headers       map[string]string
}

type metadataKey struct{}

// WithMetadata returns a context that carries md.
func WithMetadata(ctx context.Context, md Metadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, md)
}

// MetadataFromContext returns the Metadata of the event being handled. The
// boolean is false, and the Metadata zero, when ctx carries none.
func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	md, ok := ctx.Value(metadataKey{}).(Metadata)
	return md, ok
}

// ResolveOutbound resolves the Metadata of an event about to be emitted. It is
// the single place where the causal rules live.
//
// The id is the first non-empty of opts.ID, Id() of a WithId event,
// opts.IDGenerator() and NewID(). The correlation id is opts.CorrelationID,
// then the parent's CorrelationID, then the parent's ID, then the event's own
// id (a root event). The causation id is the ID of the parent Metadata found
// in ctx, and no option can override it.
func ResolveOutbound(ctx context.Context, evt Event, opts EmitOptions) Metadata {
	parent, hasParent := MetadataFromContext(ctx)

	id := opts.ID
	if id == "" {
		if w, ok := evt.(WithId); ok {
			id = w.Id()
		}
	}
	if id == "" && opts.IDGenerator != nil {
		id = opts.IDGenerator()
	}
	if id == "" {
		id = NewID()
	}

	var corr string
	switch {
	case opts.CorrelationID != "":
		corr = opts.CorrelationID
	case hasParent && parent.CorrelationID != "":
		corr = parent.CorrelationID
	case hasParent && parent.ID != "":
		corr = parent.ID
	default:
		corr = id
	}

	var causation string
	if hasParent {
		causation = parent.ID
	}

	version := 1
	if w, ok := evt.(WithVersion); ok && w.Version() > 0 {
		version = w.Version()
	}

	return Metadata{
		ID:            id,
		CorrelationID: corr,
		CausationID:   causation,
		Name:          evt.Name(),
		Version:       version,
		Timestamp:     time.Now().UTC(),
		Headers:       resolveHeaders(parent, hasParent, opts),
	}
}

// EnvelopeEncoder is an optional Registry capability: a registry that
// implements it encodes an event with Metadata the caller already resolved,
// instead of resolving it itself.
type EnvelopeEncoder interface {
	EncodeWith(ctx context.Context, evt Event, md Metadata) ([]byte, error)
}

// EncodeEvent encodes evt with md when r implements EnvelopeEncoder and falls
// back to r.Encode otherwise. The fallback receives ctx unchanged: md is never
// put into it, because the registry would read it as a parent.
func EncodeEvent(ctx context.Context, r Registry, evt Event, md Metadata) ([]byte, error) {
	if enc, ok := r.(EnvelopeEncoder); ok {
		return enc.EncodeWith(ctx, evt, md)
	}
	return r.Encode(ctx, evt)
}

// resolveHeaders builds a fresh headers map: the allowlisted parent headers
// first, then the explicit headers on top. The result never aliases the
// parent or the options maps and is nil when empty.
func resolveHeaders(parent Metadata, hasParent bool, opts EmitOptions) map[string]string {
	var out map[string]string
	if hasParent {
		for _, k := range opts.InheritHeaders {
			if v, ok := parent.Headers[k]; ok {
				if out == nil {
					out = make(map[string]string, len(opts.Headers)+1)
				}
				out[k] = v
			}
		}
	}
	for k, v := range opts.Headers {
		if out == nil {
			out = make(map[string]string, len(opts.Headers))
		}
		out[k] = v
	}
	return out
}
