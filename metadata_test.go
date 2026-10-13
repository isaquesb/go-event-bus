package event_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

// mdEvent is an event with optional id and version.
type mdEvent struct {
	name    string
	id      string
	version int
}

func (e *mdEvent) Name() string { return e.name }

// mdVersioned adds WithVersion on top of mdEvent.
type mdVersioned struct{ mdEvent }

func (e *mdVersioned) Version() int { return e.version }

// mdIDed adds WithId on top of mdEvent.
type mdIDed struct{ mdEvent }

func (e *mdIDed) Id() string { return e.id }

// mdEncoderRegistry implements Registry and EnvelopeEncoder.
type mdEncoderRegistry struct {
	gotMD     event.Metadata
	encodeHit bool
}

func (r *mdEncoderRegistry) Register(string, event.Factory, int) {}
func (r *mdEncoderRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	return ctx, nil, nil
}
func (r *mdEncoderRegistry) Encode(context.Context, event.Event) ([]byte, error) {
	r.encodeHit = true
	return []byte("encode"), nil
}
func (r *mdEncoderRegistry) EncodeWith(_ context.Context, _ event.Event, md event.Metadata) ([]byte, error) {
	r.gotMD = md
	return []byte("encodewith"), nil
}

// mdPlainRegistry implements Registry only and records the ctx Encode gets.
type mdPlainRegistry struct{ gotCtx context.Context }

func (r *mdPlainRegistry) Register(string, event.Factory, int) {}
func (r *mdPlainRegistry) Decode(ctx context.Context, _ []byte) (context.Context, event.Event, error) {
	return ctx, nil, nil
}
func (r *mdPlainRegistry) Encode(ctx context.Context, _ event.Event) ([]byte, error) {
	r.gotCtx = ctx
	return []byte("encode"), nil
}

func mdOpts(opts ...event.EmitOption) event.EmitOptions {
	return event.ApplyEmitOptions(&mdEvent{name: "x"}, opts...)
}

func TestResolveOutbound_ID_OptionWins(t *testing.T) {
	evt := &mdIDed{mdEvent{name: "x", id: "from-event"}}
	o := mdOpts(event.WithID("from-option"))
	o.IDGenerator = func() string { return "from-gen" }
	if got := event.ResolveOutbound(context.Background(), evt, o).ID; got != "from-option" {
		t.Errorf("ID %q", got)
	}
}

func TestResolveOutbound_ID_EventIdWins(t *testing.T) {
	evt := &mdIDed{mdEvent{name: "x", id: "from-event"}}
	o := mdOpts()
	o.IDGenerator = func() string { return "from-gen" }
	if got := event.ResolveOutbound(context.Background(), evt, o).ID; got != "from-event" {
		t.Errorf("ID %q", got)
	}
}

func TestResolveOutbound_ID_GeneratorWins(t *testing.T) {
	o := mdOpts()
	o.IDGenerator = func() string { return "from-gen" }
	if got := event.ResolveOutbound(context.Background(), &mdEvent{name: "x"}, o).ID; got != "from-gen" {
		t.Errorf("ID %q", got)
	}
}

func TestResolveOutbound_ID_EmptyValuesFallThrough(t *testing.T) {
	evt := &mdIDed{mdEvent{name: "x", id: ""}}
	o := mdOpts(event.WithID(""))
	o.IDGenerator = func() string { return "" }
	got := event.ResolveOutbound(context.Background(), evt, o)
	if got.ID == "" {
		t.Fatal("ID must never be empty")
	}
	if got.CorrelationID != got.ID {
		t.Errorf("root CorrelationID %q != ID %q", got.CorrelationID, got.ID)
	}
}

func TestResolveOutbound_Root(t *testing.T) {
	got := event.ResolveOutbound(context.Background(), &mdEvent{name: "x"}, mdOpts())
	if got.CorrelationID != got.ID || got.CausationID != "" {
		t.Errorf("root: %+v", got)
	}
}

func TestResolveOutbound_Child(t *testing.T) {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "p1", CorrelationID: "c1"})
	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, mdOpts())
	if got.CorrelationID != "c1" || got.CausationID != "p1" {
		t.Errorf("child: %+v", got)
	}
}

func TestResolveOutbound_ChildOfParentWithoutCorrelation(t *testing.T) {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "p1"})
	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, mdOpts())
	if got.CorrelationID != "p1" || got.CausationID != "p1" {
		t.Errorf("child: %+v", got)
	}
}

func TestResolveOutbound_LegacyParent(t *testing.T) {
	ctx := event.WithMetadata(context.Background(), event.Metadata{})
	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, mdOpts())
	if got.CorrelationID != got.ID || got.CausationID != "" {
		t.Errorf("legacy parent: %+v", got)
	}
}

func TestResolveOutbound_ExplicitCorrelationWins(t *testing.T) {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "p1", CorrelationID: "c1"})
	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, mdOpts(event.WithCorrelationID("x1")))
	if got.CorrelationID != "x1" || got.CausationID != "p1" {
		t.Errorf("explicit: %+v", got)
	}
}

func TestResolveOutbound_HeaderInheritanceAllowlist(t *testing.T) {
	parent := event.Metadata{ID: "p1", Headers: map[string]string{"tenant": "acme", "secret": "s"}}
	ctx := event.WithMetadata(context.Background(), parent)

	o := mdOpts()
	o.InheritHeaders = []string{"tenant"}
	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, o)
	if !reflect.DeepEqual(got.Headers, map[string]string{"tenant": "acme"}) {
		t.Errorf("allowlisted: %v", got.Headers)
	}

	got = event.ResolveOutbound(ctx, &mdEvent{name: "x"}, mdOpts())
	if len(got.Headers) != 0 {
		t.Errorf("empty allowlist must inherit nothing, got %v", got.Headers)
	}

	got = event.ResolveOutbound(context.Background(), &mdEvent{name: "x"}, o)
	if len(got.Headers) != 0 {
		t.Errorf("no parent, got %v", got.Headers)
	}
}

func TestResolveOutbound_ExplicitHeaderOverridesInherited(t *testing.T) {
	ctx := event.WithMetadata(context.Background(),
		event.Metadata{ID: "p1", Headers: map[string]string{"tenant": "acme"}})
	o := mdOpts(event.WithHeader("tenant", "beta"))
	o.InheritHeaders = []string{"tenant"}
	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, o)
	if got.Headers["tenant"] != "beta" {
		t.Errorf("headers %v", got.Headers)
	}
}

func TestResolveOutbound_HeadersAreCopied(t *testing.T) {
	parentHeaders := map[string]string{"tenant": "acme"}
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "p1", Headers: parentHeaders})
	o := mdOpts(event.WithHeader("k", "v"))
	o.InheritHeaders = []string{"tenant"}

	got := event.ResolveOutbound(ctx, &mdEvent{name: "x"}, o)
	got.Headers["k"] = "mutated"
	got.Headers["tenant"] = "mutated"
	got.Headers["new"] = "n"

	if o.Headers["k"] != "v" || len(o.Headers) != 1 {
		t.Errorf("opts headers changed: %v", o.Headers)
	}
	if parentHeaders["tenant"] != "acme" || len(parentHeaders) != 1 {
		t.Errorf("parent headers changed: %v", parentHeaders)
	}
}

func TestResolveOutbound_NameVersion(t *testing.T) {
	ctx := context.Background()
	got := event.ResolveOutbound(ctx, &mdVersioned{mdEvent{name: "n", version: 3}}, mdOpts())
	if got.Name != "n" || got.Version != 3 {
		t.Errorf("versioned: %+v", got)
	}
	got = event.ResolveOutbound(ctx, &mdVersioned{mdEvent{name: "n", version: 0}}, mdOpts())
	if got.Version != 1 {
		t.Errorf("zero version: %d", got.Version)
	}
	got = event.ResolveOutbound(ctx, &mdEvent{name: "n"}, mdOpts())
	if got.Version != 1 || got.Timestamp.IsZero() {
		t.Errorf("plain: %+v", got)
	}
}

func TestMetadataFromContext_Absent(t *testing.T) {
	md, ok := event.MetadataFromContext(context.Background())
	if ok || !reflect.DeepEqual(md, event.Metadata{}) {
		t.Errorf("ok %v md %+v", ok, md)
	}
}

func TestWithMetadata_RoundTrip(t *testing.T) {
	want := event.Metadata{
		ID: "a", CorrelationID: "b", CausationID: "c", Name: "n", Version: 2,
		Headers: map[string]string{"k": "v"},
	}
	got, ok := event.MetadataFromContext(event.WithMetadata(context.Background(), want))
	if !ok || !reflect.DeepEqual(got, want) {
		t.Errorf("ok %v got %+v", ok, got)
	}
}

func TestEncodeEvent_UsesEnvelopeEncoder(t *testing.T) {
	r := &mdEncoderRegistry{}
	md := event.Metadata{ID: "id-9", CorrelationID: "c", Name: "x", Version: 1}
	out, err := event.EncodeEvent(context.Background(), r, &mdEvent{name: "x"}, md)
	if err != nil || string(out) != "encodewith" {
		t.Fatalf("out %q err %v", out, err)
	}
	if !reflect.DeepEqual(r.gotMD, md) || r.encodeHit {
		t.Errorf("md %+v encodeHit %v", r.gotMD, r.encodeHit)
	}
}

func TestEncodeEvent_FallsBackToEncode(t *testing.T) {
	r := &mdPlainRegistry{}
	out, err := event.EncodeEvent(context.Background(), r, &mdEvent{name: "x"}, event.Metadata{ID: "id-9"})
	if err != nil || string(out) != "encode" {
		t.Fatalf("out %q err %v", out, err)
	}
	if _, ok := event.MetadataFromContext(r.gotCtx); ok {
		t.Error("fallback ctx must not carry the resolved Metadata")
	}
}
