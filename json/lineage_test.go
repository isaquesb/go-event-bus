package json_test

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
)

var linUUIDv7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// linIDEvent implements event.WithId.
type linIDEvent struct {
	Message string `json:"message"`
}

func (e *linIDEvent) Name() string { return "lin.with-id" }
func (e *linIDEvent) Id() string   { return "evt-1" }

func linRegistry(opts ...eventjson.Option) *eventjson.Registry {
	reg := eventjson.NewRegistry(opts...)
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("simple.event", func() event.Event { return &SimpleEvent{} }, 1)
	reg.Register("lin.with-id", func() event.Event { return &linIDEvent{} }, 1)
	return reg
}

// linRoundTrip encodes evt under ctx and decodes it with a clean context, as a
// consumer would, returning the consumer ctx and its Metadata.
func linRoundTrip(t *testing.T, reg *eventjson.Registry, ctx context.Context, evt event.Event) (context.Context, event.Metadata) {
	t.Helper()
	data, err := reg.Encode(ctx, evt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, _, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	md, ok := event.MetadataFromContext(out)
	if !ok {
		t.Fatalf("decoded ctx carries no Metadata")
	}
	return out, md
}

func linEnvelope(t *testing.T, data []byte) event.Envelope {
	t.Helper()
	var env event.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return env
}

func TestLineageRoundTrip(t *testing.T) {
	reg := linRegistry()

	ctx1, md1 := linRoundTrip(t, reg, context.Background(), &UserCreatedV1{UserID: "u1"})
	if !linUUIDv7.MatchString(md1.ID) {
		t.Fatalf("root id %q is not a UUIDv7", md1.ID)
	}
	if md1.CorrelationID != md1.ID {
		t.Errorf("root correlation = %q, want its own id %q", md1.CorrelationID, md1.ID)
	}
	if md1.CausationID != "" {
		t.Errorf("root causation = %q, want empty", md1.CausationID)
	}

	ctx2, md2 := linRoundTrip(t, reg, ctx1, &SimpleEvent{Message: "child"})
	if md2.CausationID != md1.ID {
		t.Errorf("child causation = %q, want parent id %q", md2.CausationID, md1.ID)
	}
	if md2.CorrelationID != md1.CorrelationID {
		t.Errorf("child correlation = %q, want %q", md2.CorrelationID, md1.CorrelationID)
	}
	if md2.ID == md1.ID || md2.ID == "" {
		t.Errorf("child id %q must be new and non-empty (parent %q)", md2.ID, md1.ID)
	}

	_, md3 := linRoundTrip(t, reg, ctx2, &SimpleEvent{Message: "grandchild"})
	if md3.CorrelationID != md1.ID {
		t.Errorf("grandchild correlation = %q, want root id %q", md3.CorrelationID, md1.ID)
	}
	if md3.CausationID != md2.ID {
		t.Errorf("grandchild causation = %q, want child id %q", md3.CausationID, md2.ID)
	}
}

func TestMetadata_EncodeAssignsGeneratedID(t *testing.T) {
	reg := linRegistry()
	data, err := reg.Encode(context.Background(), &SimpleEvent{Message: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	env := linEnvelope(t, data)
	if !linUUIDv7.MatchString(env.ID) {
		t.Errorf("envelope id %q is not a UUIDv7", env.ID)
	}
}

func TestMetadata_EncodeUsesEventID(t *testing.T) {
	reg := linRegistry()
	data, err := reg.Encode(context.Background(), &linIDEvent{Message: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := linEnvelope(t, data).ID; got != "evt-1" {
		t.Errorf("envelope id = %q, want evt-1 (D-10)", got)
	}
}

func TestMetadata_WithIDGenerator(t *testing.T) {
	reg := linRegistry(eventjson.WithIDGenerator(func() string { return "gen-7" }))
	data, err := reg.Encode(context.Background(), &SimpleEvent{Message: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	env := linEnvelope(t, data)
	if env.ID != "gen-7" || env.CorrelationID != "gen-7" {
		t.Errorf("id/correlation = %q/%q, want gen-7/gen-7", env.ID, env.CorrelationID)
	}

	// A nil generator keeps the default.
	def := linRegistry(eventjson.WithIDGenerator(nil))
	data, err = def.Encode(context.Background(), &SimpleEvent{Message: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if id := linEnvelope(t, data).ID; !linUUIDv7.MatchString(id) {
		t.Errorf("nil generator id %q is not a UUIDv7", id)
	}
}

func TestMetadata_EncodeWithWritesMetadata(t *testing.T) {
	reg := linRegistry()
	ts := time.Date(2026, 10, 13, 12, 30, 45, 123456789, time.FixedZone("x", 3*3600))
	md := event.Metadata{
		ID:            "id-1",
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		Timestamp:     ts,
		Headers:       map[string]string{"tenant": "acme"},
	}
	data, err := reg.EncodeWith(context.Background(), &SimpleEvent{Message: "x"}, md)
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	env := linEnvelope(t, data)
	if env.ID != "id-1" || env.CorrelationID != "corr-1" || env.CausationID != "cause-1" {
		t.Errorf("lineage = %q/%q/%q, want id-1/corr-1/cause-1", env.ID, env.CorrelationID, env.CausationID)
	}
	if !env.Timestamp.Equal(ts) {
		t.Errorf("ts = %v, want %v", env.Timestamp, ts)
	}
	if env.Timestamp.Location() != time.UTC {
		t.Errorf("ts location = %v, want UTC", env.Timestamp.Location())
	}
	if env.Headers["tenant"] != "acme" || len(env.Headers) != 1 {
		t.Errorf("headers = %v, want only tenant=acme", env.Headers)
	}
	md.Headers["tenant"] = "mutated"
	if env.Headers["tenant"] != "acme" {
		t.Errorf("envelope headers alias the caller map")
	}
}

func TestMetadata_EncodeWithEmptyIDResolves(t *testing.T) {
	reg := linRegistry()
	md := event.Metadata{CorrelationID: "req-9", Headers: map[string]string{"k": "v"}}
	data, err := reg.EncodeWith(context.Background(), &SimpleEvent{Message: "x"}, md)
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	env := linEnvelope(t, data)
	if env.ID == "" {
		t.Fatalf("envelope id is empty")
	}
	if env.CorrelationID != "req-9" {
		t.Errorf("correlation = %q, want req-9 kept", env.CorrelationID)
	}
	if env.Headers["k"] != "v" {
		t.Errorf("headers = %v, want k=v kept", env.Headers)
	}
	if env.Timestamp.IsZero() {
		t.Errorf("ts is zero")
	}
}

func TestMetadata_DecodePutsMetadataInContext(t *testing.T) {
	reg := linRegistry()
	ts := time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)
	data, err := reg.EncodeWith(context.Background(), &UserCreatedV1{UserID: "u"}, event.Metadata{
		ID:            "id-1",
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		Timestamp:     ts,
		Headers:       map[string]string{"tenant": "acme"},
	})
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	ctx, evt, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := evt.(*UserCreatedV1); !ok {
		t.Fatalf("decoded %T, want *UserCreatedV1", evt)
	}
	md, ok := event.MetadataFromContext(ctx)
	if !ok {
		t.Fatalf("no Metadata in decoded ctx")
	}
	if md.ID != "id-1" || md.CorrelationID != "corr-1" || md.CausationID != "cause-1" {
		t.Errorf("lineage = %q/%q/%q", md.ID, md.CorrelationID, md.CausationID)
	}
	if md.Name != "user.created" || md.Version != 1 {
		t.Errorf("name/version = %q/%d, want user.created/1", md.Name, md.Version)
	}
	if !md.Timestamp.Equal(ts) {
		t.Errorf("timestamp = %v, want %v", md.Timestamp, ts)
	}
	if md.Headers["tenant"] != "acme" {
		t.Errorf("headers = %v", md.Headers)
	}
}

func TestMetadata_HeadersRoundTrip(t *testing.T) {
	reg := linRegistry()
	md := event.ResolveOutbound(context.Background(), &SimpleEvent{}, event.EmitOptions{
		Headers: map[string]string{"tenant": "acme", "region": "eu"},
	})
	data, err := reg.EncodeWith(context.Background(), &SimpleEvent{Message: "x"}, md)
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	ctx, _, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, _ := event.MetadataFromContext(ctx)
	if got.Headers["tenant"] != "acme" || got.Headers["region"] != "eu" || len(got.Headers) != 2 {
		t.Errorf("headers = %v, want tenant=acme region=eu", got.Headers)
	}
}

func TestMetadata_RootOmitsEmptyFields(t *testing.T) {
	reg := linRegistry()
	data, err := reg.Encode(context.Background(), &SimpleEvent{Message: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	s := string(data)
	if strings.Contains(s, "causation_id") {
		t.Errorf("root envelope has causation_id: %s", s)
	}
	if strings.Contains(s, `"headers"`) {
		t.Errorf("headerless envelope has a headers key: %s", s)
	}
	if !strings.Contains(s, `"id"`) {
		t.Errorf("envelope has no id: %s", s)
	}
}

func TestMetadata_LegacyEnvelopeHasEmptyID(t *testing.T) {
	reg := linRegistry()
	legacy := func(version int) []byte {
		data, err := json.Marshal(event.Envelope{
			Name:      "simple.event",
			Version:   version,
			Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			Payload:   json.RawMessage(`{"message":"old"}`),
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return data
	}

	ctx, evt, err := reg.Decode(context.Background(), legacy(1))
	if err != nil {
		t.Fatalf("decode legacy v1: %v", err)
	}
	if evt.(*SimpleEvent).Message != "old" {
		t.Errorf("payload not decoded: %+v", evt)
	}
	md, ok := event.MetadataFromContext(ctx)
	if !ok || md.ID != "" || md.CorrelationID != "" || md.CausationID != "" || md.Version != 1 {
		t.Errorf("legacy md = %+v ok=%v, want ok with empty ids and version 1", md, ok)
	}

	// A wire version of 0 is read as 1 in Metadata. The decode result for
	// version 0 is owned by a later plan, so only the ctx is asserted.
	ctx0, _, _ := reg.Decode(context.Background(), legacy(0))
	md0, ok := event.MetadataFromContext(ctx0)
	if !ok || md0.ID != "" || md0.Version != 1 {
		t.Errorf("version 0 md = %+v ok=%v, want ok with empty ID and Version 1", md0, ok)
	}
}

func TestMetadata_ExplicitCorrelationSurvivesWire(t *testing.T) {
	reg := linRegistry()
	md := event.ResolveOutbound(context.Background(), &UserCreatedV1{}, event.EmitOptions{CorrelationID: "req-42"})
	data, err := reg.EncodeWith(context.Background(), &UserCreatedV1{UserID: "u"}, md)
	if err != nil {
		t.Fatalf("encode with: %v", err)
	}
	ctx, _, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	parent, _ := event.MetadataFromContext(ctx)
	if parent.CorrelationID != "req-42" {
		t.Fatalf("correlation on the wire = %q, want req-42", parent.CorrelationID)
	}

	_, child := linRoundTrip(t, reg, ctx, &SimpleEvent{Message: "c"})
	if child.CorrelationID != "req-42" {
		t.Errorf("child correlation = %q, want req-42", child.CorrelationID)
	}
	if child.CausationID != parent.ID {
		t.Errorf("child causation = %q, want %q", child.CausationID, parent.ID)
	}
}
