package json_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
)

// v0Event is an event whose Version() is 0, the shape WR-01 (G-07-1) is about.
type v0Event struct {
	Value string `json:"value"`
}

func (e *v0Event) Name() string { return "v0.evt" }
func (e *v0Event) Version() int { return 0 }

func v0Factory() event.Event { return &v0Event{} }

// v0WireVersion returns the raw top-level "version" value of an envelope.
func v0WireVersion(t *testing.T, data []byte) string {
	t.Helper()

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	v, ok := raw["version"]
	if !ok {
		t.Fatalf("envelope has no \"version\" key: %s", data)
	}
	return string(v)
}

// v0Legacy is a v0.3.0-shaped envelope literal (no id, no headers).
func v0Legacy(version string) []byte {
	if version == "" {
		return []byte(fmt.Sprintf(`{"name":"v0.evt","ts":%q,"payload":{"value":"old"}}`, compatTS))
	}
	return []byte(fmt.Sprintf(`{"name":"v0.evt","version":%s,"ts":%q,"payload":{"value":"old"}}`, version, compatTS))
}

// TestVersion0_WireVersion pins PD-1: the producer writes Version() unchanged,
// so an event whose Version() is 0 carries "version":0.
func TestVersion0_WireVersion(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("v0.evt", v0Factory, 0)

	t.Run("Encode", func(t *testing.T) {
		data, err := reg.Encode(t.Context(), &v0Event{Value: "x"})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if got := v0WireVersion(t, data); got != "0" {
			t.Errorf("wire version = %s, want 0", got)
		}
	})

	t.Run("EncodeWith", func(t *testing.T) {
		data, err := reg.EncodeWith(t.Context(), &v0Event{Value: "x"}, event.Metadata{ID: "v0-id-1"})
		if err != nil {
			t.Fatalf("EncodeWith: %v", err)
		}
		if got := v0WireVersion(t, data); got != "0" {
			t.Errorf("wire version = %s, want 0", got)
		}
	})
}

// TestVersion0_RoundTrip: an event whose Version() is 0 round-trips when it is
// registered at 0, at a negative version, or at 1 (D-18, WR-01).
func TestVersion0_RoundTrip(t *testing.T) {
	for _, registered := range []int{0, -1, 1} {
		t.Run(fmt.Sprintf("registered_at_%d", registered), func(t *testing.T) {
			check := func(t *testing.T, reg *eventjson.Registry, data []byte, wantID string) {
				t.Helper()

				res := deDecode(t, reg, data)
				if res.err != nil {
					t.Fatalf("Decode: %v", res.err)
				}
				got, ok := res.evt.(*v0Event)
				if !ok {
					t.Fatalf("Decode returned %T, want *v0Event", res.evt)
				}
				if got.Value != "payload" {
					t.Errorf("Value = %q, want %q", got.Value, "payload")
				}
				md, ok := event.MetadataFromContext(res.ctx)
				if !ok {
					t.Fatal("ctx carries no Metadata")
				}
				if md.Version != 1 {
					t.Errorf("Metadata.Version = %d, want 1", md.Version)
				}
				if wantID != "" && md.ID != wantID {
					t.Errorf("Metadata.ID = %q, want %q", md.ID, wantID)
				}
			}

			t.Run("Encode", func(t *testing.T) {
				reg := eventjson.NewRegistry()
				reg.Register("v0.evt", v0Factory, registered)

				data, err := reg.Encode(t.Context(), &v0Event{Value: "payload"})
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				check(t, reg, data, "")
			})

			t.Run("EncodeWith", func(t *testing.T) {
				reg := eventjson.NewRegistry()
				reg.Register("v0.evt", v0Factory, registered)

				data, err := reg.EncodeWith(t.Context(), &v0Event{Value: "payload"}, event.Metadata{ID: "v0-id-1"})
				if err != nil {
					t.Fatalf("EncodeWith: %v", err)
				}
				check(t, reg, data, "v0-id-1")
			})
		})
	}
}

// v0Replica is a frozen replica of the v0.3.0 json registry's version
// resolution (git show json/v0.3.0:json/registry.go): latest rises only when
// the new version is greater, the loop stops at current == latest and needs an
// upcaster otherwise (none are registered here), and the factory is looked up
// at current.
type v0Replica struct {
	factories map[v0Key]event.Factory
	latest    map[string]int
}

type v0Key struct {
	name    string
	version int
}

func newV0Replica() *v0Replica {
	return &v0Replica{factories: map[v0Key]event.Factory{}, latest: map[string]int{}}
}

func (r *v0Replica) register(name string, f event.Factory, version int) {
	r.factories[v0Key{name, version}] = f
	if r.latest[name] < version {
		r.latest[name] = version
	}
}

func (r *v0Replica) decode(data []byte) (event.Event, error) {
	var env compatLegacyEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}

	current := env.Version
	if current != r.latest[env.Name] {
		return nil, fmt.Errorf("no upcaster for %s v%d to v%d", env.Name, current, r.latest[env.Name])
	}

	factory := r.factories[v0Key{env.Name, current}]
	if factory == nil {
		return nil, errors.New("no factory reachable")
	}
	evt := factory()
	if err := json.Unmarshal(env.Payload, evt); err != nil {
		return nil, err
	}
	return evt, nil
}

// TestVersion0_V030Consumer: a consumer still on v0.3.0 that registered the
// event at 0 reads v0.4.0 output, because the wire keeps Version() (PD-1).
func TestVersion0_V030Consumer(t *testing.T) {
	producer := eventjson.NewRegistry()
	producer.Register("v0.evt", v0Factory, 0)

	encode := map[string]func() ([]byte, error){
		"Encode": func() ([]byte, error) {
			return producer.Encode(t.Context(), &v0Event{Value: "payload"})
		},
		"EncodeWith": func() ([]byte, error) {
			return producer.EncodeWith(t.Context(), &v0Event{Value: "payload"}, event.Metadata{ID: "v0-id-1"})
		},
	}

	for name, enc := range encode {
		t.Run(name, func(t *testing.T) {
			data, err := enc()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			consumer := newV0Replica()
			consumer.register("v0.evt", v0Factory, 0)

			evt, err := consumer.decode(data)
			if err != nil {
				t.Fatalf("v0.3.0 consumer rejected v0.4.0 output: %v", err)
			}
			got, ok := evt.(*v0Event)
			if !ok || got.Value != "payload" {
				t.Fatalf("v0.3.0 consumer decoded %#v, want Value %q", evt, "payload")
			}

			// Negative control: a producer that rewrote the version to 1 would
			// leave no reachable factory for the v0.3.0 consumer.
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(data, &raw); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			raw["version"] = json.RawMessage("1")
			rewritten, err := json.Marshal(raw)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if _, err := consumer.decode(rewritten); err == nil {
				t.Error("v0.3.0 replica accepted a version-1 envelope; the replica would not catch a producer that rewrites the version")
			}
		})
	}
}

// TestVersion0_LegacyEnvelope: old version-0 envelopes keep decoding on a
// registry that registered the event at 0, as version 1 (D-18).
func TestVersion0_LegacyEnvelope(t *testing.T) {
	cases := map[string][]byte{
		"explicit_version_0": v0Legacy("0"),
		"no_version_key":     v0Legacy(""),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			reg := eventjson.NewRegistry()
			reg.Register("v0.evt", v0Factory, 0)

			res := deDecode(t, reg, data)
			if res.err != nil {
				t.Fatalf("Decode: %v", res.err)
			}
			got, ok := res.evt.(*v0Event)
			if !ok {
				t.Fatalf("Decode returned %T, want *v0Event", res.evt)
			}
			if got.Value != "old" {
				t.Errorf("Value = %q, want %q", got.Value, "old")
			}
			md, _ := event.MetadataFromContext(res.ctx)
			if md.Version != 1 {
				t.Errorf("Metadata.Version = %d, want 1", md.Version)
			}
			if want, _ := time.Parse(time.RFC3339Nano, compatTS); !md.Timestamp.Equal(want) {
				t.Errorf("Metadata.Timestamp = %s, want %s", md.Timestamp, compatTS)
			}
		})
	}
}

// v0EventV2 is version 2 of v0.evt: Value plus Extra.
type v0EventV2 struct {
	Value string `json:"value"`
	Extra string `json:"extra"`
}

func (e *v0EventV2) Name() string { return "v0.evt" }
func (e *v0EventV2) Version() int { return 2 }

// v0To2Upcaster is declared from version 0 and adds Extra.
type v0To2Upcaster struct{}

func (u *v0To2Upcaster) EventName() string { return "v0.evt" }
func (u *v0To2Upcaster) FromVersion() int  { return 0 }
func (u *v0To2Upcaster) ToVersion() int    { return 2 }
func (u *v0To2Upcaster) Upcast(_ context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["extra"] = "upcast"
	return json.Marshal(m)
}

// v0To1Upcaster is declared from version 0 to version 1: a no-op upcaster that
// errors if it ever runs, so any call is visible.
type v0To1Upcaster struct{}

func (u *v0To1Upcaster) EventName() string { return "user.created" }
func (u *v0To1Upcaster) FromVersion() int  { return 0 }
func (u *v0To1Upcaster) ToVersion() int    { return 1 }
func (u *v0To1Upcaster) Upcast(context.Context, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("v0To1Upcaster must never run")
}

// TestVersion0_UpcasterFromVersion0: an upcaster declared from version 0 counts
// as an upcaster from version 1, for legacy envelopes and for new Version()==0
// emissions alike (PD-3, D-18).
func TestVersion0_UpcasterFromVersion0(t *testing.T) {
	newReg := func() *eventjson.Registry {
		reg := eventjson.NewRegistry()
		reg.Register("v0.evt", v0Factory, 0)
		reg.Register("v0.evt", func() event.Event { return &v0EventV2{} }, 2)
		reg.RegisterUpcaster(&v0To2Upcaster{})
		return reg
	}

	check := func(t *testing.T, reg *eventjson.Registry, data []byte, wantValue string) {
		t.Helper()

		res := deDecode(t, reg, data)
		if res.err != nil {
			t.Fatalf("Decode: %v", res.err)
		}
		got, ok := res.evt.(*v0EventV2)
		if !ok {
			t.Fatalf("Decode returned %T, want *v0EventV2", res.evt)
		}
		if got.Value != wantValue || got.Extra != "upcast" {
			t.Errorf("got Value %q Extra %q, want %q and %q", got.Value, got.Extra, wantValue, "upcast")
		}
	}

	t.Run("legacy_envelope", func(t *testing.T) {
		check(t, newReg(), v0Legacy("0"), "old")
	})

	t.Run("new_emission", func(t *testing.T) {
		reg := newReg()
		data, err := reg.Encode(t.Context(), &v0Event{Value: "new"})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		check(t, reg, data, "new")
	})
}

// TestVersion0_UpcasterFrom0To1IsIgnored: an upcaster from version 0 that only
// reaches version 1 is not stored, so it never replaces the real 1 -> 2
// upcaster and never runs, whatever the registration order (PD-3, T-07-26).
func TestVersion0_UpcasterFrom0To1IsIgnored(t *testing.T) {
	for _, before := range []bool{false, true} {
		name := "registered_after"
		if before {
			name = "registered_before"
		}
		t.Run(name, func(t *testing.T) {
			reg := eventjson.NewRegistry()
			reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
			reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
			if before {
				reg.RegisterUpcaster(&v0To1Upcaster{})
			}
			reg.RegisterUpcaster(&userCreatedV1ToV2Upcaster{})
			if !before {
				reg.RegisterUpcaster(&v0To1Upcaster{})
			}

			data := []byte(fmt.Sprintf(`{"name":"user.created","version":1,"ts":%q,"payload":{"user_id":"u1","name":"N"}}`, compatTS))
			res := deDecode(t, reg, data)
			if res.err != nil {
				t.Fatalf("Decode: %v", res.err)
			}
			got, ok := res.evt.(*UserCreatedV2)
			if !ok {
				t.Fatalf("Decode returned %T, want *UserCreatedV2", res.evt)
			}
			if got.Email != "unknown@example.com" {
				t.Errorf("Email = %q, want %q", got.Email, "unknown@example.com")
			}
		})
	}
}
