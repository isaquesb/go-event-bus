package json_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
)

// deWatchdog bounds every test that could hang if the registry misbehaves.
const deWatchdog = 2 * time.Second

type deResult struct {
	ctx context.Context
	evt event.Event
	err error
}

// deDecode runs Decode in a goroutine and fails the test when it does not
// finish within deWatchdog (deadlock or endless upcast loop).
func deDecode(t *testing.T, reg *eventjson.Registry, data []byte) deResult {
	t.Helper()

	done := make(chan deResult, 1)
	go func() {
		ctx, evt, err := reg.Decode(context.Background(), data)
		done <- deResult{ctx, evt, err}
	}()

	select {
	case res := <-done:
		return res
	case <-time.After(deWatchdog):
		t.Fatalf("Decode did not finish within %s", deWatchdog)
		return deResult{}
	}
}

// deRegistry registers user.created v1 and v2 with the v1 to v2 upcaster.
func deRegistry() *eventjson.Registry {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(&userCreatedV1ToV2Upcaster{})
	return reg
}

// deRegistryV1 registers user.created v1 only, so the payload reaches the
// factory without an upcaster in between.
func deRegistryV1() *eventjson.Registry {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	return reg
}

// deRegistryNoUpcaster registers user.created v2 only, with no way up from v1.
func deRegistryNoUpcaster() *eventjson.Registry {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	return reg
}

func deRegistryNilFactory() *eventjson.Registry {
	reg := eventjson.NewRegistry()
	reg.Register("nil.factory", nil, 1)
	reg.Register("nil.product", func() event.Event { return nil }, 1)
	return reg
}

func TestDecode_Errors(t *testing.T) {
	tests := []struct {
		name     string
		reg      func() *eventjson.Registry
		data     string
		want     error
		wantName string
	}{
		{"not json", deRegistry, `not json`, event.ErrMalformedEnvelope, ""},
		{"empty input", deRegistry, ``, event.ErrMalformedEnvelope, ""},
		{"null", deRegistry, `null`, event.ErrMalformedEnvelope, ""},
		{"array", deRegistry, `[]`, event.ErrMalformedEnvelope, ""},
		{"empty object", deRegistry, `{}`, event.ErrMalformedEnvelope, ""},
		{"dlq record", deRegistry, `{"event":{"user_id":"u"},"error":"boom"}`, event.ErrMalformedEnvelope, ""},
		{"negative version", deRegistry, `{"name":"user.created","version":-1,"payload":{}}`, event.ErrMalformedEnvelope, "user.created"},
		{"unknown name with version", deRegistry, `{"name":"no.such","version":1,"payload":{}}`, event.ErrUnknownEvent, "no.such"},
		{"unknown name without version", deRegistry, `{"name":"no.such","payload":{}}`, event.ErrUnknownEvent, "no.such"},
		{"future version", deRegistry, `{"name":"user.created","version":99,"payload":{}}`, event.ErrUnknownVersion, "user.created"},
		{"missing upcaster", deRegistryNoUpcaster, `{"name":"user.created","version":1,"payload":{}}`, event.ErrMissingUpcaster, "user.created"},
		{"payload is a string", deRegistryV1, `{"name":"user.created","version":1,"payload":"oops"}`, event.ErrMalformedEnvelope, "user.created"},
		{"payload is missing", deRegistryV1, `{"name":"user.created","version":1}`, event.ErrMalformedEnvelope, "user.created"},
		{"headers are not strings", deRegistryV1, `{"name":"user.created","version":1,"headers":{"a":1},"payload":{}}`, event.ErrMalformedEnvelope, "user.created"},
		{"nil factory", deRegistryNilFactory, `{"name":"nil.factory","version":1,"payload":{}}`, event.ErrUnknownEvent, "nil.factory"},
		{"factory returns nil", deRegistryNilFactory, `{"name":"nil.product","version":1,"payload":{}}`, event.ErrUnknownEvent, "nil.product"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := deDecode(t, tc.reg(), []byte(tc.data))

			if res.err == nil {
				t.Fatalf("expected an error, got event %T", res.evt)
			}
			if res.evt != nil {
				t.Errorf("expected a nil event with an error, got %T", res.evt)
			}
			if !errors.Is(res.err, tc.want) {
				t.Errorf("errors.Is(%v, %v) = false", res.err, tc.want)
			}

			var de *event.DecodeError
			if !errors.As(res.err, &de) {
				t.Fatalf("errors.As *event.DecodeError failed for %T: %v", res.err, res.err)
			}
			if de.Name != tc.wantName {
				t.Errorf("DecodeError.Name = %q, want %q", de.Name, tc.wantName)
			}
		})
	}
}

func TestDecode_ErrorsCarryVersion(t *testing.T) {
	res := deDecode(t, deRegistry(), []byte(`{"name":"user.created","version":99,"payload":{}}`))

	var de *event.DecodeError
	if !errors.As(res.err, &de) {
		t.Fatalf("expected *event.DecodeError, got %T: %v", res.err, res.err)
	}
	if de.Name != "user.created" || de.Version != 99 {
		t.Errorf("DecodeError = {%q, v%d}, want {user.created, v99}", de.Name, de.Version)
	}

	res = deDecode(t, deRegistryNoUpcaster(), []byte(`{"name":"user.created","version":1,"payload":{}}`))
	if !errors.As(res.err, &de) {
		t.Fatalf("expected *event.DecodeError, got %T: %v", res.err, res.err)
	}
	if de.Version != 1 {
		t.Errorf("missing upcaster DecodeError.Version = %d, want 1", de.Version)
	}
}

func TestDecode_Version0IsV1(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)

	res := deDecode(t, reg, []byte(`{"name":"user.created","payload":{"user_id":"u-1","name":"Ann"}}`))
	if res.err != nil {
		t.Fatalf("decode error: %v", res.err)
	}

	got, ok := res.evt.(*UserCreatedV1)
	if !ok {
		t.Fatalf("expected *UserCreatedV1, got %T", res.evt)
	}
	if got.UserID != "u-1" || got.UserName != "Ann" {
		t.Errorf("decoded %+v", got)
	}

	md, ok := event.MetadataFromContext(res.ctx)
	if !ok {
		t.Fatal("expected Metadata in the decoded ctx")
	}
	if md.Version != 1 {
		t.Errorf("Metadata.Version = %d, want 1", md.Version)
	}
}

func TestDecode_Version0UpcastsFromV1(t *testing.T) {
	res := deDecode(t, deRegistry(), []byte(`{"name":"user.created","payload":{"user_id":"u-1","name":"Ann"}}`))
	if res.err != nil {
		t.Fatalf("decode error: %v", res.err)
	}
	if _, ok := res.evt.(*UserCreatedV2); !ok {
		t.Fatalf("expected *UserCreatedV2, got %T", res.evt)
	}
}

func TestDecode_UpcasterErrorIsDecodeErrorWithoutSentinel(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(&failingUpcaster{})

	res := deDecode(t, reg, []byte(`{"name":"user.created","version":1,"payload":{"user_id":"u"}}`))
	if res.err == nil {
		t.Fatal("expected an error from the failing upcaster")
	}

	var de *event.DecodeError
	if !errors.As(res.err, &de) {
		t.Fatalf("expected *event.DecodeError, got %T: %v", res.err, res.err)
	}
	if de.Name != "user.created" {
		t.Errorf("DecodeError.Name = %q, want user.created", de.Name)
	}

	for _, sentinel := range []error{
		event.ErrUnknownEvent, event.ErrMalformedEnvelope, event.ErrMissingUpcaster, event.ErrUnknownVersion,
	} {
		if errors.Is(res.err, sentinel) {
			t.Errorf("upcaster failure must not match %v", sentinel)
		}
	}
}

// deNonAdvancingUpcaster claims to move v1 to v1, which would loop forever.
type deNonAdvancingUpcaster struct{}

func (deNonAdvancingUpcaster) EventName() string { return "user.created" }
func (deNonAdvancingUpcaster) FromVersion() int  { return 1 }
func (deNonAdvancingUpcaster) ToVersion() int    { return 1 }
func (deNonAdvancingUpcaster) Upcast(_ context.Context, raw json.RawMessage) (json.RawMessage, error) {
	return raw, nil
}

// deOvershootingUpcaster jumps past the latest registered version.
type deOvershootingUpcaster struct{}

func (deOvershootingUpcaster) EventName() string { return "user.created" }
func (deOvershootingUpcaster) FromVersion() int  { return 1 }
func (deOvershootingUpcaster) ToVersion() int    { return 5 }
func (deOvershootingUpcaster) Upcast(_ context.Context, raw json.RawMessage) (json.RawMessage, error) {
	return raw, nil
}

func TestDecode_NonAdvancingUpcasterReturnsError(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(deNonAdvancingUpcaster{})

	res := deDecode(t, reg, []byte(`{"name":"user.created","version":1,"payload":{}}`))
	if res.err == nil {
		t.Fatal("expected an error for a non-advancing upcaster")
	}

	var de *event.DecodeError
	if !errors.As(res.err, &de) {
		t.Fatalf("expected *event.DecodeError, got %T: %v", res.err, res.err)
	}
}

func TestDecode_OvershootingUpcasterReturnsError(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(deOvershootingUpcaster{})

	res := deDecode(t, reg, []byte(`{"name":"user.created","version":1,"payload":{}}`))
	if res.err == nil {
		t.Fatal("expected an error for an overshooting upcaster")
	}

	var de *event.DecodeError
	if !errors.As(res.err, &de) {
		t.Fatalf("expected *event.DecodeError, got %T: %v", res.err, res.err)
	}
}

// deReentrantUpcaster calls back into the registry while it runs. With a
// registry lock held during Upcast, Register deadlocks.
type deReentrantUpcaster struct {
	reg    *eventjson.Registry
	nested bool
}

func (deReentrantUpcaster) EventName() string { return "user.created" }
func (deReentrantUpcaster) FromVersion() int  { return 1 }
func (deReentrantUpcaster) ToVersion() int    { return 2 }
func (u deReentrantUpcaster) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if u.nested {
		// Decode a plain v2 envelope from inside the upcaster.
		_, _, err := u.reg.Decode(ctx, []byte(`{"name":"user.created","version":2,"payload":{"user_id":"n"}}`))
		if err != nil {
			return nil, err
		}
	} else {
		u.reg.Register("other.event", func() event.Event { return &SimpleEvent{} }, 1)
	}
	return json.Marshal(UserCreatedV2{UserID: "u", UserName: "Ann", Email: "a@example.com"})
}

func TestDecode_UpcasterMayRegisterWithoutDeadlock(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(deReentrantUpcaster{reg: reg})

	res := deDecode(t, reg, []byte(`{"name":"user.created","version":1,"payload":{"user_id":"u"}}`))
	if res.err != nil {
		t.Fatalf("decode error: %v", res.err)
	}
	if _, ok := res.evt.(*UserCreatedV2); !ok {
		t.Fatalf("expected *UserCreatedV2, got %T", res.evt)
	}
}

func TestDecode_UpcasterMayDecodeNested(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(deReentrantUpcaster{reg: reg, nested: true})

	res := deDecode(t, reg, []byte(`{"name":"user.created","version":1,"payload":{"user_id":"u"}}`))
	if res.err != nil {
		t.Fatalf("decode error: %v", res.err)
	}
	if _, ok := res.evt.(*UserCreatedV2); !ok {
		t.Fatalf("expected *UserCreatedV2, got %T", res.evt)
	}
}

func TestDecode_ErrorContextCarriesMetadata(t *testing.T) {
	res := deDecode(t, deRegistry(),
		[]byte(`{"id":"x-1","correlation_id":"c-1","name":"user.created","version":99,"payload":{}}`))

	if !errors.Is(res.err, event.ErrUnknownVersion) {
		t.Fatalf("expected ErrUnknownVersion, got %v", res.err)
	}

	md, ok := event.MetadataFromContext(res.ctx)
	if !ok {
		t.Fatal("the ctx returned with the decode error must carry Metadata")
	}
	if md.ID != "x-1" || md.CorrelationID != "c-1" {
		t.Errorf("Metadata = %+v, want ID x-1 and CorrelationID c-1", md)
	}
	if md.Name != "user.created" {
		t.Errorf("Metadata.Name = %q, want user.created", md.Name)
	}
}

func TestDecode_SuccessStillWorksForUpcast(t *testing.T) {
	res := deDecode(t, deRegistry(), []byte(`{"name":"user.created","version":1,"payload":{"user_id":"u-1","name":"Ann"}}`))
	if res.err != nil {
		t.Fatalf("decode error: %v", res.err)
	}

	got, ok := res.evt.(*UserCreatedV2)
	if !ok {
		t.Fatalf("expected *UserCreatedV2, got %T", res.evt)
	}
	if got.Email != "unknown@example.com" {
		t.Errorf("Email = %q, want unknown@example.com", got.Email)
	}
}
