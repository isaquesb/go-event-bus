package json_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
)

// UserCreatedV1 is a test event version 1
type UserCreatedV1 struct {
	UserID   string `json:"user_id"`
	UserName string `json:"name"`
}

func (e *UserCreatedV1) Name() string { return "user.created" }
func (e *UserCreatedV1) Version() int { return 1 }

// UserCreatedV2 is a test event version 2 (with email)
type UserCreatedV2 struct {
	UserID   string `json:"user_id"`
	UserName string `json:"name"`
	Email    string `json:"email"`
}

func (e *UserCreatedV2) Name() string { return "user.created" }
func (e *UserCreatedV2) Version() int { return 2 }

// SimpleEvent without version
type SimpleEvent struct {
	Message string `json:"message"`
}

func (e *SimpleEvent) Name() string { return "simple.event" }

func TestRegistry_EncodeAndDecode(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)

	original := &UserCreatedV1{
		UserID:   "user-123",
		UserName: "John",
	}

	data, err := reg.Encode(context.Background(), original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	_, decoded, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	result, ok := decoded.(*UserCreatedV1)
	if !ok {
		t.Fatalf("expected *UserCreatedV1, got %T", decoded)
	}

	if result.UserID != original.UserID {
		t.Errorf("expected UserID '%s', got '%s'", original.UserID, result.UserID)
	}
	if result.UserName != original.UserName {
		t.Errorf("expected UserName '%s', got '%s'", original.UserName, result.UserName)
	}
}

func TestRegistry_EnvelopeFormat(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)

	evt := &UserCreatedV1{
		UserID:   "user-123",
		UserName: "John",
	}

	data, err := reg.Encode(context.Background(), evt)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var envelope event.Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal envelope error: %v", err)
	}

	if envelope.Name != "user.created" {
		t.Errorf("expected name 'user.created', got '%s'", envelope.Name)
	}
	if envelope.Version != 1 {
		t.Errorf("expected version 1, got %d", envelope.Version)
	}
	if envelope.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
	if len(envelope.Payload) == 0 {
		t.Error("expected non-empty payload")
	}
}

func TestRegistry_DefaultVersion(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("simple.event", func() event.Event { return &SimpleEvent{} }, 1)

	evt := &SimpleEvent{Message: "hello"}

	data, err := reg.Encode(context.Background(), evt)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var envelope event.Envelope
	_ = json.Unmarshal(data, &envelope)

	// Event without Version() should default to 1
	if envelope.Version != 1 {
		t.Errorf("expected default version 1, got %d", envelope.Version)
	}
}

func TestRegistry_Upcasting(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)

	// Register upcaster from v1 to v2
	reg.RegisterUpcaster(&userCreatedV1ToV2Upcaster{})

	// Encode a v1 event
	v1Event := &UserCreatedV1{
		UserID:   "user-123",
		UserName: "John",
	}

	// Manually create v1 envelope (simulating old data)
	payload, _ := json.Marshal(v1Event)
	envelope := event.Envelope{
		Name:    "user.created",
		Version: 1,
		Payload: payload,
	}
	data, _ := json.Marshal(envelope)

	// Decode should upcast to v2
	_, decoded, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	result, ok := decoded.(*UserCreatedV2)
	if !ok {
		t.Fatalf("expected *UserCreatedV2, got %T", decoded)
	}

	if result.UserID != "user-123" {
		t.Errorf("expected UserID 'user-123', got '%s'", result.UserID)
	}
	if result.UserName != "John" {
		t.Errorf("expected UserName 'John', got '%s'", result.UserName)
	}
	if result.Email != "unknown@example.com" {
		t.Errorf("expected Email 'unknown@example.com', got '%s'", result.Email)
	}
}

func TestRegistry_MissingUpcaster(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	// No upcaster registered for v1 to v2

	// Create v1 envelope
	payload, _ := json.Marshal(&UserCreatedV1{UserID: "123", UserName: "John"})
	envelope := event.Envelope{
		Name:    "user.created",
		Version: 1,
		Payload: payload,
	}
	data, _ := json.Marshal(envelope)

	_, _, err := reg.Decode(context.Background(), data)
	if err == nil {
		t.Error("expected error for missing upcaster")
	}
}

func TestRegistry_MultipleVersions(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)

	// Encoding should use the version from the event
	v2Event := &UserCreatedV2{
		UserID:   "user-123",
		UserName: "John",
		Email:    "john@example.com",
	}

	data, _ := reg.Encode(context.Background(), v2Event)

	var envelope event.Envelope
	_ = json.Unmarshal(data, &envelope)

	if envelope.Version != 2 {
		t.Errorf("expected version 2, got %d", envelope.Version)
	}
}

func TestRegistry_LatestVersionTracking(t *testing.T) {
	reg := eventjson.NewRegistry()

	// Register out of order
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)

	// Latest should be 2, so old events should upcast to 2
	reg.RegisterUpcaster(&userCreatedV1ToV2Upcaster{})

	// v1 payload
	payload, _ := json.Marshal(&UserCreatedV1{UserID: "123", UserName: "John"})
	envelope := event.Envelope{
		Name:    "user.created",
		Version: 1,
		Payload: payload,
	}
	data, _ := json.Marshal(envelope)

	_, decoded, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if _, ok := decoded.(*UserCreatedV2); !ok {
		t.Errorf("expected *UserCreatedV2, got %T", decoded)
	}
}

func TestRegistry_InvalidJSON(t *testing.T) {
	reg := eventjson.NewRegistry()

	_, _, err := reg.Decode(context.Background(), []byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRegistry_ChainedUpcasting(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.Register("user.created", func() event.Event { return &UserCreatedV3{} }, 3)

	// Register upcasters v1->v2 and v2->v3
	reg.RegisterUpcaster(&userCreatedV1ToV2Upcaster{})
	reg.RegisterUpcaster(&userCreatedV2ToV3Upcaster{})

	// v1 payload should upcast to v3
	payload, _ := json.Marshal(&UserCreatedV1{UserID: "123", UserName: "John"})
	envelope := event.Envelope{
		Name:    "user.created",
		Version: 1,
		Payload: payload,
	}
	data, _ := json.Marshal(envelope)

	_, decoded, err := reg.Decode(context.Background(), data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	result, ok := decoded.(*UserCreatedV3)
	if !ok {
		t.Fatalf("expected *UserCreatedV3, got %T", decoded)
	}

	if result.UserID != "123" {
		t.Errorf("expected UserID '123', got '%s'", result.UserID)
	}
	if result.Email != "unknown@example.com" {
		t.Errorf("expected Email from v2 upcaster, got '%s'", result.Email)
	}
	if !result.Verified {
		t.Error("expected Verified=true from v3 upcaster")
	}
}

// UserCreatedV3 for chained upcasting test
type UserCreatedV3 struct {
	UserID   string `json:"user_id"`
	UserName string `json:"name"`
	Email    string `json:"email"`
	Verified bool   `json:"verified"`
}

func (e *UserCreatedV3) Name() string { return "user.created" }
func (e *UserCreatedV3) Version() int { return 3 }

// Upcasters

type userCreatedV1ToV2Upcaster struct{}

func (u *userCreatedV1ToV2Upcaster) EventName() string { return "user.created" }
func (u *userCreatedV1ToV2Upcaster) FromVersion() int  { return 1 }
func (u *userCreatedV1ToV2Upcaster) ToVersion() int    { return 2 }
func (u *userCreatedV1ToV2Upcaster) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var v1 UserCreatedV1
	if err := json.Unmarshal(raw, &v1); err != nil {
		return nil, err
	}

	v2 := UserCreatedV2{
		UserID:   v1.UserID,
		UserName: v1.UserName,
		Email:    "unknown@example.com", // Default value for new field
	}

	return json.Marshal(v2)
}

type userCreatedV2ToV3Upcaster struct{}

func (u *userCreatedV2ToV3Upcaster) EventName() string { return "user.created" }
func (u *userCreatedV2ToV3Upcaster) FromVersion() int  { return 2 }
func (u *userCreatedV2ToV3Upcaster) ToVersion() int    { return 3 }
func (u *userCreatedV2ToV3Upcaster) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var v2 UserCreatedV2
	if err := json.Unmarshal(raw, &v2); err != nil {
		return nil, err
	}

	v3 := UserCreatedV3{
		UserID:   v2.UserID,
		UserName: v2.UserName,
		Email:    v2.Email,
		Verified: true, // Default value for new field
	}

	return json.Marshal(v3)
}

type failingUpcaster struct{}

func (u *failingUpcaster) EventName() string { return "user.created" }
func (u *failingUpcaster) FromVersion() int  { return 1 }
func (u *failingUpcaster) ToVersion() int    { return 2 }
func (u *failingUpcaster) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("upcast failed")
}

func TestRegistry_UpcasterError(t *testing.T) {
	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(&failingUpcaster{})

	payload, _ := json.Marshal(&UserCreatedV1{UserID: "123", UserName: "John"})
	envelope := event.Envelope{
		Name:    "user.created",
		Version: 1,
		Payload: payload,
	}
	data, _ := json.Marshal(envelope)

	_, _, err := reg.Decode(context.Background(), data)
	if err == nil {
		t.Error("expected error from failing upcaster")
	}
}
