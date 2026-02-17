// Example: Registry with Schema Versioning
//
// This example demonstrates how to use the JSON registry for
// event serialization with schema evolution via upcasting.
package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
)

// =============================================================================
// Event Versions - Schema Evolution
// =============================================================================

// UserRegisteredV1 - Original event (legacy)
type UserRegisteredV1 struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func (e *UserRegisteredV1) Name() string { return "user.registered" }
func (e *UserRegisteredV1) Version() int { return 1 }

// UserRegisteredV2 - Added name field
type UserRegisteredV2 struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	UserName string `json:"name"` // New field
}

func (e *UserRegisteredV2) Name() string { return "user.registered" }
func (e *UserRegisteredV2) Version() int { return 2 }

// UserRegisteredV3 - Split name into first/last, added metadata
type UserRegisteredV3 struct {
	UserID    string            `json:"user_id"`
	Email     string            `json:"email"`
	FirstName string            `json:"first_name"` // Split from name
	LastName  string            `json:"last_name"`  // Split from name
	Metadata  map[string]string `json:"metadata"`   // New field
}

func (e *UserRegisteredV3) Name() string { return "user.registered" }
func (e *UserRegisteredV3) Version() int { return 3 }

// =============================================================================
// Upcasters - Transform old versions to new
// =============================================================================

// UserRegisteredV1ToV2 upgrades v1 to v2
type UserRegisteredV1ToV2 struct{}

func (u *UserRegisteredV1ToV2) EventName() string { return "user.registered" }
func (u *UserRegisteredV1ToV2) FromVersion() int  { return 1 }
func (u *UserRegisteredV1ToV2) ToVersion() int    { return 2 }

func (u *UserRegisteredV1ToV2) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var v1 UserRegisteredV1
	if err := json.Unmarshal(raw, &v1); err != nil {
		return nil, err
	}

	v2 := UserRegisteredV2{
		UserID:   v1.UserID,
		Email:    v1.Email,
		UserName: extractNameFromEmail(v1.Email), // Derive name from email
	}

	slog.Debug("upcasted event", "from", 1, "to", 2, "user_id", v1.UserID)
	return json.Marshal(v2)
}

// UserRegisteredV2ToV3 upgrades v2 to v3
type UserRegisteredV2ToV3 struct{}

func (u *UserRegisteredV2ToV3) EventName() string { return "user.registered" }
func (u *UserRegisteredV2ToV3) FromVersion() int  { return 2 }
func (u *UserRegisteredV2ToV3) ToVersion() int    { return 3 }

func (u *UserRegisteredV2ToV3) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var v2 UserRegisteredV2
	if err := json.Unmarshal(raw, &v2); err != nil {
		return nil, err
	}

	firstName, lastName := splitName(v2.UserName)

	v3 := UserRegisteredV3{
		UserID:    v2.UserID,
		Email:     v2.Email,
		FirstName: firstName,
		LastName:  lastName,
		Metadata: map[string]string{
			"migrated_from": "v2",
			"original_name": v2.UserName,
		},
	}

	slog.Debug("upcasted event", "from", 2, "to", 3, "user_id", v2.UserID)
	return json.Marshal(v3)
}

// =============================================================================
// Helper functions
// =============================================================================

func extractNameFromEmail(email string) string {
	// Simple extraction: john.doe@example.com -> John Doe
	for i, c := range email {
		if c == '@' {
			return email[:i]
		}
	}
	return "Unknown"
}

func splitName(name string) (first, last string) {
	for i, c := range name {
		if c == ' ' || c == '.' {
			return name[:i], name[i+1:]
		}
	}
	return name, ""
}

// =============================================================================
// Example Usage
// =============================================================================

func ExampleRegistryVersioning() {
	// Create registry
	registry := eventjson.NewRegistry()

	// Register all versions (handlers always receive latest version)
	registry.Register("user.registered", func() event.Event { return &UserRegisteredV1{} }, 1)
	registry.Register("user.registered", func() event.Event { return &UserRegisteredV2{} }, 2)
	registry.Register("user.registered", func() event.Event { return &UserRegisteredV3{} }, 3)

	// Register upcasters for schema migration
	registry.RegisterUpcaster(&UserRegisteredV1ToV2{})
	registry.RegisterUpcaster(&UserRegisteredV2ToV3{})

	ctx := context.Background()

	// ==========================================================================
	// Scenario 1: Encode a new event (uses latest version)
	// ==========================================================================
	newEvent := &UserRegisteredV3{
		UserID:    "user-new",
		Email:     "alice@example.com",
		FirstName: "Alice",
		LastName:  "Smith",
		Metadata:  map[string]string{"source": "web"},
	}

	encoded, err := registry.Encode(ctx, newEvent)
	if err != nil {
		slog.Error("encode failed", "error", err)
		return
	}

	fmt.Println("Encoded V3 event:")
	fmt.Println(string(encoded))

	// ==========================================================================
	// Scenario 2: Decode a legacy V1 event (auto-upcasts to V3)
	// ==========================================================================
	legacyV1Data := []byte(`{
		"name": "user.registered",
		"version": 1,
		"ts": "2024-01-01T00:00:00Z",
		"payload": {"user_id": "user-legacy", "email": "bob.jones@example.com"}
	}`)

	_, decoded, err := registry.Decode(ctx, legacyV1Data)
	if err != nil {
		slog.Error("decode failed", "error", err)
		return
	}

	// Handler always receives V3
	v3Event := decoded.(*UserRegisteredV3)
	fmt.Printf("\nDecoded V1 -> V3:\n")
	fmt.Printf("  UserID: %s\n", v3Event.UserID)
	fmt.Printf("  Email: %s\n", v3Event.Email)
	fmt.Printf("  FirstName: %s\n", v3Event.FirstName)
	fmt.Printf("  LastName: %s\n", v3Event.LastName)
	fmt.Printf("  Metadata: %v\n", v3Event.Metadata)

	// ==========================================================================
	// Scenario 3: Decode a V2 event (auto-upcasts to V3)
	// ==========================================================================
	legacyV2Data := []byte(`{
		"name": "user.registered",
		"version": 2,
		"ts": "2024-06-01T00:00:00Z",
		"payload": {"user_id": "user-v2", "email": "charlie@example.com", "name": "Charlie Brown"}
	}`)

	_, decoded2, err := registry.Decode(ctx, legacyV2Data)
	if err != nil {
		slog.Error("decode failed", "error", err)
		return
	}

	v3Event2 := decoded2.(*UserRegisteredV3)
	fmt.Printf("\nDecoded V2 -> V3:\n")
	fmt.Printf("  UserID: %s\n", v3Event2.UserID)
	fmt.Printf("  FirstName: %s\n", v3Event2.FirstName)
	fmt.Printf("  LastName: %s\n", v3Event2.LastName)
	fmt.Printf("  Metadata: %v\n", v3Event2.Metadata)
}
