---
layout: default
title: Registry Versioning
parent: Examples
nav_order: 3
---

# Registry Versioning

Demonstrates schema evolution with automatic upcasting from V1 → V2 → V3.

Source: [`examples/03_registry_versioning.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/03_registry_versioning.go)

## Event Versions

```go
// V1 - Original
type UserRegisteredV1 struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

// V2 - Added name
type UserRegisteredV2 struct {
    UserID   string `json:"user_id"`
    Email    string `json:"email"`
    UserName string `json:"name"`
}

// V3 - Split name, added metadata
type UserRegisteredV3 struct {
    UserID    string            `json:"user_id"`
    Email     string            `json:"email"`
    FirstName string            `json:"first_name"`
    LastName  string            `json:"last_name"`
    Metadata  map[string]string `json:"metadata"`
}
```

## Register and Upcast

```go
registry := eventjson.NewRegistry()

registry.Register("user.registered", func() event.Event { return &UserRegisteredV1{} }, 1)
registry.Register("user.registered", func() event.Event { return &UserRegisteredV2{} }, 2)
registry.Register("user.registered", func() event.Event { return &UserRegisteredV3{} }, 3)

registry.RegisterUpcaster(&UserRegisteredV1ToV2{})
registry.RegisterUpcaster(&UserRegisteredV2ToV3{})
```

## Decode Legacy Events

```go
// A V1 message from the wire
legacyV1Data := []byte(`{
    "name": "user.registered",
    "version": 1,
    "ts": "2024-01-01T00:00:00Z",
    "payload": {"user_id": "user-legacy", "email": "bob.jones@example.com"}
}`)

_, decoded, _ := registry.Decode(ctx, legacyV1Data)

// Handler always receives V3
v3Event := decoded.(*UserRegisteredV3)
// v3Event.FirstName = "bob"
// v3Event.LastName  = "jones"
// v3Event.Metadata  = {"migrated_from": "v2", "original_name": "bob.jones"}
```

The upcaster chain runs automatically: V1 → V2 → V3.
