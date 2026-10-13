---
layout: default
title: Schema Versioning
parent: Serialization
nav_order: 2
---

# Schema Versioning

Go Event Bus supports schema evolution through an **upcaster chain** pattern. Old event versions are automatically transformed to the latest version during decoding.

## Upcaster Interface

```go
type Upcaster interface {
    EventName() string
    FromVersion() int
    ToVersion() int
    Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error)
}
```

Upcasters are **pure functions** and **deterministic**. Each transforms raw JSON from one version to the next.

## Example: Three Versions

### V1 → V2: Add a field

```go
type UserRegisteredV1ToV2 struct{}

func (u *UserRegisteredV1ToV2) EventName() string { return "user.registered" }
func (u *UserRegisteredV1ToV2) FromVersion() int  { return 1 }
func (u *UserRegisteredV1ToV2) ToVersion() int    { return 2 }

func (u *UserRegisteredV1ToV2) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
    var v1 struct {
        UserID string `json:"user_id"`
        Email  string `json:"email"`
    }
    json.Unmarshal(raw, &v1)

    v2 := struct {
        UserID   string `json:"user_id"`
        Email    string `json:"email"`
        UserName string `json:"name"`
    }{
        UserID:   v1.UserID,
        Email:    v1.Email,
        UserName: extractNameFromEmail(v1.Email),
    }
    return json.Marshal(v2)
}
```

### V2 → V3: Split a field

```go
type UserRegisteredV2ToV3 struct{}

func (u *UserRegisteredV2ToV3) EventName() string { return "user.registered" }
func (u *UserRegisteredV2ToV3) FromVersion() int  { return 2 }
func (u *UserRegisteredV2ToV3) ToVersion() int    { return 3 }

func (u *UserRegisteredV2ToV3) Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
    var v2 struct {
        UserID   string `json:"user_id"`
        Email    string `json:"email"`
        UserName string `json:"name"`
    }
    json.Unmarshal(raw, &v2)

    first, last := splitName(v2.UserName)
    v3 := struct {
        UserID    string            `json:"user_id"`
        Email     string            `json:"email"`
        FirstName string            `json:"first_name"`
        LastName  string            `json:"last_name"`
        Metadata  map[string]string `json:"metadata"`
    }{
        UserID:    v2.UserID,
        Email:     v2.Email,
        FirstName: first,
        LastName:  last,
        Metadata:  map[string]string{"migrated_from": "v2"},
    }
    return json.Marshal(v3)
}
```

### Registration

```go
registry := eventjson.NewRegistry()

registry.Register("user.registered", func() event.Event { return &UserRegisteredV1{} }, 1)
registry.Register("user.registered", func() event.Event { return &UserRegisteredV2{} }, 2)
registry.Register("user.registered", func() event.Event { return &UserRegisteredV3{} }, 3)

registry.RegisterUpcaster(&UserRegisteredV1ToV2{})
registry.RegisterUpcaster(&UserRegisteredV2ToV3{})
```

### Automatic Chain

When decoding a V1 message, the registry automatically chains: V1 → V2 → V3. Handlers always receive the latest version.

If an upcaster is missing for any step, `Decode` returns an `*event.DecodeError` that wraps `event.ErrMissingUpcaster` (for example `"event: decode user.registered v1: event: missing upcaster: v1 to v3"`).

### Version rules

- **Version 0 or missing is version 1.** An envelope without a `version` key decodes as version 1, matching the default of `Encode`.
- **Newer than the latest registered version** returns `event.ErrUnknownVersion`, which is distinct from `event.ErrMissingUpcaster`. This is the rolling-deploy case where a newer producer is ahead of the consumer, so a later retry can succeed once the consumer is upgraded.
- **Upcasters must advance the version.** An upcaster whose `ToVersion()` is not greater than its `FromVersion()`, or goes past the latest registered version, makes `Decode` return an error instead of looping.
- **Upcasters run without the registry lock**, so an upcaster may call `Register` or `Decode` on the same registry.

See [JSON Registry]({{ site.baseurl }}/serialization/json-registry/#decode-errors) for the full error table.
