---
layout: default
title: Log Compaction
parent: Guides
nav_order: 1
---

# Log Compaction on JetStream

A compacted stream keeps only the latest message for each key and drops the older ones. On JetStream you get this with a per-subject limit. This guide says when that is safe for a stream that a `Bus` consumes, what it costs, and how it interacts with causation, the dead-letter queue and replay. It is JetStream only; Kafka compacted-topic guidance comes with the v0.5.0 Kafka adapter.

The library adds no compaction API and no tombstone convention. Everything below is stream configuration plus the behavior of the existing `Bus`, DLQ and `Replayer`. Every statement about the server cites the test that proves it. The tests live in [`jetstream/compaction_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/compaction_integration_test.go) and run in the integration job against `nats:2.15.1`.

## What compaction is on JetStream

Set `MaxMsgsPerSubject` on a stream. With the default discard policy (`DiscardOld`) and a value of `1`, publishing a second message on a subject evicts the first one. The stream then holds at most one message per subject.

```go
js.CreateStream(ctx, jetstream.StreamConfig{
    Name:              "USERS",
    Subjects:          []string{"user.>"},
    MaxMsgsPerSubject: 1,
})
```

A JetStream key-value bucket is built on the same mechanism, see [JetStream KV](#jetstream-kv).

## Prerequisite: entity-keyed subjects

The limit applies per subject, so compaction only means "latest per entity" when each entity has its own subject. Publish with one subject per entity, for example `user.<id>`, using `event.WithSubject`:

```go
bus.Emit(ctx, evt, event.WithSubject("user."+evt.ID))
```

`Bus.Subscribe(ctx, "user.>", handler, event.WithStream("USERS"))` sets the consumer `FilterSubject` to the subscribed subject, so a wildcard subscription receives every entity.

If every event of the stream shares one subject, `MaxMsgsPerSubject=1` keeps a single message for the whole stream. Do not compact a stream whose subjects are not keyed by entity.

## What history you lose

Proof: `TestIntegration_CompactionKeepsLatestPerEntity` (nats:2.15.1).

- **Latest per entity.** After publishing `user.1 v1`, `user.2 v1`, `user.1 v2`, `user.3 v1`, `user.1 v3`, the stream holds three messages, one per entity, and the sequences of `user.1 v1` and `user.1 v2` are gone.
- **One message per entity, in stream order.** A new wildcard consumer receives `user.2 v1`, `user.3 v1`, `user.1 v3`: the latest message of each entity, ordered by stream sequence, not by the order the entities were created.
- **Intermediate versions are lost, even for a caught-up consumer.** Versions published while no consumer is fetching are never delivered, except the latest. A consumer that is fully caught up can still miss `v4` when `v5` is published before its next fetch.
- **Rebuild current state with `DeliverLastPerSubject`.** A consumer created with `DeliverLastPerSubjectPolicy` returns the same per-entity snapshot. The `Bus` creates its consumers with the default deliver policy, so build such a consumer with the `nats.go` API when you need to rebuild state.

If your handlers need every version, do not compact.

## Rules

Each rule is a do or a don't. The proof follows the rule.

### Never compact the DLQ stream

Don't set `MaxMsgsPerSubject` on a dead-letter stream. The DLQ publisher writes to `prefix + original subject`, so a per-subject limit keeps one failure per entity and silently drops the earlier ones. Proof: `TestIntegration_CompactionDLQStreamKeepsOnlyLatestFailure`.

### Don't rely on a compacted message being dead-lettered

A message that was delivered and not yet acknowledged, and is then compacted away by a newer message of the same entity, is never redelivered and never reaches its final delivery. It is never dead-lettered, `Observer.Failure` never fires for it, and the DLQ stays empty: its failure vanishes. A `Nak` or `NakWithDelay` on it is accepted, but nothing comes back and only the newer message is delivered. Proof: `TestIntegration_CompactionInFlightMessageNeverDeadLettered`.

Do not use a compacted stream for work where losing a failed attempt is unacceptable.

### Never require the cause to be readable

`causation_id` and `correlation_id` travel in the envelope metadata and point to the event that caused the current one. On a compacted stream the causing event may have been evicted by a newer message of its entity. `causation_id` may name an event that no longer exists, so treat it as a lineage label for logs and traces, and never make a handler depend on reading the cause back from the stream.

### Make replay handlers idempotent and version-aware

`Replayer` republishes the DLQ record body byte for byte to the original subject, even when the original message was compacted away. The original consumer handles it. But the replayed snapshot is a verbatim copy of an older state: on a compacted stream it becomes the latest message of the entity and evicts the newer one. Proof: `TestIntegration_CompactionCompactedOriginalStillReplays`.

- Do give handlers idempotency and a version check, and ignore a snapshot that is older than the state you hold.
- Do pass `Limit` and replay deliberately; replaying onto a compacted stream rewrites the entity's latest state.
- Don't assume the replayed message is the newest state of the entity.

See [DLQ Invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/) and [JetStream Bus]({{ site.baseurl }}/transports/jetstream-bus/) for the record, the headers and the replay options.

### Treat compaction and the dedup window as independent

`Nats-Msg-Id` deduplication is independent of compaction. Re-publishing an id whose message was already compacted away is still reported as a duplicate while the id is inside the stream duplicate window: the publish returns `Duplicate` at the original sequence and the stream does not change. Compaction does not free an id for reuse. Proof: `TestIntegration_CompactionIndependentOfMsgIDDedup`.

A new version of an entity needs a new event id (`event.WithID`), not the id of a version you believe was compacted.

### Know that `DiscardNew` rejects instead of evicting

`DiscardNew` with `DiscardNewPerSubject` set makes the server reject the second message on a subject (API error 10077, "maximum messages per subject exceeded") and keep the first. That is a bounded log, not a compacted one: `Emit` returns an error. Proof: `TestIntegration_CompactionDiscardNewPerSubjectRejects`.

## Deleting entities

There is no tombstone API in the library, and no tombstone header is offered. Use one of these patterns.

### Model deletion as an event

Publish a regular deletion event on the entity subject, for example `UserDeleted` on `user.<id>`. It becomes the retained latest message of the entity, and consumers see "deleted" as the entity's final state. This works with every stream and needs no server feature.

### JetStream subject delete markers

With `AllowMsgTTL` and `SubjectDeleteMarkerTTL` on the stream (nats-server 2.11 or later, per the `nats.go` documentation), the server inserts a marker when the last message of a subject expires by `MaxAge` or by a per-message TTL:

- the marker is an empty-body message on the same subject;
- it carries the headers `Nats-Marker-Reason` (for example `MaxAge`), `Nats-Rollup: sub` and `Nats-TTL`;
- it appears after `MaxAge`, not exactly at `MaxAge`: expiry is lazy, so poll rather than sleep;
- the marker itself expires after its own TTL.

On nats-server 2.15.1, eviction by `MaxMsgsPerSubject` inserts no marker. A message replaced by a newer one just disappears. Proof: `TestIntegration_CompactionDeleteMarkerReachesConsumerAsEmptyBody` (observed on nats-server 2.15.1).

Don't enable delete markers on a stream that a `Bus` consumes. A consumer receives the marker as an ordinary message with an empty body, and the bus cannot decode an empty envelope: it dead-letters the marker as `decode_malformed` and the handler never sees it. Same proof test. If you need markers, consume the stream with a consumer of your own that understands them.

## JetStream KV

A KV bucket is a compacted stream with a key-value API on top. Proof: `TestIntegration_CompactionKVBucketIsAStream`.

- The bucket `<bucket>` is the stream `KV_<bucket>`, on the subjects `$KV.<bucket>.>`, with `MaxMsgsPerSubject` equal to the bucket `History`.
- `Delete` appends an empty-body message with the header `KV-Operation: DEL`. `Purge` appends `KV-Operation: PURGE` with `Nats-Rollup: sub`, which removes every earlier revision of the key. `Get` then reports the key as not found.

When to use which:

- Keep the **current state in KV** when readers need a lookup by key and do not need to react to changes.
- Use **events on a compacted stream** when consumers must react to each change and handle it with the invoker chain, the DLQ and replay.

The library has no KV API: use `nats.go` directly for a bucket.

## When not to compact

Do not compact a stream when its value is its history:

- audit logs, where every change must be provable later;
- event-sourced streams, which rebuild an aggregate from its full history;
- sagas and process managers that need every step, including intermediate ones;
- any stream where losing the failure of an in-flight message is not acceptable (see the rule above).

For these, bound the stream with `MaxAge` or size limits instead, and keep the DLQ separate.

## Proofs

All tests run against `nats:2.15.1`, and cover the server behavior with the real `Bus`, DLQ and `Replayer`.

| Claim | Test |
|---|---|
| `MaxMsgsPerSubject=1` keeps the latest per entity; a wildcard consumer sees one message per entity; intermediate versions are lost; `DeliverLastPerSubject` returns the snapshot | `TestIntegration_CompactionKeepsLatestPerEntity` |
| A compacted in-flight message is never redelivered or dead-lettered | `TestIntegration_CompactionInFlightMessageNeverDeadLettered` |
| A DLQ record whose original was compacted still replays, and the older snapshot evicts the newer state | `TestIntegration_CompactionCompactedOriginalStillReplays` |
| `Nats-Msg-Id` dedup is independent of compaction | `TestIntegration_CompactionIndependentOfMsgIDDedup` |
| A compacted DLQ stream keeps only the latest failure per entity | `TestIntegration_CompactionDLQStreamKeepsOnlyLatestFailure` |
| `DiscardNewPerSubject` rejects the second message | `TestIntegration_CompactionDiscardNewPerSubjectRejects` |
| Delete markers are empty-body messages with `Nats-Marker-Reason`; a `Bus` dead-letters them as `decode_malformed`; limit eviction leaves no marker | `TestIntegration_CompactionDeleteMarkerReachesConsumerAsEmptyBody` |
| A KV bucket is the stream `KV_<bucket>`; `Delete` and `Purge` write markers | `TestIntegration_CompactionKVBucketIsAStream` |

Source: [`jetstream/compaction_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/compaction_integration_test.go).
