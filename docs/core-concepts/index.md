---
layout: default
title: Core Concepts
nav_order: 3
has_children: true
permalink: /core-concepts/
---

# Core Concepts

Go Event Bus is built on three orthogonal layers:

```
┌─────────────────────────────────────────────┐
│           Event & Serialization             │
│  Event, WithVersion, Registry, Envelope     │
├─────────────────────────────────────────────┤
│          Execution Pipeline                 │
│  Invoker Chain (Tracing → Metrics → ...)    │
├─────────────────────────────────────────────┤
│          Transport / Delivery               │
│  LocalBus │ NatsBus │ JetStreamBus          │
└─────────────────────────────────────────────┘
```

Each layer is decoupled from the others. You define events independently of how they're dispatched, and the execution pipeline works the same way regardless of transport.
