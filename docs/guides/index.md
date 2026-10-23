---
layout: default
title: Guides
nav_order: 12
has_children: true
permalink: /guides/
---

# Guides

Guides are task-oriented pages that go beyond the reference of a single module. Each one states rules, not only trade-offs, and every claim about a broker is backed by a test that runs against a real server.

- [Log compaction]({{ site.baseurl }}/guides/log-compaction/): when a JetStream stream can keep only the latest message per entity, what that costs, and how it interacts with causation, the DLQ and replay.
