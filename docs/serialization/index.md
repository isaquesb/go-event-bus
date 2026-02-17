---
layout: default
title: Serialization
nav_order: 6
has_children: true
permalink: /serialization/
---

# Serialization

The serialization layer converts events to and from wire format. The `Registry` interface decouples event definitions from their serialization, enabling schema evolution and trace propagation.

Currently, Go Event Bus provides a JSON-based registry implementation.
