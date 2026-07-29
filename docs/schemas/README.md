# Schema Drafts

This directory contains structural schema drafts for the Phase One contracts.

They are design artifacts, not yet generated Rust types or complete JSON Schema documents. Their purposes are to:

- make boundaries reviewable;
- keep terminology consistent;
- identify versioned payloads;
- guide later Rust and JSON Schema implementation.

Files:

- `checkpoint-v0.1.yaml`
- `work-unit-v0.1.yaml`
- `work-unit-result-v0.1.yaml`
- `boot-package-manifest-v0.1.yaml`
- `event-v0.1.yaml`

All schemas reject direct runner mutation of authoritative Loop state. Domain-specific content remains inside explicitly versioned `domain` sections or referenced resources.

