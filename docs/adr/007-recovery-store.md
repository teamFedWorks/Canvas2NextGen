# ADR-007: Intermediate State Serialization and Recovery Store

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

Parsing large course packages (e.g. ZIP files containing hundreds of XML files, resources, and structures) is the slowest and most CPU/IO-intensive stage of the ingestion pipeline. If a downstream stage fails (like validation rules or Mongo exporting), restarting the job from the beginning requires downloading, extracting, and parsing the entire archive again.

## Decision

1. **Serialized Provider Model**: Save the intermediate state of the parsed format (the `provider_model`) as `provider_model.json` in the job's artifact/recovery store.
2. **Replay Triggering**: Implement the ability to start a job from the `PARSED` state by reading the serialized `provider_model.json` directly.
3. **Pipeline bypass**: When executing a replay from the recovery store, skip download, extraction, and parsing stages.

## Consequences

* **Faster Bug Fix Validation**: If a course fails due to a transformation or validation bug, developers can fix the code and rerun the pipeline instantly without re-uploading or parsing the package.
* **Reduced IO/CPU Load**: Eliminates redundant extraction and XML parsing on retries.
