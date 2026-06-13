# ADR-004: Event-Driven Job Progress and Audit Logging

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

In production, understanding *why* an ingestion job failed is difficult if the system only stores a coarse final state (e.g., `FAILED`). Knowing the history of intermediate steps (e.g., did it fail during parsing or database export?) is crucial for operations, debugging, and metrics calculation.

## Decision

1. **JobState Machine**: Implement a fine-grained state machine defining states for the pipeline lifecycle: `CREATED`, `DOWNLOAD_STARTED`, `DOWNLOAD_FINISHED`, `EXTRACTION_STARTED`, `EXTRACTION_FINISHED`, `PARSE_STARTED`, `PARSE_FINISHED`, `VALIDATION_STARTED`, `VALIDATION_FINISHED`, `EXPORT_STARTED`, `SUCCESS`, and `FAILED`.
2. **Chronological Events**: In the `jobs` database collection, track a list of `events` for each job.
3. **Event Schema**: Every `JobEvent` will store:
   * `timestamp`: Time the event occurred.
   * `state`: The state being entered or the process step.
   * `message`: Human-readable summary of the action.
   * `details`: Optional diagnostic data (e.g. exception tracebacks or count metrics).

## Consequences

* **Improved Observability**: Operational engineers can query the exact event history of any job to see exactly where it got stuck.
* **Telemetry**: Easy to calculate how long each individual phase of the pipeline took.
* **Error Tracking**: Detailed error contexts are stored directly within the state transition records rather than scattered through generic application logs.
