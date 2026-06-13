# ADR-003: Introduce PipelineContext Pattern

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

The ingestion pipeline consists of multiple distinct stages (Extraction, Detection, Parsing, Validation, Canonical Transformation, Normalization, Asset Uploading, Export).
Passing data between these stages using long parameter lists or global variables is fragile, couples stages unnecessarily, and makes it difficult to maintain track of metrics, logs, and workspace references in a structured way.

## Decision

1. **PipelineContext Definition**: Introduce a unified `PipelineContext` class that serves as the single source of truth for all runtime variables for a specific ingestion job execution.
2. **Context Fields**: The `PipelineContext` will contain:
   * `job_id`: Unique tracking ID for the execution.
   * `workspace`: Reference to the `ExtractedWorkspace`.
   * `provider_metadata`: Metadata of the detected provider.
   * `provider_model`: The parsed provider-specific model structure.
   * `canonical_course`: The mapped, vendor-neutral canonical model.
   * `validation_issues`: A list of structured issues identified during validation stages.
   * `metrics`: A dictionary of performance and volume counters.
   * `logs`: Runtime logs specific to this execution.
   * `recovery_store`: Replay/recovery artifacts.
3. **Unified Interface**: Define pipeline execution stages such that they receive the `PipelineContext` as their primary input and record their outputs/states directly back into the context.

## Consequences

* **Simplified Signatures**: Stage methods are standard: `def run(self, context: PipelineContext) -> None`.
* **State Inspection**: The context can be serialized at any point to inspect the internal state of the run, aiding in debugging and reporting.
* **Flexibility**: New properties (like execution flags or university IDs) can be added to the context without modifying method signatures across all stages.
