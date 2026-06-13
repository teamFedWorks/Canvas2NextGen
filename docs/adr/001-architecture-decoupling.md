# ADR-001: Project Renaming and Decoupling Ingestion Pipelines

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

The system was previously structured as a course migration utility named "Universal Course Automation Engine" (UCAE), targeting specific Canvas-to-MongoDB ingestion pathways.
Additionally, the base provider interface `BaseLmsProvider` was coupled with pipeline building and manifest generation (having methods like `build_pipeline()` and `generate_manifest()`). This coupled the provider parsing logic with downstream execution and reporting, leading to duplication of reporting structures and leakage of orchestration concerns into format adapters.

## Decision

1. **Project Renaming**: Formally rename the project from **UCAE (Universal Course Automation Engine)** to **ULCP (Universal Learning Content Platform)**. This name represents a generic ingestion platform capable of handling Canvas, Blackboard, Moodle, D2L, SCORM, IMS Common Cartridge, and other future format packages.
2. **Decouple Provider from Execution**: Remove `build_pipeline()` from `BaseLmsProvider`. The provider's role is restricted to parsing the input package and producing a canonical representation.
3. **Decouple Provider from Reporting**: Remove `generate_manifest()` from `BaseLmsProvider`. Ingestion reporting and manifest generation are moved to a global `ReportingService` that receives the canonical model representation.
4. **Registry Architecture**: Introduce a `ProviderRegistry` for resolving format types, and a `PipelineRegistry` for resolving and executing processing stages appropriate for the resolved provider type.

## Consequences

* **Reduced Complexity**: LMS provider modules are simpler, smaller, and focused only on parsing vendor-specific logic.
* **Separation of Concerns**: Reporting structures are unified, preventing duplicate logic.
* **Scalability**: Adding new LMS formats only requires implementing parsing/validation adapters rather than workflow execution scripts.
