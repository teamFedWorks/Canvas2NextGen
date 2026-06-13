# ULCP Architecture Design Principles

Every contributor to the Universal Learning Content Platform (ULCP) must adhere to these governing engineering principles to keep the platform maintainable, decoupled, and robust as new LMS providers are onboarded.

---

## 1. Separation of Concerns (Transport vs. Content)
*   **The Agnostic Engine**: The core `WorkflowEngine` and validation layers must remain entirely agnostic of provider identities. Hardcoded checks such as `if provider == "canvas"` are strictly forbidden.
*   **Extraction Separation**: Providers do not handle extraction or download. An `ExtractionService` handles unpacking archives (.zip, .tar.gz, etc.) based on `InputSource` types. 
*   **Decoupled Workspace & Context**: Data is managed via a unified `Workspace` abstraction. Code must pass the `PipelineContext` (encapsulating job data, metrics, provider model, canonical course, logs, and workspace references) across stages.
*   **Post-Extraction Detection**: Provider detection happens *after* package extraction has completed. The `ProviderRegistry` executes format detectors against the extracted directory structure.

## 2. Bounded Provider Decoupling & Decoupled Pipelines
*   **Encapsulation**: All provider-specific parsing, XML reading, and mapping details must reside entirely within that provider's directory (e.g., `src/ucae/providers/moodle/`).
*   **No Execution Mapping in Providers**: Providers only know how to parse and map content. They do not define execution pipelines (`build_pipeline` is forbidden in `BaseLmsProvider`). Pipelines are managed independently by the `PipelineRegistry`.
*   **Dependency Inversion**: Core modules may depend on abstractions (interfaces), but abstractions must never depend on implementations. For example, `WorkflowEngine` depends on `BaseLmsProvider`, which is valid. `WorkflowEngine` depending directly on `CanvasProvider` is forbidden.

## 3. Canonical Model Contract & Normalization
*   **Vendor-Agnostic Terminology**: The canonical model describes generic **educational objects** rather than platform-specific terminology:
    *   *Module* ──► `LearningContainer`
    *   *Page / Lesson* ──► `LearningItem`
    *   *Quiz* ──► `Assessment`
    *   *Assignment* ──► `SubmissionActivity`
    *   *File / Attachment* ──► `Resource`
*   **Parity Normalization**: To prevent minor format variations from altering hash values, all courses must pass through a `CanonicalNormalizer` (normalizing HTML, sorting non-ordered lists, cleaning timestamps) *before* the `contentFingerprint` is generated.
*   **Immutability**: Once normalized, the `CanonicalCourse` dataclass must be treated as frozen to prevent in-flight pipeline mutations from altering the parsed payload.

## 4. Layered Validations & Policy Controls
*   **Three-Layer Validation**: Ingestion validation must execute in three independent, serial layers:
    1.  **Structural Validation**: Hard schema structure checks (non-empty containers, valid item types, assessment structures).
    2.  **Semantic Validation**: Content safety and integrity checks (broken internal links, HTML syntax validation, duplicate file resources).
    3.  **Business Rules Validation**: University-specific policy constraints (forbid empty containers, minimum assessment weights, require rubrics).
*   **Structured Validation Issues**: Validations must output a structured `ValidationIssue` dataclass (storing severity, code, target path, and message) rather than simple text strings.

## 5. Ingestion Boundary, Job Isolation & Artifacts
*   **Export Boundary**: The ULCP pipeline boundary ends at the export stage (persisting the course as a `Draft` or `Needs_QA` document in the target MongoDB database). Publishing is an external concern managed by the target LMS.
*   **Checkpoints & Event Log**: Jobs must support state-persisted checkpoints and a chronological `events` log in a dedicated `jobs` MongoDB collection.
*   **Artifact & Recovery Store**: Every run must log in-flight artifacts (logs, manifests, parsed data, rewritten HTML files, and the serialized `provider_model.json` to bypass parsing on replays) to the job artifact store.
*   **Replays**: Jobs must be replayable starting from specific milestones (e.g., `PARSED` or `VALIDATED`) directly from the artifact store without needing to re-upload packages.

---

## Forbidden Patterns

Every pull request must be screened for the following architectural violations:

*   ❌ `if provider == "canvas"` or `if provider == "blackboard"` branching logic in core modules.
*   ❌ Importing Canvas-specific or Blackboard-specific dataclasses/models outside their respective `src/ucae/providers/` folder.
*   ❌ Mutating `CanonicalCourse` instances after normalization is completed.
*   ❌ Bypassing the Validation Engine or persisting courses directly to MongoDB without passing the Export layer.
*   ❌ Writing provider-specific execution steps directly in `WorkflowEngine`.
*   ❌ Bypassing the Canonical Schema Registry when serializing or deserializing job states.
*   ❌ Managing publishing APIs or publishing logic inside the ULCP pipeline codebase.
