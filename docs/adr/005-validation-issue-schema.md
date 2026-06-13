# ADR-005: Structured Validation Issues Schema

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

Validation errors in ingestion pipelines were previously returned as simple text strings (e.g. `["Broken link", "Missing quiz question"]`).
This raw format limits the downstream system's ability to build automated recovery pipelines, filter issues by severity, or show exact locations of broken elements in an admin dashboard.

## Decision

1. **ValidationIssue Structure**: Define a structured dataclass for validation issues.
2. **Dataclass Fields**:
   * `severity`: String indicating severity levels: `warning` (can proceed but needs QA), `error` (blocks ingestion), or `info` (for informational notices).
   * `code`: A machine-readable string code (e.g., `BROKEN_LINK`, `EMPTY_CONTAINER`, `MISSING_ASSET`).
   * `path`: A path representation (such as JSONPath or ID-based pathing) indicating where the issue occurs in the content tree.
   * `message`: A user-friendly, descriptive explanation of the issue.
3. **Multi-layer Output**: The validation engine will return lists of these structured `ValidationIssue` objects across all validation phases.

## Consequences

* **Clear Diagnostic UI**: Allows frontends to render clean validation summary lists, highlighting issues next to corresponding course items.
* **Granular Control**: Business rules can easily query issues by code or path to decide if a warning should block a course or go to a QA backlog.
