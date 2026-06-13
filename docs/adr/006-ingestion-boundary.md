# ADR-006: Decoupling Course Ingestion and Publishing Boundaries

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

Publishing a course (making it live and visible to students) involves scheduling, enrollment rules, and manual checks. Coupling course publication directly with the ingestion pipeline poses a risk of exposing incomplete, malformed, or unverified course packages to students in production.

## Decision

1. **Strict Ingestion Boundary**: The ULCP pipeline ends at exporting the course as a `Draft` or `Needs_QA` record.
2. **No Publication Code**: The ingestion engine will not expose `/publish` or execute live publication database writes or notifications.
3. **Downstream Service Responsibility**: Publishing and student visibility are external concerns. A separate admin workflow or next-generation LMS backend must handle publication triggers after QA checks are passed.

## Consequences

* **Improved Safety**: Ingested courses cannot accidentally be exposed to students before verification.
* **Separation of Concerns**: Ingestion performance and publishing scheduling do not interfere with each other.
* **QA Workflow Support**: Enables a clean step in the admin panel to review and click "Publish" manually or run auto-publish scripts downstream.
