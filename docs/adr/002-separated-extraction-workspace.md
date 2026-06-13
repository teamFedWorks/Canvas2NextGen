# ADR-002: Separating Ingestion Package Extraction from LMS Providers

* **Status**: Approved
* **Date**: 2026-06-12
* **Deciders**: AI Assistant (Antigravity), Technical Lead (User)

## Context

LMS packages are uploaded in different archive formats (e.g., `.zip`, `.tar.gz`, `.cc`) or could be ingested from direct folders or remote locations. Coupling LMS providers (like `CanvasProvider`) with the extraction of zip files leads to redundant extraction implementations and tightly couples providers to specific delivery formats.

## Decision

1. **Extraction Separation**: Isolate the archive unpacking phase from provider-specific logic. 
2. **InputSource Abstraction**: Introduce an `InputSource` abstract base class to handle different input transports (e.g., S3, local filesystem, API stream).
3. **Extraction Service**: Implement an `ExtractionService` that takes an `InputSource` and extracts it into a standard directory.
4. **Workspace Abstraction**: Encapsulate the extracted directory and associated metadata in an `ExtractedWorkspace` object. Provider detection and parsing methods will accept this `ExtractedWorkspace` instead of raw file paths.

## Consequences

* **Cleaner Providers**: LMS providers do not need to import zip/tar libraries or write filesystem unpacking code.
* **Format Flexibility**: The system can ingest unzipped directories or new archive types (like `.7z`) without modifying any LMS provider code.
* **Testability**: Tests can mock the `ExtractedWorkspace` with a simple directory of test files, bypassing zip creation steps.
