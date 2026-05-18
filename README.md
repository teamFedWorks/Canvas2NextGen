# Canvas & Blackboard to EduvateHub — Course Onboarding Pipeline

A production-grade Python pipeline for migrating **Canvas LMS** (IMS-CC / IMSCC) and **Blackboard Learn Ultra** course exports into the EduvateHub custom MERN-stack LMS. Handles parsing, transformation, S3 asset upload, MongoDB export, and post-ingestion validation — fully automated.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Project Structure](#project-structure)
5. [CLI — Course Ingestion](#cli--course-ingestion)
6. [Post-Ingestion Validation Report](#post-ingestion-validation-report)
7. [Pre-Ingestion Audit Report](#pre-ingestion-audit-report)
8. [Running the API Server](#running-the-api-server)
9. [Docker](#docker)
10. [Pipeline Stages](#pipeline-stages)
11. [Content Mapping](#content-mapping)
12. [API Endpoints](#api-endpoints)
13. [Utility Scripts](#utility-scripts)

---

## Prerequisites

- Python 3.11+
- MongoDB instance (Atlas or self-hosted)
- AWS S3 bucket for course assets
- AWS credentials in `.env` or `~/.aws/credentials`
- Pillow (`pip install Pillow`) — for PPTX cover thumbnail generation

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `MONGODB_URI` | MongoDB connection string |
| `MONGODB_DATABASE` | Target database name (default: `lms_db`) |
| `S3_CDN_BUCKET` | S3 bucket where course assets are uploaded |
| `S3_INGESTION_BUCKET` | S3 bucket where raw course ZIP packages are stored |
| `CDN_URL` | CDN base URL used to rewrite asset links in content |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_REGION` | AWS region (default: `us-east-1`) |
| `CANVAS_API_TOKEN` | Canvas API token for remote asset downloads |
| `DEFAULT_UNIVERSITY_ID` | Default university MongoDB ObjectId (SFC) |
| `DEFAULT_AUTHOR_ID` | Default author MongoDB ObjectId (SFC) |
| `WBU_UNIVERSITY_ID` | WBU university MongoDB ObjectId — auto-used when `--institution WBU` |
| `WBU_AUTHOR_ID` | WBU author MongoDB ObjectId — auto-used when `--institution WBU` |
| `PORT` | API server port (default: `5009`) |

---

## Project Structure

```
.
├── main.py                             # Unified CLI entry point (serve | ingest | worker | report)
├── requirements.txt
├── Dockerfile / docker-compose.yml
├── onboarding_cli/
│   └── commands.py                    # CLI command implementations
│
├── scripts/
│   ├── validate_ingestion.py          # Post-ingestion validation report (auto-runs)
│   ├── generate_ingestion_report.py   # Pre-ingestion asset audit report
│   ├── analyze_assets.py              # Quick CSV asset breakdown
│   ├── course_structure.py            # Course structure JSON snapshot
│   └── standardize_packages.py       # Fix nested ZIP packages
│
├── src/
│   ├── api/                           # FastAPI routes & middleware
│   ├── adapters/
│   │   ├── zip_adapter.py             # Canvas IMS-CC ZIP adapter (auto-routes to BB adapter)
│   │   ├── blackboard_adapter.py      # Blackboard Learn Ultra export adapter
│   │   └── canvas_adapter.py          # Canvas API adapter
│   ├── core/
│   │   └── stages/                    # package_validator, parser, asset_uploader
│   ├── parsers/                       # Per-content-type parsers (Canvas)
│   ├── transformers/
│   │   └── course_transformer.py      # Canvas/BB models → LMS curriculum models
│   ├── exporters/
│   │   └── mongodb_exporter.py        # Upsert to MongoDB with deduplication
│   ├── models/                        # Canvas, LMS, and report dataclasses
│   ├── config/                        # Schemas, namespaces, extension sets
│   ├── utils/                         # S3, file, XML, HTML, resilience helpers
│   ├── worker/
│   │   └── ingestion_worker.py        # Orchestrates the full pipeline
│   └── observability/                 # Structured JSON logger
│
└── storage/
    ├── uploads/
    │   ├── SFC/                       # St. Francis College — Canvas IMS-CC exports
    │   └── WBU/                       # Wayland Baptist University — Blackboard exports
    └── outputs/                       # Validation reports (git-ignored)
```

---

## CLI — Course Ingestion

All ingestion commands go through `main.py`. A validation report is **automatically generated** after every successful ingestion and saved to `storage/outputs/<INSTITUTION>/`.

### Ingest a local Canvas ZIP or directory (SFC)

```bash
python main.py ingest zip --path "storage/uploads/SFC/BS Information Technology/IT-1104 Programming I"
```

```bash
python main.py ingest zip --path path/to/course.zip --uni <UNIVERSITY_ID> --author <AUTHOR_ID>
```

| Flag | Required | Description |
|---|---|---|
| `--path` | Yes | Path to a Canvas export `.zip` file or extracted directory |
| `--institution` | No | Institution code — `SFC` (default) or `WBU` |
| `--uni` | No | University ObjectId (falls back to `DEFAULT_UNIVERSITY_ID` or `WBU_UNIVERSITY_ID`) |
| `--author` | No | Author ObjectId (falls back to `DEFAULT_AUTHOR_ID` or `WBU_AUTHOR_ID`) |
| `--force` | No | Force re-import even if course already exists |

### Ingest a WBU Blackboard export (local ZIP or extracted folder)

The pipeline **auto-detects** Blackboard packages — no extra flags needed beyond `--institution WBU`.

```bash
# From the extracted folder
python main.py ingest zip \
  --path "storage/uploads/WBU/phd-course-shell" \
  --institution WBU

# From the ZIP file directly
python main.py ingest zip \
  --path "storage/uploads/WBU/phd-course-shell.zip" \
  --institution WBU
```

WBU IDs are resolved automatically from `WBU_UNIVERSITY_ID` / `WBU_AUTHOR_ID` in `.env`.  
Pass `--uni` / `--author` explicitly to override.

### Batch ingest from S3

```bash
# SFC — Canvas courses
python main.py ingest s3 --institution SFC --workers 4

# WBU — Blackboard courses (IDs resolved from WBU_UNIVERSITY_ID / WBU_AUTHOR_ID)
python main.py ingest s3 --institution WBU --workers 4
```

| Flag | Required | Description |
|---|---|---|
| `--institution` | Yes | Institution folder name in S3 (`SFC` or `WBU`) |
| `--workers` | No | Parallel download threads (default: `4`) |
| `--program` | No | Limit to one program slug |
| `--course` | No | Limit to one course code prefix |
| `--uni` | No | University ObjectId override |
| `--author` | No | Author ObjectId override |
| `--force` | No | Force re-import |
| `--dry-run` | No | List packages without ingesting |

### Batch ingest from local uploads folder

```bash
python main.py ingest batch
python main.py ingest batch --uploads storage/uploads/WBU --force
```

### Ingest from Canvas API

```bash
python main.py ingest canvas --course-id <CANVAS_COURSE_ID> --uni <UNIVERSITY_ID> --author <AUTHOR_ID>
```

---

## Post-Ingestion Validation Report

After every successful ingestion the pipeline **automatically** runs `scripts/validate_ingestion.py` and saves an HTML + JSON report to `storage/outputs/<INSTITUTION>/validation_<slug>.html`.

You can also run it manually at any time:

```bash
# By MongoDB course ID
python scripts/validate_ingestion.py --course-id <MONGO_ID>

# By course slug
python scripts/validate_ingestion.py --slug mgmt-5306-spring-1st8wks-2026-vc01

# Strict mode — exits non-zero on any warning
python scripts/validate_ingestion.py --course-id <MONGO_ID> --strict

# Skip JSON output
python scripts/validate_ingestion.py --course-id <MONGO_ID> --no-json
```

### What the report covers

| Section | What it checks |
|---|---|
| **Course Mapping Status** | Source content type → LMS type coverage with progress bars per type (Lesson, Quiz, Assignment) |
| **Course Structure Integrity** | All required MongoDB fields present and populated |
| **Module & Component Validation** | Every module item has content or S3 attachments; explains WHY any item is flagged |
| **Asset Storage Validation (S3)** | HEAD-checks every uploaded file against S3 to confirm it exists and is non-zero |
| **Thumbnail & Metadata** | Course code, department, description, and featured image quality |
| **Manual Tasks Checklist** | Exact list of human actions required — nothing more, nothing less |

### Status labels

| Label | Meaning |
|---|---|
| `PASS` | Successfully imported. No action needed. |
| `WARN` | Imported but needs manual attention. The report explains exactly why and what to do. |
| `FAIL` | Critical — item is missing or broken. Must be fixed before publishing. |
| `RETRY` | Asset uploaded but is 0 bytes. Re-run ingestion to fix. |

### PDF download

The HTML report includes a **Download as PDF** button. All S3 asset URLs are rendered as full clickable links in the PDF (using CSS `a[href]:after` print rules).

### Why "Partially Complete"?

A course is marked `WARN / Partially Complete` when items exist that the pipeline cannot resolve automatically. Known cases:

1. **Respondus LockDown Browser quizzes** — Canvas does not export quiz questions for proctored exams. The quiz shell is imported but questions must be entered manually in the target LMS.
2. **Missing course thumbnail** — Neither Canvas nor Blackboard exports include a cover image. One must be provided by the course author.

Everything else is resolved automatically by the pipeline.

---

## Pre-Ingestion Audit Report

Before ingesting, run the audit script to check the health of source files.  
The report **auto-detects Canvas vs Blackboard** packages and applies the correct structural checks for each.

```bash
# Audit all courses in the default uploads folder
python scripts/generate_ingestion_report.py

# Audit a specific institution folder
python scripts/generate_ingestion_report.py --root "storage/uploads/WBU"
python scripts/generate_ingestion_report.py --root "storage/uploads/SFC/BS Information Technology"

# Audit a single course
python scripts/generate_ingestion_report.py --course "phd-course-shell"

# Skip HTML output
python scripts/generate_ingestion_report.py --no-html
```

| Flag | Default | Description |
|---|---|---|
| `--root` | `storage/uploads` | Root directory to scan |
| `--course` | _(all)_ | Filter to a single course (substring match) |
| `--output` | `storage/ingestion_report.json` | JSON output path |
| `--no-html` | `false` | Skip HTML report generation |

The report covers asset-level status (pass / fail / retry), module structure from `imsmanifest.xml`, missing PPTX thumbnails, and structural gaps.

> **Note for Blackboard packages:** The `csfiles/` directory (Blackboard's internal embedded-content store) is automatically excluded from asset scanning. Canvas-specific structural checks (`course_settings/`) are skipped for Blackboard exports.

---

## Running the API Server

```bash
python main.py serve
# or
python main.py serve --port 5009
```

- Port: `5009` (configurable via `PORT`)
- Swagger UI: http://localhost:5009/docs

---

## Docker

```bash
docker-compose up --build    # Build and start
docker-compose up -d         # Background
docker-compose down          # Stop
```

---

## Pipeline Stages

Every ingestion runs through 5 sequential stages:

| Stage | Progress | Description |
|---|---|---|
| **1. Extract** | 10% | Unzip or load the course export directory; auto-detect Canvas vs Blackboard format |
| **2. Parse** | 30% | Canvas: manifest, pages, assignments, quizzes (QTI), discussions, weblinks, PPTX, orphaned content. Blackboard: `.dat` resource files, BB QTI assessments, discussion forums, Ultra document bodies |
| **3. Transform** | 50% | Map source models to LMS curriculum schema; auto-detect course code and department from title or BB course ID |
| **4. Upload Assets** | 70% | Upload all files (PDFs, PPTXs, DOCXs, IPYNBs, CSVs, images, videos) to S3; rewrite HTML URLs to CDN; attach files to correct curriculum items |
| **5. Export & Validate** | 90–100% | Upsert course to MongoDB; auto-run post-ingestion validation report |

---

## Content Mapping

### Canvas IMS-CC

| Canvas Source | LMS Type | Notes |
|---|---|---|
| `webcontent` wiki page (HTML/XML) | Lesson | Full HTML body imported |
| `webcontent` PPTX file | Lesson | Converted to HTML slides; cover thumbnail auto-generated |
| `webcontent` PDF / DOCX / XLSX | Lesson | Uploaded to S3 as downloadable attachment |
| `webcontent` IPYNB / CSV / code file | Lesson | Uploaded to S3 as downloadable attachment |
| `imsqti` assessment | Quiz | Questions parsed from QTI XML; quizConfig populated |
| `canvas:assignment` | Assignment | Description and grading config imported |
| `imsdt` discussion topic | Lesson | Discussion prompt imported as HTML content |
| `imswl` web link | Lesson | External URL rendered as a clickable link |
| Respondus LockDown Browser quiz | Quiz (shell) | Shell imported; questions require manual entry — see Manual Tasks in report |

### Blackboard Learn Ultra

| Blackboard Source | LMS Type | Notes |
|---|---|---|
| `resource/x-bb-document` (Ultra body) | Lesson | HTML body unescaped and cleaned from `.dat` file |
| `assessment/x-bb-qti-test` | Quiz | Questions parsed from BB QTI `.dat` file |
| `resource/x-bb-discussionboard` | Discussion | Forum prompt extracted from `<DESCRIPTION>` or `<MESSAGETHREADS>` |
| `resource/x-bb-announcement` | Lesson | Announcement body imported as HTML |
| `resource/x-bb-link` | Lesson | External URL rendered as a clickable link |
| `course/x-bb-coursetoc` folder | Module | Week/unit containers become curriculum modules |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/migrate` | Ingest a course ZIP upload |
| `POST` | `/api/v1/migrate-s3` | Ingest a ZIP from S3 by key |
| `POST` | `/api/v1/migrate-canvas` | Ingest via Canvas API course ID |
| `GET` | `/api/v1/status/{job_id}` | Poll ingestion job status and progress |
| `GET` | `/api/v1/health` | Health check |
| `GET` | `/docs` | Swagger UI |

---

## Utility Scripts

| Script | Description |
|---|---|
| `scripts/analyze_assets.py` | Prints a CSV breakdown of asset counts per course |
| `scripts/course_structure.py` | Prints a JSON snapshot of each course's directory structure |
| `scripts/standardize_packages.py` | Fixes Canvas exports with an extra wrapper folder inside the ZIP |
