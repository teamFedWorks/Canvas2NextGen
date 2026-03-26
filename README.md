# Course Onboarding Service

Professional Python-based pipeline for migrating Canvas course exports (IMS-CC) into the EduvateHub custom LMS (MERN stack).

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Project Structure](#project-structure)
5. [Running the API Server](#running-the-api-server)
6. [CLI — Course Ingestion](#cli--course-ingestion)
7. [Docker](#docker)
8. [Ingestion Report](#ingestion-report)
9. [Utility Scripts](#utility-scripts)
10. [Pipeline Stages](#pipeline-stages)
11. [API Endpoints](#api-endpoints)

---

## Prerequisites

- Python 3.11+
- MongoDB instance
- AWS S3 bucket (for course assets)
- AWS credentials configured (`~/.aws/credentials` or environment variables)

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Key variables:

| Variable | Description |
|---|---|
| `MONGODB_URI` | MongoDB connection string |
| `MONGODB_DATABASE` | Target database name |
| `S3_CDN_BUCKET` | S3 bucket for uploaded course assets |
| `S3_INGESTION_BUCKET` | S3 bucket where raw ZIP packages are stored |
| `CDN_URL` | CDN base URL for rewritten asset links |
| `CANVAS_API_TOKEN` | Canvas API token for remote asset downloads |
| `DEFAULT_UNIVERSITY_ID` | Default university ObjectId |
| `DEFAULT_AUTHOR_ID` | Default author ObjectId |
| `PORT` | API server port (default: `5009`) |

---

## Project Structure

```
.
├── cli.py                        # Unified CLI entry point
├── server.py                     # FastAPI server entry point
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── scripts/
│   ├── generate_ingestion_report.py   # Consolidated audit report
│   ├── analyze_assets.py              # Quick CSV asset breakdown
│   ├── course_structure.py            # Course structure JSON snapshot
│   └── standardize_packages.py       # Fix nested ZIP packages
├── src/
│   ├── api/                      # FastAPI routes & middleware
│   ├── core/                     # Pipeline orchestrator & stages
│   │   └── stages/               # validator, parser, asset_uploader
│   ├── parsers/                  # Per-content-type parsers
│   ├── transformers/             # Canvas → LMS model transformer
│   ├── exporters/                # MongoDB exporter & report generator
│   ├── models/                   # Canvas, LMS & report data models
│   ├── adapters/                 # ZIP & Canvas API adapters
│   ├── config/                   # Schemas & constants
│   ├── utils/                    # S3, file, XML, HTML helpers
│   ├── worker/                   # Ingestion worker
│   └── observability/            # Structured logger
└── storage/
    └── uploads/                  # Course export folders (git-ignored)
```

---

## Running the API Server

```bash
# Direct
python server.py

# Via CLI
python cli.py server
```

- Port: `5009` (configurable via `PORT` in `.env`)
- Swagger docs: http://localhost:5009/docs

---

## CLI — Course Ingestion

All ingestion commands go through `cli.py`.

### Ingest a local ZIP file

```bash
python cli.py ingest-zip --path path/to/course.zip --uni <UNIVERSITY_ID> --author <AUTHOR_ID>
```

| Flag | Required | Description |
|---|---|---|
| `--path` | Yes | Path to the `.zip` Canvas export |
| `--uni` | No | University ObjectId (falls back to `DEFAULT_UNIVERSITY_ID`) |
| `--author` | No | Author ObjectId (falls back to `DEFAULT_AUTHOR_ID`) |
| `--force` | No | Force re-import even if course already exists |

### Batch ingest from S3

```bash
python cli.py ingest-s3 --workers 4
```

| Flag | Required | Description |
|---|---|---|
| `--workers` | No | Number of parallel workers (default: `4`) |
| `--prefix` | No | S3 key prefix filter (e.g. `spring-2026/`) |
| `--uni` | No | University ObjectId |
| `--author` | No | Author ObjectId |
| `--force` | No | Force re-import |

### Ingest from Canvas API

```bash
python cli.py ingest-canvas --course-id <CANVAS_COURSE_ID> --uni <UNIVERSITY_ID> --author <AUTHOR_ID>
```

| Flag | Required | Description |
|---|---|---|
| `--course-id` | Yes | Canvas course ID |
| `--uni` | Yes | University ObjectId |
| `--author` | Yes | Author ObjectId |
| `--force` | No | Force re-import |

---

## Docker

```bash
# Build and start
docker-compose up --build

# Run in background
docker-compose up -d

# Stop
docker-compose down
```

The API will be available at http://localhost:5009.

---

## Ingestion Report

Generates a full audit of course content in `storage/uploads` (or any program folder), covering:

- Asset-level status (pass / fail / retry) for XML, QTI, HTML, PPT, images, documents, videos, audio, and media files
- Deck / module status from `imsmanifest.xml`
- Course-level structural gaps
- Missing PPT/PPTX thumbnails with action steps
- Success vs remaining course counts with progress bars

### Run for all courses (default uploads folder)

```bash
python scripts/generate_ingestion_report.py
```

### Run for a specific program folder

```bash
python scripts/generate_ingestion_report.py --root "storage/uploads/BS Information Technology"
```

### Run for a specific course only

```bash
python scripts/generate_ingestion_report.py --course "01 - PHI-1114 Logic and Argumentation"
```

### Save report to a custom location

```bash
python scripts/generate_ingestion_report.py --output "storage/uploads/BS Computer Science/BS Computer Science - Ingestion Report.json"
```

### Skip HTML output (JSON only)

```bash
python scripts/generate_ingestion_report.py --no-html
```

### All flags

| Flag | Default | Description |
|---|---|---|
| `--root` | `storage/uploads` | Root directory to scan |
| `--course` | _(all)_ | Filter to a single course (substring match) |
| `--output` | `storage/ingestion_report.json` | JSON output path (HTML saved alongside) |
| `--no-html` | `false` | Skip HTML report generation |

Output files:
- `<output>.json` — machine-readable full report
- `<output>.html` — visual dashboard with PDF download button

---

## Utility Scripts

### Analyze assets (quick CSV)

Prints a CSV breakdown of asset counts per course to stdout.

```bash
python scripts/analyze_assets.py
```

### Course structure snapshot

Prints a JSON snapshot of each course's directory structure (manifest, quiz, wiki, etc.).

```bash
python scripts/course_structure.py
```

### Standardize nested ZIP packages

Fixes Canvas exports that have an extra wrapper folder inside the ZIP, re-packaging them with `imsmanifest.xml` at the root.

```bash
python scripts/standardize_packages.py
```

---

## Pipeline Stages

When a course is ingested it passes through 5 sequential stages:

| Stage | Progress | What it does |
|---|---|---|
| **1. Validate** | 10% | Checks IMS-CC structure, parses manifest, inventories files, detects orphans |
| **2. Parse** | 30% | Runs 8 specialized parsers (pages, assignments, quizzes, discussions, PPTx, orphans) |
| **3. Transform** | 50% | Maps Canvas models → LMS curriculum models (modules, lessons, quizzes, assignments) |
| **4. Upload Assets** | 70% | Migrates all local/remote files to S3, rewrites HTML URLs to CDN |
| **5. Export to DB** | 90% | Upserts course document to MongoDB with duplicate detection and job tracking |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/migrate` | Ingest a Canvas ZIP upload |
| `POST` | `/migrate-canvas` | Ingest via Canvas API course ID |
| `GET` | `/jobs/{task_id}` | Poll ingestion job status & progress |
| `GET` | `/health` | Health check |
| `GET` | `/docs` | Swagger UI |
