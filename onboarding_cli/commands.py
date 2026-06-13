"""
CLI command implementations for the EduvateHub Course Onboarding pipeline.
Each function maps to a sub-command in main.py.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


# ─────────────────────────────────────────────────────────────────────────────
# serve
# ─────────────────────────────────────────────────────────────────────────────

def serve_app(port: int = 5009):
    """Start the FastAPI server with uvicorn."""
    import uvicorn
    print(f"[serve] Starting EduvateHub Ingestion API on port {port}...")
    print(f"[serve] Swagger UI → http://localhost:{port}/docs")
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ingest zip
# ─────────────────────────────────────────────────────────────────────────────

def ingest_zip(
    path: str,
    university_id: str,
    author_id: str,
    institution: str = "SFC",
    force: bool = False,
):
    """Ingest a local ZIP / directory through the IngestionWorker."""
    from worker.ingestion_worker import IngestionWorker

    s3_bucket = os.getenv("S3_CDN_BUCKET", "")
    cdn_url = os.getenv("CDN_URL", "")

    worker = IngestionWorker(s3_bucket=s3_bucket, cdn_url=cdn_url)

    zip_path = Path(path)
    if not zip_path.exists():
        print(f"[ingest zip] ERROR: path does not exist → {zip_path}")
        sys.exit(1)

    print(f"[ingest zip] Ingesting: {zip_path}")
    result = worker.ingest(
        source_type="zip",
        payload={
            "zip_path": zip_path,
            "university_id": university_id,
            "author_id": author_id,
            "institution": institution,
            "force": force,
        },
    )
    _print_result(result)


# ─────────────────────────────────────────────────────────────────────────────
# ingest s3
# ─────────────────────────────────────────────────────────────────────────────

def ingest_s3(
    institution: str,
    program: str | None,
    course: str | None,
    university_id: str | None,
    author_id: str | None,
    force: bool = False,
    dry_run: bool = False,
    workers: int = 4,
):
    """Batch-ingest courses from the S3 ingestion bucket."""
    # Resolve institution-specific IDs before falling back to defaults
    inst_upper = institution.upper()
    if not university_id:
        university_id = (
            os.getenv(f"{inst_upper}_UNIVERSITY_ID")
            or os.getenv("DEFAULT_UNIVERSITY_ID")
        )
    if not author_id:
        author_id = (
            os.getenv(f"{inst_upper}_AUTHOR_ID")
            or os.getenv("DEFAULT_AUTHOR_ID")
        )

    try:
        from run_batch_ingestion import run_batch
    except ImportError:
        # Fallback: use the worker directly with S3 listing
        _ingest_s3_fallback(institution, program, course, university_id, author_id, force, dry_run, workers)
        return

    run_batch(
        institution=institution,
        program=program,
        course=course,
        university_id=university_id,
        author_id=author_id,
        force=force,
        dry_run=dry_run,
    )


def _ingest_s3_fallback(institution, program, course, university_id, author_id, force, dry_run, workers=4):
    import boto3
    from worker.ingestion_worker import IngestionWorker

    bucket = os.getenv("S3_INGESTION_BUCKET", "")
    cdn_url = os.getenv("CDN_URL", "")
    cdn_bucket = os.getenv("S3_CDN_BUCKET", "")
    region = os.getenv("AWS_REGION", "us-east-2")

    s3 = boto3.client("s3", region_name=region)
    prefix = f"{institution}/"
    if program:
        prefix += f"{program}/"
    if course:
        prefix += f"{course}"

    print(f"[ingest s3] Listing s3://{bucket}/{prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".zip"):
                keys.append(obj["Key"])

    if not keys:
        print("[ingest s3] No ZIP packages found.")
        return

    print(f"[ingest s3] Found {len(keys)} package(s).")
    if dry_run:
        for k in keys:
            print(f"  [dry-run] {k}")
        return

    worker = IngestionWorker(s3_bucket=cdn_bucket, cdn_url=cdn_url)
    for key in keys:
        print(f"[ingest s3] Processing: {key}")
        result = worker.ingest(
            source_type="zip",
            payload={
                "s3_key": key,
                "s3_bucket": bucket,
                "university_id": university_id or os.getenv("DEFAULT_UNIVERSITY_ID"),
                "author_id": author_id or os.getenv("DEFAULT_AUTHOR_ID"),
                "institution": institution,
                "force": force,
            },
        )
        _print_result(result)


# ─────────────────────────────────────────────────────────────────────────────
# ingest canvas
# ─────────────────────────────────────────────────────────────────────────────

def ingest_canvas(course_id: str, university_id: str, author_id: str, force: bool = False):
    """Ingest a course directly from the Canvas API."""
    from worker.ingestion_worker import IngestionWorker

    s3_bucket = os.getenv("S3_CDN_BUCKET", "")
    cdn_url = os.getenv("CDN_URL", "")

    worker = IngestionWorker(s3_bucket=s3_bucket, cdn_url=cdn_url)
    print(f"[ingest canvas] Fetching Canvas course {course_id}...")
    result = worker.ingest(
        source_type="canvas",
        payload={
            "course_id": course_id,
            "university_id": university_id,
            "author_id": author_id,
            "force": force,
        },
    )
    _print_result(result)


# ─────────────────────────────────────────────────────────────────────────────
# ingest batch (local uploads folder)
# ─────────────────────────────────────────────────────────────────────────────

def ingest_batch(uploads_root: str, force: bool = False, dry_run: bool = False, institution: str = None):
    """
    Batch-ingest all ZIP packages found under the local uploads folder.

    The ``institution`` code is written directly to the MongoDB course document
    as ``institution_code`` and used by :func:`validate_ingestion.fetch_institution`
    to produce the correct report header (e.g. WBU / Wayland Baptist University
    instead of SFC / St. Francis College).

    It is *not* optional.  When omitted it is derived from ``DEFAULT_UNIVERSITY_ID``:
    if the configured default is the WBU university, ``"WBU"`` is used;
    if it is the SFC university, ``"SFC"`` is used.
    """
    from worker.ingestion_worker import IngestionWorker

    uploads_path = Path(uploads_root)
    if not uploads_path.exists():
        print(f"[ingest batch] ERROR: uploads folder not found -> {uploads_path}")
        sys.exit(1)

    zips = sorted(uploads_path.rglob("*.zip")) + sorted(uploads_path.rglob("*.imscc"))
    if not zips:
        print(f"[ingest batch] No ZIP/IMSCC packages found under {uploads_path}")
        return

    print(f"[ingest batch] Found {len(zips)} package(s).")
    if dry_run:
        for z in zips:
            print(f"  [dry-run] {z}")
        return

    s3_bucket = os.getenv("S3_CDN_BUCKET", "")
    cdn_url = os.getenv("CDN_URL", "")

    if not institution:
        default_uni = os.getenv("DEFAULT_UNIVERSITY_ID", "")
        wbu_uni     = os.getenv("WBU_UNIVERSITY_ID", "")
        sfbu_uni    = os.getenv("SFC_UNIVERSITY_ID", "")
        institution = (
            "WBU" if default_uni and wbu_uni and default_uni == wbu_uni
            else "SFC" if default_uni and sfbu_uni and default_uni == sfbu_uni
            else "SFC"
        )
    print(f"[ingest batch] Institution: {institution}")

    worker = IngestionWorker(s3_bucket=s3_bucket, cdn_url=cdn_url)

    for zip_path in zips:
        print(f"\n[ingest batch] -> {zip_path.name}")
        result = worker.ingest(
            source_type="zip",
            payload={
                "zip_path": zip_path,
                "university_id": os.getenv("DEFAULT_UNIVERSITY_ID"),
                "author_id":   os.getenv("DEFAULT_AUTHOR_ID"),
                "institution": institution,
                "force": force,
            },
        )
        _print_result(result)


# ─────────────────────────────────────────────────────────────────────────────
# validate
# ─────────────────────────────────────────────────────────────────────────────

def validate_course(course_id: str | None, slug: str | None, strict: bool = False):
    """Run the post-ingestion validation report for a course."""
    from validate_ingestion import run_validation, save_report

    identifier = course_id or slug
    by_slug = slug is not None
    print(f"[validate] Running validation for {'slug' if by_slug else 'id'}: {identifier}")

    report = run_validation(identifier, by_slug=by_slug, strict=strict)
    out_dir = ROOT / "storage" / "outputs"
    html_path = save_report(report, out_dir, emit_json=True)
    print(f"[validate] Report saved → {html_path}")
    print(f"[validate] Verdict: {report.verdict_label}")

    if strict and report.has_failures:
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# report
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(course: str | None, output: str | None, no_html: bool = False):
    """Generate the pre-ingestion audit report."""
    from generate_ingestion_report import main as report_main

    args = []
    if course:
        args += ["--course", course]
    if output:
        args += ["--output", output]
    if no_html:
        args += ["--no-html"]

    # Patch sys.argv so the script's argparse picks up our args
    old_argv = sys.argv
    sys.argv = ["generate_ingestion_report.py"] + args
    try:
        report_main()
    finally:
        sys.argv = old_argv

def start_worker(max_workers: int = 10, queue_url: str | None = None, region: str = "us-east-2"):
    """Start the SQS-driven ingestion worker using the new UCAE IngestionQueueListener."""
    import time
    from pymongo import MongoClient
    from ucae.worker.listener import IngestionQueueListener
    from ucae.providers.registry import ProviderRegistry
    from ucae.providers.dummy import DummyProvider
    from ucae.canonical.normalizer import CanonicalNormalizer

    queue = queue_url or os.getenv("SQS_QUEUE_URL", "")
    if not queue:
        print("[worker] ERROR: SQS_QUEUE_URL not set. Pass --queue or set the env var.")
        sys.exit(1)

    mongo_uri = os.getenv("ULCP_MONGODB_URI")
    if not mongo_uri:
        print("[worker] ERROR: ULCP_MONGODB_URI not set in environment.")
        sys.exit(1)

    intake_bucket = os.getenv("S3_INGESTION_BUCKET")
    artifact_bucket = os.getenv("S3_CDN_BUCKET")
    if not intake_bucket or not artifact_bucket:
        print("[worker] ERROR: S3_INGESTION_BUCKET or S3_CDN_BUCKET not set in environment.")
        sys.exit(1)

    print(f"[worker] Starting SQS consumer on {queue}...")
    db_client = MongoClient(mongo_uri)
    
    provider_registry = ProviderRegistry()
    provider_registry.register(DummyProvider())
    normalizer = CanonicalNormalizer()

    listener = IngestionQueueListener(
        queue_url=queue,
        db_client=db_client,
        provider_registry=provider_registry,
        normalizer=normalizer
    )

    try:
        listener.run_startup_self_checks(intake_bucket, artifact_bucket)
    except Exception as e:
        print(f"[worker] Startup self-checks FAILED: {e}")
        sys.exit(1)

    print("[worker] Worker is running. Polling SQS for messages...")
    try:
        while True:
            # Poll and process messages
            listener.poll_messages(max_messages=1, wait_time_seconds=10)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[worker] Stopping worker...")
    finally:
        db_client.close()

def _print_result(result: dict):
    status = result.get("status", "unknown")
    if status == "success":
        course_id = result.get("course_id", "N/A")
        title = result.get("title", "")
        dedup = " (already existed)" if result.get("deduplicated") else ""
        print(f"  [OK] SUCCESS  course_id={course_id}  {title}{dedup}")
    else:
        error = result.get("error", "unknown error")
        print(f"  [FAIL] FAILED   {error}")


def start_promotion_worker(queue_url: str | None = None, region: str = "us-east-2"):
    """Start the SQS-driven promotion worker using the new PromotionQueueListener."""
    import time
    from pymongo import MongoClient
    from ucae.worker.promotion_listener import PromotionQueueListener

    queue = queue_url or os.getenv("PROMOTION_FIFO_QUEUE_URL", "")
    if not queue:
        print("[promotion-worker] ERROR: PROMOTION_FIFO_QUEUE_URL not set. Pass --queue or set the env var.")
        sys.exit(1)

    ulcp_mongo_uri = os.getenv("ULCP_MONGODB_URI")
    if not ulcp_mongo_uri:
        print("[promotion-worker] ERROR: ULCP_MONGODB_URI not set in environment.")
        sys.exit(1)

    platform_mongo_uri = os.getenv("PLATFORM_MONGODB_URI")
    if not platform_mongo_uri:
        print("[promotion-worker] ERROR: PLATFORM_MONGODB_URI not set in environment.")
        sys.exit(1)

    print(f"[promotion-worker] Starting SQS promotion consumer on {queue}...")
    ulcp_client = MongoClient(ulcp_mongo_uri)
    platform_client = MongoClient(platform_mongo_uri)

    listener = PromotionQueueListener(
        queue_url=queue,
        ulcp_db_client=ulcp_client,
        platform_db_client=platform_client
    )

    print("[promotion-worker] Promotion worker is running. Polling SQS for messages...")
    try:
        while True:
            listener.poll_messages(max_messages=1, wait_time_seconds=10)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[promotion-worker] Stopping promotion worker...")
    finally:
        ulcp_client.close()
        platform_client.close()
