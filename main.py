#!/usr/bin/env python3
"""
EduvateHub Course Onboarding - Unified Entry Point

A professional CLI hub for managing the course ingestion pipeline, 
starting the API server, and generating reports.
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add src and scripts to path for package and utility imports
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

# Load environment variables
load_dotenv(ROOT / ".env")

from onboarding_cli import commands

def main():
    parser = argparse.ArgumentParser(
        description="EduvateHub Course Onboarding Unified CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py serve --port 5009
  python main.py ingest zip --path ./course.zip --uni SFC-01
  python main.py ingest batch --force
  python main.py report --course IT-1104
  python main.py worker --workers 5
        """
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Serve Command ---
    serve_parser = subparsers.add_parser("serve", help="Start the FastAPI server")
    serve_parser.add_argument("--port", type=int, default=int(os.getenv("PORT", 5009)), help="Server port")

    # --- Ingest Command ---
    ingest_parser = subparsers.add_parser("ingest", help="Ingest course content from various sources")
    ingest_subparsers = ingest_parser.add_subparsers(dest="source", required=True)

    # Ingest Zip
    zip_parser = ingest_subparsers.add_parser("zip", help="Ingest a local ZIP file")
    zip_parser.add_argument("--path", required=True, help="Path to local .zip or .imscc file")
    zip_parser.add_argument("--uni", default=os.getenv("DEFAULT_UNIVERSITY_ID", "default_uni"), help="University ID")
    zip_parser.add_argument("--author", default=os.getenv("DEFAULT_AUTHOR_ID", "default_author"), help="Author ID")
    zip_parser.add_argument("--institution", default="SFC", help="Institution code (default: SFC)")
    zip_parser.add_argument("--force", action="store_true", help="Force re-import even if exists")

    # Ingest S3
    s3_parser = ingest_subparsers.add_parser("s3", help="Ingest courses from S3 bucket")
    s3_parser.add_argument("--institution", required=True, help="Institution folder name in S3 (e.g. SFC, WBU)")
    s3_parser.add_argument("--program", help="Limit to one program slug")
    s3_parser.add_argument("--course", help="Limit to one course code prefix")
    s3_parser.add_argument("--uni", help="MongoDB university ID override")
    s3_parser.add_argument("--author", help="MongoDB author ID override")
    s3_parser.add_argument("--workers", type=int, default=4, help="Parallel worker threads (default: 4)")
    s3_parser.add_argument("--force", action="store_true", help="Force re-import")
    s3_parser.add_argument("--dry-run", action="store_true", help="List packages without ingesting")

    # Ingest Canvas
    canvas_parser = ingest_subparsers.add_parser("canvas", help="Ingest from Canvas API")
    canvas_parser.add_argument("--course-id", required=True, help="Canvas Course ID")
    canvas_parser.add_argument("--uni", required=True, help="University ID")
    canvas_parser.add_argument("--author", required=True, help="Author ID")
    canvas_parser.add_argument("--force", action="store_true", help="Force re-import")

    # Ingest Batch
    batch_parser = ingest_subparsers.add_parser("batch", help="Batch ingest courses from local uploads folder")
    batch_parser.add_argument("--uploads", default=str(ROOT / "storage" / "uploads"), help="Uploads root path")
    batch_parser.add_argument("--institution", default=None, help="Institution code (e.g. WBU, SFC). Auto-detected from DEFAULT_UNIVERSITY_ID when omitted.")
    batch_parser.add_argument("--force", action="store_true", help="Force re-import")
    batch_parser.add_argument("--dry-run", action="store_true", help="List packages without ingesting")

    # --- Validate Command ---
    validate_parser = subparsers.add_parser("validate", help="Validate a course ingestion")
    v_group = validate_parser.add_mutually_exclusive_group(required=True)
    v_group.add_argument("--course-id", help="MongoDB ObjectId")
    v_group.add_argument("--slug", help="Course slug")
    validate_parser.add_argument("--strict", action="store_true", help="Enable strict validation")

    # --- Report Command ---
    report_parser = subparsers.add_parser("report", help="Generate consolidated ingestion report")
    report_parser.add_argument("--course", help="Filter to a specific course (substring match)")
    report_parser.add_argument("--output", help="Output path for the report")
    report_parser.add_argument("--no-html", action="store_true", help="Skip HTML report generation")

    # --- Worker Command ---
    worker_parser = subparsers.add_parser("worker", help="Start the automated SQS job consumer")
    worker_parser.add_argument("--workers", type=int, default=10, help="Max parallel workers")
    worker_parser.add_argument("--queue", help="SQS queue URL (overrides env var)")
    worker_parser.add_argument("--region", default="us-east-2", help="AWS region")

    # --- Promotion Worker Command ---
    promo_worker_parser = subparsers.add_parser("promotion-worker", help="Start the automated SQS FIFO promotion consumer")
    promo_worker_parser.add_argument("--queue", help="SQS promotion FIFO queue URL (overrides env var)")
    promo_worker_parser.add_argument("--region", default="us-east-2", help="AWS region")

    args = parser.parse_args()

    # Dispatch to commands
    try:
        if args.command == "serve":
            commands.serve_app(port=args.port)
        
        elif args.command == "ingest":
            if args.source == "zip":
                commands.ingest_zip(args.path, args.uni, args.author, args.institution, args.force)
            elif args.source == "s3":
                commands.ingest_s3(args.institution, args.program, args.course, args.uni, args.author, args.force, args.dry_run, args.workers)
            elif args.source == "canvas":
                commands.ingest_canvas(args.course_id, args.uni, args.author, args.force)
            elif args.source == "batch":
                commands.ingest_batch(args.uploads, args.force, args.dry_run, institution=args.institution)
        elif args.command == "validate":
            commands.validate_course(args.course_id, args.slug, args.strict)
        
        elif args.command == "report":
            commands.generate_report(args.course, args.output, args.no_html)
            
        elif args.command == "worker":
            commands.start_worker(args.workers, args.queue, args.region)
            
        elif args.command == "promotion-worker":
            commands.start_promotion_worker(args.queue, args.region)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
