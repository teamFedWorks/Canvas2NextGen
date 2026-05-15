#!/usr/bin/env python3
import sys, os, time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from worker.ingestion_worker import IngestionWorker

def main():
    if len(sys.argv) < 3:
        print("Usage: python ingest_single.py <path_to_course> <institution>")
        sys.exit(1)
        
    course_path = Path(sys.argv[1])
    institution = sys.argv[2]
    
    worker = IngestionWorker(
        s3_bucket=os.getenv("S3_CDN_BUCKET", "uhub-lms-bucket"),
        cdn_url=os.getenv("CDN_URL", "")
    )
    
    print(f"Ingesting course from: {course_path}")
    print(f"Institution: {institution}")
    
    t0 = time.time()
    try:
        result = worker.ingest(
            source_type="zip",
            payload={
                "zip_path": course_path,
                "university_id": os.getenv("DEFAULT_UNIVERSITY_ID", "WBU-01"),
                "author_id": os.getenv("DEFAULT_AUTHOR_ID", "WBU-AUTH-01"),
                "program_name": "WBU",
                "institution": institution,
                "force": True,
            }
        )
        elapsed = time.time() - t0
        status = result.get("status", "unknown")
        
        print(f"Status: {status} ({elapsed:.1f}s)")
        print(f"Result details: {result}")
    except Exception as exc:
        print(f"Failed to ingest: {exc}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
