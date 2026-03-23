import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path for package imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Load .env
load_dotenv(".env")

import argparse
import concurrent.futures
from edu_onboarding.utils.s3_utils import S3Downloader
from edu_onboarding.worker.ingestion_worker import IngestionWorker
from edu_onboarding.observability.logger import get_logger

logger = get_logger(__name__)

def process_single_key(key, info):
    """Worker function for parallel processing (Multiprocessing safe)."""
    course_name = Path(key).stem
    university_id, author_id, cdn_url, ingestion_bucket, s3_cdn_bucket = info
    
    # 1. Lazy-init workers inside the process
    downloader = S3Downloader(bucket=ingestion_bucket)
    worker = IngestionWorker(s3_bucket=s3_cdn_bucket, cdn_url=cdn_url)
    
    # Create a local temp storage for the download
    temp_dir = Path(f"storage/temp_s3/{os.getpid()}_{hash(key) % 1000}")
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        print(f"[START] Processing: {course_name} ({key})")
        # Download
        local_zip = downloader.download(key, temp_dir)
        
        # Ingest
        result = worker.process_package(
            local_zip, 
            university_id, 
            author_id, 
            title_override=course_name.replace('-', ' ').title()
        )
        
        if result.get("status") == "success":
            print(f"[SUCCESS] {course_name}: Task ID {result.get('task_id')}")
            return True
        else:
            print(f"[FAILED] {course_name}: {result.get('error')}")
            return False
            
    except Exception as e:
        print(f"[CRASH] {course_name}: {str(e)}")
        logger.exception(f"Critical failure processing {key}")
        return False
        
    finally:
        if temp_dir.exists():
            import shutil
            shutil.rmtree(temp_dir)

def main():
    """Batch process courses directly from S3 with true process-level parallelism."""
    parser = argparse.ArgumentParser(description="Parallel S3 Course Ingestion")
    parser.add_argument("--keys", nargs="+", help="Manually specify S3 keys to process")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    args = parser.parse_args()

    # S3 Configuration from .env
    ingestion_bucket = os.getenv("S3_INGESTION_BUCKET")
    s3_cdn_bucket = os.getenv("S3_CDN_BUCKET", "eduvatehub-assets-cdn")
    cdn_url = os.getenv("CDN_URL", "https://assets.eduvatehub.com")
    
    if not ingestion_bucket:
        print("ERROR: S3_INGESTION_BUCKET not found in .env")
        return

    university_id = "64f1a2b3c4d5e6f7a8b9c0d1"
    author_id = "64f1a2b3c4d5e6f7a8b9c0d2"

    print("=" * 80)
    print(f"S3 BATCH MIGRATION (Process-level Parallelism: {args.workers})")
    print("=" * 80)
    print(f"Bucket: {ingestion_bucket}")
    print()

    # Initial scan (IO-bound, single threaded is fine)
    downloader = S3Downloader(bucket=ingestion_bucket)
    if args.keys:
        s3_keys = args.keys
    else:
        print(f"Scanning S3 bucket: {ingestion_bucket}...")
        s3_keys = downloader.list_courses()
        if not s3_keys:
            print("No .zip files found in S3 bucket.")
            return

    # 2. Parallel Processing using ProcessPoolExecutor to bypass GIL
    info = (university_id, author_id, cdn_url, ingestion_bucket, s3_cdn_bucket)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_single_key, key, info) for key in s3_keys]
        
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    success_count = sum(1 for r in results if r)
    print("\n" + "=" * 80)
    print(f"S3 BATCH MIGRATION COMPLETE: {success_count}/{len(s3_keys)} successful")
    print("=" * 80)

if __name__ == "__main__":
    main()
