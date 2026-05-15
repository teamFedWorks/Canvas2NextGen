#!/usr/bin/env python3
"""Create SQS queues for the ingestion pipeline."""

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from messaging.sqs_integration import create_queues

if __name__ == "__main__":
    # Read region from .env
    import os
    region = os.getenv("AWS_REGION", "us-east-2")
    
    print(f"Creating SQS queues in region: {region}")
    queue_url, dlq_url = create_queues(region=region)
    
    print(f"\n[OK] Queues created successfully!")
    print(f"  Main Queue URL: {queue_url}")
    print(f"  DLQ URL: {dlq_url}")
    print(f"\nAdd these to your .env file:")
    print(f"  SQS_QUEUE_URL={queue_url}")
    print(f"  SQS_DLQ_URL={dlq_url}")
