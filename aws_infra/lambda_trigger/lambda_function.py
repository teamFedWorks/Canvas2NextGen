"""
course_shell_upload_trigger.py

Handles S3 upload events for Canvas course shells.

Responsibilities (in order):
  1. Format and send a human-readable email alert via SNS  (existing behaviour)
  2. POST to the CourseOnboarding API to kick off ingestion  (new behaviour)

Flow:
  S3 upload (.imscc / .zip)
      │
      ├─► SNS → email alert to team
      │
      └─► POST /api/v1/migrate-s3  → ECS ingestion pipeline

Environment variables required:
  SNS_TOPIC_ARN          ARN of the SNS topic for email alerts
  AWS_REGION_NAME        AWS region, e.g. us-east-2
  ONBOARDING_API_URL     Internal URL of the CourseOnboarding ECS service,
                         e.g. http://10.0.1.45:5009/api/v1
  ONBOARDING_API_KEY     Value of the X-API-Key header
  WBU_UNIVERSITY_ID      MongoDB university ID for WBU courses
  WBU_AUTHOR_ID          MongoDB author ID for WBU courses
  SFC_UNIVERSITY_ID      MongoDB university ID for SFC courses  (optional)
  SFC_AUTHOR_ID          MongoDB author ID for SFC courses      (optional)
"""

import boto3
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone

sns = boto3.client("sns", region_name=os.environ["AWS_REGION_NAME"])

SNS_TOPIC_ARN     = os.environ["SNS_TOPIC_ARN"]
API_BASE_URL      = os.environ.get("ONBOARDING_API_URL", "").rstrip("/")
API_KEY           = os.environ.get("ONBOARDING_API_KEY", "")

# Per-institution MongoDB IDs — looked up by institution name from the S3 key
INSTITUTION_IDS = {
    "WBU": {
        "university_id": os.environ.get("WBU_UNIVERSITY_ID", ""),
        "author_id":     os.environ.get("WBU_AUTHOR_ID", ""),
    },
    "SFC": {
        "university_id": os.environ.get("SFC_UNIVERSITY_ID", ""),
        "author_id":     os.environ.get("SFC_AUTHOR_ID", ""),
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────

EVENT_LABELS = {
    "ObjectCreated:Put":                      "File uploaded",
    "ObjectCreated:Post":                     "File uploaded (POST)",
    "ObjectCreated:Copy":                     "File copied",
    "ObjectCreated:CompleteMultipartUpload":  "File uploaded (multipart)",
    "ObjectRemoved:Delete":                   "File deleted",
    "ObjectRemoved:DeleteMarkerCreated":      "File deleted (versioned)",
}


def format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.1f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.1f} GB"


def parse_s3_key(key: str) -> dict:
    """
    Parse S3 key into its meaningful parts.

    Handles both casing variants:
      Institutions/SFC/programs/bs-computer-science/courses/it-2440.imscc
      Institutions/WBU/Programs/phd-program/courses/phd-course-shell.imscc
    """
    parts = key.strip("/").split("/")
    result = {"full_path": key, "filename": parts[-1]}
    try:
        result["institution"] = parts[1]          # SFC or WBU
        result["program"]     = parts[3]          # e.g. phd-program
        result["folder"]      = parts[4] if len(parts) > 4 else ""
    except IndexError:
        pass
    return result


def format_event_time(iso_time: str) -> str:
    dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
    return dt.strftime("%B %d, %Y at %I:%M:%S %p UTC")


def build_email(record: dict, ingestion_status: str) -> tuple:
    """Build subject and body for the email alert."""
    event_name  = record.get("eventName", "Unknown")
    event_label = EVENT_LABELS.get(event_name, event_name)
    region      = record.get("awsRegion", "unknown")
    event_time  = format_event_time(record.get("eventTime", ""))

    s3_info    = record["s3"]
    bucket     = s3_info["bucket"]["name"]
    obj        = s3_info["object"]
    key        = obj["key"]
    size_bytes = obj.get("size", 0)
    etag       = obj.get("eTag", "N/A")

    parsed      = parse_s3_key(key)
    filename    = parsed["filename"]
    institution = parsed.get("institution", "Unknown")
    program     = parsed.get("program", "Unknown")
    size_human  = format_size(size_bytes)

    is_real   = size_bytes > 10_000
    size_note = "" if is_real else "    File is very small — may be a test upload"

    subject = f" {event_label}: {filename} — {institution} / {program}"

    body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  EduVateHub — Course Shell Upload Alert
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Event:        {event_label}
  Time:         {event_time}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  FILE DETAILS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  File name:    {filename}
  File size:    {size_human}{size_note}
  Institution:  {institution}
  Program:      {program}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  S3 LOCATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Bucket:       {bucket}
  Path:         {key}
  Region:       {region}
  ETag:         {etag}

  Console link:
  https://s3.console.aws.amazon.com/s3/object/{bucket}?region={region}&prefix={key}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  INGESTION STATUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  {ingestion_status}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    return subject, body


# ── Ingestion trigger ─────────────────────────────────────────────────────────

def trigger_ingestion(s3_key: str, bucket: str, institution: str) -> str:
    """
    POST to the CourseOnboarding API to start the ingestion pipeline.

    Returns a human-readable status string for inclusion in the email.
    """
    if not API_BASE_URL or not API_KEY:
        return (
            "  Ingestion API not configured (ONBOARDING_API_URL / ONBOARDING_API_KEY missing).\n"
            "   Manual ingestion required:\n"
            f"   python main.py ingest s3 --institution {institution} --force"
        )

    ids = INSTITUTION_IDS.get(institution.upper(), {})
    university_id = ids.get("university_id") or ""
    author_id     = ids.get("author_id") or ""

    if not university_id:
        return (
            f"  No MongoDB university ID configured for institution '{institution}'.\n"
            "   Set WBU_UNIVERSITY_ID (or SFC_UNIVERSITY_ID) in the Lambda environment.\n"
            "   Manual ingestion required."
        )

    payload = json.dumps({
        "s3_key":        s3_key,
        "bucket":        bucket,
        "university_id": university_id,
        "author_id":     author_id,
    }).encode("utf-8")

    url = f"{API_BASE_URL}/migrate-s3"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-API-Key":    API_KEY,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            task_id = body.get("task_id", "unknown")
            return (
                f" Ingestion pipeline triggered successfully.\n"
                f"   Task ID : {task_id}\n"
                f"   Monitor : GET {API_BASE_URL}/status/{task_id}"
            )
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")[:300]
        return (
            f" Ingestion API returned HTTP {e.code}.\n"
            f"   Response: {error_body}\n"
            "   Manual ingestion may be required."
        )
    except Exception as e:
        return (
            f" Could not reach ingestion API: {e}\n"
            "   The ECS service may be stopped (it is adhoc).\n"
            f"   Start it with: .\\aws_infra\\start-service.ps1\n"
            f"   Then run: python main.py ingest s3 --institution {institution} --force"
        )


# ── Lambda entry point ────────────────────────────────────────────────────────

def handler(event: dict, context):
    """
    Lambda entry point.

    For each S3 record:
      1. Trigger the ingestion pipeline via the CourseOnboarding API.
      2. Send an email alert via SNS that includes the ingestion status.
    """
    print("Received event:", json.dumps(event))

    records = event.get("Records", [])
    if not records:
        print("No records — nothing to do")
        return

    for record in records:
        try:
            s3_info     = record["s3"]
            bucket      = s3_info["bucket"]["name"]
            key         = s3_info["object"]["key"]
            parsed      = parse_s3_key(key)
            institution = parsed.get("institution", "UNKNOWN")

            # Step 1: trigger ingestion (do this first so status is in the email)
            ingestion_status = trigger_ingestion(key, bucket, institution)
            print(f"Ingestion trigger result: {ingestion_status}")

            # Step 2: send email alert with ingestion status embedded
            subject, body = build_email(record, ingestion_status)
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject[:100],   # SNS subject limit
                Message=body,
            )
            print(f"Alert sent: {subject}")

        except Exception as e:
            print(f"Error processing record: {e}")
            raise
