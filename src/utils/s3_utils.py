"""
S3 Utilities Module

Handles downloading Canvas course export ZIPs from AWS S3.
"""

import os
from pathlib import Path
from typing import Optional


def _get_s3_client():
    """Create and return a boto3 S3 client using environment credentials."""
    try:
        import boto3
        return boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
        )
    except ImportError:
        raise ImportError("boto3 is required for S3 support. Install with: pip install boto3")


class S3Downloader:
    """
    Downloads Canvas course ZIP files from an AWS S3 bucket.
    """

    def __init__(self, bucket: Optional[str] = None):
        """
        Args:
            bucket: S3 bucket name. Defaults to S3_COURSE_BUCKET env var.
        """
        self.bucket = bucket or os.getenv('S3_INGESTION_BUCKET')
        if not self.bucket:
            raise ValueError(
                "S3 bucket not configured. Set S3_INGESTION_BUCKET in your .env file."
            )
        self.client = _get_s3_client()

    def construct_hierarchical_key(
        self,
        institution: str,
        program: str,
        course: str,
        programs_segment: str = "programs",
    ) -> str:
        """
        Construct the S3 key for a course using the canonical bucket layout:

            Institutions/{institution}/{programs_segment}/{program}/courses/{course}.imscc

        Args:
            institution:      Institution folder name, e.g. 'SFC' or 'WBU'.
            program:          Program slug, e.g. 'bs-computer-science' or 'phd-program'.
            course:           Course file stem, e.g. 'it-2440' or 'phd-course-shell'.
            programs_segment: The middle path segment.  SFC uses 'programs' (lowercase);
                              WBU uses 'Programs' (capitalised).  Defaults to 'programs'.

        Returns:
            Full S3 key string, e.g.
            'Institutions/SFC/programs/bs-computer-science/courses/it-2440.imscc'
        """
        return (
            f"Institutions/{institution}/{programs_segment}/"
            f"{program}/courses/{course}.imscc"
        )

    def download(self, s3_key: str, destination: Path) -> Path:
        """
        Download a file from S3 to a local path.

        Args:
            s3_key: The S3 object key (e.g. 'courses/cs101.zip').
            destination: Local directory where the file will be saved.

        Returns:
            Path to the downloaded file.

        Raises:
            ValueError: If the S3 key is invalid.
            Exception: On download failure.
        """
        if not s3_key:
            raise ValueError("s3_key must not be empty.")

        destination.mkdir(parents=True, exist_ok=True)
        filename = Path(s3_key).name
        local_path = destination / filename

        print(f"[S3] Downloading s3://{self.bucket}/{s3_key} -> {local_path}")

        self.client.download_file(
            Bucket=self.bucket,
            Key=s3_key,
            Filename=str(local_path),
            Callback=_ProgressLogger(s3_key, self._get_object_size(s3_key)),
        )

        print(f"[S3] Download complete: {local_path}")
        return local_path

    def _get_object_size(self, s3_key: str) -> int:
        """Return the size in bytes of the S3 object (0 if unknown)."""
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return resp.get('ContentLength', 0)
        except Exception:
            return 0

    def list_courses(self, prefix: str = '', extensions: tuple = ('.zip',)) -> list:
        """
        List available course packages in the bucket.

        Args:
            prefix:     Optional prefix filter (e.g. 'courses/').
            extensions: File extensions to include.  Defaults to ('.zip',) for
                        backwards compatibility.  Pass ('.zip', '.imscc') to
                        also pick up Canvas IMSCC packages.

        Returns:
            List of S3 keys matching the prefix and extensions.
        """
        paginator = self.client.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if any(key.lower().endswith(ext) for ext in extensions):
                    keys.append(key)
        return keys


class _ProgressLogger:
    """Simple progress callback for boto3 download."""

    def __init__(self, key: str, total: int):
        self._key = key
        self._total = total
        self._seen = 0

    def __call__(self, bytes_amount: int):
        self._seen += bytes_amount
        if self._total > 0:
            pct = (self._seen / self._total) * 100
            print(f"\r[S3] {self._key}: {pct:.1f}%", end='', flush=True)
        if self._seen >= self._total and self._total > 0:
            print()  # newline after 100%
