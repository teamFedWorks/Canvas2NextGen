from abc import ABC, abstractmethod
from pathlib import Path
import time
from typing import Optional
import boto3
from botocore.exceptions import ClientError


class InputSource(ABC):
    """
    Abstract base class representing package ingestion sources (S3, local zip, Azure Blob, etc.).
    Isolates extraction logic from transport details.
    """
    
    @abstractmethod
    def get_local_path(self, download_dir: Path) -> Path:
        """
        Resolves or downloads the package to a local file path inside the given download directory.
        Returns the absolute Path of the package archive.
        """
        pass

    @property
    @abstractmethod
    def checksum(self) -> str:
        """Returns the expected SHA-256 checksum of the package archive."""
        pass


class LocalFileInputSource(InputSource):
    """
    Concrete InputSource resolving a zip package from the local filesystem.
    """
    def __init__(self, file_path: Path, expected_checksum: str):
        self.file_path = Path(file_path).resolve()
        self._checksum = expected_checksum

    def get_local_path(self, download_dir: Path) -> Path:
        if not self.file_path.exists():
            raise FileNotFoundError(f"Local package file not found at: {self.file_path}")
        return self.file_path

    @property
    def checksum(self) -> str:
        return self._checksum


class S3InputSource(InputSource):
    """
    Concrete InputSource downloading a zip package from AWS S3.
    Supports versioning, automatic metadata detection, and retries.
    """
    def __init__(
        self, 
        bucket: str, 
        key: str, 
        version_id: Optional[str] = None, 
        expected_checksum: Optional[str] = None, 
        s3_client=None
    ):
        self.bucket = bucket
        self.key = key
        self.version_id = version_id
        self._expected_checksum = expected_checksum
        self._s3_client = s3_client or boto3.client("s3")

    def get_local_path(self, download_dir: Path) -> Path:
        filename = Path(self.key).name or "package.zip"
        local_path = download_dir / filename

        max_retries = 3
        backoff = 2.0

        for attempt in range(1, max_retries + 1):
            try:
                extra_args = {}
                if self.version_id:
                    extra_args["VersionId"] = self.version_id

                # Auto-resolve checksum from metadata if not explicitly provided
                if not self._expected_checksum:
                    head = self._s3_client.head_object(Bucket=self.bucket, Key=self.key, **extra_args)
                    metadata = head.get("Metadata", {})
                    # Look for custom 'sha256' metadata, otherwise fall back to ETag
                    self._expected_checksum = metadata.get("sha256") or head.get("ETag", "").strip('"')

                self._s3_client.download_file(
                    Bucket=self.bucket,
                    Key=self.key,
                    Filename=str(local_path),
                    ExtraArgs=extra_args
                )
                return local_path
            except ClientError as e:
                if attempt == max_retries:
                    raise IOError(f"Failed downloading from S3: {str(e)}") from e
                time.sleep(backoff ** attempt)
        
        raise IOError(f"Failed to download s3://{self.bucket}/{self.key} after {max_retries} attempts.")

    @property
    def checksum(self) -> str:
        if not self._expected_checksum:
            extra_args = {}
            if self.version_id:
                extra_args["VersionId"] = self.version_id
            try:
                head = self._s3_client.head_object(Bucket=self.bucket, Key=self.key, **extra_args)
                metadata = head.get("Metadata", {})
                self._expected_checksum = metadata.get("sha256") or head.get("ETag", "").strip('"')
            except Exception as e:
                raise ValueError(f"Could not resolve checksum for S3 object: {str(e)}")
        return self._expected_checksum
