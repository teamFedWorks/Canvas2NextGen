import hashlib
import zipfile
from pathlib import Path
import tempfile
import shutil
from typing import Optional

from src.ucae.workflow.input_source import InputSource
from src.ucae.workflow.workspace import ExtractedWorkspace, ZippedWorkspace


class ExtractionService:
    """
    Storage-agnostic package extraction service.
    Downloads the archive from an InputSource, verifies its SHA-256 checksum,
    and extracts it securely (preventing Zip Slip directory traversals).
    """
    def __init__(self, temp_base_dir: Optional[Path] = None):
        self.temp_base_dir = temp_base_dir

    def prepare(self, input_source: InputSource) -> ZippedWorkspace:
        """
        Verifies integrity, and returns a ZippedWorkspace supporting on-demand extraction.
        """
        from src.ucae.workflow.exceptions import CorruptedArchiveError
        from src.ucae.workflow.workspace import ZippedWorkspace
        # Create a unique temp folder
        temp_dir = Path(tempfile.mkdtemp(dir=self.temp_base_dir))
        
        try:
            # Resolve package archive locally
            archive_path = input_source.get_local_path(temp_dir)
            
            # Verify file integrity
            self._verify_checksum(archive_path, input_source.checksum)
            
            extracted_path = temp_dir / "extracted"
            extracted_path.mkdir(parents=True, exist_ok=True)
            
            # Safe check all files for Zip Slip directory traversal before returning workspace
            with zipfile.ZipFile(archive_path, "r") as zip_ref:
                for member in zip_ref.infolist():
                    member_path = (extracted_path / member.filename).resolve()
                    try:
                        member_path.relative_to(extracted_path)
                    except ValueError:
                        raise ValueError(
                            f"Zip Slip directory traversal vulnerability detected in archive: {member.filename}"
                        )
            
            # Return zipped workspace which handles single file extraction on demand
            return ZippedWorkspace(archive_path, extracted_path)
            
        except zipfile.BadZipFile as e:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise CorruptedArchiveError(f"Bad zip file structure: {e}") from e
        except ValueError as e:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise CorruptedArchiveError(str(e)) from e
        except Exception as e:
            # Cleanup temp directory on extraction failure
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise CorruptedArchiveError(f"Extraction failed: {e}") from e

    def _verify_checksum(self, file_path: Path, expected_checksum: str) -> None:
        """Calculates SHA-256 and asserts match against expected checksum."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        
        actual_checksum = sha256.hexdigest()
        if actual_checksum.lower() != expected_checksum.lower():
            raise ValueError(f"Checksum validation failed! Expected: {expected_checksum}, Got: {actual_checksum}")

    def _safe_extract(self, zip_path: Path, extract_to: Path) -> None:
        """Safely extracts zip file checking each file path for traversal vulnerabilities."""
        target_dir = extract_to.resolve()
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for member in zip_ref.infolist():
                # Resolve destination path for the member
                member_path = (target_dir / member.filename).resolve()
                
                # Assert member path remains within extraction root
                try:
                    member_path.relative_to(target_dir)
                except ValueError:
                    raise ValueError(
                        f"Zip Slip directory traversal vulnerability detected in archive: {member.filename}"
                    )
            
            # If all files are safe, extract archive contents
            zip_ref.extractall(target_dir)
