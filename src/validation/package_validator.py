import os
import zipfile
from pathlib import Path

class PackageValidator:
    """
    Validates course packages for security and size limits.
    """
    MAX_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB
    MAX_FILE_COUNT = 20000

    @staticmethod
    def validate_zip(zip_path: Path) -> tuple[bool, str]:
        """
        Validates a ZIP file for Zip Slip vulnerability and size/count limits.
        """
        if not zip_path.exists():
            return False, "File does not exist."

        # Check file size
        if zip_path.stat().st_size > PackageValidator.MAX_SIZE_BYTES:
            return False, f"Archive exceeds maximum size of 2GB."

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Check file count
                if len(zip_ref.infolist()) > PackageValidator.MAX_FILE_COUNT:
                    return False, f"Archive contains more than {PackageValidator.MAX_FILE_COUNT} files."

                # Check for Zip Slip
                for member in zip_ref.namelist():
                    # Resolve path to check for directory traversal
                    if member.startswith('/') or '..' in member or member.startswith('\\'):
                         return False, f"Security risk: Potential Zip Slip detected in {member}"

            return True, "Validation successful."
        except zipfile.BadZipFile:
            return False, "Invalid ZIP file."
        except Exception as e:
            return False, f"Unexpected error during validation: {str(e)}"

    @staticmethod
    def is_safe_path(base_dir: Path, target_path: str) -> bool:
        """
        Checks if the target path is safe (stays within base_dir).
        """
        # Ensure target_path is relative
        if target_path.startswith('/') or target_path.startswith('\\'):
            return False
            
        try:
            # Join and resolve to absolute path
            final_path = (base_dir / target_path).resolve()
            # Check if final_path is inside base_dir
            return final_path.is_relative_to(base_dir.resolve())
        except (ValueError, RuntimeError):
            return False

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """
        Calculates SHA256 checksum of a file for idempotency.
        """
        import hashlib
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
