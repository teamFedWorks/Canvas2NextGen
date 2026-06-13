from pathlib import Path
import shutil
import zipfile
from abc import ABC, abstractmethod
from typing import Generator

class Workspace(ABC):
    """
    Abstract base class for workspaces representing course content packages.
    """
    @property
    @abstractmethod
    def root_path(self) -> Path:
        """Returns the absolute root directory of the workspace."""
        pass

    @abstractmethod
    def get_file_path(self, relative_path: str) -> Path:
        """Resolves a relative path within the workspace safely."""
        pass

    @abstractmethod
    def exists(self, relative_path: str) -> bool:
        """Checks if a relative path exists inside the workspace."""
        pass

    @abstractmethod
    def rglob(self, pattern: str):
        """Finds all files matching the pattern within the workspace."""
        pass


class ExtractedWorkspace(Workspace):
    """
    Immutable Workspace representation representing a directory containing
    the extracted/unpacked course content files.
    
    Fields are exposed via read-only properties to preserve immutability.
    """
    def __init__(self, root_path: Path, is_temporary: bool = False):
        self._root_path = Path(root_path).resolve()
        self._is_temporary = is_temporary

    @property
    def root_path(self) -> Path:
        return self._root_path

    @property
    def is_temporary(self) -> bool:
        return self._is_temporary

    def get_file_path(self, relative_path: str) -> Path:
        """
        Resolves a relative path to an absolute path, ensuring safety against path traversal.
        """
        target_path = (self.root_path / relative_path).resolve()
        
        try:
            target_path.relative_to(self.root_path)
        except ValueError:
            raise ValueError(f"Path traversal detected: {relative_path} resolves outside workspace root {self.root_path}")
            
        return target_path

    def exists(self, relative_path: str) -> bool:
        try:
            return self.get_file_path(relative_path).exists()
        except ValueError:
            return False

    def list_files(self, glob_pattern: str = "**/*") -> Generator[Path, None, None]:
        """Lists files matching the glob pattern inside the workspace."""
        return self.root_path.glob(glob_pattern)

    def rglob(self, pattern: str):
        """Finds all files matching the pattern within the workspace."""
        return self.root_path.rglob(pattern)

    def cleanup(self) -> None:
        """Cleans up the workspace directory if it is flagged as temporary."""
        if self.is_temporary and self.root_path.exists():
            shutil.rmtree(self.root_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class ZippedWorkspace(Workspace):
    """
    On-demand extracting workspace. Avoids full disk unzipping by only
    extracting files from the zip file when they are explicitly requested.
    """
    def __init__(self, zip_path: Path, temp_dir: Path):
        self.zip_path = Path(zip_path).resolve()
        self.temp_dir = Path(temp_dir).resolve()
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self._zip_ref = zipfile.ZipFile(self.zip_path, "r")
        self._extracted_paths = {}

    @property
    def root_path(self) -> Path:
        return self.temp_dir

    def get_file_path(self, relative_path: str) -> Path:
        # Normalize/clean path
        rel_str = str(relative_path).replace("\\", "/").lstrip("/")
        
        # Check Zip Slip directory traversal
        target_path = (self.temp_dir / rel_str).resolve()
        try:
            target_path.relative_to(self.temp_dir)
        except ValueError:
            raise ValueError(f"Path traversal detected: {relative_path}")
            
        if rel_str in self._extracted_paths:
            return self._extracted_paths[rel_str]
            
        # Check if it exists in zip
        try:
            info = self._zip_ref.getinfo(rel_str)
            self._zip_ref.extract(rel_str, self.temp_dir)
            self._extracted_paths[rel_str] = target_path
        except KeyError:
            # Fallback check (some paths might have different case or leading slashes)
            found = False
            for name in self._zip_ref.namelist():
                if name.lower() == rel_str.lower():
                    rel_str = name
                    found = True
                    break
            if found:
                self._zip_ref.extract(rel_str, self.temp_dir)
                self._extracted_paths[rel_str] = target_path
                
        return target_path

    def exists(self, relative_path: str) -> bool:
        rel_str = str(relative_path).replace("\\", "/").lstrip("/")
        try:
            self._zip_ref.getinfo(rel_str)
            return True
        except KeyError:
            # check lowercase fallback
            for name in self._zip_ref.namelist():
                if name.lower() == rel_str.lower():
                    return True
            return False

    def list_files(self, glob_pattern: str = "**/*") -> Generator[Path, None, None]:
        for name in self._zip_ref.namelist():
            yield self.temp_dir / name

    def rglob(self, pattern: str):
        import fnmatch
        import os
        matched = []
        for name in self._zip_ref.namelist():
            if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(os.path.basename(name), pattern) or fnmatch.fnmatch(name, f"**/{pattern}"):
                try:
                    matched.append(self.get_file_path(name))
                except Exception:
                    pass
        return matched

    def cleanup(self) -> None:
        try:
            self._zip_ref.close()
        except Exception:
            pass
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
