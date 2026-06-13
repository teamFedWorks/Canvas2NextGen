class IngestionError(Exception):
    """Base exception for all ingestion errors."""
    pass

class QuarantineError(IngestionError):
    """Base exception for errors that should cause a package to be quarantined."""
    pass

class CorruptedArchiveError(QuarantineError):
    """Raised when the archive file is corrupted."""
    pass

class UnsupportedArchiveError(QuarantineError):
    """Raised when the archive format is not supported or no provider is detected."""
    pass

class PasswordProtectedArchiveError(QuarantineError):
    """Raised when the archive is password-protected."""
    pass

class MalwareDetectedError(QuarantineError):
    """Raised when malware is detected in the package."""
    pass

class LockAcquisitionError(IngestionError):
    """Raised when a distributed lock cannot be acquired."""
    pass

class DeadLetterError(IngestionError):
    """Raised when a job has exceeded maximum infrastructure retries."""
    pass
