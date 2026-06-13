from dataclasses import dataclass
from typing import Optional

@dataclass
class ValidationIssue:
    severity: str                 # "warning" | "error" | "info"
    code: str                     # e.g., "EMPTY_CONTAINER", "BROKEN_LINK", "MISSING_RESOURCE"
    path: str                     # JSONPath format mapping, e.g., "containers[0].items[2]"
    message: str                  # Human-readable warning description
    provider: Optional[str] = None
    provider_version: Optional[str] = None
    suggested_fix: Optional[str] = None
    documentation_url: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "severity": self.severity,
            "code": self.code,
            "path": self.path,
            "message": self.message,
            "provider": self.provider,
            "provider_version": self.provider_version,
            "suggested_fix": self.suggested_fix,
            "documentation_url": self.documentation_url
        }
