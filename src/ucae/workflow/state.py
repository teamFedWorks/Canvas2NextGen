from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
import uuid

class JobState(Enum):
    CREATED = "CREATED"
    DOWNLOAD_STARTED = "DOWNLOAD_STARTED"
    DOWNLOAD_FINISHED = "DOWNLOAD_FINISHED"
    EXTRACTION_STARTED = "EXTRACTION_STARTED"
    EXTRACTION_FINISHED = "EXTRACTION_FINISHED"
    DETECTED = "DETECTED"
    PARSE_STARTED = "PARSE_STARTED"
    PARSE_FINISHED = "PARSE_FINISHED"
    VALIDATION_STARTED = "VALIDATION_STARTED"
    VALIDATION_FINISHED = "VALIDATION_FINISHED"
    EXPORT_STARTED = "EXPORT_STARTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


@dataclass
class JobEvent:
    stage: str
    message: str
    actor: str = "system"
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "stage": self.stage,
            "actor": self.actor,
            "message": self.message,
            "metadata": self.metadata
        }
