import logging
import json
import sys
from datetime import datetime

class StructuredLogger:
    """
    Simple structured logger that outputs JSON logs for production observability.
    """
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            # Custom formatter could be added here
            self.logger.addHandler(handler)

    def log(self, level, message, **kwargs):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "context": kwargs
        }
        # In a real system, we'd use a professional JSON formatter
        # For now, we print JSON strings for observability demo
        if level == "INFO":
            self.logger.info(json.dumps(log_entry))
        elif level == "WARNING":
            self.logger.warning(json.dumps(log_entry))
        elif level == "ERROR":
            self.logger.error(json.dumps(log_entry))

def get_logger(name: str):
    return StructuredLogger(name)
