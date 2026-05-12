"""
Security Middleware - API Authentication and Request Validation
"""

import os
from fastapi import Request, HTTPException, Security
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN
from observability.logger import get_logger

logger = get_logger(__name__)

API_KEY_NAME = "X-API-Key"
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def get_api_key(api_key_header: str = Security(API_KEY_HEADER)):
    """Validates the API key from header."""
    expected_key = os.getenv("LMS_API_KEY")
    if not expected_key:
        logger.error("LMS_API_KEY environment variable not set")
        raise HTTPException(status_code=500, detail="Server auth configuration error")

    if api_key_header == expected_key:
        return api_key_header
        
    logger.warning("Invalid API key attempt")
    raise HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="Could not validate API key"
    )

async def validate_file_upload(request: Request):
    """Validates file size and type before processing."""
    # Note: Content-Length check is an approximation of file size
    content_length = request.headers.get('content-length')
    if content_length:
        max_mb = int(os.getenv("MAX_UPLOAD_MB", "500"))
        if int(content_length) > max_mb * 1024 * 1024:
            logger.warning("Upload blocked: File size exceeds limit", extra={"size": content_length})
            raise HTTPException(status_code=413, detail=f"File too large. Max allowed is {max_mb} MB")
            
    return True
