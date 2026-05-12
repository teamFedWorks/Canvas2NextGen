"""
API middleware for distributed tracing and correlation IDs.

Automatically injects/extracts correlation IDs from HTTP headers.
"""

from fastapi import Request, HTTPException, Depends
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Dict

from observability.tracing import (
    extract_trace_context,
    inject_trace_context,
    get_correlation_id
)
from services.canonical_migration_service import CanonicalMigrationService

# Global service instance
migration_service = CanonicalMigrationService()


class TracingMiddleware(BaseHTTPMiddleware):
    """HTTP middleware for correlation ID propagation."""
    
    async def dispatch(self, request: Request, call_next):
        # Extract trace context
        headers = dict(request.headers)
        trace_info = extract_trace_context(headers)
        
        # Process request
        response = await call_next(request)
        
        # Inject correlation ID into response
        response.headers["X-Correlation-ID"] = get_correlation_id()
        
        return response


def get_migration_service() -> CanonicalMigrationService:
    """Dependency injection for migration service."""
    return migration_service


def require_api_key(request: Request) -> str:
    """Validate API key from header."""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    expected = os.getenv("API_KEY", "")
    if not expected or api_key != expected:
        raise HTTPException(status_code=403, detail="Invalid API key")
    
    return api_key