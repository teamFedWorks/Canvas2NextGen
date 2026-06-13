"""
Distributed Tracing - Propagation of correlation IDs across all components.

Enables end-to-end request tracing across:
- API Gateway → Lambda → Pipeline → Workers → MongoDB

Every operation logs correlation_id for correlation in logs.
"""

import uuid
import contextvars
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field
from functools import wraps

from observability.logger import get_logger

logger = get_logger(__name__)

# Context variable for correlation ID propagation
_correlation_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar('correlation_id', default=None)
_trace_ctx: contextvars.ContextVar['TraceContext'] = contextvars.ContextVar('trace_context', default=None)


@dataclass
class TraceContext:
    """
    Full trace context for a single ingestion job.
    
    Contains:
    - Correlation ID (global)
    - Job ID (domain)
    - Parent spans
    - Timing information
    """
    correlation_id: str
    job_id: str
    parent_span_id: Optional[str] = None
    
    # Current span tracking
    current_span_id: Optional[str] = None
    span_stack: list = field(default_factory=list)
    
    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    
    # Custom attributes
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def start_span(self, name: str) -> 'Span':
        """Start a new timing span."""
        span = Span(
            trace_id=self.correlation_id,
            parent_id=self.current_span_id,
            name=name,
        )
        self.span_stack.append(self.current_span_id)
        self.current_span_id = span.span_id
        return span
    
    def end_span(self, span: 'Span'):
        """End current span."""
        if self.current_span_id == span.span_id:
            self.current_span_id = self.span_stack.pop() if self.span_stack else None
        span.end()
    
    def add_attribute(self, key: str, value: Any):
        """Add trace attribute."""
        self.attributes[key] = value
    
    def get_duration_ms(self) -> float:
        """Get total trace duration."""
        end = self.completed_at or datetime.utcnow()
        return (end - self.started_at).total_seconds() * 1000

    def __enter__(self) -> 'TraceContext':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.completed_at = datetime.utcnow()
        _correlation_id_ctx.set(None)
        _trace_ctx.set(None)


@dataclass
class Span:
    """Timing span within a trace."""
    trace_id: str
    parent_id: Optional[str]
    name: str
    
    span_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=datetime.utcnow)
    ended_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    
    def end(self):
        self.ended_at = datetime.utcnow()
        self.duration_ms = (self.ended_at - self.started_at).total_seconds() * 1000

    def __enter__(self) -> 'Span':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()


class TracingMiddleware:
    """
    Context manager for automatic tracing.
    
    Usage:
        with TracingMiddleware.job_trace(job_id, correlation_id) as trace:
            # Do work
            trace.add_attribute("stage", "parsing")
    """
    
    @classmethod
    def job_trace(cls, job_id: str, correlation_id: Optional[str] = None) -> TraceContext:
        """Create a trace context for a job."""
        cid = correlation_id or str(uuid.uuid4())
        trace = TraceContext(correlation_id=cid, job_id=job_id)
        
        # Set context vars
        _correlation_id_ctx.set(cid)
        _trace_ctx.set(trace)
        
        logger.info("Trace started", 
                   extra={"correlation_id": cid, "job_id": job_id})
        
        return trace
    
    @classmethod
    def get_current_correlation_id(cls) -> Optional[str]:
        """Get current correlation ID from context."""
        return _correlation_id_ctx.get()
    
    @classmethod
    def get_current_trace(cls) -> Optional[TraceContext]:
        """Get current trace context."""
        return _trace_ctx.get()
    
    @classmethod
    def span(cls, name: str):
        """Decorator to wrap function in a timed span."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                trace = _trace_ctx.get()
                if not trace:
                    # No trace context - just execute
                    return func(*args, **kwargs)
                
                with trace.start_span(name) as span:
                    try:
                        result = func(*args, **kwargs)
                        span.add_attribute("success", True)
                        return result
                    except Exception as e:
                        span.add_attribute("success", False)
                        span.add_attribute("error", str(e))
                        raise
            return wrapper
        return decorator


def get_correlation_id() -> str:
    """Get or generate correlation ID for this context."""
    cid = _correlation_id_ctx.get()
    if not cid:
        cid = str(uuid.uuid4())
        _correlation_id_ctx.set(cid)
    return cid


def inject_trace_context(headers: Dict[str, str] = None) -> Dict[str, str]:
    """
    Inject correlation ID into HTTP headers for downstream calls.
    
    Standard header: X-Correlation-ID
    Also supports: X-Trace-ID, X-Job-ID (if in trace context)
    """
    if headers is None:
        headers = {}
    
    cid = get_correlation_id()
    headers["X-Correlation-ID"] = cid
    
    trace = _trace_ctx.get()
    if trace:
        headers["X-Job-ID"] = trace.job_id
        if trace.current_span_id:
            headers["X-Span-ID"] = trace.current_span_id
    
    return headers


def extract_trace_context(headers: Dict[str, str]) -> Dict[str, str]:
    """
    Extract correlation ID from incoming HTTP request.
    Propagates existing trace or starts new one.
    """
    cid = headers.get("X-Correlation-ID") or str(uuid.uuid4())
    _correlation_id_ctx.set(cid)
    
    return {"correlation_id": cid}


# Structured logging with automatic trace context
class TraceLogger:
    """Logger that automatically includes trace context."""
    
    def __init__(self, name: str):
        self.logger = get_logger(name)
    
    def _extra(self, extra: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build extra dict with trace context."""
        base = {
            "correlation_id": get_correlation_id(),
        }
        trace = _trace_ctx.get()
        if trace:
            base["job_id"] = trace.job_id
            base["trace_duration_ms"] = trace.get_duration_ms()
        
        if extra:
            base.update(extra)
        return base
    
    def info(self, msg: str, extra: Dict[str, Any] = None):
        self.logger.info(msg, extra=self._extra(extra))
    
    def warning(self, msg: str, extra: Dict[str, Any] = None):
        self.logger.warning(msg, extra=self._extra(extra))
    
    def error(self, msg: str, extra: Dict[str, Any] = None):
        self.logger.error(msg, extra=self._extra(extra))
    
    def debug(self, msg: str, extra: Dict[str, Any] = None):
        self.logger.debug(msg, extra=self._extra(extra))

    def exception(self, msg: str, extra: Dict[str, Any] = None, *args, **kwargs):
        self.logger.exception(msg, extra=self._extra(extra), *args, **kwargs)