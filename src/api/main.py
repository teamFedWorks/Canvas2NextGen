from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from .router_enhanced import router as canonical_router
from .middleware_enhanced import TracingMiddleware

app = FastAPI(
    title="Canonical LMS Migration Service",
    description="Enterprise-grade multi-LMS ingestion platform with canonical normalization, orchestration, and observability.",
    version="3.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add distributed tracing middleware
app.add_middleware(TracingMiddleware)

# Include canonical migration routes
app.include_router(canonical_router, prefix="/api/v1")

@app.get("/")
async def root():
    return {
        "service": "NextGen LMS Migration Service",
        "version": "2.0.0",
        "status": "online",
        "docs": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5008)
