from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from .router import router

app = FastAPI(
    title="NextGen LMS Migration Service",
    description="Microservice for orchestrating Canvas course migrations to NextGen LMS.",
    version="2.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include migration routes
app.include_router(router, prefix="/api/v1")

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
    uvicorn.run(app, host="0.0.0.0", port=8000)
