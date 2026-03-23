from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Celery(
    "edu_onboarding",
    broker=redis_url,
    backend=redis_url,
    include=["src.edu_onboarding.worker.tasks"]
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1, # One task at a time for CPU-heavy ingestion
    worker_max_tasks_per_child=10, # Re-spawn workers to clear memory
)

if __name__ == "__main__":
    app.start()
