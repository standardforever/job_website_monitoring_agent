from __future__ import annotations

import os

from celery import Celery

_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery("job_monitoring", broker=_BROKER_URL, backend=_RESULT_BACKEND)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=86_400,
    task_time_limit=90_000,
    timezone="UTC",
    enable_utc=True,
)
celery_app.autodiscover_tasks(["infrastructure"], related_name="process_tasks")
