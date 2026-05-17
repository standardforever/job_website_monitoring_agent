from __future__ import annotations

from celery import Celery

from core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "job_pipeline",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["infrastructure.queue.process_tasks"],
)

celery_app.conf.update(
    accept_content=["json"],
    result_serializer="json",
    task_acks_late=True,
    task_default_queue="processes",
    task_serializer="json",
    task_track_started=True,
    timezone="UTC",
    worker_prefetch_multiplier=1,
    broker_connection_timeout=10,
    broker_connection_retry_on_startup=True,
)
