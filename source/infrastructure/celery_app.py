from __future__ import annotations

import os

from celery import Celery

celery_app = Celery("job_monitoring")

celery_app.conf.update(
    broker_url=os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0"),
    result_backend=os.getenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1"),
    result_expires=86400,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    # Safe settings for long-running browser jobs
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
    task_time_limit=86400,        # 24 h hard kill
    task_soft_time_limit=82800,   # 23 h soft warning
    broker_transport_options={"visibility_timeout": 86400},
    task_default_queue="processes",
    task_routes={"infrastructure.process_tasks.run_process_task": {"queue": "processes"}},
)

celery_app.autodiscover_tasks(["infrastructure"], related_name="process_tasks")
