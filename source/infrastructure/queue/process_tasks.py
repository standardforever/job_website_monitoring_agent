from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from celery.signals import worker_ready

from core.config import get_settings
from infrastructure.queue.celery_app import celery_app
from infrastructure.queue.recovery import recover_and_requeue_interrupted_processes
from pipeline.exceptions import BrowserCapacityUnavailable
from services.mongodb_service import MongoDBService
from tasks.process_tasks import execute_process_task
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("celery_process_tasks")


@worker_ready.connect
def recover_interrupted_processes_on_worker_startup(**_: Any) -> None:
    configure_logging()
    asyncio.run(recover_and_requeue_interrupted_processes())


@celery_app.task(name="processes.execute", bind=True)
def execute_process(self, process_id: str) -> dict[str, Any]:
    configure_logging()
    log_event(
        logger,
        "info",
        "celery_process_task_started process_id=%s task_id=%s",
        process_id,
        self.request.id,
        domain=process_id,
        process_id=process_id,
        celery_task_id=self.request.id,
    )
    try:
        result = asyncio.run(execute_process_task(process_id))
    except BrowserCapacityUnavailable as exc:
        settings = get_settings()
        retries = int(getattr(self.request, "retries", 0) or 0)
        delay = max(1, int(settings.browser_session_retry_delay_seconds))
        max_retries = max(1, int(settings.browser_session_max_retries))
        if retries >= max_retries:
            error = f"Browser capacity unavailable after {max_retries} retries"
            awaitable = _mark_process_failed(process_id, error, self.request.id)
            asyncio.run(awaitable)
            raise
        log_event(
            logger,
            "warning",
            "celery_process_waiting_for_browser process_id=%s task_id=%s retry=%s/%s countdown=%s",
            process_id,
            self.request.id,
            retries + 1,
            max_retries,
            delay,
            domain=process_id,
            process_id=process_id,
            celery_task_id=self.request.id,
            retry_number=retries + 1,
            max_retries=max_retries,
            countdown=delay,
        )
        raise self.retry(exc=exc, countdown=delay, max_retries=max_retries)
    except Exception as exc:
        asyncio.run(_mark_process_failed(process_id, str(exc), self.request.id))
        raise

    log_event(
        logger,
        "info",
        "celery_process_task_completed process_id=%s task_id=%s status=%s",
        process_id,
        self.request.id,
        result.get("status"),
        domain=process_id,
        process_id=process_id,
        celery_task_id=self.request.id,
        status=result.get("status"),
    )
    return result


async def _mark_process_failed(process_id: str, error: str, task_id: str | None) -> None:
    mongodb_service = MongoDBService()
    process = await mongodb_service.get_process_upload(process_id)
    if str((process or {}).get("status") or "").strip().lower() in {"completed", "partial_completed", "stopped"}:
        return
    process_with_domains = await mongodb_service.get_process_with_domains(process_id)
    for item in list((process_with_domains or {}).get("items") or []):
        if str(item.get("status") or "").strip().lower() in {"completed", "failed", "stopped"}:
            continue
        await mongodb_service.mark_domain_failed(
            process_id,
            str(item.get("domain_key") or ""),
            item.get("career_page_url"),
            error,
            result_payload={"status": "failed", "error": error},
        )
    await mongodb_service.update_process_upload(
        process_id,
        {
            "status": "failed",
            "completed_at": datetime.utcnow(),
            "errors": [error],
            "metadata.last_celery_task_id": task_id,
            "metadata.last_celery_error": error,
        },
    )
    log_event(
        logger,
        "exception",
        "celery_process_task_failed process_id=%s task_id=%s error=%s",
        process_id,
        task_id,
        error,
        domain=process_id,
        process_id=process_id,
        celery_task_id=task_id,
        error=error,
    )
