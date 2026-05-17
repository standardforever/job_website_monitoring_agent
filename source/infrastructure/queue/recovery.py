from __future__ import annotations

from core.config import get_settings
from infrastructure.queue.dispatcher import enqueue_process_execution
from services.mongodb_service import MongoDBService
from utils.logging import get_logger, log_event

logger = get_logger("queue_recovery")


async def recover_and_requeue_interrupted_processes() -> list[dict]:
    settings = get_settings()
    if not settings.process_recovery_enabled:
        return []

    mongodb_service = MongoDBService()
    await mongodb_service.ensure_indexes()
    recovered = await mongodb_service.recover_interrupted_processes(
        stale_after_seconds=settings.process_recovery_stale_after_seconds,
    )
    stale_queued = await mongodb_service.find_stale_queued_processes(
        queued_after_seconds=settings.process_recovery_queued_after_seconds,
    )
    recovered = [*recovered, *stale_queued]
    for item in recovered:
        if item.get("recovered_status") != "queued":
            continue
        process_id = str(item.get("process_id") or "").strip()
        if not process_id:
            continue
        celery_task_id = enqueue_process_execution(process_id)
        item["celery_task_id"] = celery_task_id

    if recovered:
        log_event(
            logger,
            "warning",
            "queue_recovery_completed recovered_count=%s requeued_count=%s",
            len(recovered),
            sum(1 for item in recovered if item.get("recovered_status") == "queued"),
            domain="queue_recovery",
            recovered_count=len(recovered),
            requeued_count=sum(1 for item in recovered if item.get("recovered_status") == "queued"),
        )
    return recovered
