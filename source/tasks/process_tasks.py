from __future__ import annotations

from typing import Any

from services.job_process_service import JobProcessService
from utils.logging import get_logger, log_event

logger = get_logger("process_tasks")
process_task_service = JobProcessService()


async def execute_process_task(process_id: str) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "execute_process_task_started process_id=%s",
        process_id,
        domain=process_id,
        process_id=process_id,
    )
    result = await process_task_service.execute_process(process_id)
    log_event(
        logger,
        "info",
        "execute_process_task_completed process_id=%s status=%s",
        process_id,
        result.get("status"),
        domain=process_id,
        process_id=process_id,
        status=result.get("status"),
    )
    return result
