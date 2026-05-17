from __future__ import annotations

from fastapi import HTTPException
from kombu.exceptions import OperationalError
from utils.logging import get_logger, log_event

logger = get_logger("queue_dispatcher")


def enqueue_process_execution(process_id: str) -> str:
    try:
        from infrastructure.queue.process_tasks import execute_process

        task = execute_process.delay(process_id)
        log_event(
            logger,
            "info",
            "process_execution_enqueued process_id=%s celery_task_id=%s",
            process_id,
            task.id,
            domain=process_id,
            process_id=process_id,
            celery_task_id=task.id,
        )
        return str(task.id)


    except OperationalError:
        # Redis/broker connection problem
        raise HTTPException(
            status_code=503,
            detail="Queue service temporarily unavailable. Try again later.",
        )

    except Exception:
        # Unexpected enqueue problem
        raise HTTPException(
            status_code=500,
            detail="Failed to enqueue process execution.",
        )
    
    
    
    

    
