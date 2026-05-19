from __future__ import annotations

import asyncio
import math
from datetime import datetime
from typing import Any

from celery.signals import worker_ready
from redis import Redis

from core.config import get_settings
from infrastructure.queue.celery_app import celery_app
from infrastructure.queue.heartbeat import (
    build_process_heartbeat,
    current_worker_id,
    decrement_active_if_marked,
    heartbeat_key,
)
from infrastructure.queue.recovery import recover_and_requeue_interrupted_processes
from pipeline.exceptions import BrowserCapacityUnavailable, BrowserSessionLost
from services.grid_session import is_grid_session_active_async
from services.mongodb_service import MongoDBService
from tasks.process_tasks import process_task_service
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
        result = asyncio.run(_execute_process_with_allocated_browser(process_id))
    except BrowserCapacityUnavailable as exc:
        _retry_after_browser_wait(self, process_id, exc, "celery_process_waiting_for_browser")
    except BrowserSessionLost as exc:
        asyncio.run(_recover_after_lost_browser(process_id, str(exc)))
        _retry_after_browser_wait(self, process_id, exc, "celery_process_lost_browser_retry")
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


async def _execute_process_with_allocated_browser(process_id: str) -> dict[str, Any]:
    browser_runtime = None
    heartbeat = None
    worker_id = current_worker_id()
    process = await process_task_service.claim_process_for_execution(process_id, worker_id)
    if process is None:
        return await process_task_service.skipped_or_invalid_process(process_id)

    try:
        process, browser_runtime = await process_task_service.acquire_browser_for_process(process)
        heartbeat = build_process_heartbeat(process_id, worker_id)
        await heartbeat.start()
        log_event(
            logger,
            "info",
            "celery_browser_session_allocated process_id=%s session_id=%s",
            process_id,
            browser_runtime.session_id,
            domain=process_id,
            process_id=process_id,
            session_id=browser_runtime.session_id,
        )
        return await _execute_with_browser_monitor(process_id, process, browser_runtime)
    finally:
        if heartbeat is not None:
            await heartbeat.stop()
        await process_task_service.close_process_browser_session(browser_runtime)


def _browser_capacity_max_retries(settings: Any) -> int:
    explicit_retries = int(getattr(settings, "browser_session_max_retries", 0) or 0)
    if explicit_retries > 0:
        return explicit_retries

    max_wait_seconds = max(1.0, float(settings.browser_session_max_wait_hours) * 3600)
    attempt_seconds = (
        max(30, int(settings.browser_session_acquire_timeout_seconds))
        + max(1, int(settings.browser_session_retry_delay_seconds))
    )
    return max(1, int(math.ceil(max_wait_seconds / attempt_seconds)))


def _retry_after_browser_wait(self: Any, process_id: str, exc: Exception, event_name: str) -> None:
    settings = get_settings()
    retries = int(getattr(self.request, "retries", 0) or 0)
    delay = max(1, int(settings.browser_session_retry_delay_seconds))
    max_retries = _browser_capacity_max_retries(settings)
    if retries >= max_retries:
        error = (
            "Browser capacity unavailable after "
            f"{max_retries} retries over roughly {settings.browser_session_max_wait_hours:g} hours"
        )
        asyncio.run(_mark_process_failed(process_id, error, self.request.id))
        raise exc
    log_event(
        logger,
        "warning",
        "%s process_id=%s task_id=%s retry=%s/%s countdown=%s",
        event_name,
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
        max_wait_hours=settings.browser_session_max_wait_hours,
        countdown=delay,
    )
    raise self.retry(exc=exc, countdown=delay, max_retries=max_retries)


async def _execute_with_browser_monitor(process_id: str, process: dict[str, Any], browser_runtime: Any) -> dict[str, Any]:
    execution_task = asyncio.create_task(process_task_service.execute_process_with_browser(process, browser_runtime))
    monitor_task = asyncio.create_task(_monitor_browser_session(process_id, browser_runtime))
    done, pending = await asyncio.wait(
        {execution_task, monitor_task},
        return_when=asyncio.FIRST_EXCEPTION,
    )
    for task in done:
        exc = task.exception()
        if exc is not None:
            for pending_task in pending:
                pending_task.cancel()
            await _drain_cancelled_tasks(pending)
            raise exc
    if execution_task in done:
        monitor_task.cancel()
        await _drain_cancelled_tasks({monitor_task})
        return execution_task.result()
    execution_task.cancel()
    await _drain_cancelled_tasks({execution_task})
    raise BrowserSessionLost(f"Browser session disappeared for process {process_id}")


async def _monitor_browser_session(process_id: str, browser_runtime: Any) -> None:
    settings = get_settings()
    interval = max(5, int(settings.browser_session_monitor_interval_seconds))
    allowed_misses = max(1, int(settings.browser_session_monitor_misses))
    misses = 0
    while True:
        await asyncio.sleep(interval)
        active = await is_grid_session_active_async(browser_runtime.grid_url, browser_runtime.session_id)
        if active:
            misses = 0
            continue
        misses += 1
        log_event(
            logger,
            "warning",
            "browser_session_monitor_miss process_id=%s session_id=%s misses=%s/%s",
            process_id,
            browser_runtime.session_id,
            misses,
            allowed_misses,
            domain=process_id,
            process_id=process_id,
            session_id=browser_runtime.session_id,
            misses=misses,
            allowed_misses=allowed_misses,
        )
        if misses >= allowed_misses:
            raise BrowserSessionLost(f"Browser session {browser_runtime.session_id} is no longer active")


async def _drain_cancelled_tasks(tasks: set[asyncio.Task]) -> None:
    if not tasks:
        return
    await asyncio.gather(*tasks, return_exceptions=True)


async def _recover_after_lost_browser(process_id: str, error: str) -> None:
    mongodb_service = MongoDBService()
    await mongodb_service.recover_process_lost_browser(process_id, error)


@celery_app.task(name="processes.watchdog_dead_processes")
def watchdog_dead_processes() -> dict[str, Any]:
    configure_logging()
    return asyncio.run(_watchdog_dead_processes())


async def _watchdog_dead_processes() -> dict[str, Any]:
    settings = get_settings()
    if not settings.watchdog_enabled:
        return {"enabled": False, "checked": 0, "dead_workers_found": 0, "requeued": []}

    redis_client = Redis.from_url(settings.celery_broker_url, decode_responses=True)
    mongodb_service = MongoDBService()
    running_processes = await mongodb_service.list_running_processes()
    requeued: list[str] = []
    cleaned_active_markers: list[str] = []

    for process in running_processes:
        process_id = str(process.get("process_id") or "").strip()
        if not process_id:
            continue
        key = heartbeat_key(process_id)
        if redis_client.exists(key):
            continue

        recovered = await mongodb_service.recover_process_missing_heartbeat(process_id)
        if recovered is None or recovered.get("recovered_status") != "queued":
            continue
        decrement_active_if_marked(redis_client, process_id)
        from infrastructure.queue.dispatcher import enqueue_process_execution

        celery_task_id = enqueue_process_execution(process_id)
        requeued.append(process_id)
        log_event(
            logger,
            "warning",
            "watchdog_dead_process_requeued process_id=%s celery_task_id=%s",
            process_id,
            celery_task_id,
            domain=process_id,
            process_id=process_id,
            celery_task_id=celery_task_id,
        )

    for process_id in _active_marker_process_ids(redis_client):
        if process_id in requeued:
            continue
        process = await mongodb_service.get_process_upload(process_id)
        status = str((process or {}).get("status") or "").strip().lower()
        if status in {"running", "acquiring_browser", "recovering", "stop_requested"}:
            continue
        if decrement_active_if_marked(redis_client, process_id):
            cleaned_active_markers.append(process_id)

    log_event(
        logger,
        "info",
        "watchdog_dead_processes_completed checked=%s dead=%s cleaned_active_markers=%s",
        len(running_processes),
        len(requeued),
        len(cleaned_active_markers),
        domain="watchdog",
        checked=len(running_processes),
        dead_workers_found=len(requeued),
        cleaned_active_marker_count=len(cleaned_active_markers),
    )
    return {
        "enabled": True,
        "checked": len(running_processes),
        "dead_workers_found": len(requeued),
        "requeued": requeued,
        "cleaned_active_markers": cleaned_active_markers,
    }


def _active_marker_process_ids(redis_client: Redis) -> list[str]:
    settings = get_settings()
    prefix = f"{settings.redis_active_process_marker_prefix}:"
    return [
        str(key).removeprefix(prefix)
        for key in redis_client.scan_iter(f"{prefix}*")
    ]


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
    await mongodb_service.rebuild_assignment_progress(process_id)
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
