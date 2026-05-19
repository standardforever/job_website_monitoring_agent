from __future__ import annotations

import asyncio
import os
import socket
from dataclasses import dataclass

from redis import Redis

from core.config import get_settings
from utils.logging import get_logger, log_event

logger = get_logger("queue_heartbeat")


def current_worker_id() -> str:
    return os.getenv("WORKER_ID") or socket.gethostname()


@dataclass(slots=True)
class ProcessHeartbeat:
    process_id: str
    worker_id: str
    redis_client: Redis
    heartbeat_key: str
    ttl_seconds: int
    interval_seconds: int
    _stop_event: asyncio.Event
    _task: asyncio.Task | None = None

    async def start(self) -> None:
        await asyncio.to_thread(self._write_heartbeat)
        self._task = asyncio.create_task(self._refresh_until_stopped())
        log_event(
            logger,
            "info",
            "process_heartbeat_started process_id=%s worker_id=%s ttl_seconds=%s interval_seconds=%s",
            self.process_id,
            self.worker_id,
            self.ttl_seconds,
            self.interval_seconds,
            domain=self.process_id,
            process_id=self.process_id,
            worker_id=self.worker_id,
            ttl_seconds=self.ttl_seconds,
            interval_seconds=self.interval_seconds,
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()
        await asyncio.to_thread(self._delete_heartbeat)
        log_event(
            logger,
            "info",
            "process_heartbeat_stopped process_id=%s worker_id=%s",
            self.process_id,
            self.worker_id,
            domain=self.process_id,
            process_id=self.process_id,
            worker_id=self.worker_id,
        )

    def _write_heartbeat(self) -> None:
        self.redis_client.setex(self.heartbeat_key, self.ttl_seconds, self.worker_id)

    def _delete_heartbeat(self) -> None:
        self.redis_client.delete(self.heartbeat_key)

    async def _refresh_until_stopped(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                await asyncio.to_thread(self._refresh)

    def _refresh(self) -> None:
        self.redis_client.setex(self.heartbeat_key, self.ttl_seconds, self.worker_id)
        log_event(
            logger,
            "info",
            "process_heartbeat_refreshed process_id=%s worker_id=%s ttl_seconds=%s",
            self.process_id,
            self.worker_id,
            self.ttl_seconds,
            domain=self.process_id,
            process_id=self.process_id,
            worker_id=self.worker_id,
            ttl_seconds=self.ttl_seconds,
        )


def build_process_heartbeat(process_id: str, worker_id: str | None = None) -> ProcessHeartbeat:
    settings = get_settings()
    resolved_worker_id = worker_id or current_worker_id()
    ttl_seconds = max(60, int(settings.redis_process_heartbeat_ttl_seconds))
    interval_seconds = max(15, min(int(settings.redis_process_heartbeat_interval_seconds), ttl_seconds // 2))
    return ProcessHeartbeat(
        process_id=process_id,
        worker_id=resolved_worker_id,
        redis_client=Redis.from_url(settings.celery_broker_url, decode_responses=True),
        heartbeat_key=heartbeat_key(process_id),
        ttl_seconds=ttl_seconds,
        interval_seconds=interval_seconds,
        _stop_event=asyncio.Event(),
    )


def heartbeat_key(process_id: str) -> str:
    settings = get_settings()
    return f"{settings.redis_process_heartbeat_prefix}:{process_id}"
