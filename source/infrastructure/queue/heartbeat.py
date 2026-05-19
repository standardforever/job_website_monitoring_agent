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
    active_marker_key: str
    active_counter_key: str
    ttl_seconds: int
    interval_seconds: int
    _stop_event: asyncio.Event
    _task: asyncio.Task | None = None

    async def start(self) -> None:
        await asyncio.to_thread(self._mark_active)
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
        await asyncio.to_thread(self._mark_inactive)
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

    def _mark_active(self) -> None:
        self.redis_client.setex(self.heartbeat_key, self.ttl_seconds, self.worker_id)
        marker_created = self.redis_client.set(self.active_marker_key, self.worker_id, ex=self.ttl_seconds, nx=True)
        if marker_created:
            self.redis_client.incr(self.active_counter_key)

    def _mark_inactive(self) -> None:
        self.redis_client.delete(self.heartbeat_key)
        marker_deleted = self.redis_client.delete(self.active_marker_key)
        if marker_deleted:
            current = int(self.redis_client.get(self.active_counter_key) or 0)
            if current > 0:
                self.redis_client.decr(self.active_counter_key)
            else:
                self.redis_client.set(self.active_counter_key, 0)

    async def _refresh_until_stopped(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                await asyncio.to_thread(self._refresh)

    def _refresh(self) -> None:
        self.redis_client.setex(self.heartbeat_key, self.ttl_seconds, self.worker_id)
        self.redis_client.expire(self.active_marker_key, self.ttl_seconds)
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
        active_marker_key=active_marker_key(process_id),
        active_counter_key=settings.redis_active_process_counter_key,
        ttl_seconds=ttl_seconds,
        interval_seconds=interval_seconds,
        _stop_event=asyncio.Event(),
    )


def heartbeat_key(process_id: str) -> str:
    settings = get_settings()
    return f"{settings.redis_process_heartbeat_prefix}:{process_id}"


def active_marker_key(process_id: str) -> str:
    settings = get_settings()
    return f"{settings.redis_active_process_marker_prefix}:{process_id}"


def decrement_active_if_marked(redis_client: Redis, process_id: str) -> bool:
    settings = get_settings()
    marker_deleted = redis_client.delete(active_marker_key(process_id))
    if not marker_deleted:
        return False
    current = int(redis_client.get(settings.redis_active_process_counter_key) or 0)
    if current > 0:
        redis_client.decr(settings.redis_active_process_counter_key)
    else:
        redis_client.set(settings.redis_active_process_counter_key, 0)
    return True
