from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime

from celery import Task
from pymongo import MongoClient

from infrastructure.celery_app import celery_app
from infrastructure.heartbeat import HeartbeatThread

MAX_ATTEMPTS: int = int(os.getenv("TASK_MAX_ATTEMPTS", "3"))
RETRY_DELAY: int = int(os.getenv("TASK_RETRY_DELAY_SECONDS", "60"))
HEARTBEAT_INTERVAL: int = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "30"))

_MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
_MONGODB_DB = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
_COLLECTION = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")


def _mongo_col():
    client = MongoClient(_MONGODB_URI)
    return client, client[_MONGODB_DB][_COLLECTION]


@celery_app.task(
    bind=True,
    name="infrastructure.process_tasks.run_process_task",
    max_retries=MAX_ATTEMPTS - 1,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_process_task(self: Task, process_id: str) -> None:
    attempt = (self.request.retries or 0) + 1
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    celery_task_id = self.request.id

    client, col = _mongo_col()
    try:
        col.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "metadata.worker_id": worker_id,
                    "metadata.celery_task_id": celery_task_id,
                    "metadata.heartbeat_at": datetime.utcnow(),
                    "metadata.max_attempts": MAX_ATTEMPTS,
                    "updated_at": datetime.utcnow(),
                },
                "$inc": {"metadata.attempt_count": 1},
            },
        )
    finally:
        client.close()

    heartbeat = HeartbeatThread(
        process_id=process_id,
        mongodb_uri=_MONGODB_URI,
        mongodb_db=_MONGODB_DB,
        collection_name=_COLLECTION,
        interval=HEARTBEAT_INTERVAL,
    )
    heartbeat.start()

    try:
        from tasks.process_tasks import execute_process_task
        asyncio.run(execute_process_task(process_id))
    except Exception as exc:
        heartbeat.stop()
        heartbeat.join(timeout=5)

        if attempt < MAX_ATTEMPTS:
            _reset_to_queued(process_id, f"attempt {attempt} failed, retrying")
            raise self.retry(exc=exc, countdown=RETRY_DELAY * attempt)

        _record_final_failure(process_id, str(exc))
        return
    else:
        heartbeat.stop()
        heartbeat.join(timeout=5)
    finally:
        _clear_runtime_metadata(process_id)


def _reset_to_queued(process_id: str, reason: str) -> None:
    client, col = _mongo_col()
    try:
        col.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "status": "queued",
                    "metadata.heartbeat_at": None,
                    "metadata.worker_id": None,
                    "metadata.celery_task_id": None,
                    "metadata.last_retry_reason": reason,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
    finally:
        client.close()


def _record_final_failure(process_id: str, error: str) -> None:
    client, col = _mongo_col()
    try:
        col.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "metadata.error_message": error,
                    "metadata.heartbeat_at": None,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
    finally:
        client.close()


def _clear_runtime_metadata(process_id: str) -> None:
    client, col = _mongo_col()
    try:
        col.update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "metadata.heartbeat_at": None,
                    "metadata.worker_id": None,
                    "metadata.celery_task_id": None,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
    finally:
        client.close()
