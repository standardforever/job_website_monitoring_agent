from __future__ import annotations

import asyncio
import os
import signal
import socket
from datetime import datetime

import pymongo
from celery import Task

from infrastructure.celery_app import celery_app
from infrastructure.heartbeat import HeartbeatThread

_MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:secret@mongo:27017")
_MONGODB_DB = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
_COLLECTION = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")
_DOMAIN_COLLECTION = os.getenv("MONGODB_DOMAIN_RUNS_COLLECTION", "domain_runs")

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "30"))
MAX_ATTEMPTS = int(os.getenv("TASK_MAX_ATTEMPTS", "3"))
RETRY_DELAY = int(os.getenv("TASK_RETRY_DELAY_SECONDS", "30"))


@celery_app.task(
    bind=True,
    name="infrastructure.process_tasks.run_process_task",
    max_retries=MAX_ATTEMPTS - 1,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_process_task(self: Task, process_id: str) -> None:
    worker_id = socket.gethostname()
    attempt = _increment_attempt(process_id, worker_id, self.request.id)
    heartbeat = HeartbeatThread(process_id, _MONGODB_URI, _MONGODB_DB, _COLLECTION, HEARTBEAT_INTERVAL)
    heartbeat.start()
    try:
        asyncio.run(_run_with_sigterm_handler(process_id))
    except (SystemExit, KeyboardInterrupt, asyncio.CancelledError):
        # Clean termination via SIGTERM revoke — finalize_if_stopped handles MongoDB
        pass
    except Exception as exc:
        error = str(exc) or type(exc).__name__
        heartbeat.stop()
        heartbeat.join(timeout=5)
        if attempt < MAX_ATTEMPTS:
            _reset_to_queued(process_id, error)
            raise self.retry(exc=exc, countdown=RETRY_DELAY * attempt)
        _record_final_failure(process_id, error)
    finally:
        heartbeat.stop()
        heartbeat.join(timeout=5)
        _finalize_if_stopped(process_id)
        _clear_runtime_metadata(process_id)


async def _run_with_sigterm_handler(process_id: str) -> None:
    """Run execute_process_task with a SIGTERM handler that cancels all coroutines immediately."""
    loop = asyncio.get_running_loop()

    def _cancel_all() -> None:
        for task in asyncio.all_tasks(loop):
            if not task.done():
                task.cancel()

    loop.add_signal_handler(signal.SIGTERM, _cancel_all)
    try:
        from tasks.process_tasks import execute_process_task
        await execute_process_task(process_id)
    finally:
        try:
            loop.remove_signal_handler(signal.SIGTERM)
        except Exception:
            pass


def _finalize_if_stopped(process_id: str) -> None:
    """If the process is still stop_requested after the task ends, mark it stopped."""
    try:
        with pymongo.MongoClient(_MONGODB_URI) as client:
            db = client[_MONGODB_DB]
            proc = db[_COLLECTION].find_one({"process_id": process_id}, {"status": 1})
            if not proc or str(proc.get("status") or "") != "stop_requested":
                return
            now = datetime.utcnow()
            db[_DOMAIN_COLLECTION].update_many(
                {"process_id": process_id, "status": {"$nin": ["completed", "failed", "stopped"]}},
                {"$set": {"status": "stopped", "updated_at": now}},
            )
            pipeline = [
                {"$match": {"process_id": process_id}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            ]
            counts = {doc["_id"]: doc["count"] for doc in db[_DOMAIN_COLLECTION].aggregate(pipeline)}
            total = sum(counts.values())
            summary = {
                "total_domain_count": total,
                "processed_domain_count": total,
                "completed_domain_count": counts.get("completed", 0),
                "failed_domain_count": counts.get("failed", 0),
                "stopped_domain_count": counts.get("stopped", 0),
            }
            db[_COLLECTION].update_one(
                {"process_id": process_id},
                {"$set": {"status": "stopped", "completed_at": now, "summary": summary}},
            )
    except Exception:
        pass


def _increment_attempt(process_id: str, worker_id: str, celery_task_id: str) -> int:
    try:
        with pymongo.MongoClient(_MONGODB_URI) as client:
            result = client[_MONGODB_DB][_COLLECTION].find_one_and_update(
                {"process_id": process_id},
                {
                    "$inc": {"metadata.attempt_count": 1},
                    "$set": {
                        "metadata.worker_id": worker_id,
                        "metadata.celery_task_id": celery_task_id,
                        "metadata.heartbeat_at": datetime.utcnow(),
                    },
                },
                return_document=pymongo.ReturnDocument.AFTER,
                projection={"metadata.attempt_count": 1},
            )
            return int(((result or {}).get("metadata") or {}).get("attempt_count") or 1)
    except Exception:
        return 1


def _reset_to_queued(process_id: str, error: str) -> None:
    try:
        with pymongo.MongoClient(_MONGODB_URI) as client:
            client[_MONGODB_DB][_COLLECTION].update_one(
                {"process_id": process_id},
                {"$set": {"status": "queued", "metadata.last_error": error, "updated_at": datetime.utcnow()}},
            )
    except Exception:
        pass


def _record_final_failure(process_id: str, error: str) -> None:
    try:
        with pymongo.MongoClient(_MONGODB_URI) as client:
            db = client[_MONGODB_DB]
            now = datetime.utcnow()
            db[_DOMAIN_COLLECTION].update_many(
                {"process_id": process_id, "status": {"$nin": ["completed", "failed", "stopped"]}},
                {"$set": {"status": "failed", "error": error, "updated_at": now}},
            )
            db[_COLLECTION].update_one(
                {"process_id": process_id},
                {"$set": {"status": "failed", "completed_at": now, "errors": [error]}},
            )
    except Exception:
        pass


def _clear_runtime_metadata(process_id: str) -> None:
    try:
        with pymongo.MongoClient(_MONGODB_URI) as client:
            client[_MONGODB_DB][_COLLECTION].update_one(
                {"process_id": process_id},
                {
                    "$unset": {
                        "metadata.worker_id": "",
                        "metadata.celery_task_id": "",
                        "metadata.heartbeat_at": "",
                    }
                },
            )
    except Exception:
        pass
