"""
Standalone watchdog: scan MongoDB for stale/crashed Celery tasks and requeue or fail them.

Run with:
    python -m infrastructure.watchdog
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

from pymongo import MongoClient

WATCHDOG_INTERVAL: int = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "60"))
STALE_SECONDS: int = int(os.getenv("STALE_TASK_SECONDS", "300"))
MAX_ATTEMPTS: int = int(os.getenv("TASK_MAX_ATTEMPTS", "3"))
RETRY_DELAY: int = int(os.getenv("TASK_RETRY_DELAY_SECONDS", "60"))

_MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
_MONGODB_DB = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
_COLLECTION = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")

_ACTIVE_STATUSES = {"running", "acquiring_browser"}


def _scan_once(col) -> None:
    stale_cutoff = datetime.utcnow() - timedelta(seconds=STALE_SECONDS)
    stale_processes = list(
        col.find(
            {
                "status": {"$in": list(_ACTIVE_STATUSES)},
                "$or": [
                    {"metadata.heartbeat_at": {"$lt": stale_cutoff}},
                    {"metadata.heartbeat_at": None},
                ],
            },
            {"process_id": 1, "metadata.attempt_count": 1, "metadata.max_attempts": 1},
        )
    )

    if not stale_processes:
        return

    from infrastructure.process_tasks import run_process_task

    for doc in stale_processes:
        process_id: str = doc["process_id"]
        attempt_count: int = int((doc.get("metadata") or {}).get("attempt_count") or 0)
        max_attempts: int = int((doc.get("metadata") or {}).get("max_attempts") or MAX_ATTEMPTS)

        if attempt_count < max_attempts:
            col.update_one(
                {"process_id": process_id},
                {
                    "$set": {
                        "status": "queued",
                        "metadata.heartbeat_at": None,
                        "metadata.worker_id": None,
                        "metadata.celery_task_id": None,
                        "metadata.last_retry_reason": "stale_task_watchdog",
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            countdown = RETRY_DELAY * attempt_count
            run_process_task.apply_async(args=[process_id], countdown=countdown)
            print(f"[watchdog] requeued {process_id} (attempt {attempt_count}/{max_attempts}, countdown={countdown}s)")
        else:
            col.update_one(
                {"process_id": process_id},
                {
                    "$set": {
                        "status": "failed",
                        "completed_at": datetime.utcnow(),
                        "metadata.heartbeat_at": None,
                        "metadata.error_message": "Max attempts exhausted (watchdog)",
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            print(f"[watchdog] failed {process_id} (exhausted {attempt_count}/{max_attempts} attempts)")


def run() -> None:
    print(f"[watchdog] starting — interval={WATCHDOG_INTERVAL}s stale_after={STALE_SECONDS}s max_attempts={MAX_ATTEMPTS}")
    mongo_client = MongoClient(_MONGODB_URI)
    col = mongo_client[_MONGODB_DB][_COLLECTION]
    try:
        while True:
            try:
                _scan_once(col)
            except Exception as exc:
                print(f"[watchdog] scan error: {exc}")
            time.sleep(WATCHDOG_INTERVAL)
    finally:
        mongo_client.close()


if __name__ == "__main__":
    run()
