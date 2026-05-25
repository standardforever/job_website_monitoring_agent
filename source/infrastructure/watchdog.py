from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta

import pymongo

_MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:secret@mongo:27017")
_MONGODB_DB = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
_COLLECTION = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")

STALE_SECONDS = int(os.getenv("STALE_TASK_SECONDS", "300"))
WATCHDOG_INTERVAL = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "60"))
MAX_ATTEMPTS = int(os.getenv("TASK_MAX_ATTEMPTS", "3"))
RETRY_DELAY = int(os.getenv("TASK_RETRY_DELAY_SECONDS", "30"))


class WatchdogService:
    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _run(self) -> None:
        while not self._stop_event.wait(WATCHDOG_INTERVAL):
            try:
                self._scan()
            except Exception:
                pass

    def _scan(self) -> None:
        stale_before = datetime.utcnow() - timedelta(seconds=STALE_SECONDS)
        with pymongo.MongoClient(_MONGODB_URI) as client:
            col = client[_MONGODB_DB][_COLLECTION]
            stale = list(col.find(
                {
                    "status": {"$in": ["running", "acquiring_browser"]},
                    "metadata.heartbeat_at": {"$lt": stale_before},
                },
                {"process_id": 1, "metadata.attempt_count": 1},
            ))
            for proc in stale:
                process_id = str(proc.get("process_id") or "")
                attempt = int(((proc.get("metadata") or {}).get("attempt_count") or 0))
                if attempt < MAX_ATTEMPTS:
                    col.update_one(
                        {"process_id": process_id},
                        {"$set": {"status": "queued", "updated_at": datetime.utcnow()}},
                    )
                    from infrastructure.process_tasks import run_process_task
                    run_process_task.apply_async(
                        args=[process_id],
                        countdown=RETRY_DELAY * max(1, attempt),
                    )
                else:
                    col.update_one(
                        {"process_id": process_id},
                        {
                            "$set": {
                                "status": "failed",
                                "completed_at": datetime.utcnow(),
                                "errors": ["Process failed after maximum retry attempts (stale heartbeat)"],
                            }
                        },
                    )


if __name__ == "__main__":
    service = WatchdogService()
    service.start()
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        service.stop()
