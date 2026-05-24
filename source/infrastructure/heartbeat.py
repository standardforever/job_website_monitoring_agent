from __future__ import annotations

import threading
from datetime import datetime


class HeartbeatThread(threading.Thread):
    """
    Background thread that writes heartbeat_at to MongoDB every `interval` seconds
    while a Celery task is running. The watchdog uses this timestamp to detect
    frozen or crashed workers.
    """

    def __init__(
        self,
        process_id: str,
        mongodb_uri: str,
        mongodb_db: str,
        collection_name: str,
        interval: int = 30,
    ) -> None:
        super().__init__(daemon=True, name=f"heartbeat-{process_id[:8]}")
        self._process_id = process_id
        self._mongodb_uri = mongodb_uri
        self._mongodb_db = mongodb_db
        self._collection_name = collection_name
        self._interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        from pymongo import MongoClient

        client = MongoClient(self._mongodb_uri)
        col = client[self._mongodb_db][self._collection_name]
        try:
            while not self._stop_event.wait(self._interval):
                try:
                    now = datetime.utcnow()
                    col.update_one(
                        {"process_id": self._process_id},
                        {"$set": {"metadata.heartbeat_at": now, "updated_at": now}},
                    )
                except Exception:
                    pass
        finally:
            client.close()

    def stop(self) -> None:
        self._stop_event.set()
