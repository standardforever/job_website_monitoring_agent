from __future__ import annotations

import threading
from datetime import datetime

import pymongo


class HeartbeatThread(threading.Thread):
    def __init__(
        self,
        process_id: str,
        mongodb_uri: str,
        db_name: str,
        collection: str,
        interval: int = 30,
    ) -> None:
        super().__init__(daemon=True)
        self._process_id = process_id
        self._mongodb_uri = mongodb_uri
        self._db_name = db_name
        self._collection = collection
        self._interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        with pymongo.MongoClient(self._mongodb_uri) as client:
            col = client[self._db_name][self._collection]
            while not self._stop_event.wait(self._interval):
                try:
                    col.update_one(
                        {"process_id": self._process_id},
                        {"$set": {"metadata.heartbeat_at": datetime.utcnow()}},
                    )
                except Exception:
                    pass

    def stop(self) -> None:
        self._stop_event.set()
