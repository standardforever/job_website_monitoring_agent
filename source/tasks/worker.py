from __future__ import annotations

import asyncio
import os
from typing import Any

from core.config import get_settings
from services.job_process_service import JobProcessService
from services.mongodb_service import MongoDBService
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("process_worker")


class ProcessWorker:
    def __init__(
        self,
        *,
        worker_id: str | None = None,
        mongodb_service: MongoDBService | None = None,
        process_service: JobProcessService | None = None,
    ) -> None:
        self._settings = get_settings()
        self._worker_id = worker_id or os.getenv("WORKER_ID") or f"worker-{os.getpid()}"
        self._mongodb_service = mongodb_service or MongoDBService()
        self._process_service = process_service or JobProcessService(self._mongodb_service)
        self._stop = asyncio.Event()

    async def run_forever(self) -> None:
        configure_logging()
        await self._mongodb_service.ensure_indexes()
        log_event(
            logger,
            "info",
            "process_worker_started worker_id=%s poll_interval=%s",
            self._worker_id,
            self._settings.worker_poll_interval_seconds,
            domain="worker",
            worker_id=self._worker_id,
            poll_interval_seconds=self._settings.worker_poll_interval_seconds,
        )
        while not self._stop.is_set():
            claimed = await self._mongodb_service.claim_next_queued_process(self._worker_id)
            if not claimed:
                await asyncio.sleep(self._settings.worker_poll_interval_seconds)
                continue
            await self._execute_claimed_process(claimed)

    def stop(self) -> None:
        self._stop.set()

    async def _execute_claimed_process(self, process: dict[str, Any]) -> None:
        process_id = str(process.get("process_id") or "").strip()
        if not process_id:
            return
        try:
            log_event(
                logger,
                "info",
                "process_worker_execution_started worker_id=%s process_id=%s",
                self._worker_id,
                process_id,
                domain=process_id,
                worker_id=self._worker_id,
                process_id=process_id,
            )
            result = await self._process_service.execute_process(process_id)
            log_event(
                logger,
                "info",
                "process_worker_execution_completed worker_id=%s process_id=%s status=%s",
                self._worker_id,
                process_id,
                result.get("status"),
                domain=process_id,
                worker_id=self._worker_id,
                process_id=process_id,
                status=result.get("status"),
            )
        except Exception as exc:
            log_event(
                logger,
                "exception",
                "process_worker_execution_failed worker_id=%s process_id=%s error=%s",
                self._worker_id,
                process_id,
                str(exc),
                domain=process_id,
                worker_id=self._worker_id,
                process_id=process_id,
                error=str(exc),
            )
            await self._mongodb_service.update_process_upload(
                process_id,
                {"status": "failed", "errors": [str(exc)]},
            )


async def main() -> None:
    await ProcessWorker().run_forever()


if __name__ == "__main__":
    asyncio.run(main())
