from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Callable

from browser.session_manager import BrowserSessionManager
from core.config import get_settings
from pipeline.agent_worker import AgentWorker
from pipeline.domain_processor import DomainProcessor
from pipeline.exceptions import BrowserCapacityUnavailable
from pipeline.process_summary import build_process_summary_from_domain_runs, derive_status_from_summary, failed_summary
from services.mongodb_service import MongoDBService
from services.openai_service import reset_openai_runtime_config, set_openai_runtime_config
from utils.logging import get_logger, log_event

logger = get_logger("process_executor")


class ProcessExecutor:
    def __init__(
        self,
        *,
        mongodb_service: MongoDBService,
        browser_manager: BrowserSessionManager,
        domain_processor: DomainProcessor,
        stop_requested: Callable[[str], bool],
    ) -> None:
        self._mongodb_service = mongodb_service
        self._browser_manager = browser_manager
        self._worker = AgentWorker(
            mongodb_service=mongodb_service,
            domain_processor=domain_processor,
            browser_manager=browser_manager,
            stop_requested=stop_requested,
        )
        self._stop_requested = stop_requested

    async def execute(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.begin_process_execution(process_id)
        if process is None:
            return await self.skipped_or_invalid_process(process_id)

        worker_id = _process_worker_id(process)
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(self._heartbeat_until_stopped(process_id, heartbeat_stop, worker_id))
        client = await self._mongodb_service.get_client(str(process.get("client_key") or ""))
        runtime_tokens = self._set_runtime_config(client, process)
        browser_runtime = None
        try:
            domains, assignments = self._domains_and_assignments(process)
            self._log_started(process_id, domains, assignments)
            grid_url = self._configured_grid_url()
            browser_runtime = await self._browser_manager.create_process_session(grid_url)
            if browser_runtime is None:
                retry_delay = get_settings().browser_session_retry_delay_seconds
                error = "Browser capacity is not available yet"
                await self._mongodb_service.release_process_for_browser_retry(process_id, error, retry_delay)
                log_event(
                    logger,
                    "warning",
                    "process_waiting_for_browser_capacity process_id=%s retry_delay_seconds=%s",
                    process_id,
                    retry_delay,
                    domain=process_id,
                    process_id=process_id,
                    retry_delay_seconds=retry_delay,
                )
                raise BrowserCapacityUnavailable(error)
            running_process = await self._mongodb_service.mark_process_running(process_id)
            if running_process is None:
                raise BrowserCapacityUnavailable("Process changed state before browser acquisition completed")
            process = {**process, **running_process}

            worker_results = await asyncio.gather(
                *[
                    self._run_assignment(process_id, assignment, browser_runtime)
                    for assignment in self._hydrate_assignments(process, assignments)
                ]
            )
            return await self._complete_process(process_id, domains, assignments, worker_results)
        finally:
            heartbeat_stop.set()
            await self._stop_heartbeat(heartbeat_task)
            reset_openai_runtime_config(runtime_tokens)
            await self._browser_manager.close_process_session(browser_runtime)

    async def execute_with_browser(self, process: dict[str, Any], browser_runtime: Any) -> dict[str, Any]:
        process_id = str(process["process_id"])
        worker_id = _process_worker_id(process)
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(self._heartbeat_until_stopped(process_id, heartbeat_stop, worker_id))
        client = await self._mongodb_service.get_client(str(process.get("client_key") or ""))
        runtime_tokens = self._set_runtime_config(client, process)
        try:
            domains, assignments = self._domains_and_assignments(process)
            self._log_started(process_id, domains, assignments)
            worker_results = await asyncio.gather(
                *[
                    self._run_assignment(process_id, assignment, browser_runtime)
                    for assignment in self._hydrate_assignments(process, assignments)
                ]
            )
            return await self._complete_process(process_id, domains, assignments, worker_results)
        finally:
            heartbeat_stop.set()
            await self._stop_heartbeat(heartbeat_task)
            reset_openai_runtime_config(runtime_tokens)

    async def skipped_or_invalid_process(self, process_id: str) -> dict[str, Any]:
        current_process = await self._mongodb_service.get_process_with_domains(process_id)
        if current_process is None:
            raise ValueError(f"Unknown process_id: {process_id}")
        status = str(current_process.get("status") or "").strip().lower()
        if status in {
            "acquiring_browser",
            "recovering",
            "running",
            "completed",
            "partial_completed",
            "failed",
            "stopped",
            "stop_requested",
        }:
            return {
                "process_id": process_id,
                "status": status,
                "errors": [],
                "worker_results": [],
                "summary": current_process.get("summary") or {},
                "skipped": True,
                "skip_reason": f"Process is already {status}.",
            }
        raise ValueError(f"Process {process_id} is not executable from status {status or 'unknown'}")

    async def _run_assignment(self, process_id: str, assignment: dict[str, Any], browser_runtime: Any) -> dict[str, Any]:
        try:
            return await self._worker.run(
                {
                    "process_id": process_id,
                    "agent_index": assignment["agent_index"],
                    "assigned_domains": assignment["domains"],
                    "session_id": browser_runtime.session_id,
                    "cdp_url": browser_runtime.cdp_url,
                    "shared_runtime": browser_runtime,
                }
            )
        except Exception as exc:
            error = str(exc)
            agent_index = int(assignment.get("agent_index", 0) or 0)
            unfinished_domains = await self._unfinished_assignment_domains(process_id, assignment)
            records = []
            for domain in unfinished_domains:
                record = await self._worker._mark_recovery_failed(process_id, domain, error)
                await self._mongodb_service.update_assignment_domain_progress(process_id, agent_index, domain, "failed")
                records.append(record)
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
            log_event(
                logger,
                "exception",
                "agent_assignment_failed process_id=%s agent_index=%s error=%s",
                process_id,
                agent_index,
                error,
                domain=process_id,
                process_id=process_id,
                agent_index=agent_index,
                error=error,
            )
            return self._worker._result(
                agent_index,
                "failed",
                list(assignment.get("domains") or []),
                [],
                records,
                {"assignment_error": error},
                [error],
            )

    async def _unfinished_assignment_domains(self, process_id: str, assignment: dict[str, Any]) -> list[dict[str, Any]]:
        process = await self._mongodb_service.get_process_with_domains(process_id)
        completed_statuses = {"completed", "failed", "stopped"}
        existing_statuses = {
            (item.get("input_index"), item.get("domain_key"), item.get("career_page_url")): str(item.get("status") or "").lower()
            for item in list((process or {}).get("items") or [])
        }
        return [
            domain
            for domain in list(assignment.get("domains") or [])
            if existing_statuses.get((domain.get("input_index"), domain.get("domain_key"), domain.get("career_page_url")))
            not in completed_statuses
        ]

    async def _heartbeat_until_stopped(self, process_id: str, stop_event: asyncio.Event, worker_id: str | None) -> None:
        interval = max(5, int(get_settings().process_heartbeat_interval_seconds))
        while not stop_event.is_set():
            try:
                await self._mongodb_service.heartbeat_process(process_id, "executing", worker_id)
            except Exception as exc:
                log_event(
                    logger,
                    "warning",
                    "process_heartbeat_failed process_id=%s error=%s",
                    process_id,
                    exc,
                    domain=process_id,
                    process_id=process_id,
                    error=str(exc),
                )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _stop_heartbeat(self, heartbeat_task: asyncio.Task) -> None:
        if heartbeat_task.done():
            return
        try:
            await asyncio.wait_for(heartbeat_task, timeout=5)
        except asyncio.TimeoutError:
            heartbeat_task.cancel()

    def _set_runtime_config(self, client: dict[str, Any] | None, process: dict[str, Any]):
        metadata = dict(process.get("metadata") or {})
        return set_openai_runtime_config(
            api_key=(client or {}).get("api_key"),
            model=(client or {}).get("model") or metadata.get("client_model") or "gpt-5-nano",
        )

    def _configured_grid_url(self) -> str | None:
        return str(get_settings().selenium_remote_url or "").strip() or None

    def _domains_and_assignments(self, process: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        domains = list(process.get("domains") or [])
        assignments = list(process.get("assignments") or [])
        return domains, assignments

    def _log_started(self, process_id: str, domains: list[dict[str, Any]], assignments: list[dict[str, Any]]) -> None:
        log_event(
            logger,
            "info",
            "execute_process_started process_id=%s domain_count=%s assignment_count=%s",
            process_id,
            len(domains),
            len(assignments),
            domain=domains[0]["domain_key"] if domains else process_id,
            process_id=process_id,
            domain_count=len(domains),
            assignment_count=len(assignments),
        )

    def _hydrate_assignments(self, process: dict[str, Any], assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        existing_runs = {
            (item.get("domain_key"), item.get("career_page_url")): item
            for item in list(process.get("items") or [])
        }
        return [
            {
                **assignment,
                "domains": [
                    {
                        **domain,
                        "previous_job_keys": self._previous_job_keys(existing_runs, domain),
                    }
                    for domain in list(assignment.get("domains") or [])
                ],
            }
            for assignment in assignments
        ]

    def _previous_job_keys(self, existing_runs: dict[tuple[Any, Any], dict[str, Any]], domain: dict[str, Any]) -> list[str]:
        existing = existing_runs.get((domain.get("domain_key"), domain.get("career_page_url"))) or {}
        return list(existing.get("previous_job_keys") or existing.get("current_job_keys") or [])

    async def _fail_startup(self, process_id: str, process: dict[str, Any], error: str) -> dict[str, Any]:
        domains = list(process.get("domains") or [])
        assignments = list(process.get("assignments") or [])
        for domain in domains:
            await self._mongodb_service.mark_domain_failed(
                process_id,
                domain["domain_key"],
                domain.get("career_page_url"),
                error,
                result_payload={"status": "failed", "error": error},
            )
        await self._mongodb_service.rebuild_assignment_progress(process_id)
        summary = failed_summary(len(domains), len(assignments))
        await self._mongodb_service.update_process_upload(
            process_id,
            {"status": "failed", "completed_at": datetime.utcnow(), "errors": [error], "summary": summary},
        )
        return {"process_id": process_id, "status": "failed", "errors": [error], "worker_results": [], "summary": summary}

    async def _complete_process(
        self,
        process_id: str,
        domains: list[dict[str, Any]],
        assignments: list[dict[str, Any]],
        worker_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        errors = [error for worker in worker_results for error in worker.get("errors", [])]
        refreshed_process = await self._mongodb_service.get_process_with_domains(process_id)
        domain_runs = list((refreshed_process or {}).get("items") or [])
        stop_requested = (
            self._stop_requested(process_id)
            or str((refreshed_process or {}).get("status") or "").strip().lower() == "stop_requested"
        )
        summary = build_process_summary_from_domain_runs(
            domains=domains,
            assignments=assignments,
            domain_runs=domain_runs,
        )
        status = derive_status_from_summary(summary, stop_requested=stop_requested, errors=errors)
        await self._mongodb_service.update_process_upload(
            process_id,
            {
                "status": status,
                "completed_at": datetime.utcnow(),
                "errors": errors,
                "summary": summary,
                "metadata.capacity_state": "finished",
            },
        )
        return {"process_id": process_id, "status": status, "errors": errors, "worker_results": worker_results, "summary": summary}


def _process_worker_id(process: dict[str, Any]) -> str | None:
    metadata = process.get("metadata") if isinstance(process.get("metadata"), dict) else {}
    worker_id = metadata.get("worker_id")
    return str(worker_id) if worker_id else None
