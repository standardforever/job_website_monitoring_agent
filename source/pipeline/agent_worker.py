from __future__ import annotations

import asyncio
from typing import Any, Callable

from browser.session_manager import AgentSessionRecoveryNeeded, BrowserSessionManager, is_recoverable_agent_session_error
from core.config import get_settings
from models.process import DomainProcessRecord, WorkerProcessResult
from pipeline.domain_processor import DomainProcessor
from pipeline.process_summary import derive_completion_status
from services.flow_safety import extract_domain
from services.grid_session import BrowserSession
from services.mongodb_service import MongoDBService


class AgentWorker:
    def __init__(
        self,
        *,
        mongodb_service: MongoDBService,
        domain_processor: DomainProcessor,
        browser_manager: BrowserSessionManager,
        stop_requested: Callable[[str], bool],
    ) -> None:
        self._mongodb_service = mongodb_service
        self._domain_processor = domain_processor
        self._browser_manager = browser_manager
        self._stop_requested = stop_requested

    async def run(self, graph_input: dict[str, Any]) -> dict[str, Any]:
        assigned_domains = list(graph_input.get("assigned_domains") or [])
        process_id = str(graph_input["process_id"])
        agent_index = int(graph_input["agent_index"])
        agent_session: BrowserSession = graph_input["agent_session"]

        if assigned_domains:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")
        if await self._stop_requested_now(process_id):
            await self._stop_remaining(process_id, assigned_domains)
            return self._result(agent_index, "stopped", assigned_domains, [], [], {"stop_requested": True})

        domain_results, errors, processed = await self._process_domains(
            process_id=process_id,
            agent_index=agent_index,
            assigned_domains=assigned_domains,
            agent_session=agent_session,
        )
        stop_requested = await self._stop_requested_now(process_id)
        await self._finish_agent(process_id, agent_index, assigned_domains)
        return self._worker_result(agent_index, assigned_domains, processed, domain_results, errors, {}, stop_requested)

    async def _process_domains(
        self,
        *,
        process_id: str,
        agent_index: int,
        assigned_domains: list[dict[str, Any]],
        agent_session: BrowserSession,
    ) -> tuple[list[dict[str, Any]], list[str], list[str]]:
        domain_results: list[dict[str, Any]] = []
        errors: list[str] = []
        processed: list[str] = []
        processed_keys: set[tuple[Any, Any, Any]] = set()

        current_session = agent_session
        for domain in assigned_domains:
            if await self._stop_requested_now(process_id):
                await self._stop_remaining(
                    process_id,
                    [p for p in assigned_domains if self._domain_run_key(p) not in processed_keys],
                )
                break
            current_session, record = await self._process_one_domain(
                process_id, domain, agent_index, current_session
            )
            domain_results.append(record)
            processed.append(domain["domain_key"])
            processed_keys.add(self._domain_run_key(domain))
            if record["status"] != "completed" and record.get("error"):
                errors.append(str(record["error"]))

        return domain_results, errors, processed

    def _domain_run_key(self, domain: dict[str, Any]) -> tuple[Any, Any, Any]:
        return domain.get("input_index"), domain.get("domain_key"), domain.get("career_page_url")

    async def _process_one_domain(
        self,
        process_id: str,
        domain: dict[str, Any],
        agent_index: int,
        session: BrowserSession,
    ) -> tuple[BrowserSession, dict[str, Any]]:
        for attempt in range(3):
            try:
                record = await self._process_domain_with_timeout(process_id, domain, session, agent_index)
                return session, record
            except (AgentSessionRecoveryNeeded, Exception) as exc:
                error_text = str(exc) or type(exc).__name__

                if await self._stop_requested_now(process_id):
                    return session, await self._mark_recovery_failed(process_id, domain, error_text)

                if is_recoverable_agent_session_error(error_text) and attempt < 2:
                    replacement = await self._browser_manager.recreate_agent_tab(session)
                    if replacement is not None:
                        session = replacement
                    continue

                if attempt == 2:
                    return session, await self._mark_recovery_failed(process_id, domain, error_text)

        return session, await self._mark_recovery_failed(process_id, domain, "Agent tab recovery failed")

    async def _process_domain_with_timeout(
        self,
        process_id: str,
        domain: dict[str, Any],
        session: BrowserSession,
        agent_index: int,
    ) -> dict[str, Any]:
        timeout_seconds = max(60, int(get_settings().domain_process_timeout_seconds))
        return await asyncio.wait_for(
            self._domain_processor.process(
                process_id=process_id,
                domain=domain,
                browser_session=session,
                agent_index=agent_index,
                agent_tab={},
            ),
            timeout=timeout_seconds,
        )

    async def _mark_recovery_failed(self, process_id: str, domain: dict[str, Any], error_text: str) -> dict[str, Any]:
        message = str(error_text).strip() or "Unknown error"
        record = DomainProcessRecord(
            domain=domain["domain"],
            main_domain=extract_domain(domain["domain"]),
            status="failed",
            error=message,
        ).model_dump(mode="json")
        await self._mongodb_service.mark_domain_failed(
            process_id,
            domain["domain_key"],
            domain.get("career_page_url"),
            error_text,
            result_payload=record,
        )
        return record

    async def _finish_agent(self, process_id: str, agent_index: int, domains: list[dict[str, Any]]) -> None:
        if domains:
            status = "stopped" if await self._stop_requested_now(process_id) else "completed"
            await self._mongodb_service.update_assignment_status(process_id, agent_index, status)

    async def _stop_requested_now(self, process_id: str) -> bool:
        if self._stop_requested(process_id):
            return True
        process = await self._mongodb_service.get_process_upload(process_id)
        return str((process or {}).get("status") or "").strip().lower() == "stop_requested"

    async def _stop_remaining(self, process_id: str, domains: list[dict[str, Any]]) -> None:
        await self._mongodb_service.mark_domains_stopped(
            process_id,
            [{"domain_key": d.get("domain_key"), "career_page_url": d.get("career_page_url")} for d in domains],
        )

    def _worker_result(
        self,
        agent_index: int,
        assigned_domains: list[dict[str, Any]],
        processed: list[str],
        records: list[dict[str, Any]],
        errors: list[str],
        metadata: dict[str, Any],
        stop_requested: bool,
    ) -> dict[str, Any]:
        status = derive_completion_status(
            stop_requested=stop_requested,
            completed_count=sum(1 for r in records if r["status"] == "completed"),
            failed_count=sum(1 for r in records if r["status"] == "failed"),
            stopped_count=sum(1 for r in records if r["status"] == "stopped"),
            errors=errors,
        )
        return self._result(agent_index, status, assigned_domains, processed, records, metadata, errors)

    def _result(
        self,
        agent_index: int,
        status: str,
        assigned_domains: list[dict[str, Any]],
        processed: list[str],
        records: list[dict[str, Any]],
        metadata: dict[str, Any],
        errors: list[str] | None = None,
    ) -> dict[str, Any]:
        return WorkerProcessResult(
            agent_index=agent_index,
            status=status,
            assigned_domains=assigned_domains,
            processed_domains=processed,
            domain_results=[DomainProcessRecord(**r) for r in records],
            errors=errors or [],
            metadata=metadata,
        ).model_dump(mode="json")
