from __future__ import annotations

from typing import Any, Callable

from browser.session_manager import AgentSessionRecoveryNeeded, BrowserSessionManager, SharedSessionRuntime
from models.process import DomainProcessRecord, WorkerProcessResult
from nodes.session_bootstrap import bootstrap_browser_node
from pipeline.domain_processor import DomainProcessor
from pipeline.process_summary import derive_completion_status
from services.flow_safety import extract_domain
from services.grid_session import close_agent_tab
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
        runtime: SharedSessionRuntime = graph_input["shared_runtime"]

        if assigned_domains:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")
        if await self._stop_requested_now(process_id):
            await self._stop_remaining(process_id, assigned_domains)
            return self._result(agent_index, "stopped", assigned_domains, [], [], {"stop_requested": True})

        try:
            browser_session, agent_tab, metadata = await self._bootstrap(graph_input, runtime, assigned_domains)
        except Exception as exc:
            return await self._bootstrap_failed(process_id, agent_index, assigned_domains, str(exc))

        try:
            domain_results, errors, processed = await self._process_domains(
                process_id=process_id,
                agent_index=agent_index,
                assigned_domains=assigned_domains,
                browser_session=browser_session,
                agent_tab=agent_tab,
                runtime=runtime,
            )
            stop_requested = await self._stop_requested_now(process_id)
            return self._worker_result(agent_index, assigned_domains, processed, domain_results, errors, metadata, stop_requested)
        finally:
            await self._finish_agent(process_id, agent_index, assigned_domains, browser_session)

    async def _bootstrap(
        self,
        graph_input: dict[str, Any],
        runtime: SharedSessionRuntime,
        assigned_domains: list[dict[str, Any]],
    ) -> tuple[Any, dict[str, Any], dict[str, Any]]:
        bootstrap_result = await bootstrap_browser_node(
            state={
                "process_id": graph_input["process_id"],
                "agent_index": graph_input["agent_index"],
                "assigned_urls": [domain["domain"] for domain in assigned_domains],
                "session_id": graph_input.get("session_id"),
                "cdp_url": graph_input.get("cdp_url"),
                "metadata": {},
            }
        )
        if bootstrap_result.get("session_established"):
            return bootstrap_result.get("browser_session"), bootstrap_result.get("agent_tab", {}), dict(bootstrap_result.get("metadata") or {})

        browser_session, agent_tab = await self._browser_manager.recover_agent_tab(
            runtime=runtime,
            browser_session=None,
            agent_index=int(graph_input["agent_index"]),
            url=assigned_domains[0]["domain"] if assigned_domains else str(graph_input["process_id"]),
        )
        return browser_session, agent_tab, {**dict(bootstrap_result.get("metadata") or {}), "bootstrap_status": "recovered"}

    async def _process_domains(
        self,
        *,
        process_id: str,
        agent_index: int,
        assigned_domains: list[dict[str, Any]],
        browser_session: Any,
        agent_tab: dict[str, Any],
        runtime: SharedSessionRuntime,
    ) -> tuple[list[dict[str, Any]], list[str], list[str]]:
        domain_results: list[dict[str, Any]] = []
        errors: list[str] = []
        processed: list[str] = []
        processed_keys: set[tuple[Any, Any, Any]] = set()
        for domain in assigned_domains:
            if await self._stop_requested_now(process_id):
                await self._stop_remaining(
                    process_id,
                    [pending for pending in assigned_domains if self._domain_run_key(pending) not in processed_keys],
                )
                break
            record, browser_session, agent_tab = await self._process_with_recovery(
                process_id, domain, browser_session, agent_index, agent_tab, runtime
            )
            domain_results.append(record)
            processed.append(domain["domain_key"])
            processed_keys.add(self._domain_run_key(domain))
            if record["status"] != "completed" and record.get("error"):
                errors.append(str(record["error"]))
        return domain_results, errors, processed

    def _domain_run_key(self, domain: dict[str, Any]) -> tuple[Any, Any, Any]:
        return domain.get("input_index"), domain.get("domain_key"), domain.get("career_page_url")

    async def _process_with_recovery(
        self,
        process_id: str,
        domain: dict[str, Any],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        runtime: SharedSessionRuntime,
    ) -> tuple[dict[str, Any], Any, dict[str, Any]]:
        for attempt in range(3):
            try:
                record = await self._domain_processor.process(
                    process_id=process_id,
                    domain=domain,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
                return record, browser_session, agent_tab
            except AgentSessionRecoveryNeeded as exc:
                if attempt == 2:
                    return await self._mark_recovery_failed(process_id, domain, str(exc)), browser_session, agent_tab
                browser_session, agent_tab = await self._browser_manager.recover_agent_tab(
                    runtime=runtime,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    url=domain["domain"],
                )
        return await self._mark_recovery_failed(process_id, domain, "Agent recovery failed"), browser_session, agent_tab

    async def _mark_recovery_failed(self, process_id: str, domain: dict[str, Any], error_text: str) -> dict[str, Any]:
        record = DomainProcessRecord(
            domain=domain["domain"],
            main_domain=extract_domain(domain["domain"]),
            status="failed",
            error=error_text,
        ).model_dump(mode="json")
        await self._mongodb_service.mark_domain_failed(
            process_id,
            domain["domain_key"],
            domain.get("career_page_url"),
            error_text,
            result_payload=record,
        )
        return record

    async def _bootstrap_failed(
        self,
        process_id: str,
        agent_index: int,
        assigned_domains: list[dict[str, Any]],
        error_text: str,
    ) -> dict[str, Any]:
        records = []
        for domain in assigned_domains:
            records.append(await self._mark_recovery_failed(process_id, domain, error_text or "Agent bootstrap failed"))
        await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
        return self._result(
            agent_index,
            "failed",
            assigned_domains,
            [],
            records,
            {"bootstrap_status": "failed"},
            [error_text],
        )

    async def _finish_agent(self, process_id: str, agent_index: int, domains: list[dict[str, Any]], browser_session: Any) -> None:
        if domains:
            status = "stopped" if await self._stop_requested_now(process_id) else "completed"
            await self._mongodb_service.update_assignment_status(process_id, agent_index, status)
        await close_agent_tab(browser_session)

    async def _stop_requested_now(self, process_id: str) -> bool:
        if self._stop_requested(process_id):
            return True
        process = await self._mongodb_service.get_process_upload(process_id)
        return str((process or {}).get("status") or "").strip().lower() == "stop_requested"

    async def _stop_remaining(self, process_id: str, domains: list[dict[str, Any]]) -> None:
        await self._mongodb_service.mark_domains_stopped(
            process_id,
            [{"domain_key": domain.get("domain_key"), "career_page_url": domain.get("career_page_url")} for domain in domains],
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
            completed_count=sum(1 for record in records if record["status"] == "completed"),
            failed_count=sum(1 for record in records if record["status"] == "failed"),
            stopped_count=sum(1 for record in records if record["status"] == "stopped"),
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
            domain_results=[DomainProcessRecord(**record) for record in records],
            errors=errors or [],
            metadata=metadata,
        ).model_dump(mode="json")
