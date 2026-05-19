from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from browser.session_manager import BrowserSessionManager
from core.config import get_settings
from models.process import JobProcessRequest
from pipeline.domain_processor import DomainProcessor
from pipeline.exceptions import BrowserCapacityUnavailable
from pipeline.process_executor import ProcessExecutor
from pipeline.process_summary import empty_summary
from services.client_service import ClientService
from services.email_service import EmailService
from services.file_input_service import UploadDomainInput
from services.flow_safety import extract_domain
from services.mongodb_service import MongoDBService
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("job_process_service")


class JobProcessService:
    def __init__(self, mongodb_service: MongoDBService | None = None) -> None:
        self._mongodb_service = mongodb_service or MongoDBService()
        self._settings = get_settings()
        self._email_service = EmailService()
        self._client_service = ClientService(self._mongodb_service)
        self._browser_manager = BrowserSessionManager()
        self._domain_processor = DomainProcessor(self._mongodb_service)
        self._executor = ProcessExecutor(
            mongodb_service=self._mongodb_service,
            browser_manager=self._browser_manager,
            domain_processor=self._domain_processor,
            stop_requested=self._is_stop_requested,
        )
        self._stop_requests: set[str] = set()
        log_event(logger, "info", "job_process_service_initialized", domain="service")

    async def submit_process(
        self,
        request: JobProcessRequest,
        *,
        domain_inputs: list[UploadDomainInput] | None = None,
        upload_filename: str | None = None,
    ) -> dict[str, Any]:
        inputs = domain_inputs or [UploadDomainInput(domain=url) for url in request.urls]
        if not inputs:
            raise ValueError("At least one domain is required")

        client = await self._client_service.require_client(request.client_name)
        process_id = request.task_id or str(uuid4())
        domains = build_domain_documents(inputs)
        assignments = allocate_domains_to_agents(domains, max(1, int(request.agent_count or 1)))
        process_document = build_process_document(
            process_id=process_id,
            client=client,
            domains=domains,
            assignments=assignments,
            upload_filename=upload_filename,
        )
        await self._mongodb_service.insert_process_upload(process_document)
        await self._mongodb_service.upsert_domain_runs(
            build_domain_run_documents(process_id=process_id, client=client, domains=domains)
        )
        log_event(
            logger,
            "info",
            "process_submission_completed process_id=%s domain_count=%s",
            process_id,
            len(domains),
            domain=domains[0]["domain_key"],
            process_id=process_id,
            domain_count=len(domains),
        )
        return process_document

    async def run_process(self, request: JobProcessRequest, process_id: str | None = None) -> dict[str, Any]:
        configure_logging()
        submitted = await self.submit_process(request.model_copy(update={"task_id": process_id or request.task_id}))
        return await self.execute_process(submitted["process_id"])

    async def execute_process(self, process_id: str) -> dict[str, Any]:
        result = await self._executor.execute(process_id)
        self._stop_requests.discard(process_id)
        await self._send_process_completion_email(process_id, result["status"])
        return result

    async def claim_process_for_execution(self, process_id: str, worker_id: str | None = None) -> dict[str, Any] | None:
        return await self._mongodb_service.begin_process_execution(process_id, worker_id)

    async def skipped_or_invalid_process(self, process_id: str) -> dict[str, Any]:
        return await self._executor.skipped_or_invalid_process(process_id)

    async def acquire_browser_for_process(self, process: dict[str, Any]) -> tuple[dict[str, Any], Any]:
        process_id = str(process["process_id"])
        grid_url = str(self._settings.selenium_remote_url or "").strip() or None
        browser_runtime = await self._browser_manager.create_process_session(grid_url)
        if browser_runtime is None:
            retry_delay = self._settings.browser_session_retry_delay_seconds
            error = "Browser capacity is not available yet"
            await self._mongodb_service.release_process_for_browser_retry(process_id, error, retry_delay)
            raise BrowserCapacityUnavailable(error)

        running_process = await self._mongodb_service.mark_process_running(process_id)
        if running_process is None:
            await self._browser_manager.close_process_session(browser_runtime)
            raise BrowserCapacityUnavailable("Process changed state before browser acquisition completed")
        return running_process, browser_runtime

    async def execute_process_with_browser(self, process: dict[str, Any], browser_runtime: Any) -> dict[str, Any]:
        result = await self._executor.execute_with_browser(process, browser_runtime)
        process_id = str(process["process_id"])
        self._stop_requests.discard(process_id)
        await self._send_process_completion_email(process_id, result["status"])
        return result

    async def close_process_browser_session(self, browser_runtime: Any) -> None:
        await self._browser_manager.close_process_session(browser_runtime)

    async def submit_rerun_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_with_domains(process_id)
        if process is None:
            raise ValueError("Process not found")
        status = str(process.get("status") or "").strip().lower()
        if status in {"queued", "acquiring_browser", "recovering", "running", "stop_requested"}:
            raise ValueError(f"Process {process_id} is currently {status} and cannot be rerun yet.")
        rerun_process = await self._mongodb_service.reset_process_for_rerun(process_id)
        if rerun_process is None:
            raise ValueError("Process not found")
        return rerun_process

    async def get_process(self, process_id: str) -> dict[str, Any] | None:
        return await self._mongodb_service.get_process_with_domains(process_id)

    async def list_processes(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        processes, total = await self._mongodb_service.list_process_uploads(page=page, page_size=page_size)
        return paginated_processes(processes, total, page, page_size)

    async def list_processes_for_client(self, client_name: str, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        client = await self._client_service.require_client(client_name)
        processes, total = await self._mongodb_service.list_process_uploads(
            client_key=client["client_key"],
            page=page,
            page_size=page_size,
        )
        return {
            "client_key": client["client_key"],
            "client_name": client["client_name"],
            **paginated_processes(processes, total, page, page_size),
        }

    async def stop_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_upload(process_id)
        if process is None:
            raise ValueError("Process not found")
        status = str(process.get("status") or "")
        if status in {"completed", "partial_completed", "failed", "stopped"}:
            return {"process_id": process_id, "status": status, "message": f"Process is already {status}."}
        self._stop_requests.add(process_id)
        updated = await self._mongodb_service.mark_process_stop_requested(process_id)
        process_with_domains = await self._mongodb_service.get_process_with_domains(process_id)
        if status in {"queued", "acquiring_browser", "recovering"} and not (process_with_domains or {}).get("started_at"):
            items = list((process_with_domains or {}).get("items") or [])
            await self._mongodb_service.mark_domains_stopped(
                process_id,
                [{"domain_key": item.get("domain_key"), "career_page_url": item.get("career_page_url")} for item in items],
            )
            summary = empty_summary(len(items), len((process_with_domains or {}).get("assignments") or []))
            summary["processed_domain_count"] = len(items)
            summary["stopped_domain_count"] = len(items)
            await self._mongodb_service.update_process_upload(
                process_id,
                {"status": "stopped", "completed_at": datetime.utcnow(), "summary": summary},
            )
            return {"process_id": process_id, "status": "stopped", "message": "Queued process stopped before execution."}
        return {
            "process_id": process_id,
            "status": str((updated or process).get("status") or "stop_requested"),
            "message": "Stop requested. Running work will stop after the current domain finishes.",
        }

    async def register_client(self, client_name: str, email: str | None, api_key: str, model: str, grid_url: str | None) -> dict[str, Any]:
        return await self._client_service.register(
            client_name=client_name,
            email=email,
            api_key=api_key,
            model=model,
            grid_url=grid_url,
        )

    async def update_client(self, current_client_name: str, **updates: Any) -> dict[str, Any]:
        return await self._client_service.update(current_client_name, **updates)

    async def get_client_configuration(self, client_name: str) -> dict[str, Any]:
        return await self._client_service.get_configuration(client_name)

    async def list_clients(self) -> dict[str, Any]:
        return await self._client_service.list_clients()

    async def _send_process_completion_email(self, process_id: str, status: str) -> None:
        if status not in {"completed", "partial_completed"}:
            return
        process = await self._mongodb_service.get_process_with_domains(process_id)
        if process is None:
            return
        client = await self._client_for_email(process)
        result = await self._email_service.send_process_completed_email(client=client, process=process)
        await self._mongodb_service.update_process_upload(
            process_id,
            {
                "metadata.last_completion_email": {
                    "status": result.get("status"),
                    "reason": result.get("reason"),
                    "error": result.get("error"),
                    "sent_at": datetime.utcnow() if result.get("status") == "sent" else None,
                }
            },
        )

    async def _client_for_email(self, process: dict[str, Any]) -> dict[str, Any]:
        client = await self._mongodb_service.get_client(str(process.get("client_key") or ""))
        return client or {
            "client_key": process.get("client_key"),
            "client_name": process.get("client_name") or self._settings.default_client_name,
            "email": self._settings.default_client_email,
        }

    def _is_stop_requested(self, process_id: str) -> bool:
        return process_id in self._stop_requests


def build_domain_documents(inputs: list[UploadDomainInput]) -> list[dict[str, Any]]:
    return [
        {
            "input_index": index,
            "domain": item.domain,
            "domain_key": normalize_domain_key(item.domain),
            "career_page_url": item.career_page_url,
        }
        for index, item in enumerate(inputs)
    ]


def build_process_document(
    *,
    process_id: str,
    client: dict[str, Any],
    domains: list[dict[str, Any]],
    assignments: list[dict[str, Any]],
    upload_filename: str | None,
) -> dict[str, Any]:
    now = datetime.utcnow()
    return {
        "process_id": process_id,
        "status": "queued",
        "domains": domains,
        "assignments": assignments,
        "errors": [],
        "summary": empty_summary(len(domains), len(assignments)),
        "metadata": {
            "workflow": "career_page_pattern_extraction",
            "upload_filename": upload_filename,
            "supplied_career_page_count": sum(1 for domain in domains if domain.get("career_page_url")),
            "client_model": client.get("model") or "gpt-5-nano",
        },
        "history": [],
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
        "client_name": client["client_name"],
        "client_key": client["client_key"],
    }


def build_domain_run_documents(process_id: str, client: dict[str, Any], domains: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now = datetime.utcnow()
    return [
        {
            "process_id": process_id,
            "input_index": domain["input_index"],
            "domain": domain["domain"],
            "raw_url": domain["domain"],
            "domain_key": domain["domain_key"],
            "career_page_url": domain["career_page_url"],
            "client_key": client["client_key"],
            "client_name": client["client_name"],
            "status": "queued",
            "error": None,
            "agent_index": None,
            "result_summary": {},
            "result_payload": {},
            "current_job_keys": [],
            "previous_job_keys": [],
            "added_job_keys": [],
            "removed_job_keys": [],
            "unchanged_job_keys": [],
            "history": [],
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "completed_at": None,
        }
        for domain in domains
    ]


def allocate_domains_to_agents(domains: list[dict[str, Any]], agent_count: int) -> list[dict[str, Any]]:
    normalized_agent_count = max(1, min(int(agent_count or 1), len(domains) or 1))
    assignments = [
        {
            "agent_index": index,
            "domains": [],
            "processed_domain": [],
            "pending_domain": [],
            "failed_domain": [],
            "status": "queued",
        }
        for index in range(normalized_agent_count)
    ]
    for index, domain in enumerate(domains):
        assignment = assignments[index % normalized_agent_count]
        assignment["domains"].append(domain)
        assignment["pending_domain"].append(_assignment_domain_ref(domain))
    return [
        assignment
        for assignment in assignments
        if assignment["domains"]
    ]


def _assignment_domain_ref(domain: dict[str, Any]) -> dict[str, Any]:
    return {
        "input_index": domain.get("input_index"),
        "domain": domain.get("domain"),
        "domain_key": domain.get("domain_key"),
        "career_page_url": domain.get("career_page_url"),
    }


def paginated_processes(processes: list[dict[str, Any]], total: int, page: int, page_size: int) -> dict[str, Any]:
    return {
        "count": len(processes),
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": page * page_size < total,
        "has_previous": page > 1,
        "processes": processes,
    }


def normalize_domain_key(url: str) -> str:
    return extract_domain(url) or str(url or "").strip().lower()
