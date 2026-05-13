from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from models.process import (
    ClientDocument,
    DomainProcessRecord,
    JobProcessRequest,
    ProcessRunDocument,
    ProcessRunItemDocument,
    RequestedCapability,
    WorkerProcessResult,
)
from nodes.ats_check_node import detect_ats
from nodes.career_page_category import (
    _build_career_page_overview,
    _collect_job_listing_patterns,
    career_page_category_node,
)
from nodes.session_bootstrap import bootstrap_browser_node
from nodes.url_extraction import career_url_extraction_node
from services.agent_allocator import allocate_urls_to_agents
from services.flow_safety import extract_domain
from services.grid_session import (
    attach_playwright_to_cdp,
    close_agent_tab,
    close_browser_attachment,
    close_shared_session_async,
    create_session_async,
    is_grid_session_active_async,
)
from services.job_extraction_service import JobExtractionService
from services.job_pattern.job_main import main as generate_job_listing_pattern
from services.job_pattern.utils.extraction import extract_jobs_with_diagnostics, validate_jobs
from services.job_pattern.utils.html_extraction import extract_clean_html
from services.content_extraction import extract_page_content
from services.email_service import EmailService
from services.navigation import navigate_to_url
from services.openai_service import (
    mask_api_key,
    reset_openai_runtime_config,
    set_openai_runtime_config,
    validate_openai_api_key,
)
from services.tab_manager import ensure_agent_tab
from services.mongodb_service import MongoDBService
from core.config import get_settings
from utils.logging import configure_logging, get_logger, log_event

logger = get_logger("job_process_service")
CAREER_DISCOVERY_REFRESH_DAYS = 7


@dataclass(slots=True)
class SharedSessionRuntime:
    grid_url: str | None
    session_id: str
    cdp_url: str
    recovery_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AgentSessionRecoveryNeeded(Exception):
    pass


class JobProcessService:
    def __init__(self, mongodb_service: MongoDBService | None = None) -> None:
        self._mongodb_service = mongodb_service or MongoDBService()
        self._job_extraction_service = JobExtractionService(self._mongodb_service)
        self._email_service = EmailService()
        self._settings = get_settings()
        self._stop_requests: set[str] = set()
        log_event(logger, "info", "job_process_service_initialized", domain="service")

    async def submit_process(self, request: JobProcessRequest) -> dict[str, Any]:
        client = await self._require_active_client(request.client_name)
        resolved_grid_url = client.get("grid_url") or self._settings.selenium_remote_url
        request = request.model_copy(update={"client_name": client["client_name"]})
        assignments = allocate_urls_to_agents(request.urls, request.agent_count)
        process_id = request.task_id or str(uuid4())
        requested_capability = self._requested_capability_for_request(request)
        now = datetime.utcnow()

        normalized_urls = [self._normalize_domain_key(url) for url in request.urls]

        run_document = ProcessRunDocument(
            process_id=process_id,
            client_key=client["client_key"],
            client_name=client["client_name"],
            status="queued",
            request=request,
            assignments=assignments,
            queued_urls=list(request.urls),
            metadata={
                "client_model": client.get("model") or "gpt-5-nano",
                "client_grid_url": resolved_grid_url,
                "ats_check": request.ats_check,
                "job_extract": request.job_extract,
                "job_monitoring": request.job_monitoring,
                "requested_capability": requested_capability,
            },
            summary={
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": 0,
                "completed_domain_count": 0,
                "failed_domain_count": 0,
                "queued_url_count": len(request.urls),
                "running_url_count": 0,
                "stopped_url_count": 0,
            },
            created_at=now,
            updated_at=now,
        )

        run_items = [
            ProcessRunItemDocument(
                process_id=process_id,
                client_key=client["client_key"],
                client_name=client["client_name"],
                raw_url=url,
                domain_key=self._normalize_domain_key(url),
                requested_capability=requested_capability,
                status="queued",
                created_at=now,
                updated_at=now,
            ).model_dump(mode="json")
            for url in request.urls
        ]

        await self._mongodb_service.insert_process_run(run_document.model_dump(mode="json"))
        await self._mongodb_service.insert_process_run_items(run_items)

        log_event(
            logger,
            "info",
            "process_submission_completed process_id=%s client_key=%s url_count=%s",
            process_id,
            client["client_key"],
            len(request.urls),
            domain=normalized_urls[0] if normalized_urls else client["client_key"],
            process_id=process_id,
            client_key=client["client_key"],
            url_count=len(request.urls),
        )
        return run_document.model_dump(mode="json")

    async def run_process(self, request: JobProcessRequest, process_id: str | None = None) -> dict[str, Any]:
        configure_logging()
        submitted_process = await self.submit_process(
            request.model_copy(update={"task_id": process_id or request.task_id})
        )
        return await self.execute_existing_process(submitted_process["process_id"])

    async def execute_existing_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(process_id)
        if process is None:
            raise ValueError(f"Unknown process_id: {process_id}")

        domain = (((process.get("request") or {}).get("urls") or ["unknown"])[0])
        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])
        client = await self._require_active_client_by_key(process["client_key"])

        await self._mongodb_service.update_process_run(
            process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

        log_event(
            logger,
            "info",
            "execute_existing_process_started process_id=%s client_key=%s",
            process_id,
            process.get("client_key"),
            domain=domain,
            process_id=process_id,
            client_key=process.get("client_key"),
        )

        runtime_tokens = set_openai_runtime_config(
            api_key=client.get("api_key"),
            model=client.get("model") or "gpt-5-nano",
        )
        try:
            result = await self._execute_process(
                process_id=process_id,
                request=request,
                assignments=assignments,
                client_key=process["client_key"],
                client_name=process["client_name"],
                grid_url=str((process.get("metadata") or {}).get("client_grid_url") or client.get("grid_url") or self._settings.selenium_remote_url),
            )
        except Exception as exc:
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [str(exc)],
                },
            )
            raise
        finally:
            self._stop_requests.discard(process_id)
            reset_openai_runtime_config(runtime_tokens)

        await self._mongodb_service.update_process_run(
            process_id,
            {
                "status": result["status"],
                "completed_at": datetime.utcnow(),
                "errors": result["errors"],
                "summary": result["summary"],
            },
        )
        await self._send_process_completion_email(process_id, process["client_key"], result["status"])
        return result

    async def submit_rerun_process(self, original_process_id: str) -> dict[str, Any]:
        original_process = await self._mongodb_service.get_process_run_with_items(original_process_id)
        if original_process is None:
            raise ValueError("Original process not found")
        original_status = str(original_process.get("status") or "").strip().lower()
        if original_status in {"running", "queued", "stop_requested"}:
            raise ValueError(
                f"Process {original_process_id} is currently {original_status} and cannot be rerun yet."
            )
        active_item = next(
            (
                item
                for item in list(original_process.get("items") or [])
                if str(item.get("status") or "").strip().lower() in {"queued", "running", "stop_requested"}
            ),
            None,
        )
        if active_item:
            raise ValueError(
                f"Process {original_process_id} still has an active item ({active_item.get('raw_url')}) and cannot be rerun yet."
            )

        await self._mongodb_service.reset_process_for_rerun(original_process_id)
        rerun_process = await self._mongodb_service.get_process_run(original_process_id)
        if rerun_process is None:
            raise ValueError("Original process not found")
        return rerun_process

    async def execute_rerun_process(self, rerun_process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(rerun_process_id)
        if process is None:
            raise ValueError(f"Unknown process_id: {rerun_process_id}")

        original_process_id = str((process.get("metadata") or {}).get("rerun_of_process_id") or rerun_process_id).strip()

        original_process = await self._mongodb_service.get_process_run_with_items(original_process_id)
        if original_process is None:
            raise ValueError("Original process not found")

        request = JobProcessRequest(**process["request"])
        assignments = process.get("assignments", [])
        client = await self._require_active_client_by_key(process["client_key"])

        await self._mongodb_service.update_process_run(
            rerun_process_id,
            {
                "status": "running",
                "started_at": datetime.utcnow(),
                "errors": [],
            },
        )

        runtime_tokens = set_openai_runtime_config(
            api_key=client.get("api_key"),
            model=client.get("model") or "gpt-5-nano",
        )
        try:
            result = await self._execute_rerun_process(
                process_id=rerun_process_id,
                request=request,
                assignments=assignments,
                client_key=process["client_key"],
                client_name=process["client_name"],
                grid_url=str((process.get("metadata") or {}).get("client_grid_url") or client.get("grid_url") or self._settings.selenium_remote_url),
                original_process=original_process,
            )
        except Exception as exc:
            await self._mongodb_service.update_process_run(
                rerun_process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [str(exc)],
                },
            )
            raise
        finally:
            self._stop_requests.discard(rerun_process_id)
            reset_openai_runtime_config(runtime_tokens)

        await self._mongodb_service.update_process_run(
            rerun_process_id,
            {
                "status": result["status"],
                "completed_at": datetime.utcnow(),
                "errors": result["errors"],
                "summary": result["summary"],
            },
        )
        await self._send_process_completion_email(rerun_process_id, process["client_key"], result["status"])
        return result

    async def get_process(self, process_id: str) -> dict[str, Any] | None:
        return await self._mongodb_service.get_process_run_with_items(process_id)

    async def register_client(
        self,
        client_name: str,
        email: str | None,
        api_key: str,
        model: str = "gpt-5-nano",
        grid_url: str | None = None,
    ) -> dict[str, Any]:
        client_key = self._build_client_key(client_name)
        validation = await validate_openai_api_key(api_key=api_key, model=model)
        if not validation.active:
            raise ValueError(validation.user_message or "The OpenAI API key could not be validated.")

        client = await self._mongodb_service.upsert_client_configuration(
            client_key=client_key,
            client_name=client_name,
            email=self._normalize_email(email),
            api_key=api_key,
            model=model,
            grid_url=grid_url or self._settings.selenium_remote_url,
            api_key_status="active",
            api_key_validation_error=None,
        )
        return self._sanitize_client_document(client)

    async def update_client(
        self,
        current_client_name: str,
        *,
        new_client_name: str | None = None,
        email: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        grid_url: str | None = None,
    ) -> dict[str, Any]:
        current_client = await self._require_client(current_client_name)
        final_client_name = new_client_name or current_client["client_name"]
        final_email = self._normalize_email(email) if email is not None else current_client.get("email")
        final_model = model or current_client.get("model") or "gpt-5-nano"
        final_api_key = api_key or current_client.get("api_key")
        final_grid_url = grid_url if grid_url is not None else current_client.get("grid_url") or self._settings.selenium_remote_url
        if not final_api_key:
            raise ValueError("Client does not have an API key configured")

        validation = await validate_openai_api_key(api_key=final_api_key, model=final_model)
        if not validation.active:
            raise ValueError(validation.user_message or "The OpenAI API key could not be validated.")

        updated = await self._mongodb_service.update_client_configuration(
            current_client_key=current_client["client_key"],
            new_client_key=self._build_client_key(final_client_name),
            client_name=final_client_name,
            email=final_email,
            api_key=final_api_key,
            model=final_model,
            grid_url=final_grid_url,
            api_key_status="active",
            api_key_validation_error=None,
        )
        if updated is None:
            raise ValueError(f"Unknown client: {current_client_name}")
        return self._sanitize_client_document(updated)

    async def get_client_configuration(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        return self._sanitize_client_document(client)

    async def list_clients(self) -> dict[str, Any]:
        clients = await self._mongodb_service.list_clients()
        return {
            "count": len(clients),
            "clients": [self._sanitize_client_document(client) for client in clients],
        }

    async def list_processes(
        self,
        client_name: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        client = await self._require_client(client_name)
        processes, total = await self._mongodb_service.list_process_runs_for_client(
            client["client_key"],
            page=page,
            page_size=page_size,
        )
        return {
            "client_key": client["client_key"],
            "client_name": client["client_name"],
            "count": len(processes),
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_next": page * page_size < total,
            "has_previous": page > 1,
            "processes": processes,
        }

    async def get_client_overview(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        client_key = client["client_key"]
        subscriptions = await self._mongodb_service.list_client_domain_summaries(client_key)
        runs, _ = await self._mongodb_service.list_process_runs_for_client(client_key)
        return {
            "client_key": client_key,
            "client_name": client["client_name"],
            "subscriptions": subscriptions,
            "process_runs": runs,
        }

    async def get_client_jobs(self, client_name: str, limit: int = 500) -> dict[str, Any]:
        client = await self._require_client(client_name)
        client_key = client["client_key"]
        jobs = await self._mongodb_service.list_client_jobs(client_key, limit=limit)
        return {
            "client_key": client_key,
            "client_name": client["client_name"],
            "count": len(jobs),
            "jobs": jobs,
        }

    async def get_process_jobs(self, process_id: str, limit: int = 500) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(process_id)
        if process is None:
            raise ValueError("Process not found")
        jobs = await self._mongodb_service.list_client_jobs_for_process(process_id, limit=limit)
        return {
            "process_id": process_id,
            "client_key": process.get("client_key"),
            "client_name": process.get("client_name"),
            "count": len(jobs),
            "jobs": jobs,
        }

    async def _send_process_completion_email(
        self,
        process_id: str,
        client_key: str,
        status: str,
    ) -> None:
        if status not in {"completed", "partial_completed"}:
            return
        client = await self._mongodb_service.get_client(client_key)
        process = await self._mongodb_service.get_process_run_with_items(process_id)
        if client is None or process is None:
            log_event(
                logger,
                "warning",
                "process_completion_email_context_missing process_id=%s client_key=%s client_found=%s process_found=%s",
                process_id,
                client_key,
                client is not None,
                process is not None,
                domain=process_id,
                process_id=process_id,
                client_key=client_key,
                client_found=client is not None,
                process_found=process is not None,
            )
            return
        result = await self._email_service.send_process_completed_email(client=client, process=process)
        await self._mongodb_service.update_process_run(
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
        log_event(
            logger,
            "info",
            "process_completion_email_result process_id=%s client_key=%s email_status=%s",
            process_id,
            client_key,
            result.get("status"),
            domain=process_id,
            process_id=process_id,
            client_key=client_key,
            email_status=result.get("status"),
            email_reason=result.get("reason"),
        )

    async def stop_process(self, process_id: str) -> dict[str, Any]:
        process = await self._mongodb_service.get_process_run(process_id)
        if process is None:
            raise ValueError("Process not found")

        current_status = str(process.get("status") or "")
        if current_status in {"completed", "partial_completed", "failed", "stopped"}:
            return {
                "process_id": process_id,
                "status": current_status,
                "message": f"Process is already {current_status}.",
            }

        self._stop_requests.add(process_id)
        updated = await self._mongodb_service.mark_process_stop_requested(process_id)
        return {
            "process_id": process_id,
            "status": str((updated or process).get("status") or "stop_requested"),
            "message": "Stop requested. Running work will stop after the current URL finishes.",
        }

    async def _execute_process(
        self,
        *,
        process_id: str,
        request: JobProcessRequest,
        assignments: list[dict[str, Any]],
        client_key: str,
        client_name: str,
        grid_url: str,
    ) -> dict[str, Any]:
        log_event(
            logger,
            "info",
            "execute_process_started process_id=%s assignment_count=%s",
            process_id,
            len(assignments),
            domain=request.urls[0] if request.urls else client_key,
            process_id=process_id,
            assignment_count=len(assignments),
        )

        session_info = await create_session_async(grid_url=grid_url, reuse_existing=True)
        if session_info is None or not session_info.cdp_url:
            error = "Unable to establish shared Selenium/CDP session"
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [error],
                    "summary": {
                        "total_urls": len(request.urls),
                        "assigned_agent_count": request.agent_count,
                        "processed_url_count": len(request.urls),
                        "completed_domain_count": 0,
                        "failed_domain_count": len(request.urls),
                        "queued_url_count": 0,
                        "running_url_count": 0,
                        "stopped_url_count": 0,
                    },
                    "queued_urls": [],
                    "failed_urls": list(request.urls),
                },
            )
            for assignment in assignments:
                await self._mongodb_service.update_assignment_status(process_id, assignment["agent_index"], "completed")
                for url in assignment["urls"]:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error,
                        result_payload={"status": "failed", "reason": error},
                        was_running=False,
                    )
            return {
                "process_id": process_id,
                "status": "failed",
                "errors": [error],
                "worker_results": [],
                "summary": {
                    "total_urls": len(request.urls),
                    "assigned_agent_count": request.agent_count,
                    "processed_url_count": len(request.urls),
                    "completed_domain_count": 0,
                    "failed_domain_count": len(request.urls),
                    "queued_url_count": 0,
                    "running_url_count": 0,
                    "stopped_url_count": 0,
                },
            }

        shared_runtime = SharedSessionRuntime(
            grid_url=grid_url,
            session_id=session_info.session_id,
            cdp_url=session_info.cdp_url,
        )

        worker_inputs = [
            {
                "process_id": process_id,
                "client_key": client_key,
                "client_name": client_name,
                "agent_index": assignment["agent_index"],
                "assigned_urls": assignment["urls"],
                "session_id": session_info.session_id,
                "cdp_url": session_info.cdp_url,
                "shared_runtime": shared_runtime,
                "metadata": {
                    "ats_check": request.ats_check,
                    "job_extract": request.job_extract,
                    "job_monitoring": request.job_monitoring,
                    "requested_capability": self._requested_capability_for_request(request),
                },
            }
            for assignment in assignments
        ]

        try:
            worker_results = await asyncio.gather(*[self._run_agent(worker_input) for worker_input in worker_inputs])
        finally:
            # await close_shared_session_async(shared_runtime.session_id)
            pass

        errors = [error for worker in worker_results for error in worker["errors"]]
        completed_domain_count = sum(
            1
            for worker in worker_results
            for record in worker["domain_results"]
            if record["status"] == "completed"
        )
        failed_domain_count = sum(
            1
            for worker in worker_results
            for record in worker["domain_results"]
            if record["status"] != "completed"
        )
        stop_requested = self._is_stop_requested(process_id)
        status = self._derive_completion_status(
            stop_requested=stop_requested,
            completed_count=completed_domain_count,
            failed_count=failed_domain_count,
            errors=errors,
        )
        process_run = await self._mongodb_service.get_process_run(process_id)
        self._stop_requests.discard(process_id)

        persisted_summary = dict((process_run or {}).get("summary") or {})
        return {
            "process_id": process_id,
            "status": status,
            "errors": errors,
            "worker_results": worker_results,
            "summary": {
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": int(persisted_summary.get("processed_url_count", completed_domain_count + failed_domain_count)),
                "completed_domain_count": int(persisted_summary.get("completed_domain_count", completed_domain_count)),
                "failed_domain_count": int(persisted_summary.get("failed_domain_count", failed_domain_count)),
                "queued_url_count": int(persisted_summary.get("queued_url_count", 0)),
                "running_url_count": int(persisted_summary.get("running_url_count", 0)),
                "stopped_url_count": int(persisted_summary.get("stopped_url_count", 0)),
            },
        }

    async def _execute_rerun_process(
        self,
        *,
        process_id: str,
        request: JobProcessRequest,
        assignments: list[dict[str, Any]],
        client_key: str,
        client_name: str,
        grid_url: str,
        original_process: dict[str, Any],
    ) -> dict[str, Any]:
        session_info = await create_session_async(grid_url=grid_url, reuse_existing=True)
        if session_info is None or not session_info.cdp_url:
            error = "Unable to establish shared Selenium/CDP session"
            await self._mongodb_service.update_process_run(
                process_id,
                {
                    "status": "failed",
                    "completed_at": datetime.utcnow(),
                    "errors": [error],
                    "summary": {
                        "total_urls": len(request.urls),
                        "assigned_agent_count": request.agent_count,
                        "processed_url_count": len(request.urls),
                        "completed_domain_count": 0,
                        "failed_domain_count": len(request.urls),
                        "queued_url_count": 0,
                        "running_url_count": 0,
                        "stopped_url_count": 0,
                    },
                    "queued_urls": [],
                    "failed_urls": list(request.urls),
                },
            )
            for assignment in assignments:
                await self._mongodb_service.update_assignment_status(process_id, assignment["agent_index"], "completed")
                for url in assignment["urls"]:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error,
                        result_payload={"status": "failed", "reason": error},
                        was_running=False,
                    )
            return {
                "process_id": process_id,
                "status": "failed",
                "errors": [error],
                "worker_results": [],
                "summary": {
                    "total_urls": len(request.urls),
                    "assigned_agent_count": request.agent_count,
                    "processed_url_count": len(request.urls),
                    "completed_domain_count": 0,
                    "failed_domain_count": len(request.urls),
                    "queued_url_count": 0,
                    "running_url_count": 0,
                    "stopped_url_count": 0,
                },
            }

        shared_runtime = SharedSessionRuntime(
            grid_url=grid_url,
            session_id=session_info.session_id,
            cdp_url=session_info.cdp_url,
        )
        original_item_map = {
            self._normalize_domain_key(item.get("raw_url") or item.get("domain_key") or ""): item
            for item in list(original_process.get("items") or [])
        }

        worker_inputs = [
            {
                "process_id": process_id,
                "client_key": client_key,
                "client_name": client_name,
                "agent_index": assignment["agent_index"],
                "assigned_urls": assignment["urls"],
                "shared_runtime": shared_runtime,
                "metadata": {
                    "ats_check": request.ats_check,
                    "job_extract": request.job_extract,
                    "job_monitoring": request.job_monitoring,
                    "requested_capability": self._requested_capability_for_request(request),
                },
                "original_item_map": original_item_map,
            }
            for assignment in assignments
        ]

        try:
            worker_results = await asyncio.gather(*[self._run_agent_rerun(worker_input) for worker_input in worker_inputs])
        finally:
            # await close_shared_session_async(shared_runtime.session_id)
            pass

        errors = [error for worker in worker_results for error in worker["errors"]]
        completed_domain_count = sum(
            1 for worker in worker_results for record in worker["domain_results"] if record["status"] == "completed"
        )
        failed_domain_count = sum(
            1 for worker in worker_results for record in worker["domain_results"] if record["status"] != "completed"
        )
        stop_requested = self._is_stop_requested(process_id)
        status = self._derive_completion_status(
            stop_requested=stop_requested,
            completed_count=completed_domain_count,
            failed_count=failed_domain_count,
            errors=errors,
        )
        process_run = await self._mongodb_service.get_process_run(process_id)
        self._stop_requests.discard(process_id)
        persisted_summary = dict((process_run or {}).get("summary") or {})
        return {
            "process_id": process_id,
            "status": status,
            "errors": errors,
            "worker_results": worker_results,
            "summary": {
                "total_urls": len(request.urls),
                "assigned_agent_count": request.agent_count,
                "processed_url_count": int(persisted_summary.get("processed_url_count", completed_domain_count + failed_domain_count)),
                "completed_domain_count": int(persisted_summary.get("completed_domain_count", completed_domain_count)),
                "failed_domain_count": int(persisted_summary.get("failed_domain_count", failed_domain_count)),
                "queued_url_count": int(persisted_summary.get("queued_url_count", 0)),
                "running_url_count": int(persisted_summary.get("running_url_count", 0)),
                "stopped_url_count": int(persisted_summary.get("stopped_url_count", 0)),
            },
        }

    async def _run_agent(self, graph_input: dict[str, Any]) -> dict[str, Any]:
        assigned_urls = list(graph_input.get("assigned_urls", []))
        process_id = str(graph_input["process_id"])
        client_key = str(graph_input["client_key"])
        client_name = str(graph_input["client_name"])
        agent_index = int(graph_input["agent_index"])
        shared_runtime: SharedSessionRuntime = graph_input["shared_runtime"]
        browser_session = None

        if assigned_urls:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")
        if self._is_stop_requested(process_id):
            await self._stop_remaining_agent_urls(process_id, agent_index, assigned_urls)
            return WorkerProcessResult(
                agent_index=agent_index,
                status="stopped",
                assigned_urls=assigned_urls,
                processed_urls=[],
                domain_results=[],
                errors=[],
                metadata={"stop_requested": True},
            ).model_dump(mode="json")

        bootstrap_result = await bootstrap_browser_node(state=graph_input)
        if not bootstrap_result.get("session_established"):
            log_event(
                logger,
                "warning",
                "agent_bootstrap_failed_attempting_recovery process_id=%s agent_index=%s",
                process_id,
                agent_index,
                domain=assigned_urls[0] if assigned_urls else client_key,
                process_id=process_id,
                agent_index=agent_index,
            )
            try:
                browser_session, agent_tab = await self._recover_agent_tab(
                    shared_runtime=shared_runtime,
                    browser_session=None,
                    agent_index=agent_index,
                    url=assigned_urls[0] if assigned_urls else client_key,
                )
                bootstrap_metadata = dict(bootstrap_result.get("metadata", {}))
                bootstrap_metadata["bootstrap_status"] = "recovered"
                bootstrap_result = {
                    **bootstrap_result,
                    "browser_session": browser_session,
                    "agent_tab": agent_tab,
                    "session_established": True,
                    "metadata": bootstrap_metadata,
                }
            except Exception as exc:
                errors = list(bootstrap_result.get("errors", []))
                error_text = str(exc) or "; ".join(errors) or "Agent bootstrap failed"
                for url in assigned_urls:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error_text,
                        result_payload={"status": "failed", "reason": error_text},
                        was_running=False,
                    )
                if assigned_urls:
                    await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
                return WorkerProcessResult(
                    agent_index=agent_index,
                    status="failed",
                    assigned_urls=assigned_urls,
                    processed_urls=[],
                    domain_results=[],
                    errors=errors + [error_text],
                    metadata=dict(bootstrap_result.get("metadata", {})),
                ).model_dump(mode="json")

        browser_session = bootstrap_result.get("browser_session")
        agent_tab = bootstrap_result.get("agent_tab", {})

        try:
            domain_results: list[dict[str, Any]] = []
            errors: list[str] = []
            processed_urls: list[str] = []

            for url in assigned_urls:
                if self._is_stop_requested(process_id):
                    remaining_urls = [pending_url for pending_url in assigned_urls if pending_url not in processed_urls]
                    await self._stop_remaining_agent_urls(process_id, agent_index, remaining_urls)
                    break
                recovery_attempt_count = 0
                mark_running = True
                while True:
                    try:
                        record = await self._process_domain(
                            process_id=process_id,
                            client_key=client_key,
                            client_name=client_name,
                            url=url,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            agent_tab=agent_tab,
                            ats_check=bool((graph_input.get("metadata") or {}).get("ats_check", True)),
                            job_extract=bool((graph_input.get("metadata") or {}).get("job_extract", False)),
                            job_monitoring=bool((graph_input.get("metadata") or {}).get("job_monitoring", False)),
                            requested_capability=str((graph_input.get("metadata") or {}).get("requested_capability", "career_page")),
                            mark_running=mark_running,
                        )
                        break
                    except AgentSessionRecoveryNeeded as exc:
                        recovery_attempt_count += 1
                        if recovery_attempt_count > 2:
                            error_text = str(exc)
                            record = DomainProcessRecord(
                                domain=url,
                                main_domain=extract_domain(url),
                                status="failed",
                                error=error_text,
                            ).model_dump(mode="json")
                            await self._mongodb_service.mark_url_failed(
                                process_id,
                                url,
                                error_text,
                                result_payload=record,
                                was_running=True,
                            )
                            log_event(
                                logger,
                                "error",
                                "agent_recovery_exhausted process_id=%s agent_index=%s url=%s error=%s",
                                process_id,
                                agent_index,
                                url,
                                error_text,
                                domain=url,
                                process_id=process_id,
                                agent_index=agent_index,
                                url=url,
                                error=error_text,
                            )
                            break

                        browser_session, agent_tab = await self._recover_agent_tab(
                            shared_runtime=shared_runtime,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            url=url,
                        )
                        mark_running = False

                domain_results.append(record)
                processed_urls.append(url)
                if record["status"] != "completed" and record.get("error"):
                    errors.append(str(record["error"]))

            return WorkerProcessResult(
                agent_index=agent_index,
                status=self._derive_completion_status(
                    stop_requested=self._is_stop_requested(process_id),
                    completed_count=sum(1 for record in domain_results if record["status"] == "completed"),
                    failed_count=sum(1 for record in domain_results if record["status"] != "completed"),
                    errors=errors,
                ),
                assigned_urls=assigned_urls,
                processed_urls=processed_urls,
                domain_results=[DomainProcessRecord(**record) for record in domain_results],
                errors=errors,
                metadata=dict(bootstrap_result.get("metadata", {})),
            ).model_dump(mode="json")
        finally:
            if assigned_urls:
                assignment_status = "stopped" if self._is_stop_requested(process_id) else "completed"
                await self._mongodb_service.update_assignment_status(process_id, agent_index, assignment_status)
            await close_agent_tab(browser_session)

    async def _run_agent_rerun(self, graph_input: dict[str, Any]) -> dict[str, Any]:
        assigned_urls = list(graph_input.get("assigned_urls", []))
        process_id = str(graph_input["process_id"])
        client_key = str(graph_input["client_key"])
        client_name = str(graph_input["client_name"])
        agent_index = int(graph_input["agent_index"])
        shared_runtime: SharedSessionRuntime = graph_input["shared_runtime"]
        original_item_map = dict(graph_input.get("original_item_map") or {})
        browser_session = None

        if assigned_urls:
            await self._mongodb_service.update_assignment_status(process_id, agent_index, "running")
        if self._is_stop_requested(process_id):
            await self._stop_remaining_agent_urls(process_id, agent_index, assigned_urls)
            return WorkerProcessResult(
                agent_index=agent_index,
                status="stopped",
                assigned_urls=assigned_urls,
                processed_urls=[],
                domain_results=[],
                errors=[],
                metadata={"stop_requested": True, "rerun": True},
            ).model_dump(mode="json")

        bootstrap_result = await bootstrap_browser_node(state=graph_input)
        if not bootstrap_result.get("session_established"):
            try:
                browser_session, agent_tab = await self._recover_agent_tab(
                    shared_runtime=shared_runtime,
                    browser_session=None,
                    agent_index=agent_index,
                    url=assigned_urls[0] if assigned_urls else client_key,
                )
                bootstrap_result = {
                    **bootstrap_result,
                    "browser_session": browser_session,
                    "agent_tab": agent_tab,
                    "session_established": True,
                    "metadata": {**dict(bootstrap_result.get("metadata", {})), "bootstrap_status": "recovered"},
                }
            except Exception as exc:
                errors = list(bootstrap_result.get("errors", []))
                error_text = str(exc) or "; ".join(errors) or "Agent bootstrap failed"
                for url in assigned_urls:
                    await self._mongodb_service.mark_url_failed(
                        process_id,
                        url,
                        error_text,
                        result_payload={"status": "failed", "reason": error_text},
                        was_running=False,
                    )
                if assigned_urls:
                    await self._mongodb_service.update_assignment_status(process_id, agent_index, "completed")
                return WorkerProcessResult(
                    agent_index=agent_index,
                    status="failed",
                    assigned_urls=assigned_urls,
                    processed_urls=[],
                    domain_results=[],
                    errors=errors + [error_text],
                    metadata={**dict(bootstrap_result.get("metadata", {})), "rerun": True},
                ).model_dump(mode="json")

        browser_session = bootstrap_result.get("browser_session")
        agent_tab = bootstrap_result.get("agent_tab", {})
        try:
            domain_results: list[dict[str, Any]] = []
            errors: list[str] = []
            processed_urls: list[str] = []

            for url in assigned_urls:
                if self._is_stop_requested(process_id):
                    remaining_urls = [pending_url for pending_url in assigned_urls if pending_url not in processed_urls]
                    await self._stop_remaining_agent_urls(process_id, agent_index, remaining_urls)
                    break

                recovery_attempt_count = 0
                mark_running = True
                while True:
                    try:
                        record = await self._rerun_domain(
                            process_id=process_id,
                            client_key=client_key,
                            client_name=client_name,
                            url=url,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            agent_tab=agent_tab,
                            ats_check=bool((graph_input.get("metadata") or {}).get("ats_check", True)),
                            job_extract=bool((graph_input.get("metadata") or {}).get("job_extract", False)),
                            job_monitoring=bool((graph_input.get("metadata") or {}).get("job_monitoring", False)),
                            requested_capability=str((graph_input.get("metadata") or {}).get("requested_capability", "career_page")),
                            original_item=original_item_map.get(self._normalize_domain_key(url)),
                            mark_running=mark_running,
                        )
                        break
                    except AgentSessionRecoveryNeeded as exc:
                        recovery_attempt_count += 1
                        if recovery_attempt_count > 2:
                            error_text = str(exc)
                            record = DomainProcessRecord(
                                domain=url,
                                main_domain=extract_domain(url),
                                status="failed",
                                error=error_text,
                            ).model_dump(mode="json")
                            await self._mongodb_service.mark_url_failed(
                                process_id,
                                url,
                                error_text,
                                result_payload=record,
                                was_running=True,
                            )
                            break

                        browser_session, agent_tab = await self._recover_agent_tab(
                            shared_runtime=shared_runtime,
                            browser_session=browser_session,
                            agent_index=agent_index,
                            url=url,
                        )
                        mark_running = False

                domain_results.append(record)
                processed_urls.append(url)
                if record["status"] != "completed" and record.get("error"):
                    errors.append(str(record["error"]))

            return WorkerProcessResult(
                agent_index=agent_index,
                status=self._derive_completion_status(
                    stop_requested=self._is_stop_requested(process_id),
                    completed_count=sum(1 for record in domain_results if record["status"] == "completed"),
                    failed_count=sum(1 for record in domain_results if record["status"] != "completed"),
                    errors=errors,
                ),
                assigned_urls=assigned_urls,
                processed_urls=processed_urls,
                domain_results=[DomainProcessRecord(**record) for record in domain_results],
                errors=errors,
                metadata={**dict(bootstrap_result.get("metadata", {})), "rerun": True},
            ).model_dump(mode="json")
        finally:
            if assigned_urls:
                assignment_status = "stopped" if self._is_stop_requested(process_id) else "completed"
                await self._mongodb_service.update_assignment_status(process_id, agent_index, assignment_status)
            await close_agent_tab(browser_session)

    async def _process_domain(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        url: str,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        ats_check: bool,
        job_extract: bool,
        job_monitoring: bool,
        requested_capability: str,
        mark_running: bool = True,
    ) -> dict[str, Any]:
        domain_key = self._normalize_domain_key(url)
        main_domain = extract_domain(url)
        if mark_running:
            await self._mongodb_service.mark_url_running(process_id, url, agent_index)

        existing_domain = await self._mongodb_service.get_domain(domain_key)
        reused_career_discovery = False
        reused_ats_detection = False

        try:
            career_url_result = {}
            cached_career_result = ((existing_domain or {}).get("career_url_extraction") or {})
            cached_career_urls = list(cached_career_result.get("career_urls") or [])
            if cached_career_result.get("status") == "career_urls_found" and cached_career_urls:
                career_url_result = cached_career_result
                reused_career_discovery = True
            else:
                career_url_result = await career_url_extraction_node(main_domain or domain_key, browser_session)

            career_urls = list(career_url_result.get("career_urls") or [])
            if career_urls:
                career_page_result = await career_page_category_node(
                    career_urls,
                    browser_session,
                    agent_index,
                    agent_tab,
                )
                await self._store_listing_snapshots_from_career_result(
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    career_page_result=career_page_result,
                )
            else:
                career_page_result = self._build_empty_career_page_result(career_url_result)

            fingerprint_source = career_page_result.get("career_pages_analysis") or career_page_result
            page_fingerprint = self._fingerprint_payload(fingerprint_source)
            previous_fingerprint = (existing_domain or {}).get("latest_page_fingerprint")
            content_changed = None if previous_fingerprint is None else previous_fingerprint != page_fingerprint

            if ats_check:
                cached_ats_detection = ((existing_domain or {}).get("ats_detection") or {})
                if cached_ats_detection and cached_ats_detection.get("confidence") == "high":
                    ats_detection = cached_ats_detection
                    reused_ats_detection = True
                else:
                    ats_detection = await detect_ats(
                        career_page_result,
                        main_domain or domain_key,
                        browser_session,
                        agent_index,
                        agent_tab,
                    )
            else:
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
                }

            if job_extract:
                jobs_extraction = await self._job_extraction_service.extract_jobs_for_domain(
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    career_page_result=career_page_result,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
            else:
                jobs_extraction = {
                    "status": "skipped",
                    "requested": False,
                    "job_count": 0,
                    "jobs": [],
                    "sources": [],
                }

            record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                career_url_extraction=career_url_result,
                career_page_result=career_page_result,
                job_listing_patterns=career_page_result.get("job_listing_patterns") or [],
                ats_detection=ats_detection,
                jobs_extraction=jobs_extraction,
                status="completed",
            ).model_dump(mode="json")

            domain_check_id = str(uuid4())
            result_summary = self._build_result_summary(
                record,
                reused_career_discovery=reused_career_discovery,
                reused_ats_detection=reused_ats_detection,
                content_changed=content_changed,
                job_monitoring=job_monitoring,
            )
            await self._mongodb_service.append_domain_history(
                domain_key,
                self._build_domain_history_item(
                    process_id=process_id,
                    client_key=client_key,
                    requested_capability=requested_capability,
                    record=record,
                    content_changed=content_changed,
                ),
            )

            now_for_domain_update = datetime.utcnow()
            domain_career_url_extraction = self._merge_domain_career_url_extraction(
                existing_domain=existing_domain,
                career_url_result=career_url_result,
            )
            domain_career_page_result = self._select_domain_career_page_result_for_storage(
                existing_domain=existing_domain,
                career_page_result=career_page_result,
            )
            await self._mongodb_service.upsert_domain(
                domain_key,
                {
                    "career_url_extraction": domain_career_url_extraction,
                    "career_page_result": domain_career_page_result,
                    "job_listing_patterns": domain_career_page_result.get("job_listing_patterns") or [],
                    "ats_detection": ats_detection,
                    "jobs_extraction_summary": {
                        "status": jobs_extraction.get("status"),
                        "requested": jobs_extraction.get("requested"),
                        "job_count": jobs_extraction.get("job_count"),
                        "source_count": jobs_extraction.get("source_count"),
                        "reused_source_count": jobs_extraction.get("reused_source_count"),
                    },
                    "latest_page_fingerprint": self._fingerprint_payload(domain_career_page_result),
                    "latest_extracted_text": self._extract_latest_page_text(domain_career_page_result),
                    "last_career_discovery_at": now_for_domain_update,
                    "next_career_url_discovery_due_at": now_for_domain_update + timedelta(days=CAREER_DISCOVERY_REFRESH_DAYS),
                    "last_career_check_at": now_for_domain_update,
                    "last_ats_check_at": now_for_domain_update if ats_check else (existing_domain or {}).get("last_ats_check_at"),
                    "last_job_extract_at": now_for_domain_update if job_extract else (existing_domain or {}).get("last_job_extract_at"),
                },
            )

            await self._mongodb_service.mark_url_completed(
                process_id,
                url,
                result_summary=result_summary,
                result_payload=record,
                domain_check_id=domain_check_id,
            )
            return record
        except Exception as exc:
            error_text = str(exc)
            if self._is_recoverable_agent_session_error(error_text):
                log_event(
                    logger,
                    "warning",
                    "agent_session_recovery_needed process_id=%s agent_index=%s url=%s error=%s",
                    process_id,
                    agent_index,
                    url,
                    error_text,
                    domain=url,
                    process_id=process_id,
                    agent_index=agent_index,
                    url=url,
                    error=error_text,
                )
                raise AgentSessionRecoveryNeeded(error_text) from exc
            failed_record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                status="failed",
                error=error_text,
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_failed(
                process_id,
                url,
                error_text,
                result_payload=failed_record,
                was_running=True,
            )
            return failed_record

    async def _rerun_domain(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        url: str,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        ats_check: bool,
        job_extract: bool,
        job_monitoring: bool,
        requested_capability: str,
        original_item: dict[str, Any] | None,
        mark_running: bool = True,
    ) -> dict[str, Any]:
        domain_key = self._normalize_domain_key(url)
        main_domain = extract_domain(url)
        if mark_running:
            await self._mongodb_service.mark_url_running(process_id, url, agent_index)

        previous_record = dict((original_item or {}).get("result_payload") or {})
        previous_career_url_extraction = dict(previous_record.get("career_url_extraction") or {})
        previous_career_page_result = dict(previous_record.get("career_page_result") or {})
        previous_ats_detection = dict(previous_record.get("ats_detection") or {})
        existing_domain = await self._mongodb_service.get_domain(domain_key)

        try:
            reused_analysis_count = 0
            changed_page_count = 0
            content_changed = False
            rerun_mode = "full_discovery"
            career_url_result = dict(previous_career_url_extraction)
            career_url_result.setdefault("used_previous_career_urls", True)
            career_page_result = self._build_empty_career_page_result(career_url_result)
            discovery_ran = False

            cached_pattern_sources = self._select_cached_pattern_sources(
                existing_domain=existing_domain,
                previous_career_page_result=previous_career_page_result,
            )
            should_run_discovery = self._career_discovery_should_run(
                existing_domain=existing_domain,
                previous_career_url_extraction=previous_career_url_extraction,
                previous_career_page_result=previous_career_page_result,
            )

            if cached_pattern_sources and not should_run_discovery:
                rerun_mode = "cached_job_pages"
                career_page_result = await self._run_cached_job_page_rerun(
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    cached_pattern_sources=cached_pattern_sources,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
                changed_page_count = int(career_page_result.get("cached_pattern_page_count") or 0)
                content_changed = bool(
                    career_page_result.get("cached_pattern_failed_count")
                    or career_page_result.get("cached_snapshot_changed")
                )
            else:
                discovery_ran = True
                fresh_career_url_result = await career_url_extraction_node(main_domain or domain_key, browser_session)
                career_url_result = self._select_rerun_career_url_result(
                    fresh_result=fresh_career_url_result,
                    previous_result=previous_career_url_extraction,
                )

                previous_not_job_related_urls = list(
                    ((previous_career_page_result.get("overview") or {}).get("not_job_related_urls") or [])
                )
                candidate_career_urls = self._filter_rerun_career_urls(
                    career_url_result.get("career_urls") or [],
                    previous_not_job_related_urls,
                    [],
                )
                cached_page_url_set = {
                    self._normalize_url_for_compare(str(source.get("page_url") or ""))
                    for source in cached_pattern_sources
                }
                candidate_career_urls = [
                    candidate_url
                    for candidate_url in candidate_career_urls
                    if self._normalize_url_for_compare(candidate_url) not in cached_page_url_set
                ]
                career_urls_changed = self._rerun_career_urls_changed(
                    previous_urls=list(previous_career_url_extraction.get("career_urls") or []),
                    current_urls=list(career_url_result.get("career_urls") or []),
                )
                content_changed = career_urls_changed

                cached_page_result = {"overview": {}, "career_pages_analysis": [], "job_listing_patterns": []}
                if cached_pattern_sources:
                    cached_page_result = await self._run_cached_job_page_rerun(
                        process_id=process_id,
                        client_key=client_key,
                        client_name=client_name,
                        raw_url=url,
                        domain_key=domain_key,
                        cached_pattern_sources=cached_pattern_sources,
                        browser_session=browser_session,
                        agent_index=agent_index,
                        agent_tab=agent_tab,
                    )
                    changed_page_count += int(cached_page_result.get("cached_pattern_page_count") or 0)
                    content_changed = bool(
                        content_changed
                        or cached_page_result.get("cached_pattern_failed_count")
                        or cached_page_result.get("cached_snapshot_changed")
                    )

                if candidate_career_urls:
                    rerun_page_result = await self._build_rerun_career_page_result(
                        career_urls=candidate_career_urls,
                        previous_career_page_result=previous_career_page_result,
                        browser_session=browser_session,
                        agent_index=agent_index,
                        agent_tab=agent_tab,
                    )
                    career_page_result = rerun_page_result["career_page_result"]
                    await self._store_listing_snapshots_from_career_result(
                        process_id=process_id,
                        client_key=client_key,
                        client_name=client_name,
                        raw_url=url,
                        domain_key=domain_key,
                        career_page_result=career_page_result,
                    )
                    reused_analysis_count = int(rerun_page_result["reused_analysis_count"])
                    changed_page_count += int(rerun_page_result["changed_page_count"])
                    content_changed = bool(content_changed or rerun_page_result["content_changed"])

                merged_analysis = (
                    list(cached_page_result.get("career_pages_analysis") or [])
                    + list(career_page_result.get("career_pages_analysis") or [])
                )
                if merged_analysis:
                    career_page_result = {
                        "overview": _build_career_page_overview(merged_analysis),
                        "career_pages_analysis": merged_analysis,
                        "job_listing_patterns": _collect_job_listing_patterns(merged_analysis),
                    }

            ats_detection: dict[str, Any]
            if ats_check:
                if not content_changed:
                    ats_detection = {
                        **previous_ats_detection,
                        "detection_method": "rerun_skipped_no_career_change",
                        "reasoning": "Career content did not change during rerun, so ATS check was skipped.",
                        "reused": True,
                    }
                else:
                    ats_detection = await detect_ats(
                        career_page_result,
                        main_domain or domain_key,
                        browser_session,
                        agent_index,
                        agent_tab,
                    )
            else:
                ats_detection = {
                    "ats_detected": None,
                    "detection_method": "skipped",
                    "reasoning": "ATS check disabled for this process.",
                }

            if rerun_mode == "cached_job_pages":
                overview = dict(career_page_result.get("overview") or {})
                jobs_extraction = {
                    "status": "completed_from_listing_pattern_cache",
                    "requested": bool(job_extract),
                    "job_count": int(overview.get("total_jobs_found") or 0),
                    "jobs": [],
                    "sources": [
                        {
                            "source_type": "job_listing_page",
                            "source_url": page.get("classified_job_listing_url") or page.get("extracted_url") or page.get("url"),
                            "job_count": len(page.get("jobs_listed_on_page") or []),
                            "snapshot": page.get("listing_job_snapshot"),
                        }
                        for page in list(career_page_result.get("career_pages_analysis") or [])
                    ],
                    "reason": "Jobs were extracted using cached job listing patterns during rerun.",
                }
            elif job_extract and content_changed:
                jobs_extraction = await self._job_extraction_service.extract_jobs_for_domain(
                    process_id=process_id,
                    client_key=client_key,
                    client_name=client_name,
                    raw_url=url,
                    domain_key=domain_key,
                    career_page_result=career_page_result,
                    browser_session=browser_session,
                    agent_index=agent_index,
                    agent_tab=agent_tab,
                )
            elif job_extract:
                jobs_extraction = {
                    "status": "skipped_no_career_change",
                    "requested": True,
                    "job_count": 0,
                    "jobs": [],
                    "sources": [],
                    "reason": "Career content did not change during rerun.",
                }
            else:
                jobs_extraction = {
                    "status": "skipped",
                    "requested": False,
                    "job_count": 0,
                    "jobs": [],
                    "sources": [],
                }

            record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                career_url_extraction=career_url_result,
                career_page_result=career_page_result,
                job_listing_patterns=career_page_result.get("job_listing_patterns") or [],
                ats_detection=ats_detection,
                jobs_extraction=jobs_extraction,
                status="completed",
            ).model_dump(mode="json")
            record["rerun_metadata"] = {
                "reused_analysis_count": reused_analysis_count,
                "changed_page_count": changed_page_count,
                "content_changed": content_changed,
                "rerun_mode": rerun_mode,
                "career_discovery_ran": discovery_ran,
                "cached_pattern_page_count": len(cached_pattern_sources),
                "career_discovery_fresh": self._career_discovery_is_fresh(existing_domain),
            }

            domain_check_id = str(uuid4())
            result_summary = self._build_result_summary(
                record,
                reused_career_discovery=bool(career_url_result.get("used_previous_career_urls")),
                reused_ats_detection=bool(ats_detection.get("reused")),
                content_changed=content_changed,
                job_monitoring=job_monitoring,
            )

            await self._mongodb_service.append_domain_history(
                domain_key,
                self._build_domain_history_item(
                    process_id=process_id,
                    client_key=client_key,
                    requested_capability=requested_capability,
                    record=record,
                    content_changed=content_changed,
                    rerun_metadata=record.get("rerun_metadata"),
                ),
            )

            now_for_domain_update = datetime.utcnow()
            last_career_discovery_at = (
                now_for_domain_update
                if discovery_ran
                else (existing_domain or {}).get("last_career_discovery_at")
            )
            next_career_discovery_due_at = (
                now_for_domain_update + timedelta(days=CAREER_DISCOVERY_REFRESH_DAYS)
                if discovery_ran
                else (existing_domain or {}).get("next_career_url_discovery_due_at")
            )
            domain_career_url_extraction = self._merge_domain_career_url_extraction(
                existing_domain=existing_domain,
                career_url_result=career_url_result,
            )
            domain_career_page_result = self._select_domain_career_page_result_for_storage(
                existing_domain=existing_domain,
                career_page_result=career_page_result,
            )

            await self._mongodb_service.upsert_domain(
                domain_key,
                {
                    "career_url_extraction": domain_career_url_extraction,
                    "career_page_result": domain_career_page_result,
                    "job_listing_patterns": domain_career_page_result.get("job_listing_patterns") or [],
                    "ats_detection": ats_detection,
                    "jobs_extraction_summary": {
                        "status": jobs_extraction.get("status"),
                        "requested": jobs_extraction.get("requested"),
                        "job_count": jobs_extraction.get("job_count"),
                        "source_count": jobs_extraction.get("source_count"),
                        "reused_source_count": jobs_extraction.get("reused_source_count"),
                    },
                    "latest_page_fingerprint": self._fingerprint_payload(domain_career_page_result),
                    "latest_extracted_text": self._extract_latest_page_text(domain_career_page_result),
                    "last_career_discovery_at": last_career_discovery_at,
                    "next_career_url_discovery_due_at": next_career_discovery_due_at,
                    "last_career_check_at": now_for_domain_update,
                    "last_ats_check_at": now_for_domain_update if ats_check and content_changed else (existing_domain or {}).get("last_ats_check_at"),
                    "last_job_extract_at": now_for_domain_update if job_extract and content_changed else (existing_domain or {}).get("last_job_extract_at"),
                },
            )

            await self._mongodb_service.mark_url_completed(
                process_id,
                url,
                result_summary=result_summary,
                result_payload=record,
                domain_check_id=domain_check_id,
            )
            return record
        except Exception as exc:
            error_text = str(exc)
            if self._is_recoverable_agent_session_error(error_text):
                raise AgentSessionRecoveryNeeded(error_text) from exc
            failed_record = DomainProcessRecord(
                domain=url,
                main_domain=main_domain,
                status="failed",
                error=error_text,
            ).model_dump(mode="json")
            await self._mongodb_service.mark_url_failed(
                process_id,
                url,
                error_text,
                result_payload=failed_record,
                was_running=True,
            )
            return failed_record

    async def _recover_agent_tab(
        self,
        *,
        shared_runtime: SharedSessionRuntime,
        browser_session: Any,
        agent_index: int,
        url: str,
    ) -> tuple[Any, dict[str, Any]]:
        async with shared_runtime.recovery_lock:
            original_session_id = shared_runtime.session_id
            session_active = await is_grid_session_active_async(shared_runtime.grid_url, original_session_id)

            if session_active:
                target_cdp_url = shared_runtime.cdp_url
                log_event(
                    logger,
                    "info",
                    "agent_tab_recovery_reusing_shared_session agent_index=%s session_id=%s url=%s",
                    agent_index,
                    original_session_id,
                    url,
                    domain=url,
                    agent_index=agent_index,
                    session_id=original_session_id,
                    url=url,
                )
            else:
                replacement = await create_session_async(
                    grid_url=shared_runtime.grid_url,
                    reuse_existing=False,
                )
                if replacement is None or not replacement.cdp_url:
                    raise RuntimeError("Shared browser session is unavailable and could not be recreated")

                await close_shared_session_async(original_session_id)
                shared_runtime.session_id = replacement.session_id
                shared_runtime.cdp_url = replacement.cdp_url
                target_cdp_url = replacement.cdp_url
                log_event(
                    logger,
                    "warning",
                    "shared_session_recreated_for_agent_recovery agent_index=%s old_session_id=%s new_session_id=%s url=%s",
                    agent_index,
                    original_session_id,
                    replacement.session_id,
                    url,
                    domain=url,
                    agent_index=agent_index,
                    old_session_id=original_session_id,
                    new_session_id=replacement.session_id,
                    url=url,
                )

        await close_browser_attachment(browser_session)
        rebuilt_session = await attach_playwright_to_cdp(shared_runtime.cdp_url)
        if rebuilt_session is None:
            raise RuntimeError("Failed to reattach Playwright during agent recovery")

        rebuilt_tab = await ensure_agent_tab(rebuilt_session, agent_index=agent_index)
        log_event(
            logger,
            "info",
            "agent_tab_recovery_completed agent_index=%s session_id=%s url=%s",
            agent_index,
            shared_runtime.session_id,
            url,
            domain=url,
            agent_index=agent_index,
            session_id=shared_runtime.session_id,
            url=url,
        )
        return rebuilt_session, rebuilt_tab

    def _build_client_key(self, client_name: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", client_name.strip().lower()).strip("_")
        return normalized or "default_client"

    def _normalize_email(self, email: str | None) -> str | None:
        normalized = str(email or "").strip()
        return normalized or None

    def _derive_completion_status(
        self,
        *,
        stop_requested: bool,
        completed_count: int,
        failed_count: int,
        errors: list[str],
    ) -> str:
        if stop_requested:
            return "stopped"
        if completed_count > 0 and (failed_count > 0 or errors):
            return "partial_completed"
        if completed_count > 0:
            return "completed"
        return "failed" if failed_count > 0 or errors else "completed"

    def _select_rerun_career_url_result(
        self,
        *,
        fresh_result: dict[str, Any],
        previous_result: dict[str, Any],
    ) -> dict[str, Any]:
        fresh_urls = list(fresh_result.get("career_urls") or [])
        previous_urls = list(previous_result.get("career_urls") or [])
        if fresh_urls:
            selected = dict(fresh_result)
            selected["used_previous_career_urls"] = False
            return selected
        if previous_urls:
            selected = dict(previous_result)
            selected["used_previous_career_urls"] = True
            selected["fallback_reason"] = str(fresh_result.get("error_message") or fresh_result.get("status") or "").strip() or None
            return selected
        selected = dict(fresh_result)
        selected["used_previous_career_urls"] = False
        return selected

    def _build_domain_history_item(
        self,
        *,
        process_id: str,
        client_key: str,
        requested_capability: str,
        record: dict[str, Any],
        content_changed: bool | None,
        rerun_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        overview = dict(((record.get("career_page_result") or {}).get("overview") or {}))
        jobs_extraction = dict(record.get("jobs_extraction") or {})
        career_url_extraction = dict(record.get("career_url_extraction") or {})
        return {
            "run_at": datetime.utcnow(),
            "process_id": process_id,
            "client_key": client_key,
            "requested_capability": requested_capability,
            "status": record.get("status"),
            "content_changed": content_changed,
            "summary": {
                "career_url_status": career_url_extraction.get("status"),
                "career_page_outcome": overview.get("outcome"),
                "jobs_found": overview.get("jobs_found"),
                "total_jobs_found": overview.get("total_jobs_found"),
                "job_extract_status": jobs_extraction.get("status"),
                "job_extract_count": jobs_extraction.get("job_count"),
            },
            "rerun_metadata": rerun_metadata or record.get("rerun_metadata") or {},
            "payload_hash": self._fingerprint_payload(record),
        }

    def _merge_domain_career_url_extraction(
        self,
        *,
        existing_domain: dict[str, Any] | None,
        career_url_result: dict[str, Any],
    ) -> dict[str, Any]:
        previous = dict((existing_domain or {}).get("career_url_extraction") or {})
        merged = dict(career_url_result or {})
        for key in ("all_urls", "career_urls", "non_domain_career_urls"):
            merged[key] = self._dedupe_preserve_order(
                [*list(previous.get(key) or []), *list(merged.get(key) or [])]
            )
        return merged

    def _select_domain_career_page_result_for_storage(
        self,
        *,
        existing_domain: dict[str, Any] | None,
        career_page_result: dict[str, Any],
    ) -> dict[str, Any]:
        existing_result = dict((existing_domain or {}).get("career_page_result") or {})
        if not existing_result:
            return career_page_result

        overview = dict((career_page_result or {}).get("overview") or {})
        outcome = str(overview.get("outcome") or "").strip()
        successful_outcomes = {
            "jobs_found",
            "career_page_no_vacancies",
            "career_page_no_vacancies_with_job_alert",
            "career_page_general_job_info",
            "no_jobs_but_job_alert_available",
        }
        if outcome in successful_outcomes or overview.get("career_page_confirmed"):
            return career_page_result

        preserved = dict(existing_result)
        preserved["last_failed_check"] = {
            "checked_at": datetime.utcnow(),
            "outcome": outcome or "unknown",
            "reason": overview.get("outcome_reason"),
        }
        return preserved

    def _dedupe_preserve_order(self, values: list[Any]) -> list[Any]:
        seen: set[str] = set()
        result: list[Any] = []
        for value in values:
            marker = str(value or "").strip()
            if not marker or marker in seen:
                continue
            seen.add(marker)
            result.append(value)
        return result

    def _parse_datetime(self, value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return None
            try:
                if normalized.endswith("Z"):
                    normalized = f"{normalized[:-1]}+00:00"
                return datetime.fromisoformat(normalized).replace(tzinfo=None)
            except ValueError:
                return None
        return None

    def _career_discovery_is_fresh(self, domain_record: dict[str, Any] | None) -> bool:
        last_discovery = self._parse_datetime((domain_record or {}).get("last_career_discovery_at"))
        if last_discovery is None:
            return False
        return datetime.utcnow() - last_discovery < timedelta(days=CAREER_DISCOVERY_REFRESH_DAYS)

    def _career_result_has_jobs(self, career_page_result: dict[str, Any] | None) -> bool:
        overview = dict((career_page_result or {}).get("overview") or {})
        return bool(overview.get("jobs_found") or overview.get("job_urls") or overview.get("job_found_on_urls"))

    def _career_discovery_should_run(
        self,
        *,
        existing_domain: dict[str, Any] | None,
        previous_career_url_extraction: dict[str, Any],
        previous_career_page_result: dict[str, Any],
    ) -> bool:
        previous_status = str(previous_career_url_extraction.get("status") or "").strip()
        failed_or_empty_statuses = {
            "",
            "no_career_page_found",
            "career_page_discovery_failed",
            "domain_access_failed",
            "domain_redirected",
        }
        if previous_status in failed_or_empty_statuses:
            return True

        domain_career_result = dict((existing_domain or {}).get("career_page_result") or {})
        has_reusable_jobs = self._career_result_has_jobs(domain_career_result) or self._career_result_has_jobs(previous_career_page_result)
        if has_reusable_jobs and self._career_discovery_is_fresh(existing_domain):
            return False
        return True

    def _select_cached_pattern_sources(
        self,
        *,
        existing_domain: dict[str, Any] | None,
        previous_career_page_result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        candidates = list((existing_domain or {}).get("job_listing_patterns") or [])
        candidates.extend(list((previous_career_page_result or {}).get("job_listing_patterns") or []))

        for page in list((previous_career_page_result or {}).get("career_pages_analysis") or []):
            pattern_result = page.get("job_listing_pattern")
            if isinstance(pattern_result, dict):
                candidates.append(
                    {
                        "page_url": (
                            page.get("job_listing_pattern_url")
                            or page.get("classified_job_listing_url")
                            or pattern_result.get("page_url")
                            or page.get("extracted_url")
                        ),
                        "status": pattern_result.get("status"),
                        "pattern": pattern_result.get("pattern"),
                        "validation": pattern_result.get("validation"),
                    }
                )

        selected: list[dict[str, Any]] = []
        seen: set[str] = set()
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            page_url = str(candidate.get("page_url") or "").strip()
            pattern = candidate.get("pattern")
            status = str(candidate.get("status") or "").strip()
            normalized_url = self._normalize_url_for_compare(page_url)
            if not page_url or not normalized_url or normalized_url in seen:
                continue
            if status != "pattern_ready" or not isinstance(pattern, dict):
                continue
            seen.add(normalized_url)
            selected.append({**candidate, "page_url": page_url, "pattern": pattern})
        return selected

    def _normalize_listing_job(self, job: dict[str, Any], page_url: str) -> dict[str, Any]:
        title = str(job.get("job_title") or job.get("title") or "").strip()
        job_url = str(job.get("job_url") or "").strip() or None
        key_source = job_url or f"{page_url}|{title}".lower()
        return {
            "job_key_source": key_source,
            "job_title": title,
            "job_url": job_url,
        }

    def _listing_job_key(self, domain_key: str, normalized_job: dict[str, Any]) -> str:
        source = f"{domain_key}|{normalized_job.get('job_key_source') or ''}".strip().lower()
        return self._fingerprint_text(source)

    async def _store_listing_job_snapshot(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        raw_url: str,
        domain_key: str,
        page_url: str,
        pattern_id: str | None,
        content_fingerprint: str | None,
        jobs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        now = datetime.utcnow()
        run_date = now.date().isoformat()
        normalized_jobs = [self._normalize_listing_job(job, page_url) for job in jobs]
        normalized_jobs = [job for job in normalized_jobs if job.get("job_title") or job.get("job_url")]
        job_details: list[dict[str, Any]] = []
        current_job_keys: list[str] = []

        for normalized_job in normalized_jobs:
            job_key = self._listing_job_key(domain_key, normalized_job)
            current_job_keys.append(job_key)
            detail = {
                "job_key": job_key,
                "title": normalized_job.get("job_title"),
                "job_url": normalized_job.get("job_url"),
            }
            job_details.append(detail)
            await self._mongodb_service.upsert_job(
                job_key,
                {
                    "domain_key": domain_key,
                    "source_type": "job_listing_page",
                    "source_url": page_url,
                    "extraction_strategy": "job_listing_pattern",
                    "page_fingerprint": content_fingerprint,
                    "title": normalized_job.get("job_title"),
                    "structured_job": {
                        "title": normalized_job.get("job_title"),
                        "job_url": normalized_job.get("job_url"),
                        "page_url": page_url,
                    },
                    "status": "active",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "source_pattern_id": pattern_id,
                },
            )
            await self._mongodb_service.upsert_client_job(
                client_key=client_key,
                client_name=client_name,
                domain_key=domain_key,
                raw_url=raw_url,
                process_id=process_id,
                job_key=job_key,
                document={
                    "source_type": "job_listing_page",
                    "source_url": page_url,
                    "page_fingerprint": content_fingerprint,
                    "title": normalized_job.get("job_title"),
                    "job_data": {
                        "title": normalized_job.get("job_title"),
                        "job_url": normalized_job.get("job_url"),
                        "page_url": page_url,
                    },
                    "status": "active",
                    "first_seen_for_client_at": now,
                    "last_seen_at": now,
                },
            )

        previous_snapshot = await self._mongodb_service.get_latest_domain_job_snapshot(domain_key, page_url)
        previous_job_keys = list((previous_snapshot or {}).get("job_keys") or [])
        previous_set = set(previous_job_keys)
        current_set = set(current_job_keys)
        added_job_keys = sorted(current_set - previous_set)
        removed_job_keys = sorted(previous_set - current_set)
        unchanged_job_keys = sorted(current_set & previous_set)

        for removed_key in removed_job_keys:
            await self._mongodb_service.upsert_job(
                removed_key,
                {
                    "domain_key": domain_key,
                    "source_type": "job_listing_page",
                    "source_url": page_url,
                    "extraction_strategy": "job_listing_pattern",
                    "status": "inactive",
                    "inactive_at": now,
                    "last_missing_at": now,
                },
            )

        snapshot_key = self._fingerprint_text(f"{domain_key}|{page_url}|{run_date}")
        snapshot = {
            "snapshot_key": snapshot_key,
            "domain_key": domain_key,
            "page_url": page_url,
            "run_date": run_date,
            "extracted_at": now,
            "pattern_id": pattern_id,
            "content_fingerprint": content_fingerprint,
            "job_keys": current_job_keys,
            "job_details": job_details,
            "job_count": len(current_job_keys),
            "added_job_keys": added_job_keys,
            "removed_job_keys": removed_job_keys,
            "unchanged_job_keys": unchanged_job_keys,
        }
        await self._mongodb_service.upsert_domain_job_snapshot(snapshot_key, snapshot)
        return snapshot

    async def _extract_jobs_from_cached_pattern_page(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        raw_url: str,
        domain_key: str,
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
        pattern_source: dict[str, Any],
    ) -> dict[str, Any]:
        page_url = str(pattern_source.get("page_url") or "").strip()
        pattern = dict(pattern_source.get("pattern") or {})
        nav_response = await navigate_to_url(
            browser_session.page if browser_session is not None else None,
            agent_index=agent_index,
            tab_handle=agent_tab["handle"],
            url=page_url,
            post_navigation_delay_ms=0,
        )
        print('\n\n\n\n\n\n')
        navigation_result = {**nav_response}
        navigation_result["navigation_url"] = page_url
        navigation_result["classified_job_listing_url"] = page_url
        navigation_result["job_listing_pattern_url"] = page_url
        navigation_result["rerun_used_cached_pattern"] = True

        if nav_response.get("status") != "navigated":
            navigation_result["status"] = nav_response.get("status") or "navigation_failed"
            navigation_result["job_listing_pattern"] = {
                "status": "pattern_cache_navigation_failed",
                "page_url": page_url,
                "pattern": pattern,
                "error": nav_response.get("error"),
            }
            return navigation_result

        html = await extract_clean_html(browser_session.page if browser_session is not None else None)
        content_fingerprint = self._fingerprint_text(html.strip())
        extraction_result = extract_jobs_with_diagnostics(html, pattern, base_url=page_url)
        jobs = extraction_result["jobs"]
        diagnostics = extraction_result["diagnostics"]
        validation = validate_jobs(jobs, pattern, diagnostics=diagnostics)

        navigation_result["extracted_url"] = page_url
        navigation_result["extracted_content_fingerprint"] = content_fingerprint
        navigation_result["extracted_content_length"] = len(html)
        navigation_result["extracted_at"] = datetime.utcnow().isoformat()
        navigation_result["jobs_listed_on_page"] = [
            str(job.get("job_url") or "").strip()
            for job in jobs
            if str(job.get("job_url") or "").strip()
        ]
        navigation_result["job_listing_pattern"] = {
            "status": "pattern_ready" if validation.get("valid") else "pattern_validation_failed",
            "page_url": page_url,
            "pattern": pattern,
            "jobs": jobs,
            "validation": validation,
            "diagnostics": diagnostics,
            "reused_cached_pattern": True,
        }

        if not validation.get("valid") or not jobs:
            repaired_pattern_result = await generate_job_listing_pattern(
                browser_session.page if browser_session is not None else None,
                url=page_url,
                example_jobs=jobs,
                seed_failed_pattern=pattern,
                seed_extracted_jobs=jobs,
                seed_validation=validation,
            )
            navigation_result["job_listing_pattern"] = repaired_pattern_result
            repaired_jobs = list(repaired_pattern_result.get("jobs") or [])
            repaired_validation = dict(repaired_pattern_result.get("validation") or {})
            if repaired_pattern_result.get("status") != "pattern_ready" or not repaired_jobs:
                navigation_result["status"] = "cached_pattern_failed"
                navigation_result["error"] = "; ".join(repaired_validation.get("problems") or []) or "Cached pattern repair extracted no jobs."
                return navigation_result
            pattern = dict(repaired_pattern_result.get("pattern") or pattern)
            jobs = repaired_jobs
            navigation_result["jobs_listed_on_page"] = [
                str(job.get("job_url") or "").strip()
                for job in jobs
                if str(job.get("job_url") or "").strip()
            ]

        pattern_id = str(
            (navigation_result.get("job_listing_pattern") or {}).get("generated_at")
            or pattern_source.get("pattern_id")
            or pattern_source.get("page_url")
            or page_url
        )
        snapshot = await self._store_listing_job_snapshot(
            process_id=process_id,
            client_key=client_key,
            client_name=client_name,
            raw_url=raw_url,
            domain_key=domain_key,
            page_url=page_url,
            pattern_id=pattern_id,
            content_fingerprint=content_fingerprint,
            jobs=jobs,
        )
        navigation_result["status"] = "jobs_listed_on_page"
        navigation_result["listing_job_snapshot"] = {
            "snapshot_key": snapshot.get("snapshot_key"),
            "run_date": snapshot.get("run_date"),
            "job_count": snapshot.get("job_count"),
            "added_job_keys": snapshot.get("added_job_keys"),
            "removed_job_keys": snapshot.get("removed_job_keys"),
            "unchanged_job_keys": snapshot.get("unchanged_job_keys"),
        }
        return navigation_result

    async def _run_cached_job_page_rerun(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        raw_url: str,
        domain_key: str,
        cached_pattern_sources: list[dict[str, Any]],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> dict[str, Any]:
        cached_results: list[dict[str, Any]] = []
        failed_cached_pages: list[str] = []
        snapshot_changed = False
        for pattern_source in cached_pattern_sources:
            result = await self._extract_jobs_from_cached_pattern_page(
                process_id=process_id,
                client_key=client_key,
                client_name=client_name,
                raw_url=raw_url,
                domain_key=domain_key,
                browser_session=browser_session,
                agent_index=agent_index,
                agent_tab=agent_tab,
                pattern_source=pattern_source,
            )
            cached_results.append(result)
            if result.get("status") != "jobs_listed_on_page":
                failed_page_url = str(pattern_source.get("page_url") or "").strip()
                if failed_page_url:
                    failed_cached_pages.append(failed_page_url)
            snapshot = result.get("listing_job_snapshot") or {}
            if snapshot.get("added_job_keys") or snapshot.get("removed_job_keys"):
                snapshot_changed = True

        fallback_result = {"overview": {}, "career_pages_analysis": [], "job_listing_patterns": []}
        if failed_cached_pages:
            fallback_result = await career_page_category_node(
                failed_cached_pages,
                browser_session,
                agent_index,
                agent_tab,
            )
            await self._store_listing_snapshots_from_career_result(
                process_id=process_id,
                client_key=client_key,
                client_name=client_name,
                raw_url=raw_url,
                domain_key=domain_key,
                career_page_result=fallback_result,
            )

        merged_analysis = [
            result for result in cached_results if result.get("status") == "jobs_listed_on_page"
        ] + list(fallback_result.get("career_pages_analysis") or [])
        overview = _build_career_page_overview(merged_analysis)
        return {
            "overview": overview,
            "career_pages_analysis": merged_analysis,
            "job_listing_patterns": _collect_job_listing_patterns(merged_analysis),
            "cached_pattern_page_count": len(cached_pattern_sources),
            "cached_pattern_failed_count": len(failed_cached_pages),
            "cached_snapshot_changed": snapshot_changed,
        }

    async def _store_listing_snapshots_from_career_result(
        self,
        *,
        process_id: str,
        client_key: str,
        client_name: str,
        raw_url: str,
        domain_key: str,
        career_page_result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        snapshots: list[dict[str, Any]] = []
        for page in list(career_page_result.get("career_pages_analysis") or []):
            pattern_result = page.get("job_listing_pattern")
            if not isinstance(pattern_result, dict):
                continue
            jobs = list(pattern_result.get("jobs") or [])
            if not jobs:
                continue
            page_url = str(
                page.get("job_listing_pattern_url")
                or page.get("classified_job_listing_url")
                or pattern_result.get("page_url")
                or page.get("extracted_url")
                or page.get("url")
                or ""
            ).strip()
            if not page_url:
                continue
            snapshot = await self._store_listing_job_snapshot(
                process_id=process_id,
                client_key=client_key,
                client_name=client_name,
                raw_url=raw_url,
                domain_key=domain_key,
                page_url=page_url,
                pattern_id=str(pattern_result.get("generated_at") or page_url),
                content_fingerprint=page.get("extracted_content_fingerprint"),
                jobs=jobs,
            )
            page["listing_job_snapshot"] = {
                "snapshot_key": snapshot.get("snapshot_key"),
                "run_date": snapshot.get("run_date"),
                "job_count": snapshot.get("job_count"),
                "added_job_keys": snapshot.get("added_job_keys"),
                "removed_job_keys": snapshot.get("removed_job_keys"),
                "unchanged_job_keys": snapshot.get("unchanged_job_keys"),
            }
            snapshots.append(snapshot)
        return snapshots

    def _filter_rerun_career_urls(
        self,
        urls: list[str],
        not_job_related_urls: list[str],
        general_job_info_urls: list[str] | None = None,
    ) -> list[str]:
        excluded = {
            self._normalize_url_for_compare(url)
            for url in [*not_job_related_urls, *(general_job_info_urls or [])]
        }
        filtered: list[str] = []
        seen: set[str] = set()
        for url in urls:
            normalized = self._normalize_url_for_compare(url)
            if not normalized or normalized in seen or normalized in excluded:
                continue
            seen.add(normalized)
            filtered.append(str(url).strip())
        return filtered

    def _rerun_career_urls_changed(self, *, previous_urls: list[str], current_urls: list[str]) -> bool:
        previous_set = {self._normalize_url_for_compare(url) for url in previous_urls if self._normalize_url_for_compare(url)}
        current_set = {self._normalize_url_for_compare(url) for url in current_urls if self._normalize_url_for_compare(url)}
        return previous_set != current_set

    def _normalize_url_for_compare(self, url: str | None) -> str:
        value = str(url or "").strip().lower()
        if not value:
            return ""
        value = value.rstrip("/")
        return value

    def _fingerprint_text(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _build_previous_page_analysis_map(self, career_page_result: dict[str, Any]) -> dict[str, dict[str, Any]]:
        previous_map: dict[str, dict[str, Any]] = {}
        for page in list(career_page_result.get("career_pages_analysis") or []):
            page_url = (
                page.get("extracted_url")
                or page.get("current_url")
                or page.get("url")
                or page.get("navigation_url")
                or ""
            )
            normalized = self._normalize_url_for_compare(str(page_url))
            if normalized:
                previous_map[normalized] = dict(page)
        return previous_map

    def _should_retry_failed_previous_page(self, previous_page: dict[str, Any] | None) -> bool:
        if not previous_page:
            return False

        status = str(previous_page.get("status") or "").strip()
        page_access_status = str(previous_page.get("page_access_status") or "").strip()
        error_text = str(previous_page.get("error") or "").strip()

        retryable_statuses = {
            "navigation_skipped",
            "navigation_timeout",
            "navigation_non_web_url",
            "action_failed",
            "download_started",
            "extraction_failed",
            "ai_analysis_failed",
            "access_issue",
        }
        retryable_page_access_statuses = {
            "blocked",
            "forbidden",
            "login_required",
            "captcha",
            "bot_check",
            "rate_limited",
            "timeout",
            "unknown",
        }
        retryable_error_signatures = (
            "timeout",
            "unable to extract page content",
            "access denied",
            "forbidden",
            "captcha",
            "blocked",
            "navigation",
            "connection closed",
            "session closed",
            "browser has been closed",
            "page has been closed",
        )

        if status in retryable_statuses:
            return True
        if page_access_status and page_access_status != "accessible" and page_access_status in retryable_page_access_statuses:
            return True

        normalized_error = error_text.lower()
        return any(signature in normalized_error for signature in retryable_error_signatures)

    async def _build_rerun_career_page_result(
        self,
        *,
        career_urls: list[str],
        previous_career_page_result: dict[str, Any],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> dict[str, Any]:
        previous_analysis_map = self._build_previous_page_analysis_map(previous_career_page_result)
        reused_results: list[dict[str, Any]] = []
        prechecked_failures: list[dict[str, Any]] = []
        changed_urls: list[str] = []

        for career_url in career_urls:
            normalized_url = self._normalize_url_for_compare(career_url)
            previous_page = previous_analysis_map.get(normalized_url)
            if self._should_retry_failed_previous_page(previous_page):
                changed_urls.append(career_url)
                continue

            previous_markdown = str((previous_page or {}).get("extracted_content") or "").strip()
            if not previous_markdown:
                changed_urls.append(career_url)
                continue

            nav_response = await navigate_to_url(
                browser_session.page if browser_session is not None else None,
                agent_index=agent_index,
                tab_handle=agent_tab["handle"],
                url=career_url,
                post_navigation_delay_ms=0,
            )
            if nav_response.get("status") != "navigated":
                prechecked_failures.append({**nav_response, "navigation_url": career_url})
                continue

            extracted_content_response = await extract_page_content(
                browser_session.page if browser_session is not None else None,
                sections=["body"],
            )
            current_markdown = str((extracted_content_response or {}).get("markdown") or "").strip()
            if not current_markdown:
                failure_record = {
                    **nav_response,
                    "navigation_url": career_url,
                    "status": "extraction_failed",
                    "error": "Unable to extract page content",
                }
                prechecked_failures.append(failure_record)
                continue

            if self._fingerprint_text(current_markdown) == self._fingerprint_text(previous_markdown):
                reused_result = dict(previous_page or {})
                reused_result["rerun_content_reused"] = True
                reused_result["current_url"] = nav_response.get("current_url") or reused_result.get("current_url")
                reused_results.append(reused_result)
            else:
                changed_urls.append(career_url)

        changed_result = {"overview": {}, "career_pages_analysis": []}
        if changed_urls:
            changed_result = await career_page_category_node(
                changed_urls,
                browser_session,
                agent_index,
                agent_tab,
            )

        merged_analysis = reused_results + list(changed_result.get("career_pages_analysis") or []) + prechecked_failures
        overview = _build_career_page_overview(merged_analysis)
        content_changed = bool(changed_urls or prechecked_failures)
        return {
            "career_page_result": {
                "overview": overview,
                "career_pages_analysis": merged_analysis,
                "job_listing_patterns": _collect_job_listing_patterns(merged_analysis),
            },
            "content_changed": content_changed,
            "reused_analysis_count": len(reused_results),
            "changed_page_count": len(changed_urls) + len(prechecked_failures),
        }

    def _is_stop_requested(self, process_id: str) -> bool:
        return process_id in self._stop_requests

    async def _stop_remaining_agent_urls(
        self,
        process_id: str,
        agent_index: int,
        urls: list[str],
    ) -> None:
        await self._mongodb_service.mark_urls_stopped(
            process_id,
            urls,
            agent_index=agent_index,
            reason="Process stop requested.",
        )

    async def _require_client(self, client_name: str) -> dict[str, Any]:
        client_key = self._build_client_key(client_name)
        client = await self._mongodb_service.get_client(client_key)
        if client is None:
            raise ValueError(f"Client '{client_name}' is not registered")
        return client

    async def _require_active_client(self, client_name: str) -> dict[str, Any]:
        client = await self._require_client(client_name)
        if str(client.get("api_key_status") or "").lower() != "active":
            raise ValueError(f"Client '{client_name}' does not have an active API key")
        if not client.get("api_key"):
            raise ValueError(f"Client '{client_name}' does not have an API key configured")
        return client

    async def _require_active_client_by_key(self, client_key: str) -> dict[str, Any]:
        client = await self._mongodb_service.get_client(client_key)
        if client is None:
            raise ValueError(f"Client '{client_key}' is not registered")
        if str(client.get("api_key_status") or "").lower() != "active":
            raise ValueError(f"Client '{client.get('client_name') or client_key}' does not have an active API key")
        if not client.get("api_key"):
            raise ValueError(f"Client '{client.get('client_name') or client_key}' does not have an API key configured")
        return client

    def _sanitize_client_document(self, client: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(client)
        sanitized["api_key"] = mask_api_key(client.get("api_key"))
        return sanitized

    def _is_recoverable_agent_session_error(self, error_text: str) -> bool:
        normalized = str(error_text or "").lower()
        return any(
            signature in normalized
            for signature in (
                "connection closed while reading from the driver",
                "target page, context or browser has been closed",
                "browser has been closed",
                "page has been closed",
                "context closed",
                "session closed",
                "cdp session closed",
                "websocket closed",
                "closed while reading from the driver",
            )
        )

    def _normalize_domain_key(self, raw_value: str) -> str:
        extracted = extract_domain(raw_value)
        return extracted or raw_value.strip().lower()

    def _build_empty_career_page_result(self, career_url_result: dict[str, Any]) -> dict[str, Any]:
        status = str(career_url_result.get("status") or "").strip()
        error_message = str(career_url_result.get("error_message") or "").strip() or None

        outcome = "career_page_not_analyzed"
        outcome_reason = "Career page analysis was skipped because no career URLs were available."

        if status == "no_career_page_found":
            outcome = "no_career_page_found"
            outcome_reason = "No career or job page candidates were found for this domain."
        elif status == "career_page_discovery_failed":
            outcome = "career_page_discovery_failed"
            outcome_reason = (
                "Career page discovery could not be completed."
                + (f" {error_message}" if error_message else "")
            )
        elif status == "domain_access_failed":
            outcome = "career_page_discovery_failed"
            outcome_reason = (
                f"Career page discovery failed because the domain could not be accessed."
                + (f" {error_message}" if error_message else "")
            )
        elif status == "domain_redirected":
            outcome = "career_page_domain_redirected"
            outcome_reason = (
                f"Career page discovery stopped because the domain redirected externally."
                + (f" {error_message}" if error_message else "")
            )
        elif error_message:
            outcome_reason = error_message

        return {
            "overview": {
                "outcome": outcome,
                "outcome_reason": outcome_reason,
                "jobs_found": False,
                "total_jobs_found": 0,
                "job_urls": [],
                "job_found_on_urls": [],
                "listing_ui": None,
                "job_alert": False,
                "job_alert_note": None,
                "job_alert_urls": [],
                "career_page_confirmed": False,
                "no_vacancy_urls": [],
                "general_job_info_urls": [],
                "blocked_platform_urls": {},
                "external_redirect_urls": [],
                "embedded_urls": [],
                "navigation_blocked_urls": [],
                "navigation_issues": [],
                "not_job_related_urls": [],
                "access_issue_urls": [],
                "unknown_urls": [],
                "total_urls_processed": 0,
            },
            "career_pages_analysis": [],
            "job_listing_patterns": [],
        }

    def _requested_capability_for_request(self, request: JobProcessRequest) -> RequestedCapability:
        if request.job_monitoring:
            return "job_monitoring"
        if request.ats_check and request.job_extract:
            return "ats_and_job_extract"
        if request.job_extract:
            return "job_extract"
        if request.ats_check:
            return "ats_check"
        return "career_page"

    def _fingerprint_payload(self, payload: Any) -> str:
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _extract_latest_page_text(self, career_page_result: dict[str, Any]) -> str | None:
        analyses = career_page_result.get("career_pages_analysis") or []
        for page in analyses:
            content = str(page.get("extracted_content") or "").strip()
            if content:
                return content
        return None

    def _build_result_summary(
        self,
        record: dict[str, Any],
        *,
        reused_career_discovery: bool,
        reused_ats_detection: bool,
        content_changed: bool | None,
        job_monitoring: bool,
    ) -> dict[str, Any]:
        ats_detection = record.get("ats_detection") or {}
        career_url_extraction = record.get("career_url_extraction") or {}
        jobs_extraction = record.get("jobs_extraction") or {}
        return {
            "status": record.get("status"),
            "main_domain": record.get("main_domain"),
            "career_url_status": career_url_extraction.get("status"),
            "career_url_count": len(career_url_extraction.get("career_urls") or []),
            "ats_detected": ats_detection.get("ats_detected"),
            "ats_provider": ats_detection.get("ats_provider"),
            "job_extract_requested": bool(jobs_extraction.get("requested")),
            "job_count": int(jobs_extraction.get("job_count") or 0),
            "reused_career_discovery": reused_career_discovery,
            "reused_ats_detection": reused_ats_detection,
            "content_changed": content_changed,
            "job_monitoring": job_monitoring,
        }
