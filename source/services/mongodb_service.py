from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from typing import Any

import xxhash
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument

from core.config import get_settings
from utils.logging import get_logger, log_event

logger = get_logger("mongodb_service")
MAX_HISTORY_ENTRIES = 30


class MongoDBService:
    def __init__(self) -> None:
        settings = get_settings()
        self._uri = settings.mongodb_uri
        self._database_name = settings.mongodb_database
        self._collection_names = {
            "clients": settings.mongodb_clients_collection,
            "process_uploads": settings.mongodb_process_uploads_collection,
            "domain_runs": settings.mongodb_domain_runs_collection,
        }
        self._client: MongoClient | None = None
        self._database = None
        log_event(
            logger,
            "info",
            "mongodb_service_initialized database=%s collections=%s",
            self._database_name,
            self._collection_names,
            domain="mongodb",
            database=self._database_name,
            collections=self._collection_names,
        )

    def _get_database(self):
        if self._database is None:
            log_event(
                logger,
                "info",
                "mongodb_connecting database=%s",
                self._database_name,
                domain="mongodb",
                database=self._database_name,
            )
            self._client = MongoClient(self._uri)
            self._database = self._client[self._database_name]
        return self._database

    def _get_collection(self, key: str):
        return self._get_database()[self._collection_names[key]]

    async def ensure_indexes(self) -> None:
        await asyncio.to_thread(self._ensure_indexes_sync)

    def _ensure_indexes_sync(self) -> None:
        self._get_collection("clients").create_index(
            [("client_key", ASCENDING)],
            unique=True,
            name="clients_client_key_unique",
        )
        self._get_collection("clients").create_index(
            [("updated_at", DESCENDING)],
            name="clients_updated_at_desc",
        )
        self._get_collection("process_uploads").create_index(
            [("process_id", ASCENDING)],
            unique=True,
            name="process_uploads_process_id_unique",
        )
        self._get_collection("process_uploads").create_index(
            [("client_key", ASCENDING), ("created_at", DESCENDING)],
            name="process_uploads_client_created",
        )
        self._get_collection("process_uploads").create_index(
            [("status", ASCENDING), ("updated_at", DESCENDING)],
            name="process_uploads_status_updated",
        )
        self._get_collection("domain_runs").create_index(
            [("process_id", ASCENDING), ("domain_key", ASCENDING), ("career_page_url", ASCENDING)],
            unique=True,
            name="domain_runs_process_domain_career_unique",
        )
        self._get_collection("domain_runs").create_index(
            [("process_id", ASCENDING), ("input_index", ASCENDING)],
            name="domain_runs_process_input_order",
        )
        self._get_collection("domain_runs").create_index(
            [("process_id", ASCENDING), ("status", ASCENDING)],
            name="domain_runs_process_status",
        )
        self._get_collection("domain_runs").create_index(
            [("domain_key", ASCENDING), ("updated_at", DESCENDING)],
            name="domain_runs_domain_updated",
        )
        log_event(logger, "info", "mongodb_ensure_indexes_completed", domain="mongodb")

    async def upsert_client_configuration(
        self,
        *,
        client_key: str,
        client_name: str,
        email: str | None,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._upsert_client_configuration_sync,
            client_key,
            client_name,
            email,
            api_key,
            model,
            grid_url,
            api_key_status,
            api_key_validation_error,
        )

    def _upsert_client_configuration_sync(
        self,
        client_key: str,
        client_name: str,
        email: str | None,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any]:
        now = datetime.utcnow()
        self._get_collection("clients").update_one(
            {"client_key": client_key},
            {
                "$set": {
                    "client_name": client_name,
                    "email": email,
                    "api_key": api_key,
                    "model": model,
                    "grid_url": grid_url,
                    "api_key_status": api_key_status,
                    "api_key_last_validated_at": now,
                    "api_key_validation_error": api_key_validation_error,
                    "updated_at": now,
                },
                "$setOnInsert": {"client_key": client_key, "created_at": now},
            },
            upsert=True,
        )
        return self._get_client_sync(client_key) or {}

    async def update_client_configuration(
        self,
        *,
        current_client_key: str,
        new_client_key: str,
        client_name: str,
        email: str | None,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            self._update_client_configuration_sync,
            current_client_key,
            new_client_key,
            client_name,
            email,
            api_key,
            model,
            grid_url,
            api_key_status,
            api_key_validation_error,
        )

    def _update_client_configuration_sync(
        self,
        current_client_key: str,
        new_client_key: str,
        client_name: str,
        email: str | None,
        api_key: str,
        model: str,
        grid_url: str | None,
        api_key_status: str,
        api_key_validation_error: str | None,
    ) -> dict[str, Any] | None:
        existing = self._get_client_sync(current_client_key)
        if existing is None:
            return None
        now = datetime.utcnow()
        self._get_collection("clients").update_one(
            {"client_key": current_client_key},
            {
                "$set": {
                    "client_key": new_client_key,
                    "client_name": client_name,
                    "email": email,
                    "api_key": api_key,
                    "model": model,
                    "grid_url": grid_url,
                    "api_key_status": api_key_status,
                    "api_key_last_validated_at": now,
                    "api_key_validation_error": api_key_validation_error,
                    "updated_at": now,
                }
            },
        )
        if new_client_key != current_client_key:
            self._get_collection("process_uploads").update_many(
                {"client_key": current_client_key},
                {"$set": {"client_key": new_client_key, "client_name": client_name, "updated_at": now}},
            )
            self._get_collection("domain_runs").update_many(
                {"client_key": current_client_key},
                {"$set": {"client_key": new_client_key, "client_name": client_name, "updated_at": now}},
            )
        else:
            self._get_collection("process_uploads").update_many(
                {"client_key": current_client_key},
                {"$set": {"client_name": client_name, "updated_at": now}},
            )
            self._get_collection("domain_runs").update_many(
                {"client_key": current_client_key},
                {"$set": {"client_name": client_name, "updated_at": now}},
            )
        return self._get_client_sync(new_client_key)

    async def get_client(self, client_key: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_client_sync, client_key)

    def _get_client_sync(self, client_key: str) -> dict[str, Any] | None:
        return self._get_collection("clients").find_one({"client_key": client_key}, {"_id": 0})

    async def list_clients(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_clients_sync)

    def _list_clients_sync(self) -> list[dict[str, Any]]:
        return list(self._get_collection("clients").find({}, {"_id": 0}).sort("updated_at", DESCENDING))

    async def insert_process_upload(self, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert_process_upload_sync, document)

    def _insert_process_upload_sync(self, document: dict[str, Any]) -> None:
        self._get_collection("process_uploads").insert_one(document)
        log_event(
            logger,
            "info",
            "mongodb_insert_process_upload process_id=%s",
            document.get("process_id"),
            domain="mongodb",
            process_id=document.get("process_id"),
        )

    async def upsert_domain_runs(self, documents: list[dict[str, Any]]) -> None:
        if documents:
            await asyncio.to_thread(self._upsert_domain_runs_sync, documents)

    def _upsert_domain_runs_sync(self, documents: list[dict[str, Any]]) -> None:
        for document in documents:
            key = {
                "process_id": document["process_id"],
                "domain_key": document["domain_key"],
                "career_page_url": document.get("career_page_url"),
            }
            created_at = document.get("created_at") or datetime.utcnow()
            set_document = {key: value for key, value in document.items() if key != "created_at"}
            self._get_collection("domain_runs").update_one(
                key,
                {
                    "$set": {**set_document, "updated_at": document.get("updated_at") or datetime.utcnow()},
                    "$setOnInsert": {"created_at": created_at},
                },
                upsert=True,
            )
        log_event(
            logger,
            "info",
            "mongodb_upsert_domain_runs process_id=%s count=%s",
            documents[0].get("process_id"),
            len(documents),
            domain="mongodb",
            process_id=documents[0].get("process_id"),
            domain_run_count=len(documents),
        )

    async def get_process_upload(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_process_upload_sync, process_id)

    def _get_process_upload_sync(self, process_id: str) -> dict[str, Any] | None:
        return self._get_collection("process_uploads").find_one({"process_id": process_id}, {"_id": 0})

    async def get_process_with_domains(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_process_with_domains_sync, process_id)

    def _get_process_with_domains_sync(self, process_id: str) -> dict[str, Any] | None:
        process = self._get_process_upload_sync(process_id)
        if process is None:
            return None
        process["items"] = list(
            self._get_collection("domain_runs")
            .find({"process_id": process_id}, {"_id": 0})
            .sort("input_index", ASCENDING)
        )
        return process

    async def list_process_uploads(
        self,
        *,
        client_key: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict[str, Any]], int]:
        return await asyncio.to_thread(self._list_process_uploads_sync, client_key, page, page_size)

    def _list_process_uploads_sync(
        self,
        client_key: str | None,
        page: int,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        normalized_page = max(1, int(page or 1))
        normalized_page_size = max(1, min(int(page_size or 50), 200))
        skip = (normalized_page - 1) * normalized_page_size
        query = {"client_key": client_key} if client_key else {}
        total = self._get_collection("process_uploads").count_documents(query)
        cursor = (
            self._get_collection("process_uploads")
            .find(query, {"_id": 0})
            .sort("created_at", DESCENDING)
            .skip(skip)
            .limit(normalized_page_size)
        )
        return list(cursor), total

    async def update_process_upload(self, process_id: str, updates: dict[str, Any]) -> None:
        await asyncio.to_thread(self._update_process_upload_sync, process_id, updates)

    def _update_process_upload_sync(self, process_id: str, updates: dict[str, Any]) -> None:
        updates = {**updates, "updated_at": datetime.utcnow()}
        self._get_collection("process_uploads").update_one({"process_id": process_id}, {"$set": updates})
        log_event(
            logger,
            "info",
            "mongodb_update_process_upload process_id=%s fields=%s",
            process_id,
            sorted(updates.keys()),
            domain="mongodb",
            process_id=process_id,
            update_fields=sorted(updates.keys()),
        )

    async def begin_process_execution(self, process_id: str, worker_id: str | None = None) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._begin_process_execution_sync, process_id, worker_id)

    def _begin_process_execution_sync(self, process_id: str, worker_id: str | None) -> dict[str, Any] | None:
        now = datetime.utcnow()
        resolved_worker_id = worker_id or os.getenv("WORKER_ID") or os.uname().nodename
        claimed = self._get_collection("process_uploads").find_one_and_update(
            {"process_id": process_id, "status": "queued"},
            {
                "$set": {
                    "status": "acquiring_browser",
                    "started_at": None,
                    "updated_at": now,
                    "metadata.worker_id": resolved_worker_id,
                    "metadata.claimed_at": now,
                    "metadata.capacity_state": "acquiring_browser",
                }
            },
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
        if claimed is None:
            return None
        claimed["items"] = list(
            self._get_collection("domain_runs")
            .find({"process_id": process_id}, {"_id": 0})
            .sort("input_index", ASCENDING)
        )
        return claimed

    async def mark_process_running(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._mark_process_running_sync, process_id)

    def _mark_process_running_sync(self, process_id: str) -> dict[str, Any] | None:
        now = datetime.utcnow()
        process = self._get_collection("process_uploads").find_one_and_update(
            {"process_id": process_id, "status": "acquiring_browser"},
            {
                "$set": {
                    "status": "running",
                    "started_at": now,
                    "updated_at": now,
                    "metadata.capacity_state": "browser_acquired",
                    "metadata.browser_acquired_at": now,
                },
                "$unset": {
                    "metadata.requeue_claimed_at": "",
                    "metadata.next_browser_retry_at": "",
                    "metadata.next_browser_retry_after_seconds": "",
                    "metadata.last_browser_acquire_error": "",
                    "metadata.last_browser_wait_at": "",
                },
            },
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
        if process is None:
            return None
        process["items"] = list(
            self._get_collection("domain_runs")
            .find({"process_id": process_id}, {"_id": 0})
            .sort("input_index", ASCENDING)
        )
        return process

    async def mark_process_stop_requested(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._mark_process_stop_requested_sync, process_id)

    def _mark_process_stop_requested_sync(self, process_id: str) -> dict[str, Any] | None:
        now = datetime.utcnow()
        self._get_collection("process_uploads").update_one(
            {"process_id": process_id, "status": {"$in": ["queued", "acquiring_browser", "recovering", "running", "stop_requested"]}},
            {"$set": {"status": "stop_requested", "updated_at": now}},
        )
        return self._get_process_upload_sync(process_id)

    async def reset_process_for_rerun(self, process_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._reset_process_for_rerun_sync, process_id)

    def _reset_process_for_rerun_sync(self, process_id: str) -> dict[str, Any] | None:
        now = datetime.utcnow()
        process = self._get_process_with_domains_sync(process_id)
        if process is None:
            return None

        assignments = list(process.get("assignments") or [])
        self._reset_assignment_progress_for_rerun(assignments)

        history_item = {
            "run_at": process.get("completed_at") or process.get("updated_at") or now,
            "status": process.get("status"),
            "summary": process.get("summary") or {},
            "errors": process.get("errors") or [],
            "payload_hash": self._fingerprint_payload(
                {
                    "summary": process.get("summary") or {},
                    "errors": process.get("errors") or [],
                    "items": [
                        {
                            "domain_key": item.get("domain_key"),
                            "status": item.get("status"),
                            "result_summary": item.get("result_summary") or {},
                        }
                        for item in process.get("items") or []
                    ],
                }
            ),
        }

        domain_count = len(process.get("domains") or [])
        self._get_collection("process_uploads").update_one(
            {"process_id": process_id},
            {
                "$set": {
                    "status": "queued",
                    "assignments": assignments,
                    "summary": self._empty_summary(domain_count, len(assignments)),
                    "errors": [],
                    "metadata.workflow_mode": "rerun",
                    "metadata.last_rerun_requested_at": now,
                    "started_at": None,
                    "completed_at": None,
                    "updated_at": now,
                },
                "$push": {"history": {"$each": [history_item], "$slice": -MAX_HISTORY_ENTRIES}},
            },
        )

        for item in process.get("items") or []:
            item_history = self._domain_run_history_item(item, now)
            update_doc: dict[str, Any] = {
                "$set": {
                    "status": "queued",
                    "error": None,
                    "agent_index": None,
                    "result_summary": {},
                    "result_payload": {},
                    "added_job_keys": [],
                    "removed_job_keys": [],
                    "unchanged_job_keys": [],
                    "previous_job_keys": list(item.get("current_job_keys") or []),
                    "started_at": None,
                    "completed_at": None,
                    "updated_at": now,
                }
            }
            if item_history:
                update_doc["$push"] = {"history": {"$each": [item_history], "$slice": -MAX_HISTORY_ENTRIES}}
            self._get_collection("domain_runs").update_one(
                {
                    "process_id": process_id,
                    "domain_key": item.get("domain_key"),
                    "career_page_url": item.get("career_page_url"),
                },
                update_doc,
            )
        return self._get_process_upload_sync(process_id)

    def _empty_summary(self, domain_count: int, agent_count: int) -> dict[str, int]:
        return {
            "total_domain_count": domain_count,
            "assigned_agent_count": agent_count,
            "processed_domain_count": 0,
            "completed_domain_count": 0,
            "failed_domain_count": 0,
            "stopped_domain_count": 0,
            "job_count": 0,
            "new_job_count": 0,
        }

    def _reset_assignment_progress_for_rerun(self, assignments: list[dict[str, Any]]) -> None:
        for assignment in assignments:
            assignment["status"] = "queued"
            assignment.pop("processed_domain", None)
            assignment.pop("pending_domain", None)
            assignment.pop("failed_domain", None)

    def _domain_run_history_item(self, item: dict[str, Any], fallback_time: datetime) -> dict[str, Any] | None:
        if not item.get("result_summary") and not item.get("result_payload") and not item.get("error"):
            return None
        return {
            "run_at": item.get("completed_at") or item.get("updated_at") or fallback_time,
            "status": item.get("status"),
            "error": item.get("error"),
            "summary": item.get("result_summary") or {},
            "current_job_keys": item.get("current_job_keys") or [],
            "payload_hash": self._fingerprint_payload(item.get("result_payload") or {}),
        }

    async def update_assignment_status(self, process_id: str, agent_index: int, status: str) -> None:
        await asyncio.to_thread(self._update_assignment_status_sync, process_id, agent_index, status)

    def _update_assignment_status_sync(self, process_id: str, agent_index: int, status: str) -> None:
        self._get_collection("process_uploads").update_one(
            {"process_id": process_id, "assignments.agent_index": agent_index},
            {"$set": {"assignments.$.status": status, "updated_at": datetime.utcnow()}},
        )

    async def mark_domain_running(self, process_id: str, domain_key: str, career_page_url: str | None, agent_index: int) -> None:
        await asyncio.to_thread(self._mark_domain_running_sync, process_id, domain_key, career_page_url, agent_index)

    def _mark_domain_running_sync(self, process_id: str, domain_key: str, career_page_url: str | None, agent_index: int) -> None:
        now = datetime.utcnow()
        key = {"process_id": process_id, "domain_key": domain_key, "career_page_url": career_page_url}
        self._get_collection("domain_runs").update_one(
            key,
            {"$set": {"status": "running", "agent_index": agent_index, "started_at": now, "updated_at": now}},
        )

    async def mark_domain_completed(
        self,
        process_id: str,
        domain_key: str,
        career_page_url: str | None,
        updates: dict[str, Any],
    ) -> None:
        await asyncio.to_thread(self._mark_domain_completed_sync, process_id, domain_key, career_page_url, updates)

    def _mark_domain_completed_sync(
        self,
        process_id: str,
        domain_key: str,
        career_page_url: str | None,
        updates: dict[str, Any],
    ) -> None:
        now = datetime.utcnow()
        self._get_collection("domain_runs").update_one(
            {"process_id": process_id, "domain_key": domain_key, "career_page_url": career_page_url},
            {
                "$set": {
                    **updates,
                    "status": "completed",
                    "error": None,
                    "completed_at": now,
                    "updated_at": now,
                }
            },
        )
        job_count = len(updates.get("current_job_keys") or [])
        new_job_count = len(updates.get("added_job_keys") or [])
        self._get_collection("process_uploads").update_one(
            {"process_id": process_id},
            {
                "$inc": {
                    "summary.processed_domain_count": 1,
                    "summary.completed_domain_count": 1,
                    "summary.job_count": job_count,
                    "summary.new_job_count": new_job_count,
                },
                "$set": {"updated_at": now},
            },
        )

    async def mark_domain_failed(
        self,
        process_id: str,
        domain_key: str,
        career_page_url: str | None,
        error: str,
        *,
        result_payload: dict[str, Any] | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._mark_domain_failed_sync,
            process_id,
            domain_key,
            career_page_url,
            error,
            result_payload or {},
        )

    def _mark_domain_failed_sync(
        self,
        process_id: str,
        domain_key: str,
        career_page_url: str | None,
        error: str,
        result_payload: dict[str, Any],
    ) -> None:
        now = datetime.utcnow()
        self._get_collection("domain_runs").update_one(
            {"process_id": process_id, "domain_key": domain_key, "career_page_url": career_page_url},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "result_payload": result_payload,
                    "completed_at": now,
                    "updated_at": now,
                }
            },
        )
        self._get_collection("process_uploads").update_one(
            {"process_id": process_id},
            {
                "$inc": {
                    "summary.processed_domain_count": 1,
                    "summary.failed_domain_count": 1,
                },
                "$set": {"updated_at": now},
            },
        )

    async def mark_domains_stopped(self, process_id: str, domain_run_keys: list[dict[str, Any]]) -> None:
        if domain_run_keys:
            await asyncio.to_thread(self._mark_domains_stopped_sync, process_id, domain_run_keys)

    def _mark_domains_stopped_sync(self, process_id: str, domain_run_keys: list[dict[str, Any]]) -> None:
        now = datetime.utcnow()
        for key in domain_run_keys:
            self._get_collection("domain_runs").update_one(
                {
                    "process_id": process_id,
                    "domain_key": key.get("domain_key"),
                    "career_page_url": key.get("career_page_url"),
                    "status": {"$in": ["queued", "acquiring_browser", "recovering", "running", "stop_requested"]},
                },
                {
                    "$set": {
                        "status": "stopped",
                        "error": "Process stop requested.",
                        "completed_at": now,
                        "updated_at": now,
                    }
                },
            )

    def _fingerprint_payload(self, payload: Any) -> str:
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return xxhash.xxh64(serialized).hexdigest()
