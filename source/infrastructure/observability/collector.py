from __future__ import annotations

import json
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any

import requests
from pymongo import MongoClient

from core.config import Settings, get_settings
from utils.logging import get_logger, log_event

logger = get_logger("observability")


class ObservabilityCollector:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._mongo = MongoClient(self._settings.mongodb_uri)[self._settings.mongodb_database]
        self._snapshot_cache: dict[str, Any] | None = None
        self._snapshot_cache_until = 0.0

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        if self._snapshot_cache is not None and now < self._snapshot_cache_until:
            return dict(self._snapshot_cache)
        snapshot = self._build_snapshot()
        ttl = max(0, int(self._settings.observability_cache_ttl_seconds))
        if ttl:
            self._snapshot_cache = dict(snapshot)
            self._snapshot_cache_until = now + ttl
        return snapshot

    def _build_snapshot(self) -> dict[str, Any]:
        process_metrics = self._process_metrics()
        domain_metrics = self._domain_metrics()
        email_metrics = self._email_metrics()
        browser_metrics = self._browser_metrics()
        queue_metrics = self._queue_metrics()
        container_metrics = self._container_metrics()
        llm_metrics = self._llm_metrics()
        alerts = self._alerts(
            process_metrics=process_metrics,
            domain_metrics=domain_metrics,
            email_metrics=email_metrics,
            browser_metrics=browser_metrics,
            queue_metrics=queue_metrics,
        )
        return {
            "collected_at": datetime.utcnow().isoformat(),
            "queue": queue_metrics,
            "workers": {
                "active_processes": process_metrics["running_process_count"],
                "queued_processes": process_metrics["queued_process_count"],
            },
            "browsers": browser_metrics,
            "processes": process_metrics,
            "domains": domain_metrics,
            "llm": llm_metrics,
            "email": email_metrics,
            "containers": container_metrics,
            "alerts": alerts,
        }

    def prometheus_metrics(self) -> str:
        snapshot = self.snapshot()
        lines = ["# HELP job_pipeline_observability_info Observability scrape information"]
        lines.append("# TYPE job_pipeline_observability_info gauge")
        lines.append("job_pipeline_observability_info 1")
        self._append_flat_metrics(lines, "job_pipeline_queue", snapshot["queue"])
        self._append_flat_metrics(lines, "job_pipeline_browser", snapshot["browsers"])
        self._append_flat_metrics(lines, "job_pipeline_process", snapshot["processes"])
        self._append_flat_metrics(lines, "job_pipeline_domain", snapshot["domains"])
        self._append_flat_metrics(lines, "job_pipeline_llm", snapshot["llm"])
        self._append_flat_metrics(lines, "job_pipeline_email", snapshot["email"])
        lines.append(f"job_pipeline_alert_count {len(snapshot['alerts'])}")
        return "\n".join(lines) + "\n"

    def _queue_metrics(self) -> dict[str, Any]:
        try:
            from redis import Redis

            client = Redis.from_url(self._settings.celery_broker_url, decode_responses=True)
            depth = int(client.llen(self._settings.observability_redis_queue))
            active_jobs = self._mongo[self._settings.mongodb_process_uploads_collection].count_documents(
                {"status": {"$in": ["acquiring_browser", "recovering", "running", "stop_requested"]}}
            )
            return {
                "queue_name": self._settings.observability_redis_queue,
                "depth": depth,
                "active_jobs": active_jobs,
                "available": True,
            }
        except Exception as exc:
            log_event(logger, "warning", "observability_queue_unavailable error=%s", str(exc), domain="observability")
            return {
                "queue_name": self._settings.observability_redis_queue,
                "depth": 0,
                "active_jobs": 0,
                "available": False,
                "error": str(exc),
            }

    def _browser_metrics(self) -> dict[str, Any]:
        try:
            response = requests.get(self._settings.observability_selenium_status_url, timeout=5)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            return {"available": False, "used_slots": 0, "total_slots": 0, "free_slots": 0, "saturation": 0.0, "error": str(exc)}

        value = payload.get("value") if isinstance(payload, dict) else {}
        nodes = value.get("nodes") if isinstance(value, dict) else []
        used_slots = 0
        total_slots = 0
        for node in nodes or []:
            for slot in node.get("slots") or []:
                total_slots += 1
                if slot.get("session"):
                    used_slots += 1
        saturation = used_slots / total_slots if total_slots else 0.0
        return {
            "available": True,
            "node_count": len(nodes or []),
            "used_slots": used_slots,
            "total_slots": total_slots,
            "free_slots": max(0, total_slots - used_slots),
            "saturation": saturation,
        }

    def _process_metrics(self) -> dict[str, Any]:
        collection = self._mongo[self._settings.mongodb_process_uploads_collection]
        statuses = self._status_counts(collection)
        durations = list(
            collection.find(
                {"started_at": {"$ne": None}, "completed_at": {"$ne": None}},
                {"_id": 0, "started_at": 1, "completed_at": 1},
            ).sort("completed_at", -1).limit(200)
        )
        duration_seconds = [
            max(0.0, (item["completed_at"] - item["started_at"]).total_seconds())
            for item in durations
            if item.get("started_at") and item.get("completed_at")
        ]
        stale_before = datetime.utcnow() - timedelta(seconds=self._settings.process_recovery_stale_after_seconds)
        stale_running = collection.count_documents({"status": "running", "updated_at": {"$lte": stale_before}})
        return {
            "total_process_count": collection.count_documents({}),
            "queued_process_count": statuses.get("queued", 0),
            "running_process_count": statuses.get("running", 0),
            "completed_process_count": statuses.get("completed", 0),
            "partial_completed_process_count": statuses.get("partial_completed", 0),
            "failed_process_count": statuses.get("failed", 0),
            "stopped_process_count": statuses.get("stopped", 0),
            "stale_running_process_count": stale_running,
            "avg_duration_seconds": sum(duration_seconds) / len(duration_seconds) if duration_seconds else 0.0,
            "max_duration_seconds": max(duration_seconds) if duration_seconds else 0.0,
        }

    def _domain_metrics(self) -> dict[str, Any]:
        collection = self._mongo[self._settings.mongodb_domain_runs_collection]
        statuses = self._status_counts(collection)
        total = collection.count_documents({})
        failed = statuses.get("failed", 0)
        return {
            "total_domain_count": total,
            "queued_domain_count": statuses.get("queued", 0),
            "running_domain_count": statuses.get("running", 0),
            "completed_domain_count": statuses.get("completed", 0),
            "failed_domain_count": failed,
            "stopped_domain_count": statuses.get("stopped", 0),
            "failure_rate": failed / total if total else 0.0,
        }

    def _email_metrics(self) -> dict[str, Any]:
        collection = self._mongo[self._settings.mongodb_process_uploads_collection]
        sent = collection.count_documents({"metadata.last_completion_email.status": "sent"})
        failed = collection.count_documents({"metadata.last_completion_email.status": "failed"})
        skipped = collection.count_documents({"metadata.last_completion_email.status": "skipped"})
        total = sent + failed + skipped
        return {
            "sent_count": sent,
            "failed_count": failed,
            "skipped_count": skipped,
            "failure_rate": failed / total if total else 0.0,
        }

    def _llm_metrics(self) -> dict[str, Any]:
        totals = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "records_with_tokens": 0}
        cursor = self._mongo[self._settings.mongodb_domain_runs_collection].find(
            {"result_payload": {"$ne": {}}},
            {"_id": 0, "result_payload": 1},
        ).sort("updated_at", -1).limit(500)
        for item in cursor:
            token_sets = list(self._find_token_usage(item.get("result_payload") or {}))
            for token_usage in token_sets:
                totals["input_tokens"] += int(token_usage.get("input_token") or token_usage.get("input_tokens") or 0)
                totals["output_tokens"] += int(token_usage.get("output_token") or token_usage.get("output_tokens") or 0)
                totals["total_tokens"] += int(token_usage.get("total_token") or token_usage.get("total_tokens") or 0)
                totals["records_with_tokens"] += 1
        totals["estimated_cost_usd"] = None
        return totals

    def _container_metrics(self) -> dict[str, Any]:
        if not shutil.which("docker"):
            return {"available": False, "reason": "docker_cli_not_available", "containers": []}
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "json"],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
        if result.returncode != 0:
            return {"available": False, "reason": result.stderr.strip(), "containers": []}
        containers = []
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                containers.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return {"available": True, "containers": containers}

    def _alerts(
        self,
        *,
        process_metrics: dict[str, Any],
        domain_metrics: dict[str, Any],
        email_metrics: dict[str, Any],
        browser_metrics: dict[str, Any],
        queue_metrics: dict[str, Any],
    ) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        if queue_metrics.get("depth", 0) >= self._settings.observability_queue_depth_alert_threshold:
            alerts.append({"severity": "warning", "code": "queue_depth_high", "value": queue_metrics.get("depth")})
        if domain_metrics.get("failure_rate", 0.0) >= self._settings.observability_failure_rate_alert_threshold:
            alerts.append({"severity": "warning", "code": "domain_failure_rate_high", "value": domain_metrics.get("failure_rate")})
        if email_metrics.get("failure_rate", 0.0) >= self._settings.observability_failure_rate_alert_threshold:
            alerts.append({"severity": "warning", "code": "email_failure_rate_high", "value": email_metrics.get("failure_rate")})
        if browser_metrics.get("saturation", 0.0) >= self._settings.observability_browser_saturation_alert_threshold:
            alerts.append({"severity": "warning", "code": "browser_saturation_high", "value": browser_metrics.get("saturation")})
        if process_metrics.get("stale_running_process_count", 0) > 0:
            alerts.append({"severity": "critical", "code": "stale_running_processes", "value": process_metrics.get("stale_running_process_count")})
        return alerts

    def _status_counts(self, collection: Any) -> dict[str, int]:
        result: dict[str, int] = {}
        for item in collection.aggregate([{"$group": {"_id": "$status", "count": {"$sum": 1}}}]):
            result[str(item.get("_id") or "unknown")] = int(item.get("count") or 0)
        return result

    def _find_token_usage(self, value: Any):
        if isinstance(value, dict):
            if isinstance(value.get("token_used"), dict):
                yield value["token_used"]
            for child in value.values():
                yield from self._find_token_usage(child)
        elif isinstance(value, list):
            for child in value:
                yield from self._find_token_usage(child)

    def _append_flat_metrics(self, lines: list[str], prefix: str, payload: dict[str, Any]) -> None:
        for key, value in payload.items():
            if isinstance(value, bool):
                lines.append(f"{prefix}_{key} {1 if value else 0}")
            elif isinstance(value, (int, float)):
                lines.append(f"{prefix}_{key} {value}")
