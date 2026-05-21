from __future__ import annotations

from typing import Any


def empty_summary(domain_count: int, agent_count: int) -> dict[str, int]:
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


def failed_summary(domain_count: int, agent_count: int) -> dict[str, int]:
    summary = empty_summary(domain_count, agent_count)
    summary.update(
        {
            "processed_domain_count": domain_count,
            "failed_domain_count": domain_count,
        }
    )
    return summary


def build_process_summary(
    *,
    domains: list[dict[str, Any]],
    assignments: list[dict[str, Any]],
    worker_results: list[dict[str, Any]],
) -> dict[str, int]:
    records = [record for worker in worker_results for record in worker.get("domain_results", [])]
    completed_count = sum(1 for record in records if record.get("status") == "completed")
    failed_count = sum(1 for record in records if record.get("status") == "failed")
    stopped_count = sum(1 for record in records if record.get("status") == "stopped")
    return {
        "total_domain_count": len(domains),
        "assigned_agent_count": len(assignments),
        "processed_domain_count": completed_count + failed_count + stopped_count,
        "completed_domain_count": completed_count,
        "failed_domain_count": failed_count,
        "stopped_domain_count": stopped_count,
        "job_count": sum(len(record.get("extracted_jobs") or []) for record in records),
        "new_job_count": sum(len(((record.get("career_page_result") or {}).get("added_job_keys") or [])) for record in records),
    }


def build_process_summary_from_domain_runs(
    *,
    domains: list[dict[str, Any]],
    assignments: list[dict[str, Any]],
    domain_runs: list[dict[str, Any]],
) -> dict[str, int]:
    completed_count = sum(1 for item in domain_runs if item.get("status") == "completed")
    failed_count = sum(1 for item in domain_runs if item.get("status") == "failed")
    stopped_count = sum(1 for item in domain_runs if item.get("status") == "stopped")
    return {
        "total_domain_count": len(domains),
        "assigned_agent_count": len(assignments),
        "processed_domain_count": completed_count + failed_count + stopped_count,
        "completed_domain_count": completed_count,
        "failed_domain_count": failed_count,
        "stopped_domain_count": stopped_count,
        "job_count": sum(len(item.get("current_job_keys") or []) for item in domain_runs),
        "new_job_count": sum(len(item.get("added_job_keys") or []) for item in domain_runs),
    }


def derive_completion_status(
    *,
    stop_requested: bool,
    completed_count: int,
    failed_count: int,
    stopped_count: int,
    errors: list[str],
) -> str:
    if stop_requested or stopped_count:
        return "stopped" if completed_count == 0 else "partial_completed"
    if completed_count > 0 and (failed_count > 0 or errors):
        return "partial_completed"
    if completed_count > 0:
        return "completed"
    return "failed" if failed_count > 0 or errors else "completed"


def derive_status_from_summary(summary: dict[str, int], *, stop_requested: bool, errors: list[str]) -> str:
    return derive_completion_status(
        stop_requested=stop_requested,
        completed_count=int(summary.get("completed_domain_count") or 0),
        failed_count=int(summary.get("failed_domain_count") or 0),
        stopped_count=int(summary.get("stopped_domain_count") or 0),
        errors=errors,
    )
