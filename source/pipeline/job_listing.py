from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any


def collect_jobs_for_report(domain_key: str, career_page_result: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in list(career_page_result.get("career_pages_analysis") or []):
        page_url = listing_page_url(page)
        llm_analysis = dict(page.get("llm_analysis") or {})
        for job in list(llm_analysis.get("jobs_listed_on_page") or []):
            append_job(jobs, seen, domain_key, page_url, job, "llm_analysis")

        pattern_result = page.get("job_listing_pattern")
        if isinstance(pattern_result, dict):
            for job in list(pattern_result.get("jobs") or []):
                append_job(jobs, seen, domain_key, page_url, job, "job_listing_pattern")

        for job_url in list(page.get("jobs_listed_on_page") or []):
            append_job(jobs, seen, domain_key, page_url, job_url, "page_jobs_listed")
    return jobs


def append_job(
    jobs: list[dict[str, Any]],
    seen: set[str],
    domain_key: str,
    page_url: str,
    job: Any,
    source: str,
) -> None:
    title = job_title(job)
    url = job_url(job)
    if not title and not url:
        return
    key_source = url or f"{page_url}|{title}".lower()
    key = fingerprint_text(f"{domain_key}|{key_source}")
    if key in seen:
        return
    seen.add(key)
    jobs.append(
        {
            "job_key": key,
            "title": title,
            "job_url": url,
            "listing_page_url": page_url,
            "source": source,
        }
    )


def attach_listing_snapshots(
    career_page_result: dict[str, Any],
    *,
    current_job_keys: list[str],
    added_job_keys: list[str],
    removed_job_keys: list[str],
    unchanged_job_keys: list[str],
) -> None:
    snapshot = {
        "run_date": datetime.utcnow().date().isoformat(),
        "job_count": len(current_job_keys),
        "added_job_keys": added_job_keys,
        "removed_job_keys": removed_job_keys,
        "unchanged_job_keys": unchanged_job_keys,
    }
    for page in list(career_page_result.get("career_pages_analysis") or []):
        page["listing_job_snapshot"] = snapshot


def listing_page_url(page: dict[str, Any]) -> str:
    pattern_result = page.get("job_listing_pattern")
    pattern_page_url = pattern_result.get("page_url") if isinstance(pattern_result, dict) else None
    return str(
        page.get("job_listing_pattern_url")
        or page.get("classified_job_listing_url")
        or pattern_page_url
        or page.get("extracted_url")
        or page.get("current_url")
        or page.get("url")
        or page.get("navigation_url")
        or ""
    ).strip()


def job_title(job: Any) -> str | None:
    if isinstance(job, dict):
        return str(job.get("title") or job.get("job_title") or "").strip() or None
    return None


def job_url(job: Any) -> str | None:
    if isinstance(job, dict):
        return str(job.get("job_url") or job.get("url") or "").strip() or None
    return str(job or "").strip() or None


def fingerprint_text(value: str) -> str:
    return hashlib.sha256(str(value or "").strip().lower().encode("utf-8")).hexdigest()
