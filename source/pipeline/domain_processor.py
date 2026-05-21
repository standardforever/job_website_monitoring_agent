from __future__ import annotations

from typing import Any

from browser.session_manager import AgentSessionRecoveryNeeded, is_recoverable_agent_session_error
from models.process import DomainProcessRecord
from nodes.career_page_category import career_page_category_node
from nodes.url_extraction import career_url_extraction_node
from pipeline.job_listing import attach_listing_snapshots, collect_jobs_for_report
from services.flow_safety import extract_domain
from services.mongodb_service import MongoDBService


class DomainProcessor:
    def __init__(self, mongodb_service: MongoDBService) -> None:
        self._mongodb_service = mongodb_service

    async def process(
        self,
        *,
        process_id: str,
        domain: dict[str, Any],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> dict[str, Any]:
        raw_url = str(domain["domain"])
        domain_key = str(domain["domain_key"])
        career_page_url = domain.get("career_page_url")
        main_domain = extract_domain(raw_url)
        await self._mongodb_service.mark_domain_running(process_id, domain_key, career_page_url, agent_index)

        try:
            career_url_result = await self._get_career_urls(raw_url, main_domain, domain_key, career_page_url, browser_session)
            career_page_result = await self._analyze_career_pages(career_url_result, browser_session, agent_index, agent_tab)
            extracted_jobs = collect_jobs_for_report(domain_key, career_page_result)
            current_job_keys = [job["job_key"] for job in extracted_jobs]
            previous_job_keys = list(domain.get("previous_job_keys") or [])
            change_set = self._build_change_set(current_job_keys, previous_job_keys)
            attach_listing_snapshots(career_page_result, current_job_keys=current_job_keys, **change_set)
            career_page_result.update(change_set)

            record = self._build_record(raw_url, main_domain, career_url_result, career_page_result, extracted_jobs)
            await self._mark_completed(process_id, domain_key, career_page_url, record, current_job_keys, change_set)
            return record
        except Exception as exc:
            if is_recoverable_agent_session_error(str(exc)):
                raise AgentSessionRecoveryNeeded(str(exc)) from exc
            return await self._mark_failed(process_id, domain_key, career_page_url, raw_url, main_domain, str(exc))

    async def _get_career_urls(
        self,
        raw_url: str,
        main_domain: str | None,
        domain_key: str,
        career_page_url: str | None,
        browser_session: Any,
    ) -> dict[str, Any]:
        if career_page_url:
            return {
                "status": "career_urls_found",
                "error_message": None,
                "career_urls": [career_page_url],
                "non_domain_career_urls": [],
                "all_urls": [career_page_url],
                "used_provided_career_page_url": True,
            }
        return await career_url_extraction_node(main_domain or domain_key or raw_url, browser_session)

    async def _analyze_career_pages(
        self,
        career_url_result: dict[str, Any],
        browser_session: Any,
        agent_index: int,
        agent_tab: dict[str, Any],
    ) -> dict[str, Any]:
        career_urls = list(career_url_result.get("career_urls") or [])
        if career_urls:
            return await career_page_category_node(career_urls, browser_session, agent_index, agent_tab)
        return build_empty_career_page_result(career_url_result)

    def _build_record(
        self,
        raw_url: str,
        main_domain: str | None,
        career_url_result: dict[str, Any],
        career_page_result: dict[str, Any],
        extracted_jobs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return DomainProcessRecord(
            domain=raw_url,
            main_domain=main_domain,
            career_url_extraction=career_url_result,
            career_page_result=career_page_result,
            job_listing_patterns=career_page_result.get("job_listing_patterns") or [],
            extracted_jobs=extracted_jobs,
            status="completed",
        ).model_dump(mode="json")

    def _build_change_set(self, current: list[str], previous: list[str]) -> dict[str, list[str]]:
        current_set = set(current)
        previous_set = set(previous)
        return {
            "added_job_keys": sorted(current_set - previous_set),
            "removed_job_keys": sorted(previous_set - current_set),
            "unchanged_job_keys": sorted(current_set & previous_set),
        }

    async def _mark_completed(
        self,
        process_id: str,
        domain_key: str,
        career_page_url: str | None,
        record: dict[str, Any],
        current_job_keys: list[str],
        change_set: dict[str, list[str]],
    ) -> None:
        await self._mongodb_service.mark_domain_completed(
            process_id,
            domain_key,
            career_page_url,
            {
                "result_summary": build_result_summary(record, new_job_count=len(change_set["added_job_keys"])),
                "result_payload": record,
                "current_job_keys": current_job_keys,
                **change_set,
            },
        )

    async def _mark_failed(
        self,
        process_id: str,
        domain_key: str,
        career_page_url: str | None,
        raw_url: str,
        main_domain: str | None,
        error_text: str,
    ) -> dict[str, Any]:
        failed_record = DomainProcessRecord(
            domain=raw_url,
            main_domain=main_domain,
            status="failed",
            error=error_text,
        ).model_dump(mode="json")
        await self._mongodb_service.mark_domain_failed(
            process_id,
            domain_key,
            career_page_url,
            error_text,
            result_payload=failed_record,
        )
        return failed_record


def build_empty_career_page_result(career_url_result: dict[str, Any]) -> dict[str, Any]:
    status = str(career_url_result.get("status") or "no_career_page_found")
    return {
        "overview": {
            "outcome": "no_career_page_found",
            "outcome_reason": career_url_result.get("error_message") or status,
            "jobs_found": False,
            "total_jobs_found": 0,
            "job_urls": [],
            "job_found_on_urls": [],
            "career_page_confirmed": False,
            "total_urls_processed": 0,
        },
        "career_pages_analysis": [],
        "job_listing_patterns": [],
    }


def build_result_summary(record: dict[str, Any], *, new_job_count: int) -> dict[str, Any]:
    career_url_extraction = dict(record.get("career_url_extraction") or {})
    career_page_result = dict(record.get("career_page_result") or {})
    overview = dict(career_page_result.get("overview") or {})
    return {
        "career_url_status": career_url_extraction.get("status"),
        "career_page_outcome": overview.get("outcome"),
        "jobs_found": overview.get("jobs_found"),
        "total_jobs_found": overview.get("total_jobs_found"),
        "extracted_job_count": len(record.get("extracted_jobs") or []),
        "new_job_count": new_job_count,
    }
