from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from core.config import get_settings
from services.grid_session import (
    attach_playwright_to_cdp,
    close_browser_attachment,
    close_shared_session_async,
    create_session_async,
    is_grid_session_active_async,
)
from services.tab_manager import ensure_agent_tab
from utils.logging import get_logger, log_event

logger = get_logger("browser_session_manager")


@dataclass(slots=True)
class SharedSessionRuntime:
    grid_url: str | None
    session_id: str
    cdp_url: str
    recovery_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AgentSessionRecoveryNeeded(Exception):
    pass


class BrowserSessionManager:
    async def create_process_session(self, grid_url: str | None) -> SharedSessionRuntime | None:
        settings = get_settings()
        timeout_seconds = max(30, int(settings.browser_session_acquire_timeout_seconds))
        log_event(
            logger,
            "info",
            "process_browser_session_acquire_started timeout_seconds=%s",
            timeout_seconds,
            domain=grid_url or "grid",
            timeout_seconds=timeout_seconds,
        )
        session_info = await create_session_async(
            grid_url=grid_url,
            reuse_existing=False,
            timeout_seconds=timeout_seconds,
        )
        if session_info is None or not session_info.cdp_url:
            log_event(
                logger,
                "warning",
                "process_browser_session_acquire_failed timeout_seconds=%s",
                timeout_seconds,
                domain=grid_url or "grid",
                timeout_seconds=timeout_seconds,
            )
            return None
        log_event(
            logger,
            "info",
            "process_browser_session_created session_id=%s",
            session_info.session_id,
            domain=grid_url or "grid",
            session_id=session_info.session_id,
        )
        return SharedSessionRuntime(
            grid_url=grid_url,
            session_id=session_info.session_id,
            cdp_url=session_info.cdp_url,
        )

    async def close_process_session(self, runtime: SharedSessionRuntime | None) -> None:
        if runtime is None:
            return
        await close_shared_session_async(runtime.session_id)

    async def recover_agent_tab(
        self,
        *,
        runtime: SharedSessionRuntime,
        browser_session: Any,
        agent_index: int,
        url: str,
    ) -> tuple[Any, dict[str, Any]]:
        async with runtime.recovery_lock:
            await self._refresh_runtime_if_needed(runtime)
            await close_browser_attachment(browser_session)
            rebuilt_session = await attach_playwright_to_cdp(runtime.cdp_url)
            if rebuilt_session is None:
                raise RuntimeError("Failed to reattach Playwright during agent recovery")
            rebuilt_tab = await ensure_agent_tab(rebuilt_session, agent_index=agent_index)
            log_event(
                logger,
                "info",
                "agent_tab_recovery_completed agent_index=%s session_id=%s url=%s",
                agent_index,
                runtime.session_id,
                url,
                domain=url,
                agent_index=agent_index,
                session_id=runtime.session_id,
            )
            return rebuilt_session, rebuilt_tab

    async def _refresh_runtime_if_needed(self, runtime: SharedSessionRuntime) -> None:
        if await is_grid_session_active_async(runtime.grid_url, runtime.session_id):
            return
        replacement = await create_session_async(grid_url=runtime.grid_url, reuse_existing=False)
        if replacement is None or not replacement.cdp_url:
            raise RuntimeError("Shared browser session is unavailable and could not be recreated")
        await close_shared_session_async(runtime.session_id)
        runtime.session_id = replacement.session_id
        runtime.cdp_url = replacement.cdp_url


def is_recoverable_agent_session_error(error_text: str) -> bool:
    lowered = str(error_text or "").lower()
    markers = (
        "target page, context or browser has been closed",
        "browser has been closed",
        "websocket",
        "cdp",
        "session deleted",
        "invalid session id",
    )
    return any(marker in lowered for marker in markers)
