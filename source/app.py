from __future__ import annotations

import asyncio
from pathlib import Path
from contextlib import suppress

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from api.process_routes import router as process_router
from core.config import get_settings
from infrastructure.observability.router import router as observability_router
from infrastructure.queue.recovery import recover_and_requeue_interrupted_processes
from services.mongodb_service import MongoDBService
from utils.logging import get_logger, log_event

app = FastAPI(title="Career Page Job Extractor", root_path="")
logger = get_logger("app")
ui_directory = Path(__file__).resolve().parent / "ui"
mongodb_service = MongoDBService()
settings = get_settings()
recovery_task: asyncio.Task | None = None

app.include_router(process_router, prefix="/api")
if settings.observability_enabled:
    app.include_router(observability_router, prefix="/api")
app.mount("/assets", StaticFiles(directory=ui_directory), name="ui-assets")
log_event(
    logger,
    "info",
    "fastapi_application_initialized",
    domain="api",
)


@app.on_event("startup")
async def ensure_mongodb_indexes() -> None:
    global recovery_task
    await mongodb_service.ensure_indexes()
    log_event(logger, "info", "fastapi_startup_indexes_ensured", domain="mongodb")
    if settings.process_execution_mode == "celery":
        recovered = await recover_and_requeue_interrupted_processes()
        log_event(
            logger,
            "info",
            "fastapi_startup_recovery_completed recovered_count=%s",
            len(recovered),
            domain="queue_recovery",
            recovered_count=len(recovered),
        )
        recovery_task = asyncio.create_task(_periodic_queue_recovery())


@app.on_event("shutdown")
async def stop_periodic_recovery() -> None:
    if recovery_task is None:
        return
    recovery_task.cancel()
    with suppress(asyncio.CancelledError):
        await recovery_task


async def _periodic_queue_recovery() -> None:
    interval = max(10, int(settings.process_recovery_interval_seconds))
    while True:
        await asyncio.sleep(interval)
        try:
            recovered = await recover_and_requeue_interrupted_processes()
            if recovered:
                log_event(
                    logger,
                    "warning",
                    "fastapi_periodic_recovery_completed recovered_count=%s",
                    len(recovered),
                    domain="queue_recovery",
                    recovered_count=len(recovered),
                )
        except Exception as exc:
            log_event(
                logger,
                "exception",
                "fastapi_periodic_recovery_failed error=%s",
                exc,
                domain="queue_recovery",
                error=str(exc),
            )


def _render_ui_html(request: Request) -> HTMLResponse:
    log_event(logger, "info", "ui_index_requested", domain="ui")
    forwarded_prefix = str(request.headers.get("x-forwarded-prefix", "") or "").strip()
    root_path = forwarded_prefix or str(request.scope.get("root_path", "") or "") or str(app.root_path or "")
    if root_path and not root_path.startswith("/"):
        root_path = f"/{root_path}"
    root_path = root_path.rstrip("/")
    api_base = f"{request.base_url.scheme}://{request.base_url.netloc}{root_path}/api/"
    asset_base = f"{root_path}/assets/"
    html = (ui_directory / "index.html").read_text(encoding="utf-8")
    html = html.replace("__API_BASE__", api_base)
    html = html.replace("__ASSET_BASE__", asset_base)
    return HTMLResponse(content=html)


@app.get("/", include_in_schema=False)
async def ui_index(request: Request) -> HTMLResponse:
    return _render_ui_html(request)


@app.get("/ui", include_in_schema=False)
async def ui_index_alias(request: Request) -> HTMLResponse:
    return _render_ui_html(request)


@app.get("/ui/index.html", include_in_schema=False)
async def ui_index_file_alias(request: Request) -> HTMLResponse:
    return _render_ui_html(request)


if __name__ == "__main__":
    log_event(
        logger,
        "info",
        "uvicorn_start_requested host=%s port=%s reload=%s",
        "127.0.0.1",
        8110,
        True,
        domain="api",
        host="127.0.0.1",
        port=8110,
        reload=True,
    )
    uvicorn.run("app:app", host="127.0.0.1", port=8110, reload=True)
