from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from core.config import get_settings
from models.process import ClientRegistrationRequest, ClientUpdateRequest, JobProcessRequest
from services.email_service import (
    IMPORTANT_CSV_FIELDS,
    ROLES_CSV_FIELDS,
    build_process_csv_bundle_attachment,
    build_process_important_csv_rows,
    build_process_roles_csv_rows,
)
from services.file_input_service import FileInputService
from tasks.process_tasks import execute_process_task, process_task_service
from utils.logging import get_logger, log_event

router = APIRouter()
job_process_service = process_task_service
file_input_service = FileInputService()
logger = get_logger("process_routes")
settings = get_settings()


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _safe_filename(value: str) -> str:
    return "".join(character if character.isalnum() or character in {"-", "_", "."} else "_" for character in value)


def _downloadable_json_response(payload: Any, filename: str) -> StreamingResponse:
    content = json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default)
    return StreamingResponse(
        iter([content.encode("utf-8")]),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _downloadable_csv_response(
    rows: list[dict[str, Any]],
    filename: str,
    fieldnames: list[str] | None = None,
) -> StreamingResponse:
    output = io.StringIO()
    resolved_fieldnames = fieldnames or (list(rows[0].keys()) if rows else [])
    writer = csv.DictWriter(output, fieldnames=resolved_fieldnames, extrasaction="ignore")
    if resolved_fieldnames:
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_default(value) for key, value in row.items()})
    content = output.getvalue()
    output.close()
    return StreamingResponse(
        iter([content.encode("utf-8")]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _validate_admin_password(x_registration_password: str | None) -> None:
    configured_password = str(settings.client_registration_password or "").strip()
    provided_password = str(x_registration_password or "").strip()
    if not configured_password:
        raise HTTPException(status_code=500, detail="Client registration password is not configured")
    if provided_password != configured_password:
        raise HTTPException(status_code=401, detail="Invalid registration password")


def _maybe_run_in_background(background_tasks: BackgroundTasks, process_id: str) -> None:
    if settings.process_execution_mode == "background":
        background_tasks.add_task(execute_process_task, process_id)
    elif settings.process_execution_mode == "celery":
        from infrastructure.queue.dispatcher import enqueue_process_execution

        enqueue_process_execution(process_id)


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    log_event(logger, "info", "healthcheck_requested", domain="api")
    return {"status": "ok"}


@router.post("/clients")
async def register_client(
    request: ClientRegistrationRequest,
    x_registration_password: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_admin_password(x_registration_password)
    try:
        client = await job_process_service.register_client(
            client_name=request.client_name,
            email=request.email,
            api_key=request.api_key,
            model=request.model,
            grid_url=request.grid_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ready", "client": client, "message": "Client is ready."}


@router.get("/clients")
async def list_clients(x_registration_password: str | None = Header(default=None)) -> dict[str, Any]:
    _validate_admin_password(x_registration_password)
    return await job_process_service.list_clients()


@router.patch("/clients/{client_name}/config")
async def update_client(
    client_name: str,
    request: ClientUpdateRequest,
    x_registration_password: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_admin_password(x_registration_password)
    try:
        client = await job_process_service.update_client(
            client_name,
            new_client_name=request.client_name,
            email=request.email,
            api_key=request.api_key,
            model=request.model,
            grid_url=request.grid_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "updated", "client": client}


@router.get("/clients/{client_name}/config")
async def get_client_config(client_name: str) -> dict[str, Any]:
    try:
        return await job_process_service.get_client_configuration(client_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/processes")
async def create_process(
    request: JobProcessRequest,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    log_event(
        logger,
        "info",
        "process_create_requested url_count=%s agent_count=%s",
        len(request.urls),
        request.agent_count,
        domain=request.urls[0] if request.urls else "unknown",
        url_count=len(request.urls),
        agent_count=request.agent_count,
    )
    try:
        process_document = await job_process_service.submit_process(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _maybe_run_in_background(background_tasks, process_document["process_id"])
    return {"process_id": process_document["process_id"], "status": process_document["status"]}


@router.post("/processes/upload")
async def create_process_from_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_name: str = Form("default_client"),
    agent_count: int = Form(1),
    task_id: str | None = Form(None),
) -> dict[str, str]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename")

    content = await file.read()
    try:
        domain_inputs = file_input_service.extract_domain_inputs(file.filename, content)
    except ValueError as exc:
        log_event(
            logger,
            "warning",
            "process_upload_invalid_file filename=%s error=%s",
            file.filename,
            str(exc),
            domain=file.filename,
            upload_filename=file.filename,
            error=str(exc),
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    request = JobProcessRequest(
        client_name=client_name,
        urls=[item.domain for item in domain_inputs],
        agent_count=agent_count,
        task_id=task_id,
    )
    supplied_career_page_count = sum(1 for item in domain_inputs if item.career_page_url)
    log_event(
        logger,
        "info",
        "process_upload_requested filename=%s domain_count=%s agent_count=%s supplied_career_page_count=%s",
        file.filename,
        len(domain_inputs),
        agent_count,
        supplied_career_page_count,
        domain=domain_inputs[0].domain if domain_inputs else file.filename,
        upload_filename=file.filename,
        client_name=client_name,
        domain_count=len(domain_inputs),
        agent_count=agent_count,
        supplied_career_page_count=supplied_career_page_count,
    )
    try:
        process_document = await job_process_service.submit_process(
            request,
            domain_inputs=domain_inputs,
            upload_filename=file.filename,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _maybe_run_in_background(background_tasks, process_document["process_id"])
    return {"process_id": process_document["process_id"], "status": process_document["status"]}


@router.get("/processes")
async def list_processes(client_name: str | None = None, page: int = 1, page_size: int = 10) -> dict[str, Any]:
    log_event(
        logger,
        "info",
        "process_list_requested page=%s page_size=%s",
        page,
        page_size,
        domain="api",
        page=page,
        page_size=page_size,
    )
    try:
        if client_name:
            return await job_process_service.list_processes_for_client(client_name, page=page, page_size=page_size)
        return await job_process_service.list_processes(page=page, page_size=page_size)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/processes/{process_id}")
async def get_process(process_id: str) -> StreamingResponse:
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    return _downloadable_json_response(process, f"process_{_safe_filename(process_id)}.json")


@router.post("/processes/{process_id}/rerun")
async def rerun_process(
    process_id: str,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    try:
        process_document = await job_process_service.submit_rerun_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _maybe_run_in_background(background_tasks, process_document["process_id"])
    return {"process_id": process_document["process_id"], "status": process_document["status"]}


@router.post("/processes/{process_id}/stop")
async def stop_process(process_id: str) -> dict[str, Any]:
    try:
        return await job_process_service.stop_process(process_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/processes/{process_id}/important.csv")
async def get_process_important_csv(process_id: str) -> StreamingResponse:
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    return _downloadable_csv_response(
        build_process_important_csv_rows(process),
        f"process_{_safe_filename(process_id)}_important.csv",
        fieldnames=IMPORTANT_CSV_FIELDS,
    )


@router.get("/processes/{process_id}/important-roles.csv")
async def get_process_roles_csv(process_id: str) -> StreamingResponse:
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    return _downloadable_csv_response(
        build_process_roles_csv_rows(process),
        f"process_{_safe_filename(process_id)}_roles.csv",
        fieldnames=ROLES_CSV_FIELDS,
    )


@router.get("/processes/{process_id}/csv-bundle.zip")
async def download_process_csv_bundle(process_id: str) -> StreamingResponse:
    process = await job_process_service.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    attachment = build_process_csv_bundle_attachment(process)
    return StreamingResponse(
        iter([attachment.content]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{attachment.filename}"'},
    )
