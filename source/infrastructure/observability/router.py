from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import PlainTextResponse

from core.config import get_settings
from infrastructure.observability.collector import ObservabilityCollector

router = APIRouter(prefix="/observability", tags=["observability"])
settings = get_settings()
collector = ObservabilityCollector(settings)


def _validate_observability_access(x_registration_password: str | None) -> None:
    configured_password = str(settings.client_registration_password or "").strip()
    if configured_password and str(x_registration_password or "").strip() != configured_password:
        raise HTTPException(status_code=401, detail="Invalid registration password")


@router.get("/summary")
async def observability_summary(x_registration_password: str | None = Header(default=None)) -> dict[str, Any]:
    _validate_observability_access(x_registration_password)
    return collector.snapshot()


@router.get("/alerts")
async def observability_alerts(x_registration_password: str | None = Header(default=None)) -> dict[str, Any]:
    _validate_observability_access(x_registration_password)
    snapshot = collector.snapshot()
    return {
        "collected_at": snapshot["collected_at"],
        "alert_count": len(snapshot["alerts"]),
        "alerts": snapshot["alerts"],
    }


@router.get("/metrics", response_class=PlainTextResponse)
async def observability_metrics(x_registration_password: str | None = Header(default=None)) -> PlainTextResponse:
    return PlainTextResponse(
        collector.prometheus_metrics(),
        media_type="text/plain; version=0.0.4",
    )
