from __future__ import annotations

import re
from typing import Any

from core.config import get_settings
from services.mongodb_service import MongoDBService
from services.openai_service import mask_api_key, validate_openai_api_key


class ClientService:
    def __init__(self, mongodb_service: MongoDBService) -> None:
        self._mongodb_service = mongodb_service
        self._settings = get_settings()

    async def register(
        self,
        *,
        client_name: str,
        email: str | None,
        api_key: str,
        model: str,
        grid_url: str | None,
    ) -> dict[str, Any]:
        validation = await validate_openai_api_key(api_key=api_key, model=model)
        if not validation.active:
            raise ValueError(validation.user_message or "The OpenAI API key could not be validated.")
        client = await self._mongodb_service.upsert_client_configuration(
            client_key=build_client_key(client_name),
            client_name=client_name.strip(),
            email=normalize_email(email),
            api_key=api_key,
            model=model,
            grid_url=grid_url or self._settings.selenium_remote_url,
            api_key_status="active",
            api_key_validation_error=None,
        )
        return sanitize_client_document(client)

    async def update(
        self,
        current_client_name: str,
        *,
        new_client_name: str | None,
        email: str | None,
        api_key: str | None,
        model: str | None,
        grid_url: str | None,
    ) -> dict[str, Any]:
        current_client = await self.require_client(current_client_name)
        final_client_name = str(new_client_name or current_client["client_name"]).strip()
        final_model = str(model or current_client.get("model") or "gpt-5-nano").strip()
        final_api_key = api_key or current_client.get("api_key")
        if not final_api_key:
            raise ValueError("Client does not have an API key configured")

        validation = await validate_openai_api_key(api_key=final_api_key, model=final_model)
        if not validation.active:
            raise ValueError(validation.user_message or "The OpenAI API key could not be validated.")

        updated = await self._mongodb_service.update_client_configuration(
            current_client_key=current_client["client_key"],
            new_client_key=build_client_key(final_client_name),
            client_name=final_client_name,
            email=normalize_email(email) if email is not None else current_client.get("email"),
            api_key=final_api_key,
            model=final_model,
            grid_url=grid_url if grid_url is not None else current_client.get("grid_url") or self._settings.selenium_remote_url,
            api_key_status="active",
            api_key_validation_error=None,
        )
        if updated is None:
            raise ValueError(f"Unknown client: {current_client_name}")
        return sanitize_client_document(updated)

    async def get_configuration(self, client_name: str) -> dict[str, Any]:
        return sanitize_client_document(await self.require_client(client_name))

    async def list_clients(self) -> dict[str, Any]:
        clients = await self._mongodb_service.list_clients()
        return {"count": len(clients), "clients": [sanitize_client_document(client) for client in clients]}

    async def require_client(self, client_name: str) -> dict[str, Any]:
        client = await self._mongodb_service.get_client(build_client_key(client_name))
        if client is None:
            raise ValueError(f"Unknown client: {client_name}")
        return client


def build_client_key(client_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(client_name or "").strip().lower()).strip("_")
    return normalized or "default_client"


def normalize_email(email: str | None) -> str | None:
    normalized = str(email or "").strip()
    return normalized or None


def sanitize_client_document(client: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(client)
    sanitized["api_key"] = mask_api_key(sanitized.get("api_key"))
    return sanitized
