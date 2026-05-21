from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from redis import Redis

from core.config import get_settings
from services.mongodb_service import MongoDBService
from utils.logging import get_logger, log_event

logger = get_logger("broker_recovery")


@dataclass(slots=True)
class BrokerRecoveryResult:
    stale_unacked_count: int
    active_process_count: int
    cleared_keys: list[str]
    action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "stale_unacked_count": self.stale_unacked_count,
            "active_process_count": self.active_process_count,
            "cleared_keys": self.cleared_keys,
            "action": self.action,
        }


async def clear_stale_unacked_when_mongo_idle(mongodb_service: MongoDBService | None = None) -> BrokerRecoveryResult:
    settings = get_settings()
    mongo = mongodb_service or MongoDBService()
    active_process_count = await mongo.count_active_processes()
    redis_client = Redis.from_url(settings.celery_broker_url, decode_responses=True)
    unacked_count = _unacked_count(redis_client, settings.celery_unacked_key)

    if unacked_count <= 0:
        return BrokerRecoveryResult(0, active_process_count, [], "none")

    if active_process_count > 0:
        log_event(
            logger,
            "info",
            "broker_unacked_preserved unacked=%s active_processes=%s",
            unacked_count,
            active_process_count,
            domain="broker_recovery",
            unacked_count=unacked_count,
            active_process_count=active_process_count,
        )
        return BrokerRecoveryResult(unacked_count, active_process_count, [], "preserved_active_processes")

    keys = _celery_unacked_keys(settings.celery_unacked_key)
    cleared = [key for key in keys if redis_client.delete(key)]
    log_event(
        logger,
        "warning",
        "broker_stale_unacked_cleared unacked=%s active_processes=%s cleared_keys=%s",
        unacked_count,
        active_process_count,
        cleared,
        domain="broker_recovery",
        unacked_count=unacked_count,
        active_process_count=active_process_count,
        cleared_keys=cleared,
    )
    return BrokerRecoveryResult(unacked_count, active_process_count, cleared, "cleared_stale_unacked")


def _unacked_count(redis_client: Redis, key: str) -> int:
    try:
        key_type = redis_client.type(key)
        if key_type == "hash":
            return int(redis_client.hlen(key))
        if key_type == "list":
            return int(redis_client.llen(key))
        if key_type == "set":
            return int(redis_client.scard(key))
        if key_type == "zset":
            return int(redis_client.zcard(key))
    except Exception as exc:
        log_event(
            logger,
            "warning",
            "broker_unacked_count_failed key=%s error=%s",
            key,
            exc,
            domain="broker_recovery",
            key=key,
            error=str(exc),
        )
    return 0


def _celery_unacked_keys(base_key: str) -> list[str]:
    return [base_key, f"{base_key}_index", f"{base_key}_mutex"]
