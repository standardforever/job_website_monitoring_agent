from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from utils.logging import get_logger, log_event

logger = get_logger("config")


def load_environment() -> None:
    """Load env files from both repo root and source/ for local API runs."""
    source_env = Path(__file__).resolve().parents[1] / ".env"
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    for env_path in (repo_env, source_env):
        if env_path.exists():
            load_dotenv(env_path, override=False)
            log_event(
                logger,
                "info",
                "environment_loaded env_path=%s",
                str(env_path),
                domain="config",
                env_path=str(env_path),
            )


load_environment()


@dataclass(slots=True)
class Settings:
    selenium_remote_url: str = os.getenv("SELENIUM_REMOTE_URL", "http://127.0.0.1:4445/wd/hub")
    client_registration_password: str = os.getenv("CLIENT_REGISTRATION_PASSWORD", "")
    default_agent_count: int = int(os.getenv("DEFAULT_AGENT_COUNT", "1"))
    post_navigation_delay_ms: int = int(os.getenv("POST_NAVIGATION_DELAY_MS", "5000"))
    process_execution_mode: str = os.getenv("PROCESS_EXECUTION_MODE", "background").strip().lower()
    worker_poll_interval_seconds: float = float(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "3"))
    celery_broker_url: str = os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
    celery_result_backend: str = os.getenv("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1")
    celery_visibility_timeout_seconds: int = int(os.getenv("CELERY_VISIBILITY_TIMEOUT_SECONDS", "300"))
    browser_session_acquire_timeout_seconds: int = int(os.getenv("BROWSER_SESSION_ACQUIRE_TIMEOUT_SECONDS", "600"))
    browser_session_retry_delay_seconds: int = int(os.getenv("BROWSER_SESSION_RETRY_DELAY_SECONDS", "20"))
    browser_session_max_retries: int = int(os.getenv("BROWSER_SESSION_MAX_RETRIES", "20"))
    process_recovery_enabled: bool = os.getenv("PROCESS_RECOVERY_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    process_recovery_stale_after_seconds: int = int(os.getenv("PROCESS_RECOVERY_STALE_AFTER_SECONDS", "300"))
    process_recovery_queued_after_seconds: int = int(os.getenv("PROCESS_RECOVERY_QUEUED_AFTER_SECONDS", "30"))
    process_recovery_interval_seconds: int = int(os.getenv("PROCESS_RECOVERY_INTERVAL_SECONDS", "60"))
    process_heartbeat_interval_seconds: int = int(os.getenv("PROCESS_HEARTBEAT_INTERVAL_SECONDS", "30"))
    observability_enabled: bool = os.getenv("OBSERVABILITY_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    observability_redis_queue: str = os.getenv("OBSERVABILITY_REDIS_QUEUE", "processes")
    observability_selenium_status_url: str = os.getenv("OBSERVABILITY_SELENIUM_STATUS_URL", "http://selenium-hub:4444/status")
    observability_failure_rate_alert_threshold: float = float(os.getenv("OBSERVABILITY_FAILURE_RATE_ALERT_THRESHOLD", "0.25"))
    observability_queue_depth_alert_threshold: int = int(os.getenv("OBSERVABILITY_QUEUE_DEPTH_ALERT_THRESHOLD", "50"))
    observability_browser_saturation_alert_threshold: float = float(os.getenv("OBSERVABILITY_BROWSER_SATURATION_ALERT_THRESHOLD", "0.9"))
    observability_cache_ttl_seconds: int = int(os.getenv("OBSERVABILITY_CACHE_TTL_SECONDS", "20"))
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://admin:secret@127.0.0.1:27017")
    mongodb_database: str = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
    mongodb_clients_collection: str = os.getenv("MONGODB_CLIENTS_COLLECTION", "clients")
    mongodb_process_uploads_collection: str = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")
    mongodb_domain_runs_collection: str = os.getenv("MONGODB_DOMAIN_RUNS_COLLECTION", "domain_runs")
    default_client_name: str = os.getenv("DEFAULT_CLIENT_NAME", "ProcessZero")
    default_client_email: str = os.getenv("DEFAULT_CLIENT_EMAIL", "")
    process_email_enabled: bool = os.getenv("PROCESS_EMAIL_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    resend_api_key: str = os.getenv("RESEND_API_KEY", "")
    email_from_address: str = os.getenv("EMAIL_FROM_ADDRESS", "")
    email_from_name: str = os.getenv("EMAIL_FROM_NAME", "Career Page Job Extractor")
    email_reply_to: str = os.getenv("EMAIL_REPLY_TO", "")
    process_email_subject_prefix: str = os.getenv("PROCESS_EMAIL_SUBJECT_PREFIX", "")


def get_settings() -> Settings:
    settings = Settings()
    log_event(
        logger,
        "info",
        "settings_loaded mongodb_database=%s process_uploads_collection=%s",
        settings.mongodb_database,
        settings.mongodb_process_uploads_collection,
        domain="config",
        mongodb_database=settings.mongodb_database,
        mongodb_process_uploads_collection=settings.mongodb_process_uploads_collection,
    )
    return settings
