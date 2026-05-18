from __future__ import annotations

import logging
import math
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Any

import psutil
import requests
from pymongo import MongoClient
from redis import Redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("autoscaler")


@dataclass(slots=True)
class ScalerConfig:
    scaling_enabled: bool = os.getenv("AUTOSCALER_SCALING_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    redis_url: str = os.getenv("AUTOSCALER_REDIS_URL", os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"))
    redis_queue_name: str = os.getenv("AUTOSCALER_REDIS_QUEUE", "processes")
    redis_unacked_key: str = os.getenv("AUTOSCALER_REDIS_UNACKED_KEY", "unacked")
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://admin:secret@mongo:27017")
    mongodb_database: str = os.getenv("MONGODB_DATABASE", "job_monitoring_agent")
    process_collection: str = os.getenv("MONGODB_PROCESS_UPLOADS_COLLECTION", "process_uploads")
    selenium_status_url: str = os.getenv("SELENIUM_STATUS_URL", "http://selenium-hub:4444/status")
    compose_project_name: str = os.getenv("COMPOSE_PROJECT_NAME", "")
    compose_project_dir: str = os.getenv("AUTOSCALER_COMPOSE_PROJECT_DIR", "/app")
    poll_seconds: int = int(os.getenv("AUTOSCALER_POLL_SECONDS", "15"))
    scale_cooldown_seconds: int = int(os.getenv("AUTOSCALER_COOLDOWN_SECONDS", "90"))
    scale_up_cooldown_seconds: int = int(os.getenv("AUTOSCALER_SCALE_UP_COOLDOWN_SECONDS", "30"))
    idle_scale_down_seconds: int = int(os.getenv("AUTOSCALER_IDLE_SCALE_DOWN_SECONDS", "180"))
    compose_timeout_seconds: int = int(os.getenv("AUTOSCALER_COMPOSE_TIMEOUT_SECONDS", "30"))
    tasks_per_worker: int = int(os.getenv("AUTOSCALER_TASKS_PER_WORKER", "1"))
    chrome_nodes_per_worker: int = int(os.getenv("AUTOSCALER_CHROME_NODES_PER_WORKER", "1"))
    min_workers: int = int(os.getenv("AUTOSCALER_MIN_WORKERS", "1"))
    max_workers: int = int(os.getenv("AUTOSCALER_MAX_WORKERS", "8"))
    min_chrome_nodes: int = int(os.getenv("AUTOSCALER_MIN_CHROME_NODES", "1"))
    max_chrome_nodes: int = int(os.getenv("AUTOSCALER_MAX_CHROME_NODES", "16"))
    max_cpu_percent: float = float(os.getenv("AUTOSCALER_MAX_CPU_PERCENT", "75"))
    max_memory_percent: float = float(os.getenv("AUTOSCALER_MAX_MEMORY_PERCENT", "80"))
    memory_buffer_mb: int = int(os.getenv("AUTOSCALER_MEMORY_BUFFER_MB", "2048"))
    estimated_worker_memory_mb: int = int(os.getenv("AUTOSCALER_ESTIMATED_WORKER_MEMORY_MB", "350"))
    estimated_chrome_memory_mb: int = int(os.getenv("AUTOSCALER_ESTIMATED_CHROME_MEMORY_MB", "1200"))
    chrome_shm_size_mb: int = int(os.getenv("AUTOSCALER_CHROME_SHM_SIZE_MB", "2048"))
    memory_safety_factor: float = float(os.getenv("AUTOSCALER_MEMORY_SAFETY_FACTOR", "1.25"))
    worker_health_check_enabled: bool = os.getenv("AUTOSCALER_WORKER_HEALTH_CHECK_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    worker_health_timeout_seconds: int = int(os.getenv("AUTOSCALER_WORKER_HEALTH_TIMEOUT_SECONDS", "8"))
    worker_service: str = os.getenv("AUTOSCALER_WORKER_SERVICE", "worker")
    chrome_service: str = os.getenv("AUTOSCALER_CHROME_SERVICE", "chrome-node")

    def __post_init__(self) -> None:
        if self.min_workers > self.max_workers:
            raise ValueError("AUTOSCALER_MIN_WORKERS cannot be greater than AUTOSCALER_MAX_WORKERS")
        if self.min_chrome_nodes > self.max_chrome_nodes:
            raise ValueError("AUTOSCALER_MIN_CHROME_NODES cannot be greater than AUTOSCALER_MAX_CHROME_NODES")
        if self.tasks_per_worker < 1:
            raise ValueError("AUTOSCALER_TASKS_PER_WORKER must be at least 1")
        if self.chrome_nodes_per_worker < 1:
            raise ValueError("AUTOSCALER_CHROME_NODES_PER_WORKER must be at least 1")
        if self.compose_timeout_seconds < 5:
            raise ValueError("AUTOSCALER_COMPOSE_TIMEOUT_SECONDS must be at least 5")


@dataclass(slots=True)
class ScaleState:
    queue_depth: int
    celery_unacked: int
    mongo_queued: int
    mongo_acquiring_browser: int
    mongo_recovering: int
    mongo_running: int
    used_browser_slots: int
    total_browser_slots: int
    workers: int
    healthy_workers: int
    chrome_nodes: int
    cpu_percent: float
    memory_percent: float
    available_memory_mb: int


class Autoscaler:
    def __init__(self, config: ScalerConfig) -> None:
        self._config = config
        self._redis = Redis.from_url(config.redis_url, decode_responses=True)
        self._mongo = MongoClient(config.mongodb_uri)[config.mongodb_database]
        self._last_scale_at = 0.0
        self._last_scale_up_at = 0.0
        self._idle_since: float | None = None

    def run_forever(self) -> None:
        mode = "autoscale" if self._config.scaling_enabled else "monitor"
        logger.info("autoscaler_started mode=%s config=%s", mode, self._config)
        while True:
            try:
                state = self._read_state()
                worker_target, chrome_target = self._desired_counts(state)
                logger.info(
                    "autoscaler_state mode=%s queue=%s unacked=%s mongo_queued=%s acquiring_browser=%s recovering=%s running=%s workers=%s healthy_workers=%s/%s chrome=%s/%s slots=%s/%s cpu=%.1f mem=%.1f target_workers=%s target_chrome=%s",
                    mode,
                    state.queue_depth,
                    state.celery_unacked,
                    state.mongo_queued,
                    state.mongo_acquiring_browser,
                    state.mongo_recovering,
                    state.mongo_running,
                    state.workers,
                    state.healthy_workers,
                    self._config.max_workers,
                    state.chrome_nodes,
                    self._config.max_chrome_nodes,
                    state.used_browser_slots,
                    state.total_browser_slots,
                    state.cpu_percent,
                    state.memory_percent,
                    worker_target,
                    chrome_target,
                )
                if self._config.scaling_enabled:
                    self._scale_if_needed(state, worker_target, chrome_target)
            except Exception as exc:
                logger.exception("autoscaler_error error=%s", exc)
            time.sleep(self._config.poll_seconds)

    def _read_state(self) -> ScaleState:
        memory = psutil.virtual_memory()
        used_slots, total_slots = self._selenium_slots()
        workers = self._compose_count(self._config.worker_service)
        return ScaleState(
            queue_depth=self._queue_depth(),
            celery_unacked=self._celery_unacked_count(),
            mongo_queued=self._process_count("queued"),
            mongo_acquiring_browser=self._process_count("acquiring_browser"),
            mongo_recovering=self._process_count("recovering"),
            mongo_running=self._process_count("running"),
            used_browser_slots=used_slots,
            total_browser_slots=total_slots,
            workers=workers,
            healthy_workers=self._healthy_worker_count(workers),
            chrome_nodes=self._compose_count(self._config.chrome_service),
            cpu_percent=psutil.cpu_percent(interval=1),
            memory_percent=float(memory.percent),
            available_memory_mb=int(memory.available / 1024 / 1024),
        )

    def _queue_depth(self) -> int:
        try:
            return int(self._redis.llen(self._config.redis_queue_name))
        except Exception:
            return 0

    def _celery_unacked_count(self) -> int:
        try:
            key_type = self._redis.type(self._config.redis_unacked_key)
            if key_type == "hash":
                return int(self._redis.hlen(self._config.redis_unacked_key))
            if key_type == "list":
                return int(self._redis.llen(self._config.redis_unacked_key))
            if key_type == "set":
                return int(self._redis.scard(self._config.redis_unacked_key))
        except Exception:
            return 0
        return 0

    def _process_count(self, status: str) -> int:
        return int(self._mongo[self._config.process_collection].count_documents({"status": status}))

    def _selenium_slots(self) -> tuple[int, int]:
        try:
            response = requests.get(self._config.selenium_status_url, timeout=5)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return 0, 0

        value = payload.get("value") if isinstance(payload, dict) else {}
        nodes = value.get("nodes") if isinstance(value, dict) else []
        used = 0
        total = 0
        for node in nodes or []:
            for slot in node.get("slots") or []:
                total += 1
                if slot.get("session"):
                    used += 1
        return used, total

    def _compose_count(self, service: str) -> int:
        command = [*self._compose_command(), "ps", "--format", "json", service]
        result = subprocess.run(
            command,
            cwd=self._config.compose_project_dir,
            env=self._compose_env(),
            text=True,
            capture_output=True,
            check=False,
            timeout=self._config.compose_timeout_seconds,
        )
        if result.returncode != 0:
            logger.warning("compose_count_failed service=%s stderr=%s", service, result.stderr.strip())
            return 0
        return sum(1 for line in result.stdout.splitlines() if line.strip())

    def _healthy_worker_count(self, worker_count: int) -> int:
        if not self._config.worker_health_check_enabled or worker_count <= 0:
            return worker_count
        container_ids = self._compose_service_container_ids(self._config.worker_service)
        if not container_ids:
            return 0
        healthy = 0
        for container_id in container_ids:
            if self._worker_container_is_healthy(container_id):
                healthy += 1
        if healthy < worker_count:
            logger.warning("worker_health_degraded healthy_workers=%s total_workers=%s", healthy, worker_count)
        return healthy

    def _compose_service_container_ids(self, service: str) -> list[str]:
        command = [*self._compose_command(), "ps", "-q", service]
        result = subprocess.run(
            command,
            cwd=self._config.compose_project_dir,
            env=self._compose_env(),
            text=True,
            capture_output=True,
            check=False,
            timeout=self._config.compose_timeout_seconds,
        )
        if result.returncode != 0:
            logger.warning("compose_container_ids_failed service=%s stderr=%s", service, result.stderr.strip())
            return []
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def _worker_container_is_healthy(self, container_id: str) -> bool:
        command = [
            "docker",
            "exec",
            container_id,
            "sh",
            "-c",
            "celery -A infrastructure.queue.celery_app:celery_app inspect ping -d worker@$(hostname) --timeout=3",
        ]
        try:
            result = subprocess.run(
                command,
                text=True,
                capture_output=True,
                check=False,
                timeout=self._config.worker_health_timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            logger.warning("worker_health_check_timeout container_id=%s", container_id)
            return False
        except Exception as exc:
            logger.warning("worker_health_check_failed container_id=%s error=%s", container_id, exc)
            return False
        return result.returncode == 0 and "OK" in result.stdout

    def _desired_counts(self, state: ScaleState) -> tuple[int, int]:
        if self._can_scale_down(state):
            return self._config.min_workers, self._config.min_chrome_nodes

        pressure = max(state.queue_depth, state.mongo_queued)
        effective_workers = max(0, min(state.workers, state.healthy_workers))
        process_slots_needed = (
            state.mongo_running
            + state.mongo_acquiring_browser
            + state.mongo_recovering
            + math.ceil(pressure / max(1, self._config.tasks_per_worker))
        )
        workers = max(self._config.min_workers, process_slots_needed)
        if pressure > 0 and effective_workers < workers:
            workers = max(workers, state.workers + (workers - effective_workers))
        workers = min(workers, self._resource_safe_worker_limit(state), self._config.max_workers)

        chrome_nodes = max(self._config.min_chrome_nodes, workers * self._config.chrome_nodes_per_worker)
        if state.used_browser_slots >= state.total_browser_slots and pressure > 0:
            chrome_nodes = max(chrome_nodes, state.chrome_nodes + 1)
        chrome_nodes = min(chrome_nodes, self._resource_safe_chrome_limit(state), self._config.max_chrome_nodes)
        return max(self._config.min_workers, workers), max(self._config.min_chrome_nodes, chrome_nodes)

    def _resource_safe_worker_limit(self, state: ScaleState) -> int:
        if state.cpu_percent >= self._config.max_cpu_percent or state.memory_percent >= self._config.max_memory_percent:
            return max(self._config.min_workers, state.workers)
        usable_memory = max(0, state.available_memory_mb - self._config.memory_buffer_mb)
        worker_memory = max(1, int(self._config.estimated_worker_memory_mb * self._config.memory_safety_factor))
        extra = usable_memory // worker_memory
        return max(self._config.min_workers, min(self._config.max_workers, state.workers + extra))

    def _resource_safe_chrome_limit(self, state: ScaleState) -> int:
        if state.cpu_percent >= self._config.max_cpu_percent or state.memory_percent >= self._config.max_memory_percent:
            return max(self._config.min_chrome_nodes, state.chrome_nodes)
        usable_memory = max(0, state.available_memory_mb - self._config.memory_buffer_mb)
        chrome_memory = self._config.estimated_chrome_memory_mb + self._config.chrome_shm_size_mb
        chrome_memory = max(1, int(chrome_memory * self._config.memory_safety_factor))
        extra = usable_memory // chrome_memory
        return max(self._config.min_chrome_nodes, min(self._config.max_chrome_nodes, state.chrome_nodes + extra))

    def _can_scale_down(self, state: ScaleState) -> bool:
        idle = (
            state.queue_depth == 0
            and state.celery_unacked == 0
            and state.mongo_queued == 0
            and state.mongo_acquiring_browser == 0
            and state.mongo_recovering == 0
            and state.mongo_running == 0
            and state.used_browser_slots == 0
        )
        if not idle:
            self._idle_since = None
            return False
        now = time.time()
        self._idle_since = self._idle_since or now
        return now - self._idle_since >= self._config.idle_scale_down_seconds

    def _scale_if_needed(self, state: ScaleState, workers: int, chrome_nodes: int) -> None:
        now = time.time()
        if workers == state.workers and chrome_nodes == state.chrome_nodes:
            return
        scaling_up = workers > state.workers or chrome_nodes > state.chrome_nodes
        if scaling_up and now - self._last_scale_up_at < self._config.scale_up_cooldown_seconds:
            return
        if not scaling_up and now - self._last_scale_at < self._config.scale_cooldown_seconds:
            return
        command = [
            *self._compose_command(),
            "up",
            "-d",
            "--scale",
            f"{self._config.worker_service}={workers}",
            "--scale",
            f"{self._config.chrome_service}={chrome_nodes}",
        ]
        logger.info("autoscaler_scaling command=%s", " ".join(command))
        subprocess.run(
            command,
            cwd=self._config.compose_project_dir,
            env=self._compose_env(),
            check=True,
            timeout=self._config.compose_timeout_seconds,
        )
        self._last_scale_at = now
        if scaling_up:
            self._last_scale_up_at = now

    def _compose_command(self) -> list[str]:
        if shutil.which("docker") and self._docker_compose_available():
            return ["docker", "compose"]
        if shutil.which("docker-compose"):
            return ["docker-compose"]
        return ["docker", "compose"]

    def _docker_compose_available(self) -> bool:
        result = subprocess.run(
            ["docker", "compose", "version"],
            text=True,
            capture_output=True,
            check=False,
            timeout=self._config.compose_timeout_seconds,
        )
        return result.returncode == 0

    def _compose_env(self) -> dict[str, str]:
        return {**os.environ, "PWD": self._config.compose_project_dir}


def main() -> None:
    Autoscaler(ScalerConfig()).run_forever()


if __name__ == "__main__":
    main()
