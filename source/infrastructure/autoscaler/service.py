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
    idle_scale_down_seconds: int = int(os.getenv("AUTOSCALER_IDLE_SCALE_DOWN_SECONDS", "180"))
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
    worker_service: str = os.getenv("AUTOSCALER_WORKER_SERVICE", "worker")
    chrome_service: str = os.getenv("AUTOSCALER_CHROME_SERVICE", "chrome-node")


@dataclass(slots=True)
class ScaleState:
    queue_depth: int
    celery_unacked: int
    mongo_queued: int
    mongo_acquiring_browser: int
    mongo_running: int
    used_browser_slots: int
    total_browser_slots: int
    workers: int
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
        self._idle_since: float | None = None

    def run_forever(self) -> None:
        logger.info("autoscaler_started config=%s", self._config)
        while True:
            try:
                state = self._read_state()
                worker_target, chrome_target = self._desired_counts(state)
                logger.info(
                    "autoscaler_state queue=%s unacked=%s mongo_queued=%s acquiring_browser=%s running=%s workers=%s/%s chrome=%s/%s slots=%s/%s cpu=%.1f mem=%.1f target_workers=%s target_chrome=%s",
                    state.queue_depth,
                    state.celery_unacked,
                    state.mongo_queued,
                    state.mongo_acquiring_browser,
                    state.mongo_running,
                    state.workers,
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
                self._scale_if_needed(state, worker_target, chrome_target)
            except Exception as exc:
                logger.exception("autoscaler_error error=%s", exc)
            time.sleep(self._config.poll_seconds)

    def _read_state(self) -> ScaleState:
        memory = psutil.virtual_memory()
        used_slots, total_slots = self._selenium_slots()
        return ScaleState(
            queue_depth=self._queue_depth(),
            celery_unacked=self._celery_unacked_count(),
            mongo_queued=self._process_count("queued"),
            mongo_acquiring_browser=self._process_count("acquiring_browser"),
            mongo_running=self._process_count("running"),
            used_browser_slots=used_slots,
            total_browser_slots=total_slots,
            workers=self._compose_count(self._config.worker_service),
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
        result = subprocess.run(command, cwd=self._config.compose_project_dir, text=True, capture_output=True, check=False)
        if result.returncode != 0:
            logger.warning("compose_count_failed service=%s stderr=%s", service, result.stderr.strip())
            return 0
        return sum(1 for line in result.stdout.splitlines() if line.strip())

    def _desired_counts(self, state: ScaleState) -> tuple[int, int]:
        if self._can_scale_down(state):
            return self._config.min_workers, self._config.min_chrome_nodes

        pressure = max(state.queue_depth, state.mongo_queued)
        process_slots_needed = (
            state.mongo_running
            + state.mongo_acquiring_browser
            + math.ceil(pressure / max(1, self._config.tasks_per_worker))
        )
        workers = max(self._config.min_workers, process_slots_needed)
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
        extra = usable_memory // max(1, self._config.estimated_worker_memory_mb)
        return max(self._config.min_workers, min(self._config.max_workers, state.workers + extra))

    def _resource_safe_chrome_limit(self, state: ScaleState) -> int:
        if state.cpu_percent >= self._config.max_cpu_percent or state.memory_percent >= self._config.max_memory_percent:
            return max(self._config.min_chrome_nodes, state.chrome_nodes)
        usable_memory = max(0, state.available_memory_mb - self._config.memory_buffer_mb)
        extra = usable_memory // max(1, self._config.estimated_chrome_memory_mb)
        return max(self._config.min_chrome_nodes, min(self._config.max_chrome_nodes, state.chrome_nodes + extra))

    def _can_scale_down(self, state: ScaleState) -> bool:
        idle = (
            state.queue_depth == 0
            and state.celery_unacked == 0
            and state.mongo_queued == 0
            and state.mongo_acquiring_browser == 0
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
            env={**os.environ, "PWD": self._config.compose_project_dir},
            check=True,
        )
        self._last_scale_at = now

    def _compose_command(self) -> list[str]:
        if shutil.which("docker") and self._docker_compose_available():
            return ["docker", "compose"]
        if shutil.which("docker-compose"):
            return ["docker-compose"]
        return ["docker", "compose"]

    def _docker_compose_available(self) -> bool:
        result = subprocess.run(["docker", "compose", "version"], text=True, capture_output=True, check=False)
        return result.returncode == 0


def main() -> None:
    Autoscaler(ScalerConfig()).run_forever()


if __name__ == "__main__":
    main()
