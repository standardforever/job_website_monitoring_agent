"""
Generate docker-compose.workers.yml with N worker + standalone-selenium pairs.

Usage:
    python scripts/generate_compose_workers.py --workers 4
    docker compose -f docker-compose.yaml -f docker-compose.workers.yml up -d
"""
from __future__ import annotations

import argparse
import sys

SELENIUM_BASE_PORT = 4441
WORKER_TEMPLATE = """\
  worker_{n}:
    build:
      context: .
      dockerfile: Dockerfile
    env_file:
      - ./source/.env
    environment:
      CELERY_BROKER_URL: redis://redis:6379/0
      CELERY_RESULT_BACKEND: redis://redis:6379/1
      MONGODB_URI: mongodb://${{MONGO_ROOT_USERNAME:-admin}}:${{MONGO_ROOT_PASSWORD:-secret}}@mongo:27017
      MONGODB_DATABASE: ${{MONGODB_DATABASE:-job_monitoring_agent}}
      MONGODB_CLIENTS_COLLECTION: clients
      MONGODB_PROCESS_UPLOADS_COLLECTION: process_uploads
      MONGODB_DOMAIN_RUNS_COLLECTION: domain_runs
      SELENIUM_REMOTE_URL: http://selenium_{n}:4444/wd/hub
      TASK_MAX_ATTEMPTS: ${{TASK_MAX_ATTEMPTS:-3}}
      HEARTBEAT_INTERVAL_SECONDS: ${{HEARTBEAT_INTERVAL_SECONDS:-30}}
    volumes:
      - ./logs:/app/logs
    command: celery -A infrastructure.celery_app worker --loglevel=info --concurrency=1 --queues=processes --hostname=worker_{n}@%h
    depends_on:
      redis:
        condition: service_healthy
      mongo:
        condition: service_started
      selenium_{n}:
        condition: service_started
    restart: unless-stopped
    stop_grace_period: 300s
"""

SELENIUM_TEMPLATE = """\
  selenium_{n}:
    image: selenium/standalone-chromium:latest
    shm_size: "${{CHROME_SHM_SIZE:-2g}}"
    environment:
      SE_NODE_MAX_SESSIONS: 1
      SE_NODE_OVERRIDE_MAX_SESSIONS: "true"
      SE_NODE_SESSION_TIMEOUT: ${{SE_NODE_SESSION_TIMEOUT:-900}}
      SE_BROWSER_ARGS_DISABLE_DSHM: --disable-dev-shm-usage
      SE_BROWSER_ARGS_NO_SANDBOX: --no-sandbox
    ports:
      - "{port}:4444"
    restart: unless-stopped
"""


def generate(worker_count: int) -> str:
    blocks: list[str] = ["services:"]
    for n in range(1, worker_count + 1):
        blocks.append(WORKER_TEMPLATE.format(n=n))
        blocks.append(SELENIUM_TEMPLATE.format(n=n, port=SELENIUM_BASE_PORT + n - 1))
    return "\n".join(blocks) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate docker-compose.workers.yml")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker+selenium pairs (default: 2)")
    parser.add_argument("--output", default="docker-compose.workers.yml", help="Output file path")
    args = parser.parse_args()

    if args.workers < 1:
        print("--workers must be >= 1", file=sys.stderr)
        sys.exit(1)

    content = generate(args.workers)
    with open(args.output, "w") as f:
        f.write(content)
    print(f"Generated {args.output} with {args.workers} worker(s).")
    print(f"Run with: docker compose -f docker-compose.yaml -f {args.output} up -d")


if __name__ == "__main__":
    main()
