# Production Infrastructure

This deployment separates the system into five responsibilities:

- `api`: receives uploads, manages clients, stores process documents, and serves reports.
- `redis`: broker and result backend for Celery.
- `worker`: Celery worker that executes queued process tasks.
- `selenium-hub`: receives WebDriver sessions from workers.
- `chrome-node`: scalable Chromium browser capacity registered with Selenium Hub.
- `mongo`: durable process state, client configuration, and run history.
- `autoscaler`: optional resource-aware service that scales workers and Chrome nodes.

## Why This Scales Better

The API no longer needs to hold long-running browser work in its own FastAPI background task when `PROCESS_EXECUTION_MODE=celery`.
It creates the process record in MongoDB, then enqueues the process id into Redis. A Celery worker receives that id and runs the existing process pipeline.

This means:

- API replicas can scale independently from workers.
- Worker count controls how many processes can run concurrently.
- Chrome node count controls browser capacity.
- Redis handles task delivery and worker scheduling.
- MongoDB remains the source of truth for process status, results, stop requests, and history.
- The queue layer is isolated under `source/infrastructure/queue`, so it can be replaced without changing the pipeline internals.

## Restart Recovery

MongoDB remains the source of truth after a server restart. Redis is persistent, but active browser sessions are still lost when the server crashes.

On API or Celery worker startup, recovery checks stale interrupted processes:

- stale `running` processes are moved back to `queued`
- unfinished `domain_runs` are moved back to `queued`
- completed `domain_runs` stay completed
- recovered queued processes are re-enqueued to Celery
- stale `queued` processes are also re-enqueued in case the API crashed after Mongo write but before Redis enqueue
- stale `stop_requested` processes are finalized as `stopped`

Control this with:

```env
PROCESS_RECOVERY_ENABLED=true
PROCESS_RECOVERY_STALE_AFTER_SECONDS=300
PROCESS_RECOVERY_QUEUED_AFTER_SECONDS=30
```

## Observability

The observability layer is isolated under `source/infrastructure/observability` and can be disabled with:

```env
OBSERVABILITY_ENABLED=false
```

Available endpoints:

```text
GET /api/observability/summary
GET /api/observability/alerts
GET /api/observability/metrics
```

They use the same `x-registration-password` header as admin routes when `CLIENT_REGISTRATION_PASSWORD` is configured.

The summary includes:

- Redis queue depth
- active/queued process counts
- Selenium browser slots and saturation
- process duration
- domain failure rate
- LLM token usage found in stored payloads
- email success/failure rate
- container stats when Docker CLI/socket is available
- alerts for queue depth, failure rate, stale processes, and browser saturation

## Recommended Production Defaults

Use fresh browser sessions per process. This is the safer default for multiple users because it prevents cookies, auth state, and broken browser contexts from leaking between users or runs.

Start conservative:

- `SE_NODE_MAX_SESSIONS=1`
- `CELERY_WORKER_CONCURRENCY=1`
- one Celery worker process per active process slot
- one or more `chrome-node` replicas per worker, depending on `agent_count`
- keep `agent_count` modest until memory use is measured

## Commands

Create a production env file:

```bash
cp .env.production.example .env
cp .env.production.example source/.env
```

Docker Compose reads the root `.env` for service variables such as ports and Mongo credentials.
The application containers also load `source/.env`, so keep both aligned for production.

Start the stack:

```bash
docker compose up -d --build
```

Scale workers and browser nodes:

```bash
docker compose up -d --scale worker=4 --scale chrome-node=8
```

Enable autoscaling:

```bash
docker compose --profile autoscale up -d --build autoscaler
```

The autoscaler watches:

- Redis queue depth
- Mongo queued/running processes
- Selenium used/total browser slots
- server CPU and available RAM
- current Docker Compose worker/chrome-node counts

It scales up with:

```bash
docker compose up -d --scale worker=N --scale chrome-node=M
```

It scales down only after the queue is empty, Mongo has no running process, Selenium has no active browser slots, and the idle cooldown has passed.

If you need the older Mongo polling worker for debugging, it is still available behind a profile:

```bash
docker compose --profile legacy-worker up -d legacy-worker
```

Open the API/UI:

```text
http://localhost:8110
```

Open Selenium Hub:

```text
http://localhost:4445
```

Optional debug UI for Mongo:

```bash
docker compose --profile debug up -d mongo-express
```

## Capacity Notes

With a 16 GB server, do not try to run 200 users' browser processes at the same time. Queue them.
For browser automation, practical concurrency is usually limited by Chrome memory, network, target-site throttling, and LLM API limits.

A realistic first production setting on 16 GB:

- 2-4 worker containers
- 4-8 Chrome node containers
- `agent_count` between 1 and 3 per process
- increase only after measuring memory and failure rate

For 200+ users, the correct model is queued throughput, not 200 simultaneous full browser sessions on one VPS.
