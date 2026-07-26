# Thermite

A lightweight **Redis-backed task scheduler and HTTP worker** written in Rust. Thermite processes distributed tasks with support for both one-time and periodic (cron-scheduled) execution.

Thermite accepts tasks over HTTP or fetches them from another service, stores them in Redis, and executes them when they are due by sending an HTTP `POST` request to the target task URL. It also supports **periodic jobs** using cron expressions.

In short: it's a small, self-hosted alternative to things like Celery Beat / delayed-job queues, where "task execution" simply means "POST to a webhook URL at the scheduled time," with cron recurrence, retries, and a dead-letter queue built in.

## What this tool does

Thermite runs in one of two modes:

- **`receiver` mode**: exposes an HTTP API for submitting one or more tasks.
- **`fetcher` mode**: periodically pulls tasks from a remote endpoint and enqueues them.

Once a task is in Redis, Thermite:

1. stores it in a sorted set using `scheduled_at` as the score,
2. polls for due tasks,
3. executes each due task by `POST`ing to its `task` URL,
4. re-enqueues periodic tasks with their next cron-based run time,
5. retries failed deliveries with exponential backoff and eventually moves exhausted tasks to a Redis dead-letter queue.

## How it works

Both run modes share the same internal pipeline:

```
POST /submit-task ─┐
POST /submit-tasks ┼─► validate ─► Redis ZSET "task_queue" ─► dispatcher ─► in-memory channel ─► worker ─► HTTP POST to task URL
GET  FETCH_URL   ──┘   (fetcher mode, every 10s)                 (1s poll)      (capacity 32)                 │
                                                                                              failure ◄─────┘
                                                                                                │
                                              requeue with backoff (ZADD) ◄── retries left? ────┘
                                              push to LIST "dead_letter_queue" ◄── retries exhausted
```

### The pipeline, step by step

1. **Intake.** Tasks arrive via the HTTP API (`receiver` mode) or by polling `FETCH_URL` every 10 seconds (`fetcher` mode). Every task is validated before it is accepted (see [Validation rules](#validation-rules)).
2. **Storage.** Accepted tasks are serialized to JSON and stored in a Redis **sorted set** named `task_queue`, scored by `scheduled_at` (Unix seconds). Because the serialized task JSON is the set member, re-submitting the exact same task is idempotent — Redis deduplicates it.
3. **Dispatch.** A background loop checks the queue **once per second** for tasks whose score is `<= now` (`ZREVRANGEBYSCORE task_queue now -inf`). The due task is **removed** from the set (`ZREM`) and handed to the worker through an in-memory Tokio channel with capacity 32.
4. **Periodic rescheduling.** If the dequeued task is `periodic` (and not a retry), its next run time is computed from `cron_scheduled_at` and it is immediately re-enqueued — *before* execution. This means a periodic schedule keeps advancing even if an individual run fails.
5. **Execution.** The worker sends `POST {task.task}` with a JSON body of `{"task_id": "...", "args": {...}}` using an HTTP client with a **15-second timeout**.
6. **Failure handling.** Any non-2xx status, connection error, or timeout counts as a failure. The task's `retry_count` is incremented, `last_error` is recorded, and it is requeued with exponential backoff. When retries are exhausted, the task is appended to the Redis **list** `dead_letter_queue`, inspectable via `GET /dead-letter-tasks`.

### Redis schema

| Key | Type | Contents |
|---|---|---|
| `task_queue` | Sorted set | Pending tasks; member = task JSON, score = `scheduled_at` |
| `dead_letter_queue` | List | Tasks that exhausted all retries, oldest first |

You can inspect these directly with `redis-cli ZRANGE task_queue 0 -1 WITHSCORES` and `redis-cli LRANGE dead_letter_queue 0 -1`.

## Task model

Each task contains:

| Field | Description |
|---|---|
| `id` | Unique task identifier |
| `name` | Human-readable task name |
| `description` | Task description |
| `category` | `non_periodic` or `periodic` |
| `priority` | Priority label |
| `task` | Target URL to call when the task runs |
| `scheduled_at` | Unix timestamp for the next run |
| `cron_scheduled_at` | Cron expression used for periodic jobs |
| `args` | Optional JSON payload passed through to the target URL |
| `max_retries` | Optional retry limit before the task is moved to the dead-letter queue (defaults to `THERMITE_MAX_RETRIES` or `3`) |
| `retry_count` | Current retry attempt count tracked by Thermite |
| `last_error` | Last delivery error recorded for retry/dead-letter inspection |
| `is_retry` | Set to `true` by Thermite when the current run is a retry (omit when submitting) |

> **Note:** `cron_scheduled_at` is required in the payload even for `non_periodic` tasks, but it is only evaluated when `category` is `periodic`.

### Validation rules

Every submitted (or fetched) task is validated before enqueueing. A task is rejected with a `400 Bad Request` if:

- the `task` URL is not a valid `http://` or `https://` URL with a host,
- the host is `localhost`, ends in `.localhost`, or is a private/loopback/link-local/broadcast/multicast IP literal (always blocked, regardless of configuration — this is SSRF protection),
- the host is not in `THERMITE_ALLOWED_HOSTS` (when that variable is set; subdomain matches are allowed),
- the scheme is `http://` while `THERMITE_REQUIRE_HTTPS` is enabled,
- `category` is `periodic` but `cron_scheduled_at` is not a parseable cron expression.

> **Note:** because localhost targets are always rejected, point tasks at a real hostname (e.g. `host.docker.internal` from Docker, or a LAN/DNS name) when developing locally.

### What your task endpoint receives

When a task runs, Thermite sends a request like:

```http
POST /jobs/reminder HTTP/1.1
Content-Type: application/json

{
  "task_id": "123",
  "args": {
    "email": "user@example.com"
  }
}
```

Your endpoint must respond within **15 seconds**. Any `2xx` status marks the task as successfully executed; any other status (or a timeout/connection error) triggers the retry flow.

## HTTP API

All endpoints return JSON. Unknown routes return `404 {"error": "Not Found"}`. If `THERMITE_API_KEY` is set, every endpoint except `/healthz` requires either an `x-api-key: <key>` header or `Authorization: Bearer <key>`, otherwise it returns `401 {"error": "Unauthorized"}`.

### `POST /submit-task`

Submit a single task. The request body is one task object.

- `200 OK` — `{"status": "Task submitted"}`
- `400 Bad Request` — `{"error": "..."}` (validation failed)
- `401 Unauthorized` — missing/invalid API key

```bash
curl -X POST http://localhost:8080/submit-task \
  -H "Content-Type: application/json" \
  -H "x-api-key: $THERMITE_API_KEY" \
  -d '{
    "id": "task-1",
    "name": "Send reminder",
    "description": "Send a reminder email",
    "category": "non_periodic",
    "priority": "high",
    "task": "https://jobs.example.com/reminder",
    "scheduled_at": 1893456000,
    "cron_scheduled_at": "0 0 * * *",
    "args": {"user_id": 42, "channel": "email"}
  }'
```

### `POST /submit-tasks`

Submit multiple tasks in one request. The request body is an array of task objects. Each task is validated and enqueued independently — one bad task does not reject the whole batch.

- `200 OK` — all tasks accepted: `{"status": "Tasks submitted", "submitted": 2}`
- `400 Bad Request` — some tasks failed validation: `{"status": "Some tasks failed validation", "submitted": 1, "failed": [{"id": "bad-task", "error": "..."}]}`
- `500 Internal Server Error` — a server-side failure (e.g. Redis unavailable) occurred for at least one task

```bash
curl -X POST http://localhost:8080/submit-tasks \
  -H "Content-Type: application/json" \
  -H "x-api-key: $THERMITE_API_KEY" \
  -d '[{"id": "task-1", ...}, {"id": "task-2", ...}]'
```

### `GET /dead-letter-tasks`

Inspect tasks that exhausted retries and were moved to the dead-letter queue.

- `200 OK` — `{"tasks": [...], "count": 2}` (each entry includes `retry_count` and `last_error`)

```bash
curl -H "x-api-key: $THERMITE_API_KEY" http://localhost:8080/dead-letter-tasks
```

### `GET /healthz`

Liveness probe. Always returns `200 {"status": "ok"}` and never requires authentication.

### Example task payload

```json
{
  "id": "task-1",
  "name": "Send reminder",
  "description": "Send a reminder email",
  "category": "non_periodic",
  "priority": "high",
  "task": "https://jobs.example.com/reminder",
  "scheduled_at": 1893456000,
  "cron_scheduled_at": "0 0 * * *",
  "args": {
    "user_id": 42,
    "channel": "email"
  }
}
```

## Retries and the dead-letter queue

Failed deliveries are retried with **exponential backoff**:

```
delay = THERMITE_RETRY_BASE_DELAY_SECS * 2^(retry_count - 1)
```

With the defaults (`THERMITE_MAX_RETRIES=3`, `THERMITE_RETRY_BASE_DELAY_SECS=30`) a failing task is retried after roughly **30s, then 60s, then 120s**, and moved to `dead_letter_queue` on the next failure after the 3rd retry. Per-task `max_retries` overrides the global default.

Retries interact with periodic tasks as follows: the *next* cron occurrence is already re-enqueued when the task is first dequeued, so a retry only re-runs the failed occurrence — it never shifts or duplicates the cron schedule.

## Periodic tasks and cron expressions

Set `category` to `periodic` and provide a cron expression in `cron_scheduled_at`:

- **5-field** (`minute hour day-of-month month day-of-week`, e.g. `"0 9 * * 1-5"`) or **6-field** (with a leading seconds field, e.g. `"0 30 9 * * 1-5"`) expressions are accepted. A 5-field expression is automatically expanded with a `0` seconds field.
- Schedules are evaluated in **UTC**.
- After each run, the next occurrence is computed from the current time and the task is re-enqueued — the `scheduled_at` you submit only determines the *first* run.

## Fetcher mode

In fetcher mode Thermite does not start the HTTP API. Instead it polls `FETCH_URL` with a `GET` request **every 10 seconds**, expecting a `200` response whose body is a JSON **array of task objects** (same schema as `/submit-tasks`). Each task is validated and enqueued exactly as if it had been submitted over HTTP, and the shared dispatcher/worker pipeline executes them.

```bash
export FETCH_URL=https://scheduler.example.com/api/tasks/periodic
cargo run -- --mode fetcher --redis-url redis://localhost:6379
```

## Running locally

### Redis Version Requirements

Thermite requires **Redis 5.0 or higher**. Redis 5.0+ provides support for sorted sets and streams that Thermite depends on for task scheduling and queue management.

### With Docker Compose

```bash
docker compose up --build
```

This starts:

- the Thermite worker on `http://localhost:8080` (bound to `0.0.0.0:8080` inside the container)
- Redis on `localhost:6379`

### With Cargo

```bash
cargo run -- --mode receiver --redis-url redis://localhost:6379
```

To run in fetcher mode:

```bash
export FETCH_URL=http://localhost:8000/api/tasks/periodic
cargo run -- --mode fetcher --redis-url redis://localhost:6379
```

Useful CLI flags (see `cargo run -- --help`): `--mode/-m` (`receiver` or `fetcher`), `--redis-url/-r`, `--tasks-url/-t` (bind address in receiver mode).

## Configuration

Thermite uses these environment variables and CLI options:

| Name | Purpose | Default |
|---|---|---|
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `TASKS_URL` | Address the HTTP server binds to in `receiver` mode | `127.0.0.1:8080` |
| `FETCH_URL` | Endpoint to poll for tasks in `fetcher` mode | required for fetcher |
| `THERMITE_API_KEY` | Optional API key required on `POST /submit-task`, `POST /submit-tasks`, and `GET /dead-letter-tasks` via `x-api-key` or `Authorization: Bearer ...` | unset |
| `THERMITE_ALLOWED_HOSTS` | Optional comma-separated allowlist of task target hosts/domains such as `jobs.example.com,hooks.example.org` | unset |
| `THERMITE_REQUIRE_HTTPS` | If set to `true`, `1`, `yes`, or `on`, only `https://` task targets are accepted | unset |
| `THERMITE_MAX_RETRIES` | Default retry count before a failed task is moved to the Redis dead-letter queue | `3` |
| `THERMITE_RETRY_BASE_DELAY_SECS` | Base retry delay in seconds; Thermite applies exponential backoff from this value | `30` |
| `RUST_LOG` | Log level / filter for structured logs, e.g. `info` or `thermite=debug,actix_web=info` | `info` |
| `--mode` | Run mode: `receiver` or `fetcher` | `receiver` |

### Observability

Thermite emits structured logs via `tracing`. Tune verbosity with `RUST_LOG`, e.g.:

```bash
RUST_LOG=thermite=debug,actix_web=info cargo run -- --mode receiver
```

Key lifecycle events (task enqueued, dequeued, executed, retried, dead-lettered, unauthorized requests) are logged with the `task_id` attached, so you can trace a task end to end.

## Security

Thermite implements multiple layers of security to protect task execution and data integrity:

### Layer 1: API Authentication
- **API Key Protection**: Set `THERMITE_API_KEY` to require authentication on task submission endpoints (`/submit-task`, `/submit-tasks`) and the dead-letter endpoint (`/dead-letter-tasks`)
- **Authorization Methods**: Supports both `x-api-key` header and `Authorization: Bearer <token>` formats
- **Impact**: Prevents unauthorized task submission and ensures only trusted clients can enqueue tasks

### Layer 2: Host & Protocol Validation
- **Built-in SSRF Protection**: `localhost`, `*.localhost`, and private/loopback/link-local/broadcast/multicast IP targets are **always rejected**, with no opt-out
- **Host Allowlisting**: Use `THERMITE_ALLOWED_HOSTS` to restrict task execution to a whitelist of approved domains (e.g., `jobs.example.com,hooks.example.org`); subdomains of listed hosts are also accepted
- **HTTPS Enforcement**: Enable `THERMITE_REQUIRE_HTTPS=true` to reject any task with non-HTTPS target URLs
- **Impact**: Prevents execution of tasks pointing to untrusted or internal hosts, protects against SSRF attacks

### Layer 3: Redis Security
- **Secure Connection**: Always use `redis://` with TLS support or `rediss://` for production deployments
- **Redis ACL**: Configure Redis username and password in the connection string (e.g., `redis://user:password@host:6379`)
- **Network Isolation**: Keep Redis on a private network, not exposed to the public internet
- **Impact**: Prevents unauthorized access to task queues and sensitive task data

### Layer 4: Task Data Protection
- **Payload Encryption**: Encrypt sensitive arguments in the `args` field at the application level before submission
- **Audit Logging**: Use `RUST_LOG` with appropriate level (e.g., `thermite=info`) to monitor task execution
- **Dead-Letter Queue Access**: Protect access to `GET /dead-letter-tasks` endpoint with API key authentication
- **Impact**: Protects sensitive task parameters and enables compliance auditing

## Typical workflow

1. Submit or fetch tasks.
2. Thermite stores them in Redis.
3. When `scheduled_at` is due, Thermite executes the target URL.
4. If the task is `periodic`, it computes the next run from `cron_scheduled_at` and requeues it.
5. If execution keeps failing after the configured retries, the task is stored in `dead_letter_queue` and can be reviewed via `GET /dead-letter-tasks`.

## Delivery semantics and limitations

Thermite is deliberately simple; be aware of these properties before relying on it:

- **At-most-once per scheduled occurrence, plus retries.** A task is removed from `task_queue` *before* it is executed. If the process crashes between dequeue and delivery, that occurrence is lost (periodic tasks simply run again at their next cron occurrence).
- **No in-flight recovery.** There is no visibility timeout or heartbeat for executing tasks, so a crash mid-execution does not requeue the task.
- **Latest-due first.** The dispatcher picks the due task with the highest score (most recently due) first; `priority` is stored as a label but does not influence ordering.
- **Single in-memory channel.** Due tasks flow through one channel with capacity 32 per process; for higher throughput, scale out worker processes against the same Redis.

## Heroku container deployment

1. Install the Heroku CLI and log in:

```bash
heroku login
heroku container:login
```

2. Create the app:

```bash
heroku create thermite
```

3. Build and push the image:

```bash
docker build --platform linux/amd64 -t registry.heroku.com/thermite/worker .
docker push registry.heroku.com/thermite/worker
```

4. Set the stack and release:

```bash
heroku stack:set container -a thermite
heroku container:release worker -a thermite
```

5. Open the app or inspect logs:

```bash
heroku open -a thermite
heroku logs --tail -a thermite
```
