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
4. re-enqueues periodic tasks with their next cron-based run time.
5. retries failed deliveries with exponential backoff and eventually moves exhausted tasks to a Redis dead-letter queue.

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
| `max_retries` | Optional retry limit before the task is moved to the dead-letter queue |
| `retry_count` | Current retry attempt count tracked by Thermite |
| `last_error` | Last delivery error recorded for retry/dead-letter inspection |

When a task is executed, Thermite sends a request like:

```json
{
  "task_id": "123",
  "args": {
    "email": "user@example.com"
  }
}
```

## HTTP API

### `POST /submit-task`
Submit a single task.

### `POST /submit-tasks`
Submit multiple tasks in one request.

### `GET /dead-letter-tasks`
Inspect tasks that exhausted retries and were moved to the dead-letter queue. If `THERMITE_API_KEY` is set, include `x-api-key` or `Authorization: Bearer ...`.

### Example task payload

```json
{
  "id": "task-1",
  "name": "Send reminder",
  "description": "Send a reminder email",
  "category": "non_periodic",
  "priority": "high",
  "task": "http://localhost:9000/jobs/reminder",
  "scheduled_at": 1893456000,
  "cron_scheduled_at": "0 0 * * *",
  "args": {
    "user_id": 42,
    "channel": "email"
  }
}
```

## Running locally

### Redis Version Requirements

Thermite requires **Redis 5.0 or higher**. Redis 5.0+ provides support for sorted sets and streams that Thermite depends on for task scheduling and queue management.

### With Docker Compose

```bash
docker compose up --build
```

This starts:

- the Thermite worker on `http://localhost:8080`
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

## Configuration

Thermite uses these environment variables and CLI options:

| Name | Purpose | Default |
|---|---|---|
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `TASKS_URL` | Address the HTTP server binds to in `receiver` mode | `127.0.0.1:8080` |
| `FETCH_URL` | Endpoint to poll for tasks in `fetcher` mode | required for fetcher |
| `THERMITE_API_KEY` | Optional API key required on `POST /submit-task` and `POST /submit-tasks` via `x-api-key` or `Authorization: Bearer ...` | unset |
| `THERMITE_ALLOWED_HOSTS` | Optional comma-separated allowlist of task target hosts/domains such as `jobs.example.com,hooks.example.org` | unset |
| `THERMITE_REQUIRE_HTTPS` | If set to `true`, `1`, `yes`, or `on`, only `https://` task targets are accepted | unset |
| `THERMITE_MAX_RETRIES` | Default retry count before a failed task is moved to the Redis dead-letter queue | `3` |
| `THERMITE_RETRY_BASE_DELAY_SECS` | Base retry delay in seconds; Thermite applies exponential backoff from this value | `30` |
| `RUST_LOG` | Log level / filter for structured logs, e.g. `info` or `thermite=debug,actix_web=info` | `info` |
| `--mode` | Run mode: `receiver` or `fetcher` | `receiver` |

## Security

Thermite implements multiple layers of security to protect task execution and data integrity:

### Layer 1: API Authentication
- **API Key Protection**: Set `THERMITE_API_KEY` to require authentication on task submission endpoints (`/submit-task`, `/submit-tasks`)
- **Authorization Methods**: Supports both `x-api-key` header and `Authorization: Bearer <token>` formats
- **Impact**: Prevents unauthorized task submission and ensures only trusted clients can enqueue tasks

### Layer 2: Host & Protocol Validation
- **Host Allowlisting**: Use `THERMITE_ALLOWED_HOSTS` to restrict task execution to a whitelist of approved domains (e.g., `jobs.example.com,hooks.example.org`)
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
