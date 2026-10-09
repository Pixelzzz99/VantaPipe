# dataflow-rs

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

**JSON pipelines. A Rust engine, a thin Node.js gateway.**

Async **ETL** (Extract → Transform → Load): poll, transform, and load data with a config file — Postgres, ClickHouse, CSV — plus incremental state, retries, and a live Web dashboard. No code changes needed to reconfigure the pipeline. The Rust engine owns all ETL/scheduling logic behind an internal-only API; a small Node.js gateway (`server/`) is the public surface — dashboard, auth, reverse proxy. See [Architecture](#architecture).

---

## Features

| Area | What you get |
|------|----------------|
| **Sources** | PostgreSQL, ClickHouse, CSV file watching |
| **Transforms** | Filter, column rename (map), group + sum aggregate |
| **Destination** | PostgreSQL (batched inserts, optional upsert via `unique_key`) |
| **Multi-pipeline** | Load one JSON file or a directory of `*.json` configs |
| **Dynamic registration** | Directory mode can start with **zero** pipelines and grow at runtime — via `POST /api/pipelines` or by dropping a new `*.json` file into the config directory — no restart |
| **Dependencies** | `depends_on` — a pipeline only ticks once its dependencies last succeeded (optionally within a freshness window); a dependent triggers **immediately** on a dependency's success, not just on its own next tick; cycles and unknown ids are rejected at load *and* reload time |
| **Scheduling** | Fixed interval (`poll_interval_secs`) or cron (`schedule`) per pipeline |
| **Parallel** | One Tokio worker per pipeline; overlapping ticks are **skipped** |
| **Controls** | Soft Pause / Resume / Stop + Run-once from the Web UI |
| **Hot reload** | Edit JSON in the browser or on disk; pipeline rebuilds without process restart |
| **History** | Run timeline + simple Gantt (last hour); JSONL under `{state}/history/` |
| **Incremental** | Tracks `last_run` / processed files so restarts don't re-load everything |
| **Reliability** | Exponential backoff retries on pipeline failures |
| **Error visibility** | Every error carries an `error_kind` (`connection`/`query`/`config`/`load`/`transform`) shown in the dashboard, not just a message string |
| **Auth** | Optional HTTP Basic Auth (`ETL_AUTH_USER`/`ETL_AUTH_PASS`) gating the whole dashboard + API |
| **Observability** | Prometheus `/metrics`, `LOG_FORMAT=json` structured logs, dashboard with per-pipeline status and live logs |
| **Alerting** | Optional per-pipeline `alert_webhook` — fires on Error/Blocked (once per transition) and on recovery; Slack-compatible JSON body |
| **Ops** | Docker image + `docker-compose` for local Postgres + engine |

---

## How this compares

Short answer: dataflow-rs isn't trying to be a smaller Airflow. It targets
a narrower problem — small-to-mid scale "extract, reshape, load" pipelines
— and trades Airflow's breadth for operational simplicity. Worth being
precise about where that trade actually helps, and where it doesn't.

### Where the operational model differs

Airflow needs a metadata database (Postgres/MySQL — not optional, it's the
source of truth for DAG state, task instances, and XComs), a scheduler, a
webserver/API server, workers, and for `CeleryExecutor` a message broker —
5-7 moving parts with their own failure modes before a single task runs.
[Prefect's own comparison](https://prefect.io/blog/airflow-local-development)
puts local Airflow setup at "4+ services, 8GB RAM, and days of
configuration." Dagster and Prefect improved the authoring experience
(software-defined assets, decorator-based flows) but didn't remove this —
both are still Python, both still need a long-running daemon/scheduler
process plus a database for self-hosted use (`dagster-daemon` + Postgres;
Prefect Server + its own DB), with the convenient managed path behind a
paid cloud tier in both cases.

dataflow-rs's engine is a single Rust process with no metadata database —
pipeline state is flat JSONL/JSON files, not Postgres. There's no
scheduler/webserver/worker split to keep in sync. The dashboard is served
by a small Node.js gateway in front of it (see [Architecture](#architecture))
— that's two processes total, not a cluster.

### Where the authoring model differs

Airflow DAGs are Python files, and XCom is how data passes between tasks —
but XCom was built for small values (state, file paths, IDs), not bulk
rows: it's stored in the metadata DB, JSON-serialized by default, with
size ceilings that follow the DB engine (roughly 1 GiB on Postgres in
Airflow 2, tighter in Airflow 3; far smaller on MySQL). Airflow is built to
*orchestrate calls* to external systems, not move row data in-process
between steps.

dataflow-rs pipelines are JSON config, not code, and rows flow directly
between the built-in `filter`/`map`/`aggregate` steps in-process — there's
no metadata-DB bottleneck because the row data never touches one. When
declarative transforms aren't enough, a `custom` transform step runs a
real JS function (embedded QuickJS, no external Node/Python process) with
a wall-clock timeout — "I need actual code" doesn't mean "now write a
whole DAG file."

### Where dependency triggering differs

Airflow/Dagster/Prefect orchestrate task-to-task; dataflow-rs's
`depends_on` is pipeline-to-pipeline, and intentionally simpler — one
pipeline waits for another's last run to have succeeded (optionally within
a freshness window), and the moment it does, the dependent ticks
immediately via an in-process broadcast rather than waiting on its own
next poll. There's no cross-pipeline data passing, no DAG graph UI, no
backfill/catch-up — it's a dependency gate, not a workflow engine.

### What we give up

This half matters more, and it's worth stating plainly rather than
burying it:

- **Connector ecosystem.** Airflow has 100+ official provider packages and
  1,500+ operators/hooks/sensors. dataflow-rs has four sources (Postgres,
  ClickHouse, CSV, S3) and three destinations (Postgres, ClickHouse, S3).
  Need Salesforce, Kafka, GCS? That's a custom JS transform or a new Rust
  extractor, not a package install.
- **Scale and track record.** Airflow has a decade of production use at
  Airbnb/Lyft/Netflix/Adobe scale. dataflow-rs is new, built over one
  extended development effort, with no production deployments, no
  community, and no battle-testing under real load. "Simpler" is not the
  same claim as "proven" — don't read this section as the latter.
- **Horizontal scale.** No Celery/Kubernetes-executor equivalent — one
  engine process runs every pipeline's ticks as Tokio tasks on one
  machine. Fine for dozens of lightweight pipelines; not a fit for
  thousands of heavy ones across a cluster.
- **Visibility.** The dashboard shows pipeline status, a run timeline, and
  a Gantt view — not a DAG graph, not data lineage, not a Dagster-style
  asset catalog.
- **Dynamic task generation, cross-task data passing** — neither exists.
  `depends_on` is a gate, not an orchestrator. Backfill exists (see
  [Replay](#replay-backfill)) but it's one explicit request per
  range/file-list — no scheduled/bulk backfill ("replay the last 30 days")
  and no backfill of a pipeline's *dependents* when it replays.

### The honest positioning

dataflow-rs sits closer to single-binary, declarative-config tools like
[Benthos/Bento](https://github.com/Jeffail/benthos) (Go, one binary, YAML
config, no custom code required for the common case) or
[Vector](https://vector.dev) (Rust, one binary, no runtime dependency)
than to Airflow/Dagster/Prefect's "full orchestration platform" category.
Worth noting: Prefect
[announced it's acquiring Dagster Labs](https://dagster.io/blog/prefect-is-acquiring-dagster)
in mid-2026 — both pledge to stay independent OSS projects, but it signals
the "post-Airflow" generation consolidating around two players, not
expanding. If the goal is a pipeline that's easy to stand up, has no
Python/JVM runtime requirement on the engine itself, and doesn't need a
Postgres instance just to track its own scheduling state — that's the
niche this project fills. For hundreds of integrations, distributed
execution, or a decade of production hardening, Airflow (or Dagster/
Prefect) is still the right choice.

---

## Quick start

### Prerequisites

- [Rust](https://rustup.rs/) **1.85+** (edition 2024)
- [Node.js](https://nodejs.org/) 20+ (for the gateway — see [Architecture](#architecture))
- PostgreSQL for the destination (and for Postgres sources)

### Architecture

Two processes: the Rust **engine** does all ETL/scheduling work and exposes
an **internal-only** HTTP+WebSocket API (no auth, not meant to be reachable
from outside your network); the Node **gateway** (`server/`) is the public
surface — it serves the dashboard, does Basic Auth, and reverse-proxies
everything else straight through to the engine. Never expose the engine's
port directly; always front it with the gateway.

```
Browser → Node gateway (public port, auth) → Rust engine (internal port) → Postgres/ClickHouse/CSV
```

### Build & run

```bash
# Clone
git clone https://github.com/Pixelzzz99/dataflow-rs.git
cd dataflow-rs

# Build the engine
cargo build --release

# Terminal 1 — engine (internal port, single pipeline; legacy-friendly:
# state file ends with .json)
RUST_LOG=info cargo run -- config/pipeline_csv.json etl_state.json 4000
# ...or multiple pipelines from a directory (state dir + per-id files):
RUST_LOG=info cargo run -- config/active etl_state 4000

# Terminal 2 — gateway
cd server && npm install
ETL_ENGINE_URL=http://localhost:4000 PORT=3456 npm start
```

Open the dashboard: **http://localhost:3456** (the gateway's port — not the engine's)

### CLI arguments

```text
etl-engine [CONFIG|CONFIG_DIR] [STATE|STATE_DIR] [PORT]

CONFIG       Pipeline JSON file or directory of *.json   (default: config/pipeline.json)
STATE        If ends with .json → single state file (legacy)
             Otherwise → directory; writes {id}.json per pipeline
             Defaults: etl_state.json (file mode) or etl_state/ (dir mode)
PORT         Internal API port                            (default: 3000)
             Not for direct browser access — front it with the Node
             gateway in server/ (see Architecture above).
```

**Overlap policy:** if a pipeline is still running when the next interval/cron tick fires, that tick is **skipped** (logged as a warning). No backlog queue.

### Soft controls (v4)

Pause / Stop do **not** cancel an in-flight run — they only block *new* scheduled ticks. **Run** triggers one immediate run and bypasses both the pause/stop gate and the `depends_on` gate (works while paused, or while blocked on a dependency). **Resume** clears the pause so the schedule continues.

| Action | Behavior |
|--------|----------|
| Pause | Status `paused`; ticks ignored |
| Resume | Status `idle`; schedule continues |
| Stop | Same as pause; status `stopped` |
| Run | Force one run now (bypasses pause/stop and `depends_on`) |

### Pipeline status values

The dashboard's status column (and `GET /api/status`) can show:

| Status | Meaning |
|--------|---------|
| `running` / `idle` / `paused` / `stopped` | Normal lifecycle |
| `{"error": "<message>"}` | Last run failed. The pipeline row also carries `error_kind`: `connection`, `query`, `config`, or `load` — the category, independent of the message text |
| `{"blocked": "<reason>"}` | A scheduled tick was skipped because a `depends_on` entry hasn't (yet, or freshly enough) succeeded. Clears on the next successful tick; a manual **Run** bypasses it entirely |

Hot reload: **Save & reload** in the dashboard writes the JSON file and rebuilds that pipeline. Editing the file on disk also triggers reload (debounced file watcher).

Run history is stored as `{state_dir}/history/{pipeline_id}.jsonl` (or `history/` next to a legacy `.json` state file).

### Control API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/pipelines/:id/pause` | Pause |
| `POST` | `/api/pipelines/:id/resume` | Resume |
| `POST` | `/api/pipelines/:id/stop` | Soft stop |
| `POST` | `/api/pipelines/:id/run` | Run once |
| `GET` | `/api/pipelines/:id/config` | Read config JSON text |
| `PUT` | `/api/pipelines/:id/config` | Validate, write file, reload |
| `GET` | `/api/pipelines/:id/history` | Runs for one pipeline |
| `GET` | `/api/history` | All recent runs |
| `POST` | `/api/pipelines` | Register a brand-new pipeline (directory mode only) — see below |

### Dynamic pipeline registration (directory mode)

In directory mode you don't need every pipeline present before you start the
engine. You can:

```bash
mkdir -p config/active   # can be empty
RUST_LOG=info cargo run -- config/active etl_state 3456
```

(The `curl` examples below hit the engine's port directly, which is fine
for local dev/testing — same as running the two processes side by side in
[Quick start](#quick-start). Through the gateway, the exact same requests
just go to the gateway's port instead, with auth if enabled.)

...and then add pipelines while it's running, two ways:

**1. Drop a new file into the same directory** — the file watcher notices any
`*.json` file it doesn't already recognize, validates it, and spawns it as a
new pipeline automatically:

```bash
cat > config/active/new_pipeline.json <<'EOF'
{
  "id": "new_pipeline",
  "source": { "type": "csv", "watch_dir": "data/watched", "processed_dir": "data/processed", "poll_interval_secs": 10 },
  "transforms": [],
  "destination": { "type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "imported_data" }
}
EOF
```

**2. `POST /api/pipelines`** — body is the full pipeline config JSON and
**must include an explicit `"id"`** (that's what the file gets named on
disk):

```bash
curl -X POST http://localhost:3456/api/pipelines -d '{
  "id": "new_pipeline",
  "source": { "type": "csv", "watch_dir": "data/watched", "processed_dir": "data/processed", "poll_interval_secs": 10 },
  "transforms": [],
  "destination": { "type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "imported_data" }
}'
```

Both paths go through the same validation as everything else: unknown
`depends_on` ids, dependency cycles, and duplicate ids are rejected —
via the API, before anything is written to disk; via a dropped file, the
file is left in place but ignored (logged) until it's fixed. Not available
in legacy single-file mode (`POST /api/pipelines` returns `400` — there's
no directory to add a file to).

### Authentication

Auth lives entirely in the **gateway** (`server/`), not the engine — the
engine has no auth of its own and is never meant to be reachable except
from the gateway. No authentication by default on the gateway either —
fine for local/trusted-network use, but the dashboard can trigger runs,
pause pipelines, and rewrite config files (including connection strings),
so **do not expose the gateway's port beyond localhost without enabling
auth**.

Set both env vars on the gateway to turn on HTTP Basic Auth for the entire
app (dashboard + every proxied `/api/*` route + the `/ws/logs` WebSocket):

```bash
cd server
ETL_AUTH_USER=admin ETL_AUTH_PASS=change-me \
  ETL_ENGINE_URL=http://localhost:4000 PORT=3456 npm start
```

- Neither set → auth disabled (default), a warning is logged at startup.
- Only one set → the gateway refuses to start (fail-fast, rather than run half-protected).
- Browsers prompt for credentials natively on first visit and cache them for the origin; no changes needed to use the dashboard once logged in. `curl` clients: `curl -u admin:change-me http://localhost:3456/api/status`.

---

## Configuration

Pipelines are JSON files. Examples live under `config/`:

| File | Source |
|------|--------|
| `config/pipeline.json` | PostgreSQL |
| `config/pipeline_csv.json` | CSV watch directory |
| `config/pipeline_clickhouse.json` | ClickHouse |

For multi-pipeline runs, put only the pipelines you want active into a dedicated folder such as [`config/active/`](config/active/) — do **not** point the engine at the whole `config/` directory unless you intend to start every example at once (they may fight over the same destinations).

### Pipeline id and schedule

Optional top-level fields:

| Field | Description |
|-------|-------------|
| `id` | Stable pipeline id (default: config file stem, e.g. `pipeline_csv`) |
| `schedule` | Cron expression — **sec min hour day month dow** (e.g. `0 */10 * * * *` = every 10 minutes). When set, it controls *when* the pipeline runs; `poll_interval_secs` on the source is ignored for timing. |

```json
{
  "id": "csv_import",
  "schedule": "0 */10 * * * *",
  "source": {
    "type": "csv",
    "watch_dir": "data/watched",
    "processed_dir": "data/processed",
    "poll_interval_secs": 10
  },
  "transforms": [],
  "destination": {
    "type": "postgres",
    "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb",
    "table": "imported_data"
  }
}
```

Without `schedule`, the engine uses `poll_interval_secs` from the source (fixed interval).

### Pipeline dependencies (`depends_on`)

Optional: make a pipeline wait for one or more others to have last succeeded before it ticks.

```json
{
  "id": "downstream",
  "depends_on": [
    "upstream_a",
    { "id": "upstream_b", "max_staleness_secs": 3600 }
  ],
  ...
}
```

- Bare string (`"upstream_a"`) — satisfied by *any* past success of that pipeline, however old.
- Object form — also requires that success to be no older than `max_staleness_secs`.
- A dependency with no run history yet, or whose most recent run did not succeed (even if an earlier one did), blocks the tick.
- Checked at every scheduled tick, **and immediately when a dependency succeeds** — a successful run broadcasts to its dependents, so `downstream` doesn't wait for its own next tick to notice `upstream` finished. A manual **Run** always bypasses the gate (for both the pipeline you trigger and, if it succeeds, still wakes up its own dependents).
- Validated at load time and again at every hot-reload (via the API or editing the file on disk): unknown ids, self-dependency, and dependency cycles are all rejected before the change takes effect — the previous, valid pipeline keeps running.
- Not validated: dependency cycles are the only structural check; there's no staleness-window-aware backfill or catch-up scheduling — `max_staleness_secs` is a simple age check on the dependency's last success.

### PostgreSQL source

```json
{
  "source": {
    "type": "postgres",
    "connection_string": "postgresql://user:password@localhost:5432/source_db",
    "query": "SELECT id, user_id, amount, status, updated_at FROM transactions WHERE updated_at > $1",
    "poll_interval_secs": 5
  },
  "transforms": [
    { "type": "filter", "column": "status", "value": "active" },
    { "type": "map", "rename": { "user_id": "client_id", "amount": "total_amount" } },
    { "type": "aggregate", "group_by": "client_id", "sum": "total_amount" }
  ],
  "destination": {
    "type": "postgres",
    "connection_string": "postgresql://user:password@localhost:5432/destination_db",
    "table": "orders_summary",
    "unique_key": "client_id"
  }
}
```

The query parameter `$1` is bound to the last successful run timestamp (incremental extract).

### CSV source

```json
{
  "source": {
    "type": "csv",
    "watch_dir": "data/watched",
    "processed_dir": "data/processed",
    "delimiter": ",",
    "chunk_size": 10000,
    "poll_interval_secs": 10
  },
  "transforms": [],
  "destination": {
    "type": "postgres",
    "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb",
    "table": "imported_data",
    "unique_key": null
  }
}
```

Drop `.csv` files into `watch_dir`. After a successful load, files are tracked in state (and can be moved under `processed_dir`).

### ClickHouse source

```json
{
  "source": {
    "type": "clickhouse",
    "host": "http://localhost:8123",
    "database": "default",
    "query": "SELECT id, user_id, amount, status, updated_at FROM orders WHERE updated_at > '{last_run}' FORMAT JSONEachRow",
    "username": "default",
    "password": "",
    "chunk_size": 10000,
    "poll_interval_secs": 30
  },
  "transforms": [],
  "destination": {
    "type": "postgres",
    "connection_string": "postgresql://postgres:password@localhost:5432/dest_db",
    "table": "orders",
    "unique_key": "id"
  }
}
```

Use `{last_run}` in the query template; it is replaced with the last run timestamp.

### ClickHouse destination

```json
{
  "destination": {
    "type": "clickhouse",
    "host": "http://localhost:8123",
    "database": "default",
    "table": "orders_summary",
    "username": "default",
    "password": ""
  }
}
```

Inserted via ClickHouse's HTTP interface (`INSERT ... FORMAT JSONEachRow`)
— same transport/auth style as the ClickHouse source above. `username`/
`password` are optional (omit both for an unauthenticated instance). No
`unique_key`/upsert concept here — ClickHouse's `MergeTree` engines handle
deduplication their own way (e.g. `ReplacingMergeTree`) if you need it;
every `load()` call is a plain insert.

### S3 source

```json
{
  "source": {
    "type": "s3",
    "bucket": "my-bucket",
    "prefix": "incoming",
    "format": "jsonl",
    "delimiter": ",",
    "region": "us-east-1",
    "endpoint_url": null,
    "poll_interval_secs": 30
  },
  "transforms": [],
  "destination": {
    "type": "postgres",
    "connection_string": "postgresql://postgres:password@localhost:5432/dest_db",
    "table": "imported_data",
    "unique_key": null
  }
}
```

Polls for new objects under `prefix` (`format` is `"jsonl"` or `"csv"`,
`delimiter` only applies to CSV). Each new object key is tracked in state
the same way CSV source filenames are — already-processed keys are
skipped on the next poll, no local disk involved.

`endpoint_url` points at an S3-compatible store (MinIO, R2, or the
`s3mock`/`minio` service in `docker-compose.yml`) instead of real AWS;
leave it `null` for AWS itself. **Credentials are never part of the
config** — they come from the standard AWS environment variables
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) at process
start, since pipeline configs flow through the dashboard/API and
embedding secrets there would be a credential-leak risk.

### S3 destination

```json
{
  "destination": {
    "type": "s3",
    "bucket": "my-bucket",
    "prefix": "exports",
    "format": "jsonl",
    "region": "us-east-1",
    "endpoint_url": null
  }
}
```

Each `load()` call writes one new object to
`{prefix}/{unix millis}.{jsonl|csv}` — never overwrites, so concurrent or
successive loads can't collide. Same credential policy as the S3 source
above (env vars only, never in the config).

### Transform types

| Type | Fields | Description |
|------|--------|-------------|
| `filter` | `column`, `value` | Keep rows where the column equals `value` (text match) |
| `map` | `rename` | Rename columns (`old_name` → `new_name`) |
| `aggregate` | `group_by`, `sum` | Group by one column and sum a numeric column |
| `custom` | `script`, `function`, `timeout_ms` | Run a user-authored JS function over the row batch — see below |

### Custom JS transform

When `filter`/`map`/`aggregate` can't express the reshaping you need, drop
to real code:

```json
{
  "type": "custom",
  "script": "scripts/my_transform.js",
  "function": "transform",
  "timeout_ms": 5000
}
```

```js
// scripts/my_transform.js
function transform(rows) {
    return rows
        .filter(r => r.status === 'active')
        .map(r => ({ ...r, total_amount: r.amount * 1.1 }));
}
```

- `script` (required) — path to a `.js` file, resolved the same way as
  every other path in a config (relative to the working directory, or
  absolute).
- `function` (optional, default `"transform"`) — which top-level function
  in the script to call with the row array.
- `timeout_ms` (optional, default `5000`) — execution is interrupted if it
  runs longer than this; that's the *only* sandboxing (no filesystem/
  network restriction) — this engine assumes a trusted operator, not
  untrusted multi-tenant code.
- Runs in-process via an embedded QuickJS engine (`rquickjs`) — no system
  Node/JS runtime needed.
- The script is **re-read from disk on every tick**, not cached — edit it
  and the very next run picks up the change, no reload/restart required.
- Rows cross the boundary as plain JSON (`JSON.parse`/`JSON.stringify`),
  so the function is ordinary, dependency-free JS.
- A thrown error, a missing function, or a timeout all fail the tick with
  `error_kind: "transform"`, same as any other pipeline error.

---

## Docker

```bash
# Postgres + ClickHouse + s3mock only (default profile — for local dev
# against the engine run via `cargo run`, or for exploring the seed data)
docker compose up --build

# Full stack: Postgres + ClickHouse + s3mock + engine (internal-only) + gateway
docker compose --profile app up --build
```

- Postgres: `localhost:5434` (`etl` / `etlpassword` / `etldb`)
- ClickHouse: `localhost:8123` (HTTP), see `docker/clickhouse-init/`
- s3mock (S3-compatible, for the S3 source/destination without real AWS
  credentials): `localhost:9199`, pre-seeded with an `etl-demo` bucket by
  the `s3mock-init` one-shot container. Point `endpoint_url` at
  `http://localhost:9199` (from the host) or `http://s3mock:9090` (from
  another container on the compose network), and set
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` to any non-empty value —
  s3mock doesn't validate them, `object_store` just requires them to be set.
- Gateway (public, dashboard + auth): `localhost:3000`
- Engine: **no published port** — only the `server` (gateway) container can
  reach it, over the compose network at `http://etl-engine:3000`
- Engine config mounted from `./docker`
- Data & state under `./data`

Or run the engine image alone (still internal-only in spirit — pair it with
the gateway, don't publish this port to untrusted networks):

```bash
docker build -t etl-engine .
docker run --rm -e RUST_LOG=info \
  -v "$(pwd)/config:/app/config:ro" \
  -v "$(pwd)/data:/app/data" \
  -p 4000:3000 \
  etl-engine /app/config/pipeline_csv.json /app/data/etl_state.json 3000

# then, separately:
cd server && ETL_ENGINE_URL=http://localhost:4000 npm start
```

---

## Project layout

```text
├── config/                 # Example pipeline configs
│   └── active/             # Put pipelines here for multi-run demos
├── docker/                 # Compose-mounted config
├── src/
│   ├── main.rs             # CLI + startup wiring
│   ├── config.rs           # JSON config types + dir loader + depends_on validation
│   ├── registry.rs         # Build+register+spawn a pipeline (startup, API, or watcher)
│   ├── runtime.rs          # Build extractor/transform/loader
│   ├── scheduler.rs        # Interval / cron + soft controls + depends_on gate + DAG-trigger broadcast
│   ├── history.rs          # Run history ring + JSONL
│   ├── watcher.rs          # Config file hot-reload + new-pipeline auto-registration
│   ├── pipeline.rs         # Extract → transform → load
│   ├── extractor/          # Postgres, CSV, ClickHouse
│   ├── transformer/        # Filter, map, aggregate
│   ├── loader/             # Postgres loader
│   ├── state.rs            # Persistent state + log buffer
│   ├── retry.rs            # Backoff retries
│   └── web/                # Internal control API + WebSocket logs (no auth, no dashboard route)
├── server/                 # Node.js gateway — public surface: dashboard, auth, reverse proxy
│   ├── src/
│   │   ├── index.js        # Express wiring: static dashboard, auth, proxy, listen
│   │   ├── auth.js         # Basic Auth (ETL_AUTH_USER/PASS)
│   │   └── proxy.js        # REST + WebSocket proxy to the engine
│   ├── public/dashboard.html
│   └── Dockerfile
├── docker-compose.yml
└── Dockerfile               # builds the Rust engine image
```

---

## Observability

### Metrics

`GET /metrics` on the engine's internal API (not proxied through the
gateway — same network-isolation story as the rest of `/api/*`; point
Prometheus at `http://etl-engine:3000/metrics` from a container on the
same compose network, or `http://localhost:<port>` locally):

| Metric | Type | Labels |
|---|---|---|
| `etl_pipeline_runs_total` | counter | `pipeline`, `outcome` (`success`/`empty`/`error`) |
| `etl_pipeline_rows_total` | counter | `pipeline` |
| `etl_pipeline_errors_total` | counter | `pipeline`, `kind` (same categories as `error_kind` in the dashboard) |
| `etl_pipeline_duration_seconds` | histogram | `pipeline` |
| `etl_pipeline_blocked_total` | counter | `pipeline` — incremented each time a scheduled tick is skipped because a `depends_on` entry hasn't succeeded |

Deliberately no live gauges (pipeline count, currently-running count) —
`GET /api/status` already answers those; metrics here are for the
over-time questions status polling can't answer.

### Structured logs

Plain text by default. Set `LOG_FORMAT=json` for one-JSON-object-per-line
output (`{"timestamp", "level", "target", "message"}`) — no other change
in behavior, every log call site is unaffected.

```bash
LOG_FORMAT=json RUST_LOG=info cargo run -- config/pipeline.json etl_state.json 4000
```

### Alerting

Optional per-pipeline webhook — set `alert_webhook` in the pipeline config
(or fill it in on the dashboard's Visual tab):

```json
{
  "id": "orders",
  "alert_webhook": "https://hooks.slack.com/services/T000/B000/XXXX",
  "schedule": "*/30 * * * * *",
  ...
}
```

- Fires once when the pipeline **transitions** into `Error` or `Blocked`
  (not on every repeated failing/blocked tick — a pipeline stuck failing
  every 30s sends one alert, not one every 30s) — and again, once, when it
  **recovers**.
- The JSON body always includes a `"text"` field, which is all a Slack
  incoming webhook needs, plus structured fields (`pipeline`, `status`,
  `error_kind`/`reason`) for any other webhook consumer (Discord-via-
  adapter, a custom catcher, n8n/Zapier, etc.):
  ```json
  { "text": "🔴 [orders] error (connection): ...", "pipeline": "orders", "status": "error", "error_kind": "connection", "message": "..." }
  { "text": "🟣 [orders] blocked: waiting on 'upstream'...", "pipeline": "orders", "status": "blocked", "reason": "..." }
  { "text": "✅ [orders] recovered", "pipeline": "orders", "status": "recovered" }
  ```
- Fire-and-forget — a failed webhook delivery is logged and dropped, never
  retried and never slows down a tick. This is a notification channel, not
  a guaranteed-delivery system.
- Hot-reloadable like `schedule`/`depends_on` — editing `alert_webhook` and
  saving (via the API or the file on disk) takes effect on the pipeline's
  next tick, no restart needed.

---

## Replay (backfill)

Re-run a pipeline over already-processed data — "rerun yesterday" or
"reprocess this one file again" — without disturbing the live pipeline's
normal incremental cursor/dedup state. `POST /api/pipelines/:id/replay`
builds a standalone, throwaway pipeline from the config file for one
explicit request, runs it, and tears it down; the live scheduled pipeline
and its state are never touched.

This is a distinct endpoint from `POST /api/pipelines/:id/run` — `/run`
forces an ordinary extra tick against the *live* shared pipeline/state
(same as a normal scheduled tick, just off-schedule); `/replay` is a
separate, isolated run with an explicit range or file/key list.

The request shape depends on the pipeline's source type:

- **Postgres/ClickHouse** (cursor-based) — `{"from": "...", "until": "..."}`,
  an RFC3339 time range:
  ```bash
  curl -X POST http://localhost:3000/api/pipelines/orders/replay \
    -H 'Content-Type: application/json' \
    -d '{"from": "2026-10-08T00:00:00Z", "until": "2026-10-09T00:00:00Z"}'
  ```
  By default a query only has a lower-bound placeholder (`$1`/`{last_run}`)
  and has no upper bound — fine for normal ticks, but a replay needs both
  ends pinned. To make a query replay-bounded, add a second placeholder:
  Postgres `$2`, ClickHouse `{until}`. A query without the second
  placeholder still works for both normal ticks and `/replay` — it's just
  unbounded on the high end (everything after `from`, same as a normal
  tick). When the second placeholder *is* present but no `until` is given
  (an ordinary scheduled tick against a query template someone wrote to
  also support replay), it defaults to "now" — one query template safely
  serves both normal ticks and replay.
  ```json
  { "query": "SELECT ... FROM orders WHERE updated_at > $1 AND updated_at <= $2" }
  ```
- **CSV/S3** (dedup-based — time is irrelevant, a file/key either has or
  hasn't been processed) — `{"keys": [...]}`, specific already-ingested
  filenames (CSV, read from `processed_dir`) or object keys (S3), bypassing
  the dedup check entirely:
  ```bash
  curl -X POST http://localhost:3000/api/pipelines/s3-import/replay \
    -H 'Content-Type: application/json' \
    -d '{"keys": ["incoming/orders-2026-10-08.jsonl"]}'
  ```

Sending the wrong shape for a pipeline's source type (e.g. `keys` against
a Postgres source) is a `400` with an explanatory error, not a silent
no-op.

**Idempotency caveat**: a Postgres destination only dedups a replay against
the original load if `unique_key` is set (`ON CONFLICT (key) DO NOTHING`)
— without it, replaying re-inserts the rows a second time. ClickHouse/S3
destinations have no upsert concept at all; replaying into them always
produces new rows/objects (same as a normal tick). This isn't a bug — it's
the same idempotency model every other tick already has, replay doesn't
change it.

From the dashboard: hover a bar in the Gantt chart and click "Replay this
window" — pre-fills `from`/`until` from that run's start/end. Picking
specific CSV/S3 keys to replay is API-only for now (no dashboard picker).

---

## Development

```bash
cargo test
RUST_LOG=debug cargo run -- config/pipeline.json etl_state.json 3456
```

---

## License

This project is licensed under the [MIT License](LICENSE).
