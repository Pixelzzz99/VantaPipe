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
| **Error visibility** | Every error carries an `error_kind` (`connection`/`query`/`config`/`load`) shown in the dashboard, not just a message string |
| **Auth** | Optional HTTP Basic Auth (`ETL_AUTH_USER`/`ETL_AUTH_PASS`) gating the whole dashboard + API |
| **Observability** | `RUST_LOG` logging + Web UI with per-pipeline status and live logs |
| **Ops** | Docker image + `docker-compose` for local Postgres + engine |

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

### Transform types

| Type | Fields | Description |
|------|--------|-------------|
| `filter` | `column`, `value` | Keep rows where the column equals `value` (text match) |
| `map` | `rename` | Rename columns (`old_name` → `new_name`) |
| `aggregate` | `group_by`, `sum` | Group by one column and sum a numeric column |

---

## Docker

```bash
# Postgres + ClickHouse only (default profile — for local dev against the
# engine run via `cargo run`, or for exploring the seed data)
docker compose up --build

# Full stack: Postgres + ClickHouse + engine (internal-only) + gateway
docker compose --profile app up --build
```

- Postgres: `localhost:5434` (`etl` / `etlpassword` / `etldb`)
- ClickHouse: `localhost:8123` (HTTP), see `docker/clickhouse-init/`
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

## Development

```bash
cargo test
RUST_LOG=debug cargo run -- config/pipeline.json etl_state.json 3456
```

---

## License

This project is licensed under the [MIT License](LICENSE).
