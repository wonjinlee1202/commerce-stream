# E-Commerce Stream Engine

A from-scratch real-time stream processing system inspired by Apache Kafka,
Apache Flink, and Spark Streaming, built with Python, FastAPI, asyncio, and React.

This project simulates high-volume e-commerce traffic, partitions it by user,
processes it in near real time, persists broker/state logs for recovery, and
renders live operational metrics in a dashboard.

## Highlights

- Sustained high-throughput demo mode tuned for roughly `10k` events/sec
- Partitioned broker with bounded queues and visible backpressure
- Batched producer, broker logging, consumer draining, and WAL writes
- Adaptive flash-sale controller that stresses the system without destroying latency
- Live dashboard with throughput, lag, latency, funnel, geo, and product metrics
- Full system reset button in the UI
- Clear control-plane/data-plane separation inside the codebase
- Config-driven engine modes with shard-aware execution

## Architecture

```text
                +---------------------------+
                |        FastAPI API        |
                |  REST + WebSocket control |
                +-------------+-------------+
                              |
                              v
                    +-------------------+
                    |   EngineRuntime   |
                    | lifecycle/config  |
                    +---------+---------+
                              |
          +-------------------+-------------------+
          |                                       |
          v                                       v
   +--------------+                      +----------------+
   |  Simulator   |                      | StreamProcessor|
   | load source  |                      | shard workers  |
   +------+-------+                      +--------+-------+
          |                                       |
          v                                       v
     +----------+                         +---------------+
     | Producer | ---- hash(user_id) ---> |  Partitions   |
     +----------+                         | queue + log   |
                                          +-------+-------+
                                                  |
                                                  v
                                            +-----------+
                                            |StateStore |
                                            |WAL+snapsh.|
                                            +-----------+
```

## What Changed From The Initial Version

The engine has been tuned significantly beyond the original prototype:

- Increased from `4` to `8` partitions
- Added batched `send_batch()` routing by partition
- Added batched partition log appends and batched consumer draining
- Added batched state-store WAL writes and batched event application
- Added smoothed EPS metrics and latency charts in the dashboard
- Added adaptive flash-sale throttling
- Added backend reset endpoint and dashboard reset button
- Added snapshot-based startup/recovery path to avoid giant WAL replays
- Changed dev startup behavior to default to a fresh fast boot
- Split engine lifecycle out of `api/main.py` into `engine/runtime.py`
- Added config-driven engine modes in `engine/config.py`
- Changed execution from conceptually "one worker per partition" to shard-oriented workers

## Setup

### Backend

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn api.main:app --reload --port 8000
```

### Frontend

```bash
npm install
npm run dev
```

Dashboard URL:

```text
http://localhost:3000
```

Backend URL:

```text
http://localhost:8000
```

## Startup Modes

### Fast Dev Mode

This is now the default:

```bash
uvicorn api.main:app --reload --port 8000
```

Behavior:

- uses `ENGINE_MODE=demo` unless overridden
- clears old runtime files on startup
- avoids replaying huge historical logs/WAL during normal development
- gives much faster boot times with `--reload`

### Recovery Demo Mode

Use this when you want to show persistence and restart recovery:

```bash
ENGINE_MODE=recovery uvicorn api.main:app --reload --port 8000
```

Behavior:

- restores state from a snapshot first
- replays the remaining WAL tail
- preserves the fault-tolerance story without paying full replay cost every time

### Other Engine Modes

Examples:

```bash
ENGINE_MODE=dev uvicorn api.main:app --reload --port 8000
ENGINE_MODE=stress uvicorn api.main:app --reload --port 8000
```

Modes tune things like:

- base simulator rate
- flash-sale aggressiveness
- reset vs recovery startup behavior
- shard count and batching overrides through environment variables

## Dashboard Controls

The dashboard now includes:

- `Trigger Flash Sale`
- `Reset System`

## Key Endpoints

| Endpoint | Description |
|---|---|
| `GET /engine/config` | Active engine mode and runtime configuration |
| `GET /metrics/snapshot` | Current metrics, broker stats, worker stats, simulator stats |
| `GET /metrics/broker` | Per-partition broker stats |
| `WS /ws/live` | Live metrics stream pushed at `1 Hz` |
| `POST /simulator/flash-sale` | Trigger an adaptive flash sale |
| `POST /simulator/rate?rate=9000` | Set simulator base rate directly |
| `POST /system/reset` | Fully reset live state, queues, logs, and checkpoints |
| `GET /replay/{partition_id}?from_offset=0` | Replay partition log contents |

## Performance Story

The best demo metrics for this project are not "maximum chaos"; they are
high throughput with stable latency.

### Recommended Demo Numbers

Using the current tuned simulator settings, local benchmark results were in this range:

- roughly `10k` processed events/sec
- average latency roughly `75-105 ms`
- `p99` latency roughly `80-115 ms`
- little to no steady-state partition lag

That is the range I recommend showing in interviews.

### Why This Looks Good To Recruiters

- It shows sustained throughput, not a one-second spike
- It shows operational maturity: lag, p50, p99, adaptive throttling
- It demonstrates that you understand the tradeoff between peak load and SLOs
- It shows the system can remain stable during traffic surges

## Fault Tolerance

The engine persists:

- append-only partition logs in `logs/`
- state mutations in `checkpoints/state.wal`
- compacted state snapshots in `checkpoints/state.snapshot.json`

The current recovery path is:

1. load snapshot
2. replay WAL tail
3. resume processing

This is much faster than replaying the full WAL from scratch every startup.

## Project Structure

```text
commerce-stream/
├── api/
│   └── main.py
├── engine/
│   ├── config.py
│   ├── runtime.py
│   └── __init__.py
├── broker/
│   ├── consumer.py
│   ├── partition.py
│   └── producer.py
├── dashboard/
│   └── src/
│       └── App.jsx
├── processor/
│   ├── pipeline.py
│   ├── state_store.py
│   └── stream_processor.py
├── simulator/
│   └── event_generator.py
├── checkpoints/
├── logs/
├── requirements.txt
├── package.json
└── README.md
```

## Design Notes

### Broker

- each partition has:
  - a bounded in-memory `asyncio.Queue`
  - an append-only log file
  - offset/checkpoint metadata
- producers naturally experience backpressure when queues fill

### Control Plane vs Data Plane

- `api/main.py` is the control plane:
  - HTTP endpoints
  - WebSocket broadcasting
  - observability surface
- `engine/runtime.py` is the data-plane orchestrator:
  - engine startup/shutdown/reset
  - simulator lifecycle
  - broker + processor wiring

### Processing

- execution is shard-oriented rather than implicitly tied to the API layer
- each shard worker owns one or more partitions
- workers consume batches instead of one event at a time
- state updates are WAL-backed and applied in batches

### Simulator

- realistic session mode still exists
- high-throughput mode is used when `session_delay_scale=0`
- adaptive flash sales scale against queue lag rather than blindly flooding the system

## Demo Script

If you want a clean recruiter walkthrough:

1. Start backend in default mode
2. Start dashboard
3. Show the live steady-state metrics
4. Point out:
   - `Events/sec`
   - `Latency P50 / P99`
   - partition lag
   - conversion funnel
5. Trigger one flash sale
6. Show that throughput stays high while latency remains bounded
7. Use `Reset System` to demonstrate operational control
8. Mention recovery mode with `ENGINE_MODE=recovery`

## Interview Talking Points

- **Why partition by user?** It preserves per-user event ordering and enables horizontal scaling.
- **How is backpressure implemented?** Bounded `asyncio.Queue` instances force producers to wait instead of dropping messages.
- **Why batching?** Batching cuts Python overhead, file I/O overhead, and event-loop scheduling overhead across the broker and processor.
- **Why split control plane from data plane?** It keeps the API focused on control/observability and makes the execution runtime easier to scale or replace.
- **Why shard workers?** Shard ownership is a cleaner stepping stone toward a multi-process or distributed design than tightly coupling one coroutine to one partition forever.
- **Why adaptive flash sales?** A realistic production system should protect latency under burst, not just maximize ingress blindly.
- **Why snapshots plus WAL?** Snapshots keep recovery fast; WAL preserves durability and replayability.

## Notes

- The backend is intentionally tuned for fast iterative development by default.
- If you want full persistence/recovery semantics on every run, use `ENGINE_MODE=recovery`.
- The `Reset System` action clears runtime state and persisted files, so use it when you want a clean slate.
