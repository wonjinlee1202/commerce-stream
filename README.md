# E-Commerce Stream Engine

A from-scratch real-time stream processing system inspired by Apache Kafka,
Apache Flink, and Apache Spark Streaming — built entirely in Python + React.

## Architecture

```
Simulator (Poisson arrivals)
    │
    ▼
Producer ──hash(user_id)──► Partition 0 ──► Worker 0 ──► Pipeline ──► StateStore (WAL)
                         ──► Partition 1 ──► Worker 1 ──► Pipeline ──┘
                         ──► Partition 2 ──► Worker 2 ──► Pipeline ──┘
                         ──► Partition 3 ──► Worker 3 ──► Pipeline ──┘
                                                                         │
                                                                    FastAPI
                                                                    WebSocket
                                                                         │
                                                                    React Dashboard
```

## Features

| Feature | Implementation |
|---|---|
| **Partitioning** | `MD5(user_id) % N` consistent hash routing |
| **Backpressure** | Bounded `asyncio.Queue(maxsize=10_000)` — producers block when full |
| **Replay** | Append-only log files; consumers seek to committed offset |
| **Fault Tolerance** | Write-Ahead Log for state; offset checkpoints for consumers |
| **Real-time processing** | Event-triggered, not batch — each event processed on arrival |
| **Map/Filter/Aggregate** | Composable `Pipeline` class with `TumblingWindow` |
| **Async workers** | One `asyncio` coroutine per partition running concurrently |
| **Flash sales** | Poisson rate multiplier; stress-tests backpressure |

## Setup

### Backend

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the engine
uvicorn api.main:app --reload --port 8000
```

### Frontend

```bash
cd dashboard
npm install
npm run dev
# Opens at http://localhost:3000
```

### Both at once (two terminals)

Terminal 1:
```bash
uvicorn api.main:app --port 8000
```

Terminal 2:
```bash
cd dashboard && npm run dev
```

## Key Endpoints

| Endpoint | Description |
|---|---|
| `GET /metrics/snapshot` | Full analytics snapshot |
| `WS /ws/live` | WebSocket — 1Hz live push |
| `POST /simulator/flash-sale` | Trigger traffic spike |
| `POST /simulator/rate?rate=100` | Adjust event rate |
| `GET /replay/{partition_id}?from_offset=0` | Replay partition log |

## Fault Tolerance Demo

1. Start the engine and let it run for 30+ seconds
2. Kill the process (`Ctrl+C`)
3. Check `checkpoints/state.wal` — all state mutations are there
4. Restart: `uvicorn api.main:app --port 8000`
5. The StateStore replays the WAL and recovers exact totals
6. Check `logs/partition_N.checkpoint` — consumer offsets recovered

## Project Structure

```
ecommerce-stream-engine/
├── broker/
│   ├── partition.py        # Append log + bounded asyncio.Queue (backpressure)
│   ├── producer.py         # Hash-based partition routing
│   └── consumer.py         # Offset-tracking consumer with replay
├── processor/
│   ├── pipeline.py         # Composable operators: filter/map/aggregate/sink
│   ├── state_store.py      # WAL-backed aggregation state
│   └── stream_processor.py # Manages one async worker per partition
├── simulator/
│   └── event_generator.py  # Poisson arrivals, funnel probabilities, flash sales
├── api/
│   └── main.py             # FastAPI: REST + WebSocket broadcaster
├── dashboard/
│   └── src/App.jsx         # React live dashboard (Recharts)
├── logs/                   # Partition log files (auto-created)
└── checkpoints/            # WAL + consumer offsets (auto-created)
```

## Interview Talking Points

- **Why partitioning?** Ordering guarantee within a key (user's events stay ordered), horizontal scalability
- **How does backpressure work?** `asyncio.Queue(maxsize=N)` — when full, `await queue.put()` suspends the producer coroutine, creating natural back-pressure without dropping events
- **What's the WAL for?** Before updating any in-memory state, we write the operation to disk. On crash, we replay from the last checkpoint — same technique used by PostgreSQL, Flink, Kafka
- **How is this different from batch?** Every event triggers computation immediately via the pipeline; windows are tumbling time-based aggregations, not scheduled jobs
- **How does replay work?** Every event is appended to a partition log with a monotonic offset. Consumer checkpoints store the last committed offset. On restart, we seek to that offset and re-read
