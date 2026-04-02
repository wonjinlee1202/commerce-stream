"""
API Server: FastAPI app exposing analytics state via REST + WebSocket.
The WebSocket broadcaster pushes metric updates at 1Hz to all connected clients.
This decouples event processing rate from UI refresh rate.
"""

import asyncio
import json
import time
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from broker.partition import Partition
from broker.producer import Producer
from processor.state_store import StateStore
from processor.stream_processor import StreamProcessor
from simulator.event_generator import EventSimulator

# ─── System Bootstrap ────────────────────────────────────────────────────────

NUM_PARTITIONS = 4
LOG_DIR = "logs"
WAL_PATH = "checkpoints/state.wal"

state_store: StateStore = None
producer: Producer = None
simulator: EventSimulator = None
stream_processor: StreamProcessor = None
partitions: list = []

connected_clients: Set[WebSocket] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and wire up the entire engine on startup."""
    global state_store, producer, simulator, stream_processor, partitions

    print("=" * 50)
    print("  E-Commerce Stream Engine Starting")
    print("=" * 50)

    # 1. Create partitions (the log-backed queues)
    partitions = [Partition(i, LOG_DIR) for i in range(NUM_PARTITIONS)]
    for p in partitions:
        p.open()

    # 2. State store with WAL (recovers from previous run if logs exist)
    state_store = StateStore(WAL_PATH)

    # 3. Wire producer (hash-routes events to partitions)
    producer = Producer(partitions)

    # 4. Stream processor (one async worker per partition)
    stream_processor = StreamProcessor(partitions, state_store)
    await stream_processor.start()

    # 5. Event simulator (Poisson arrivals, flash sales)
    simulator = EventSimulator(producer, base_rate=40.0)

    # 6. Launch all background tasks
    asyncio.create_task(simulator.run())
    asyncio.create_task(simulator.run_flash_sale_scheduler())
    asyncio.create_task(_websocket_broadcaster())

    print("✓ Engine online — dashboard at http://localhost:3000")
    yield

    # Shutdown
    simulator.stop()
    await stream_processor.stop()
    for p in partitions:
        p.close()
    print("Engine shut down cleanly")


app = FastAPI(title="E-Commerce Stream Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── WebSocket Broadcaster ───────────────────────────────────────────────────

async def _websocket_broadcaster():
    """
    Push analytics snapshot to all connected WebSocket clients every second.
    1Hz update rate regardless of event volume — smooth UI without overload.
    """
    global connected_clients
    while True:
        await asyncio.sleep(1.0)
        if not connected_clients:
            continue

        snapshot = state_store.snapshot()
        broker_stats = producer.get_partition_stats() if producer else []
        worker_stats = stream_processor.worker_stats() if stream_processor else []
        sim_stats = simulator.stats() if simulator else {}

        payload = json.dumps({
            "type": "metrics_update",
            "ts": time.time(),
            "metrics": snapshot,
            "broker": broker_stats,
            "workers": worker_stats,
            "simulator": sim_stats,
        })

        dead_clients = set()
        for client in connected_clients.copy():
            try:
                await client.send_text(payload)
            except Exception:
                dead_clients.add(client)

        connected_clients -= dead_clients


# ─── REST Endpoints ──────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "running", "partitions": NUM_PARTITIONS}


@app.get("/metrics/snapshot")
def get_snapshot():
    """Initial data load for the dashboard."""
    return {
        "metrics": state_store.snapshot(),
        "broker": producer.get_partition_stats(),
        "workers": stream_processor.worker_stats(),
        "simulator": simulator.stats(),
    }


@app.get("/metrics/broker")
def get_broker_stats():
    return {"partitions": producer.get_partition_stats()}


@app.post("/simulator/flash-sale")
async def trigger_flash_sale(duration: float = 30.0, multiplier: float = 8.0):
    """Manually trigger a flash sale from the dashboard."""
    asyncio.create_task(simulator.trigger_flash_sale(duration, multiplier))
    return {"status": "triggered", "duration": duration, "multiplier": multiplier}


@app.post("/simulator/rate")
async def set_rate(rate: float = 40.0):
    """Adjust the base event rate."""
    simulator.base_rate = rate
    simulator.current_rate = rate
    return {"status": "updated", "rate": rate}


@app.get("/replay/{partition_id}")
def replay_partition(partition_id: int, from_offset: int = 0, limit: int = 100):
    """Replay events from a partition log — demonstrates replay capability."""
    if partition_id >= len(partitions):
        return {"error": "Partition not found"}
    events = []
    for event in partitions[partition_id].replay_from_offset(from_offset):
        events.append(event.to_dict())
        if len(events) >= limit:
            break
    return {"partition_id": partition_id, "from_offset": from_offset, "events": events, "count": len(events)}


# ─── WebSocket Endpoint ──────────────────────────────────────────────────────

@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        # Send initial snapshot immediately on connect
        snapshot = state_store.snapshot()
        await websocket.send_text(json.dumps({
            "type": "initial",
            "metrics": snapshot,
            "broker": producer.get_partition_stats(),
        }))
        # Keep connection alive — broadcaster handles updates
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(websocket)