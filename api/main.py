"""
API Server: control plane for the stream engine.
FastAPI owns HTTP/WebSocket concerns while EngineRuntime owns the data plane.
"""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from engine.config import EngineConfig
from engine.runtime import EngineRuntime


config = EngineConfig.from_env()
runtime = EngineRuntime(config)
connected_clients: Set[WebSocket] = set()
broadcaster_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global broadcaster_task

    print("=" * 50)
    print("  E-Commerce Stream Engine Starting")
    print("=" * 50)
    print(f"[Engine] Mode={config.mode} Partitions={config.num_partitions} Shards={config.shard_count}")

    await runtime.start()
    broadcaster_task = asyncio.create_task(_websocket_broadcaster())

    print("✓ Engine online — dashboard at http://localhost:3000")
    yield

    if broadcaster_task is not None:
        broadcaster_task.cancel()
        await asyncio.gather(broadcaster_task, return_exceptions=True)
        broadcaster_task = None
    await runtime.stop()
    print("Engine shut down cleanly")


app = FastAPI(title="E-Commerce Stream Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _websocket_broadcaster():
    """
    Push analytics snapshots to clients at 1Hz.
    The API acts purely as a read-only observability/control surface.
    """
    global connected_clients
    while True:
        await asyncio.sleep(1.0)
        if not connected_clients or not runtime.is_ready():
            continue

        payload = json.dumps(
            {
                "type": "metrics_update",
                "ts": time.time(),
                **runtime.metrics_snapshot(),
            }
        )

        dead_clients = set()
        for client in connected_clients.copy():
            try:
                await client.send_text(payload)
            except Exception:
                dead_clients.add(client)
        connected_clients -= dead_clients


@app.get("/")
def root():
    return {"status": "running", "mode": config.mode, "partitions": config.num_partitions}


@app.get("/engine/config")
def get_engine_config():
    return {"engine": config.to_dict()}


@app.get("/metrics/snapshot")
def get_snapshot():
    return runtime.metrics_snapshot()


@app.get("/metrics/broker")
def get_broker_stats():
    snapshot = runtime.metrics_snapshot()
    return {"partitions": snapshot["broker"], "engine": snapshot["engine"]}


@app.post("/simulator/flash-sale")
async def trigger_flash_sale(
    duration: float | None = None,
    multiplier: float | None = None,
):
    duration = config.flash_sale_duration_seconds if duration is None else duration
    multiplier = config.flash_sale_multiplier if multiplier is None else multiplier
    await runtime.trigger_flash_sale(duration, multiplier)
    return {"status": "triggered", "duration": duration, "multiplier": multiplier}


@app.post("/simulator/rate")
async def set_rate(rate: float = 40.0):
    runtime.set_rate(rate)
    return {"status": "updated", "rate": rate}


@app.post("/system/reset")
async def reset_system():
    await runtime.reset()
    return {"status": "reset", "partitions": config.num_partitions}


@app.get("/replay/{partition_id}")
def replay_partition(partition_id: int, from_offset: int = 0, limit: int = 100):
    if partition_id >= len(runtime.partitions):
        return {"error": "Partition not found"}
    events = []
    for event in runtime.partitions[partition_id].replay_from_offset(from_offset):
        events.append(event.to_dict())
        if len(events) >= limit:
            break
    return {
        "partition_id": partition_id,
        "from_offset": from_offset,
        "events": events,
        "count": len(events),
    }


@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        if runtime.is_ready():
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "initial",
                        **runtime.metrics_snapshot(),
                    }
                )
            )
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(websocket)
