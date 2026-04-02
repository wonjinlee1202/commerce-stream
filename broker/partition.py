"""
Partition: The core unit of the broker.
Each partition is an append-only log file + bounded in-memory queue.
Backpressure is enforced by the bounded queue — producers block when full.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Optional


@dataclass
class Event:
    event_id: str
    event_type: str
    timestamp: float
    user_id: str
    session_id: str
    data: dict
    partition_key: str = ""

    def to_dict(self):
        return asdict(self)


class Partition:
    def __init__(self, partition_id: int, log_dir: str, max_queue_size: int = 10_000):
        self.partition_id = partition_id
        self.log_path = os.path.join(log_dir, f"partition_{partition_id}.log")
        self.checkpoint_path = os.path.join(log_dir, f"partition_{partition_id}.checkpoint")

        # Bounded queue enforces backpressure — producers await when full
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)

        self.offset = 0
        self.committed_offset = 0
        self._log_file = None
        self._lock = asyncio.Lock()

        # Metrics
        self.total_produced = 0
        self.total_consumed = 0
        self.backpressure_events = 0

        os.makedirs(log_dir, exist_ok=True)
        self._load_checkpoint()

    def _load_checkpoint(self):
        """Restore committed offset from disk on startup."""
        if os.path.exists(self.checkpoint_path):
            with open(self.checkpoint_path, "r") as f:
                data = json.load(f)
                self.committed_offset = data.get("committed_offset", 0)
                self.offset = data.get("offset", 0)

    def _save_checkpoint(self):
        """Persist consumer offset to disk for fault tolerance."""
        with open(self.checkpoint_path, "w") as f:
            json.dump({"committed_offset": self.committed_offset, "offset": self.offset}, f)

    def open(self):
        """Open the append-only log file."""
        self._log_file = open(self.log_path, "a", buffering=1)  # line-buffered

    def close(self):
        if self._log_file:
            self._log_file.flush()
            self._log_file.close()

    async def produce(self, event: Event) -> int:
        """
        Append event to log and enqueue for consumers.
        Blocks (backpressure) if queue is full.
        Returns the offset of the written event.
        """
        if self.queue.full():
            self.backpressure_events += 1

        # This await blocks the producer if queue is at capacity — backpressure
        await self.queue.put(event)

        # Durably append to log file (replay source)
        async with self._lock:
            current_offset = self.offset
            log_entry = {"offset": current_offset, **event.to_dict()}
            self._log_file.write(json.dumps(log_entry) + "\n")
            self.offset += 1
            self.total_produced += 1

        return current_offset

    async def consume(self, timeout: float = 1.0) -> Optional[Event]:
        """
        Consume the next event from the queue.
        Returns None on timeout (allows consumers to check shutdown signals).
        """
        try:
            event = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            self.total_consumed += 1
            return event
        except asyncio.TimeoutError:
            return None

    def commit_offset(self, offset: int):
        """Mark events up to this offset as processed. Persists to disk."""
        self.committed_offset = offset
        self._save_checkpoint()

    def get_lag(self) -> int:
        """How many events are waiting to be consumed."""
        return self.queue.qsize()

    def replay_from_offset(self, start_offset: int):
        """
        Generator that replays events from the log file starting at start_offset.
        This is how we recover from crashes — re-read from last committed offset.
        """
        if not os.path.exists(self.log_path):
            return

        with open(self.log_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry["offset"] >= start_offset:
                        yield Event(
                            event_id=entry["event_id"],
                            event_type=entry["event_type"],
                            timestamp=entry["timestamp"],
                            user_id=entry["user_id"],
                            session_id=entry["session_id"],
                            data=entry["data"],
                            partition_key=entry.get("partition_key", ""),
                        )
                except (json.JSONDecodeError, KeyError):
                    continue

    def stats(self) -> dict:
        return {
            "partition_id": self.partition_id,
            "offset": self.offset,
            "committed_offset": self.committed_offset,
            "queue_size": self.queue.qsize(),
            "lag": self.get_lag(),
            "total_produced": self.total_produced,
            "total_consumed": self.total_consumed,
            "backpressure_events": self.backpressure_events,
        }
