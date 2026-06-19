"""
Engine runtime: owns data-plane lifecycle and exposes a clean control-plane API.
"""

from __future__ import annotations

import asyncio
import os
import time

from broker.partition import Partition
from broker.producer import Producer
from engine.config import EngineConfig
from processor.state_store import StateStore
from processor.stream_processor import StreamProcessor
from simulator.event_generator import EventSimulator


class EngineRuntime:
    def __init__(self, config: EngineConfig):
        self.config = config
        self.state_store: StateStore | None = None
        self.producer: Producer | None = None
        self.simulator: EventSimulator | None = None
        self.stream_processor: StreamProcessor | None = None
        self.partitions: list[Partition] = []
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()
        self._started_at: float | None = None

    async def start(self):
        async with self._lock:
            await self._start_unlocked()

    async def _start_unlocked(self):
        if self.config.reset_on_start and not self.config.recover_on_start:
            self.clear_persistent_state()

        self.partitions = [
            Partition(
                partition_id=i,
                log_dir=self.config.log_dir,
                max_queue_size=self.config.max_queue_size,
                log_max_bytes=self.config.log_max_bytes_per_partition,
            )
            for i in range(self.config.num_partitions)
        ]
        for partition in self.partitions:
            partition.open()

        self.state_store = StateStore(
            self.config.wal_path,
            snapshot_path=self.config.snapshot_path,
            recover_on_start=self.config.recover_on_start,
        )
        self.producer = Producer(self.partitions)
        self.stream_processor = StreamProcessor(
            partitions=self.partitions,
            state_store=self.state_store,
            shard_count=self.config.shard_count,
            batch_size=self.config.batch_size,
        )
        await self.stream_processor.start()

        self.simulator = EventSimulator(
            producer=self.producer,
            base_rate=self.config.base_rate,
            session_delay_scale=self.config.session_delay_scale,
        )
        self.simulator.default_flash_multiplier = self.config.flash_sale_multiplier
        self.simulator.flash_duration_seconds = self.config.flash_sale_duration_seconds

        self._tasks = [
            asyncio.create_task(self.simulator.run()),
            asyncio.create_task(self.simulator.run_flash_sale_scheduler()),
            asyncio.create_task(self._compaction_loop()),
        ]
        self._started_at = time.time()

    async def _compaction_loop(self):
        """Periodically snapshot state and truncate the WAL to bound disk usage."""
        while True:
            await asyncio.sleep(self.config.wal_compact_interval_seconds)
            if self.state_store is not None:
                self.state_store.compact()
                print(f"[Engine] WAL compacted — snapshot saved, WAL reset")

    async def stop(self):
        async with self._lock:
            await self._stop_unlocked()

    async def _stop_unlocked(self):
        if self.simulator is not None:
            self.simulator.stop()

        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

        if self.stream_processor is not None:
            await self.stream_processor.stop()

        for partition in self.partitions:
            partition.close()

        if self.state_store is not None:
            self.state_store.close(compact=self.config.recover_on_start)

        self.state_store = None
        self.producer = None
        self.simulator = None
        self.stream_processor = None
        self.partitions = []
        self._started_at = None

    async def reset(self):
        async with self._lock:
            await self._stop_unlocked()
            self.clear_persistent_state()
            await self._start_unlocked()

    def clear_persistent_state(self):
        for root in (self.config.log_dir, os.path.dirname(self.config.wal_path)):
            if not root or not os.path.exists(root):
                continue
            for dirpath, _, filenames in os.walk(root):
                for filename in filenames:
                    try:
                        os.remove(os.path.join(dirpath, filename))
                    except FileNotFoundError:
                        continue

    async def trigger_flash_sale(self, duration: float, multiplier: float):
        if self.simulator is None:
            return
        asyncio.create_task(self.simulator.trigger_flash_sale(duration, multiplier))

    def set_rate(self, rate: float):
        if self.simulator is None:
            return
        self.simulator.base_rate = rate
        self.simulator.current_rate = rate

    def is_ready(self) -> bool:
        return all(
            component is not None
            for component in (self.state_store, self.producer, self.simulator, self.stream_processor)
        )

    def metrics_snapshot(self) -> dict:
        return {
            "metrics": self.state_store.snapshot(),
            "broker": self.producer.get_partition_stats(),
            "workers": self.stream_processor.worker_stats(),
            "simulator": self.simulator.stats(),
            "engine": self.engine_stats(),
        }

    def engine_stats(self) -> dict:
        uptime_seconds = 0.0 if self._started_at is None else round(time.time() - self._started_at, 2)
        return {
            "mode": self.config.mode,
            "num_partitions": self.config.num_partitions,
            "shard_count": self.config.shard_count,
            "batch_size": self.config.batch_size,
            "max_queue_size": self.config.max_queue_size,
            "recover_on_start": self.config.recover_on_start,
            "reset_on_start": self.config.reset_on_start,
            "uptime_seconds": uptime_seconds,
        }
