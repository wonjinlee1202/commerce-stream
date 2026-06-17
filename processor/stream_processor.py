"""
StreamProcessor: Shard-oriented execution manager.
Each worker owns one or more partitions and drains them in batches.
This keeps the current in-process runtime efficient while modeling
the same ownership boundaries we would use in a multi-process design.
"""

import asyncio
from typing import List
from broker.partition import Partition
from processor.pipeline import Pipeline
from processor.state_store import StateStore


class ShardWorker:
    """
    One worker per shard. A shard owns one or more partitions and
    consumes from them in a round-robin loop.
    """
    def __init__(
        self,
        partitions: List[Partition],
        pipeline: Pipeline,
        state_store: StateStore,
        shard_id: str,
        batch_size: int,
    ):
        self.partitions = partitions
        self.pipeline = pipeline
        self.state_store = state_store
        self.shard_id = shard_id
        self.batch_size = batch_size
        self.running = False
        self.processed = 0
        self.errors = 0

    async def run(self):
        self.running = True
        partition_ids = [partition.partition_id for partition in self.partitions]
        print(f"[Shard {self.shard_id}] Started for partitions {partition_ids}")

        while self.running:
            drained_any = False
            for partition in self.partitions:
                events = await partition.consume_batch(max_items=self.batch_size, timeout=0.01)
                if not events:
                    continue
                drained_any = True
                try:
                    await self.state_store.record_events(events)
                    self.processed += len(events)
                except Exception as e:
                    self.errors += 1
                    print(f"[Shard {self.shard_id}] Error processing event: {e}")
            if not drained_any:
                await asyncio.sleep(0.01)

    def stop(self):
        self.running = False

    def stats(self) -> dict:
        return {
            "shard_id": self.shard_id,
            "partition_ids": [partition.partition_id for partition in self.partitions],
            "processed": self.processed,
            "errors": self.errors,
            "queue_lag": sum(partition.get_lag() for partition in self.partitions),
        }


class StreamProcessor:
    """
    Manages the full set of partition workers.
    Each shard gets its own Pipeline instance and ShardWorker.
    """

    def __init__(
        self,
        partitions: List[Partition],
        state_store: StateStore,
        shard_count: int | None = None,
        batch_size: int = 256,
    ):
        self.partitions = partitions
        self.state_store = state_store
        self.shard_count = max(1, min(shard_count or len(partitions), len(partitions)))
        self.batch_size = batch_size
        self.workers: List[ShardWorker] = []
        self._tasks: List[asyncio.Task] = []

    def _build_shards(self) -> List[List[Partition]]:
        shards: List[List[Partition]] = [[] for _ in range(self.shard_count)]
        for index, partition in enumerate(self.partitions):
            shards[index % self.shard_count].append(partition)
        return [shard for shard in shards if shard]

    def _build_pipeline(self) -> Pipeline:
        """
        Build the processing pipeline.
        All events flow through: record in state store (with WAL).
        Purchases additionally update revenue aggregations.
        """
        pipeline = Pipeline()

        # Pass every event into the state store
        pipeline.sink(self.state_store.record_event)

        return pipeline

    async def start(self):
        """Spawn one worker per shard as concurrent asyncio tasks."""
        for shard_index, shard_partitions in enumerate(self._build_shards()):
            pipeline = self._build_pipeline()
            worker = ShardWorker(
                partitions=shard_partitions,
                pipeline=pipeline,
                state_store=self.state_store,
                shard_id=f"shard-{shard_index}",
                batch_size=self.batch_size,
            )
            self.workers.append(worker)
            task = asyncio.create_task(worker.run())
            self._tasks.append(task)

        print(f"[StreamProcessor] Started {len(self.workers)} shard workers")

    async def stop(self):
        for worker in self.workers:
            worker.stop()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        print("[StreamProcessor] All workers stopped")

    def worker_stats(self) -> List[dict]:
        return [w.stats() for w in self.workers]
