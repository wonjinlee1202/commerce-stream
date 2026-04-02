"""
StreamProcessor: Spawns one async worker per partition.
Each worker runs a Pipeline on events as they arrive.
Multiple workers = true parallelism at the partition level.
"""

import asyncio
from typing import List
from broker.partition import Partition, Event
from processor.pipeline import Pipeline
from processor.state_store import StateStore


class PartitionWorker:
    """
    One worker per partition. Consumes events and runs them through
    the processing pipeline. Workers are independent — a slow worker
    on partition 2 doesn't block partition 0.
    """
    def __init__(self, partition: Partition, pipeline: Pipeline, worker_id: str):
        self.partition = partition
        self.pipeline = pipeline
        self.worker_id = worker_id
        self.running = False
        self.processed = 0
        self.errors = 0

    async def run(self):
        self.running = True
        print(f"[Worker {self.worker_id}] Started on partition {self.partition.partition_id}")

        while self.running:
            event = await self.partition.consume(timeout=0.1)
            if event is None:
                continue
            try:
                await self.pipeline.process(event)
                self.processed += 1
            except Exception as e:
                self.errors += 1
                print(f"[Worker {self.worker_id}] Error processing event: {e}")

    def stop(self):
        self.running = False

    def stats(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "partition_id": self.partition.partition_id,
            "processed": self.processed,
            "errors": self.errors,
            "queue_lag": self.partition.get_lag(),
        }


class StreamProcessor:
    """
    Manages the full set of partition workers.
    Each partition gets its own Pipeline instance and PartitionWorker.
    """

    def __init__(self, partitions: List[Partition], state_store: StateStore):
        self.partitions = partitions
        self.state_store = state_store
        self.workers: List[PartitionWorker] = []
        self._tasks: List[asyncio.Task] = []

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
        """Spawn one worker per partition as concurrent asyncio tasks."""
        for partition in self.partitions:
            pipeline = self._build_pipeline()
            worker = PartitionWorker(
                partition=partition,
                pipeline=pipeline,
                worker_id=f"worker-p{partition.partition_id}",
            )
            self.workers.append(worker)
            task = asyncio.create_task(worker.run())
            self._tasks.append(task)

        print(f"[StreamProcessor] Started {len(self.workers)} partition workers")

    async def stop(self):
        for worker in self.workers:
            worker.stop()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        print("[StreamProcessor] All workers stopped")

    def worker_stats(self) -> List[dict]:
        return [w.stats() for w in self.workers]
