"""
Producer: Routes events to partitions using a consistent hash.
Ensures events with the same partition_key always go to the same partition
(ordering guarantee for per-user event streams).
"""

import asyncio
import hashlib
from typing import List
from broker.partition import Partition, Event


class Producer:
    def __init__(self, partitions: List[Partition]):
        self.partitions = partitions
        self.num_partitions = len(partitions)
        self.total_sent = 0

    def _get_partition(self, partition_key: str) -> Partition:
        """
        Consistent hash routing: same key always maps to same partition.
        This guarantees ordering for a given user's events.
        """
        if not partition_key:
            # Round-robin for keyless events
            idx = self.total_sent % self.num_partitions
        else:
            hash_val = int(hashlib.md5(partition_key.encode()).hexdigest(), 16)
            idx = hash_val % self.num_partitions
        return self.partitions[idx]

    async def send(self, event: Event) -> tuple[int, int]:
        """
        Send an event to the appropriate partition.
        Returns (partition_id, offset).
        Blocks if the target partition queue is full (backpressure propagation).
        """
        partition = self._get_partition(event.partition_key or event.user_id)
        offset = await partition.produce(event)
        self.total_sent += 1
        return partition.partition_id, offset

    async def send_batch(self, events: List[Event]):
        """Send multiple events while batching writes per partition."""
        partitioned: dict[int, list[Event]] = {}
        for event in events:
            partition = self._get_partition(event.partition_key or event.user_id)
            partitioned.setdefault(partition.partition_id, []).append(event)

        results = []
        for partition_id, batch in partitioned.items():
            partition = self.partitions[partition_id]
            offsets = await partition.produce_batch(batch)
            self.total_sent += len(batch)
            results.extend((partition_id, offset) for offset in offsets)
        return results

    def get_partition_stats(self) -> List[dict]:
        return [p.stats() for p in self.partitions]
