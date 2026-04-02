"""
Consumer: Reads from a single partition with offset tracking.
On startup, replays from last committed offset for fault tolerance.
"""

import asyncio
from typing import Optional, Callable, Awaitable
from broker.partition import Partition, Event


class Consumer:
    def __init__(self, consumer_id: str, partition: Partition, replay_on_start: bool = False):
        self.consumer_id = consumer_id
        self.partition = partition
        self.current_offset = 0
        self.running = False
        self._replay_on_start = replay_on_start

    async def start(self, handler: Callable[[Event], Awaitable[None]]):
        """
        Start consuming. If replay_on_start=True, re-processes all events
        from the last committed offset before switching to live consumption.
        """
        self.running = True

        # Fault tolerance: replay unprocessed events from log
        if self._replay_on_start and self.partition.committed_offset > 0:
            print(f"[Consumer {self.consumer_id}] Replaying from offset {self.partition.committed_offset}")
            for event in self.partition.replay_from_offset(self.partition.committed_offset):
                await handler(event)
                self.current_offset = self.partition.committed_offset

        # Live consumption loop
        while self.running:
            event = await self.partition.consume(timeout=0.1)
            if event is not None:
                await handler(event)
                self.current_offset += 1
                # Commit every 100 events to avoid checkpoint overhead
                if self.current_offset % 100 == 0:
                    self.partition.commit_offset(self.current_offset)

    def stop(self):
        self.running = False
        self.partition.commit_offset(self.current_offset)
