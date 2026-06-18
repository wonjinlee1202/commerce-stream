"""
Pipeline: Composable stream processing operators.
Inspired by Flink's DataStream API — chain filter/map/aggregate operators
that execute on each event as it arrives (real-time, not batch).
"""

import asyncio
import time
from typing import Callable, Any, Optional, List
from collections import defaultdict
from broker.partition import Event


class WindowAccumulator:
    """Tracks aggregation state for a single time window."""
    def __init__(self, window_start: float, window_size: float):
        self.window_start = window_start
        self.window_end = window_start + window_size
        self.values: List[Any] = []
        self.count = 0

    def add(self, value: Any):
        self.values.append(value)
        self.count += 1

    def is_closed(self, current_time: float) -> bool:
        return current_time >= self.window_end


class TumblingWindow:
    """
    Non-overlapping time windows. Each event belongs to exactly one window.
    When a window closes, emit the aggregated result downstream.
    """
    def __init__(self, window_seconds: float, agg_fn: Callable, emit_fn: Callable):
        self.window_seconds = window_seconds
        self.agg_fn = agg_fn
        self.emit_fn = emit_fn
        self.windows: dict = {}  # window_start -> WindowAccumulator

    def _get_window_start(self, timestamp: float) -> float:
        return (timestamp // self.window_seconds) * self.window_seconds

    async def process(self, value: Any, event_time: float):
        window_start = self._get_window_start(event_time)
        if window_start not in self.windows:
            self.windows[window_start] = WindowAccumulator(window_start, self.window_seconds)

        self.windows[window_start].add(value)

        # Check and emit closed windows (watermark-based)
        current_time = time.time()
        closed = [ws for ws, acc in self.windows.items() if acc.is_closed(current_time)]
        for ws in closed:
            acc = self.windows.pop(ws)
            result = self.agg_fn(acc.values)
            await self.emit_fn({
                "window_start": ws,
                "window_end": ws + self.window_seconds,
                "result": result,
                "count": acc.count,
            })


class Pipeline:
    """
    Composable pipeline of stream operators.

    Usage:
        pipeline = Pipeline()
            .filter(lambda e: e.event_type == "purchase")
            .map(lambda e: e.data.get("amount", 0))
            .aggregate(window_seconds=60, fn=sum, emit=my_handler)

        await pipeline.process(event)
    """

    def __init__(self):
        self._operators: List[Callable] = []
        self._windows: List[TumblingWindow] = []
        self._sink: Optional[Callable] = None

    def filter(self, predicate: Callable[[Event], bool]) -> "Pipeline":
        """Drop events that don't match the predicate."""
        async def _filter(event: Event) -> Optional[Event]:
            return event if predicate(event) else None
        self._operators.append(_filter)
        return self

    def map(self, transform: Callable[[Event], Any]) -> "Pipeline":
        """Transform each event into a new value."""
        async def _map(value: Any) -> Any:
            return transform(value)
        self._operators.append(_map)
        return self

    def aggregate(self, window_seconds: float, fn: Callable, emit: Callable) -> "Pipeline":
        """
        Tumbling window aggregation.
        fn: e.g. sum, len, lambda vals: sum(vals)/len(vals)
        emit: async callback receiving window result dict
        """
        window = TumblingWindow(window_seconds, fn, emit)
        self._windows.append(window)

        async def _aggregate(value: Any) -> Any:
            await window.process(value, time.time())
            return value  # Pass through for further operators
        self._operators.append(_aggregate)
        return self

    def sink(self, output_fn: Callable) -> "Pipeline":
        """Terminal operator — final destination for processed events."""
        self._sink = output_fn
        return self

    async def process(self, event: Event):
        """
        Run the event through all operators in sequence.
        Short-circuits on None (filtered out).
        """
        value = event
        for operator in self._operators:
            value = await operator(value)
            if value is None:
                return  # Filtered out

        if self._sink and value is not None:
            await self._sink(value)

    async def process_batch(self, events: list) -> None:
        """Run a batch of events through the pipeline sequentially."""
        for event in events:
            await self.process(event)
