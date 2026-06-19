"""
StateStore: Write-Ahead Log backed state for stream aggregations.
Before updating any state, write the operation to disk.
On crash/restart, replay the WAL to reconstruct exact state.
This is how databases (and Flink) achieve fault tolerance.
"""

import json
import os
import time
import statistics
from collections import Counter, deque


class WriteAheadLog:
    """
    Append-only log of state mutations.
    Every state change is recorded here before being applied to memory.
    On restart, replay to recover exact state.
    """
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        os.makedirs(os.path.dirname(wal_path), exist_ok=True)
        self._file = open(wal_path, "a", buffering=1024 * 1024)
        self._pending_entries: list[str] = []
        self._flush_every = 0.05
        self._flush_batch_size = 512
        self._flush_task = None

    def write(self, operation: dict):
        """Durably record a state mutation."""
        entry = {"ts": time.time(), **operation}
        self._pending_entries.append(json.dumps(entry) + "\n")
        if len(self._pending_entries) >= self._flush_batch_size:
            self.flush()

    def write_batch(self, operations: list[dict]):
        """Durably record a batch of state mutations."""
        ts = time.time()
        self._pending_entries.extend(
            json.dumps({"ts": ts, **operation}) + "\n" for operation in operations
        )
        if len(self._pending_entries) >= self._flush_batch_size:
            self.flush()

    def start(self):
        try:
            import asyncio
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._flush_task = loop.create_task(self._flush_loop())

    async def _flush_loop(self):
        import asyncio

        while True:
            await asyncio.sleep(self._flush_every)
            self.flush()

    def flush(self):
        if not self._pending_entries:
            return
        self._file.writelines(self._pending_entries)
        self._pending_entries.clear()
        self._file.flush()

    def replay(self):
        """Generator: yield all WAL entries for state reconstruction."""
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue

    def close(self):
        self.flush()
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None
        self._file.close()

    def reset(self):
        self.flush()
        self._file.seek(0)
        self._file.truncate()


class StateStore:
    """
    Central aggregation state for the analytics engine.
    All mutations go through the WAL before being applied.
    Thread-safe via asyncio lock.
    """

    def __init__(
        self,
        wal_path: str = "checkpoints/state.wal",
        snapshot_path: str | None = None,
        recover_on_start: bool = True,
    ):
        self.wal = WriteAheadLog(wal_path)
        self.wal.start()
        self.snapshot_path = snapshot_path or f"{os.path.splitext(wal_path)[0]}.snapshot.json"

        # Live metrics state
        self.revenue_per_minute: deque = deque(maxlen=60)
        self.orders_per_minute: deque = deque(maxlen=60)
        self.events_per_second: deque = deque(maxlen=60)
        self.active_users: set = set()
        self.top_products: Counter = Counter()
        self.top_categories: Counter = Counter()
        self.funnel: dict = {"page_view": 0, "product_click": 0, "add_to_cart": 0, "checkout_start": 0, "purchase": 0}
        self.total_revenue: float = 0.0
        self.total_orders: int = 0
        self.total_events: int = 0
        self.refunds: int = 0
        self.refund_amount: float = 0.0
        self.avg_order_value: float = 0.0
        self.geo_distribution: Counter = Counter()
        self.device_distribution: Counter = Counter()
        self.search_terms: Counter = Counter()

        # Latency tracking (event creation time → state store processing time)
        self._latency_samples: deque = deque(maxlen=500)
        self.p50_latency_ms: float = 0.0
        self.p99_latency_ms: float = 0.0
        self.avg_latency_ms: float = 0.0
        self._latency_recalc_counter: int = 0

        # Time-series buckets (current minute)
        self._current_minute: int = 0
        self._minute_revenue: float = 0.0
        self._minute_orders: int = 0
        self._second_events: int = 0
        self._current_second: int = 0

        # Exactly-once deduplication: track seen event IDs within a rolling window.
        # Events arriving with a previously seen ID are dropped before state is touched.
        self._seen_event_ids: dict[str, float] = {}  # event_id -> timestamp first seen
        self._dedup_window_seconds: float = 30.0
        self._dedup_prune_counter: int = 0
        self._dedup_prune_interval: int = 5_000
        self.duplicates_rejected: int = 0

        if recover_on_start:
            self._recover()

    def _recover(self):
        """Restore from snapshot first, then replay any WAL tail."""
        self._recover_from_snapshot()
        self._recover_from_wal()

    def _recover_from_snapshot(self):
        if not os.path.exists(self.snapshot_path):
            return
        try:
            with open(self.snapshot_path, "r") as snapshot_file:
                snapshot = json.load(snapshot_file)
        except (OSError, json.JSONDecodeError):
            return

        self.revenue_per_minute = deque(snapshot.get("revenue_per_minute", []), maxlen=60)
        self.orders_per_minute = deque(snapshot.get("orders_per_minute", []), maxlen=60)
        self.events_per_second = deque(snapshot.get("events_per_second", []), maxlen=60)
        self.active_users = set(snapshot.get("active_users", []))
        self.top_products = Counter(snapshot.get("top_products", {}))
        self.top_categories = Counter(snapshot.get("top_categories", {}))
        self.funnel = snapshot.get("funnel", self.funnel)
        self.total_revenue = snapshot.get("total_revenue", 0.0)
        self.total_orders = snapshot.get("total_orders", 0)
        self.total_events = snapshot.get("total_events", 0)
        self.refunds = snapshot.get("refunds", 0)
        self.refund_amount = snapshot.get("refund_amount", 0.0)
        self.avg_order_value = snapshot.get("avg_order_value", 0.0)
        self.geo_distribution = Counter(snapshot.get("geo_distribution", {}))
        self.device_distribution = Counter(snapshot.get("device_distribution", {}))
        self.search_terms = Counter(snapshot.get("search_terms", {}))
        self._current_minute = snapshot.get("_current_minute", 0)
        self._minute_revenue = snapshot.get("_minute_revenue", 0.0)
        self._minute_orders = snapshot.get("_minute_orders", 0)
        self._current_second = snapshot.get("_current_second", 0)
        self._second_events = snapshot.get("_second_events", 0)
        now = time.time()
        self._seen_event_ids = {
            eid: ts for eid, ts in snapshot.get("seen_event_ids", {}).items()
            if ts > now - self._dedup_window_seconds
        }
        self.duplicates_rejected = snapshot.get("duplicates_rejected", 0)

    def _recover_from_wal(self):
        """Replay WAL to reconstruct state after crash."""
        count = 0
        for entry in self.wal.replay():
            self._apply_entry(entry)
            count += 1
        if count > 0:
            print(f"[StateStore] Recovered {count} operations from WAL")

    def _save_snapshot(self):
        os.makedirs(os.path.dirname(self.snapshot_path), exist_ok=True)
        snapshot = {
            "revenue_per_minute": list(self.revenue_per_minute),
            "orders_per_minute": list(self.orders_per_minute),
            "events_per_second": list(self.events_per_second),
            "active_users": list(self.active_users),
            "top_products": dict(self.top_products),
            "top_categories": dict(self.top_categories),
            "funnel": self.funnel,
            "total_revenue": self.total_revenue,
            "total_orders": self.total_orders,
            "total_events": self.total_events,
            "refunds": self.refunds,
            "refund_amount": self.refund_amount,
            "avg_order_value": self.avg_order_value,
            "geo_distribution": dict(self.geo_distribution),
            "device_distribution": dict(self.device_distribution),
            "search_terms": dict(self.search_terms),
            "_current_minute": self._current_minute,
            "_minute_revenue": self._minute_revenue,
            "_minute_orders": self._minute_orders,
            "_current_second": self._current_second,
            "_second_events": self._second_events,
            "seen_event_ids": {
                eid: ts for eid, ts in self._seen_event_ids.items()
                if ts > time.time() - self._dedup_window_seconds
            },
            "duplicates_rejected": self.duplicates_rejected,
        }
        with open(self.snapshot_path, "w") as snapshot_file:
            json.dump(snapshot, snapshot_file)

    def _apply_entry(self, entry: dict):
        """Apply a single WAL entry to in-memory state (used during recovery)."""
        op = entry.get("op")
        if op != "event":
            return

        # Rebuild seen IDs during WAL recovery so dedup survives restarts
        event_id = entry.get("event_id")
        if event_id:
            self._seen_event_ids[event_id] = entry.get("ts", time.time())

        event_type = entry.get("event_type")
        data = entry.get("data", {})

        if event_type == "purchase":
            amount = data.get("amount", 0)
            self.total_revenue += amount
            self.total_orders += 1
            if data.get("product_name"):
                self.top_products[data["product_name"]] += 1
            if data.get("category"):
                self.top_categories[data["category"]] += 1

        if event_type in self.funnel:
            self.funnel[event_type] += 1

        user_id = entry.get("user_id")
        if user_id:
            self.active_users.add(user_id)

        if event_type == "refund":
            self.refunds += 1
            self.refund_amount += data.get("amount", 0)

        if "country" in data:
            self.geo_distribution[data["country"]] += 1

        if "device" in data:
            self.device_distribution[data["device"]] += 1

        if event_type == "search_query" and "query" in data:
            self.search_terms[data["query"]] += 1

    async def record_event(self, event):
        """Process a raw event into state. WAL-first, then apply."""
        await self.record_events([event])

    async def record_events(self, events):
        """Process a batch of raw events into state."""
        if not events:
            return

        now_time = time.time()

        # Exactly-once: drop any event whose ID we've already processed.
        # Prune stale IDs periodically to keep memory bounded.
        self._dedup_prune_counter += len(events)
        if self._dedup_prune_counter >= self._dedup_prune_interval:
            self._dedup_prune_counter = 0
            cutoff = now_time - self._dedup_window_seconds
            self._seen_event_ids = {
                eid: ts for eid, ts in self._seen_event_ids.items() if ts > cutoff
            }

        unique_events = []
        for event in events:
            if event.event_id in self._seen_event_ids:
                self.duplicates_rejected += 1
            else:
                self._seen_event_ids[event.event_id] = now_time
                unique_events.append(event)

        if not unique_events:
            return
        events = unique_events

        second = int(now_time)
        minute = second // 60
        wal_entries = []
        purchase_amounts = []

        for event in events:
            latency_ms = (now_time - event.timestamp) * 1000
            self._latency_samples.append(latency_ms)
            self._latency_recalc_counter += 1
            self.total_events += 1

            entry = {
                "op": "event",
                "event_id": event.event_id,
                "event_type": event.event_type,
                "user_id": event.user_id,
                "data": event.data,
            }
            wal_entries.append(entry)
            self._apply_entry(entry)

            if event.event_type == "purchase":
                purchase_amounts.append(event.data.get("amount", 0))

        if self._latency_recalc_counter >= 250:
            self._latency_recalc_counter = 0
            sorted_s = sorted(self._latency_samples)
            n = len(sorted_s)
            self.p50_latency_ms = round(sorted_s[int(n * 0.50)], 2)
            self.p99_latency_ms = round(sorted_s[min(int(n * 0.99), n - 1)], 2)
            self.avg_latency_ms = round(statistics.mean(sorted_s), 2)

        if second != self._current_second:
            self.events_per_second.append({"ts": self._current_second, "count": self._second_events})
            self._second_events = 0
            self._current_second = second
        self._second_events += len(events)

        if minute != self._current_minute:
            self.revenue_per_minute.append({"ts": self._current_minute * 60, "revenue": self._minute_revenue})
            self.orders_per_minute.append({"ts": self._current_minute * 60, "orders": self._minute_orders})
            self._minute_revenue = 0.0
            self._minute_orders = 0
            self._current_minute = minute

        self.wal.write_batch(wal_entries)

        if purchase_amounts:
            self._minute_revenue += sum(purchase_amounts)
            self._minute_orders += len(purchase_amounts)
            if self.total_orders > 0:
                self.avg_order_value = self.total_revenue / self.total_orders

    def snapshot(self) -> dict:
        """Return a serializable snapshot of current analytics state."""
        eps_window = [point["count"] for point in list(self.events_per_second)[-4:]]
        eps_window.append(self._second_events)
        avg_eps_5s = round(sum(eps_window) / len(eps_window), 2) if eps_window else 0.0

        return {
            "total_revenue": round(self.total_revenue, 2),
            "total_orders": self.total_orders,
            "total_events": self.total_events,
            "avg_order_value": round(self.avg_order_value, 2),
            "active_users": len(self.active_users),
            "refunds": self.refunds,
            "refund_amount": round(self.refund_amount, 2),
            "revenue_per_minute": list(self.revenue_per_minute)[-20:],
            "orders_per_minute": list(self.orders_per_minute)[-20:],
            "events_per_second": list(self.events_per_second)[-30:],
            "top_products": self.top_products.most_common(10),
            "top_categories": self.top_categories.most_common(8),
            "funnel": dict(self.funnel),
            "geo_distribution": self.geo_distribution.most_common(10),
            "device_distribution": dict(self.device_distribution),
            "search_terms": self.search_terms.most_common(8),
            "current_eps": self._second_events,
            "avg_eps_5s": avg_eps_5s,
            "p50_latency_ms": self.p50_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "avg_latency_ms": self.avg_latency_ms,
            "duplicates_rejected": self.duplicates_rejected,
        }

    def close(self, compact: bool = True):
        if compact:
            self._save_snapshot()
            self.wal.reset()
        self.wal.close()
