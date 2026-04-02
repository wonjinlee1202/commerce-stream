"""
StateStore: Write-Ahead Log backed state for stream aggregations.
Before updating any state, write the operation to disk.
On crash/restart, replay the WAL to reconstruct exact state.
This is how databases (and Flink) achieve fault tolerance.
"""

import json
import os
import time
import asyncio
from collections import defaultdict, Counter, deque
from typing import Any


class WriteAheadLog:
    """
    Append-only log of state mutations.
    Every state change is recorded here before being applied to memory.
    On restart, replay to recover exact state.
    """
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        os.makedirs(os.path.dirname(wal_path), exist_ok=True)
        self._file = open(wal_path, "a", buffering=1)

    def write(self, operation: dict):
        """Durably record a state mutation."""
        entry = {"ts": time.time(), **operation}
        self._file.write(json.dumps(entry) + "\n")

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
        self._file.close()


class StateStore:
    """
    Central aggregation state for the analytics engine.
    All mutations go through the WAL before being applied.
    Thread-safe via asyncio lock.
    """

    def __init__(self, wal_path: str = "checkpoints/state.wal"):
        self.wal = WriteAheadLog(wal_path)
        self._lock = asyncio.Lock()

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

        # Time-series buckets (current minute)
        self._current_minute: int = 0
        self._minute_revenue: float = 0.0
        self._minute_orders: int = 0
        self._second_events: int = 0
        self._current_second: int = 0

        self._recover_from_wal()

    def _recover_from_wal(self):
        """Replay WAL to reconstruct state after crash."""
        count = 0
        for entry in self.wal.replay():
            self._apply_entry(entry)
            count += 1
        if count > 0:
            print(f"[StateStore] Recovered {count} operations from WAL")

    def _apply_entry(self, entry: dict):
        """Apply a single WAL entry to in-memory state (used during recovery)."""
        op = entry.get("op")
        if op == "purchase":
            self.total_revenue += entry["amount"]
            self.total_orders += 1
            if entry.get("product"):
                self.top_products[entry["product"]] += 1
            if entry.get("category"):
                self.top_categories[entry["category"]] += 1
        elif op == "funnel":
            event_type = entry.get("event_type")
            if event_type in self.funnel:
                self.funnel[event_type] += 1
        elif op == "active_user":
            self.active_users.add(entry["user_id"])
        elif op == "refund":
            self.refunds += 1
            self.refund_amount += entry.get("amount", 0)
        elif op == "geo":
            self.geo_distribution[entry["country"]] += 1
        elif op == "device":
            self.device_distribution[entry["device"]] += 1
        elif op == "search":
            self.search_terms[entry["term"]] += 1

    async def record_event(self, event):
        """Process a raw event into state. WAL-first, then apply."""
        async with self._lock:
            self.total_events += 1
            event_type = event.event_type
            data = event.data

            # Track time-series
            now = int(time.time())
            minute = now // 60
            second = now

            if second != self._current_second:
                self.events_per_second.append({"ts": self._current_second, "count": self._second_events})
                self._second_events = 0
                self._current_second = second
            self._second_events += 1

            if minute != self._current_minute:
                self.revenue_per_minute.append({"ts": self._current_minute * 60, "revenue": self._minute_revenue})
                self.orders_per_minute.append({"ts": self._current_minute * 60, "orders": self._minute_orders})
                self._minute_revenue = 0.0
                self._minute_orders = 0
                self._current_minute = minute

            # WAL write + state apply
            if event_type == "purchase":
                amount = data.get("amount", 0)
                product = data.get("product_name", "")
                category = data.get("category", "")
                entry = {"op": "purchase", "amount": amount, "product": product, "category": category}
                self.wal.write(entry)
                self._apply_entry(entry)
                self._minute_revenue += amount
                self._minute_orders += 1
                # Update rolling AOV
                if self.total_orders > 0:
                    self.avg_order_value = self.total_revenue / self.total_orders

            if event_type in self.funnel:
                entry = {"op": "funnel", "event_type": event_type}
                self.wal.write(entry)
                self._apply_entry(entry)

            # Active users (session-level tracking)
            user_entry = {"op": "active_user", "user_id": event.user_id}
            self.wal.write(user_entry)
            self._apply_entry(user_entry)

            if event_type == "refund":
                entry = {"op": "refund", "amount": data.get("amount", 0)}
                self.wal.write(entry)
                self._apply_entry(entry)

            if "country" in data:
                entry = {"op": "geo", "country": data["country"]}
                self.wal.write(entry)
                self._apply_entry(entry)

            if "device" in data:
                entry = {"op": "device", "device": data["device"]}
                self.wal.write(entry)
                self._apply_entry(entry)

            if event_type == "search_query" and "query" in data:
                entry = {"op": "search", "term": data["query"]}
                self.wal.write(entry)
                self._apply_entry(entry)

    def snapshot(self) -> dict:
        """Return a serializable snapshot of current analytics state."""
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
        }
