"""
EventSimulator: Generates realistic, bursty e-commerce traffic.
- Poisson arrival process (expovariate inter-arrival times)
- Conversion funnel probabilities (realistic drop-offs)
- Flash sale scenario (sudden 10x traffic spike)
- User sessions with consistent user_id/session_id
- Geographic and device diversity
"""

import asyncio
import random
import time
from dataclasses import dataclass
from typing import List
from broker.partition import Event
from broker.producer import Producer


# ─── Product Catalog ────────────────────────────────────────────────────────

PRODUCTS = [
    {"slug": "wireless-headphones-pro", "name": "Wireless Headphones Pro", "category": "Electronics", "price": 199.99},
    {"slug": "running-shoes-elite", "name": "Running Shoes Elite", "category": "Footwear", "price": 129.99},
    {"slug": "organic-coffee-blend", "name": "Organic Coffee Blend", "category": "Food & Drink", "price": 24.99},
    {"slug": "yoga-mat-premium", "name": "Yoga Mat Premium", "category": "Sports", "price": 49.99},
    {"slug": "smart-watch-series-x", "name": "Smart Watch Series X", "category": "Electronics", "price": 349.99},
    {"slug": "leather-wallet", "name": "Leather Wallet", "category": "Accessories", "price": 59.99},
    {"slug": "gaming-mouse", "name": "Gaming Mouse", "category": "Electronics", "price": 79.99},
    {"slug": "winter-jacket", "name": "Winter Jacket", "category": "Clothing", "price": 189.99},
    {"slug": "protein-powder-vanilla", "name": "Protein Powder Vanilla", "category": "Health", "price": 44.99},
    {"slug": "mechanical-keyboard", "name": "Mechanical Keyboard", "category": "Electronics", "price": 159.99},
    {"slug": "sunglasses-uv400", "name": "Sunglasses UV400", "category": "Accessories", "price": 89.99},
    {"slug": "backpack-30l", "name": "Backpack 30L", "category": "Bags", "price": 74.99},
    {"slug": "skincare-set", "name": "Skincare Set", "category": "Beauty", "price": 69.99},
    {"slug": "novel-the-last-shore", "name": "Novel: The Last Shore", "category": "Books", "price": 16.99},
    {"slug": "standing-desk-mat", "name": "Standing Desk Mat", "category": "Office", "price": 34.99},
]

COUNTRIES = [
    ("US", 0.45), ("UK", 0.12), ("CA", 0.08), ("AU", 0.06),
    ("DE", 0.07), ("FR", 0.05), ("JP", 0.05), ("BR", 0.04),
    ("IN", 0.05), ("MX", 0.03),
]

DEVICES = [("desktop", 0.48), ("mobile", 0.42), ("tablet", 0.10)]

SEARCH_QUERIES = [
    "wireless headphones", "running shoes sale", "organic coffee",
    "gaming setup", "winter jacket men", "yoga accessories",
    "smartwatch review", "mechanical keyboard", "skincare routine",
    "home office essentials", "protein supplements", "travel backpack",
]


def weighted_choice(choices):
    """Pick from [(value, weight)] list."""
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


@dataclass
class UserSession:
    user_id: str
    session_id: str
    country: str
    device: str
    cart: List[dict]

    @classmethod
    def new(cls):
        return cls(
            user_id=f"u_{random.getrandbits(40):010x}",
            session_id=f"s_{random.getrandbits(32):08x}",
            country=weighted_choice(COUNTRIES),
            device=weighted_choice(DEVICES),
            cart=[],
        )


class EventSimulator:
    """
    Simulates a stream of e-commerce events with realistic behavior.
    Base rate is configurable; flash sales temporarily spike it.
    """

    AVG_EVENTS_PER_SESSION = 2.2

    def __init__(
        self,
        producer: Producer,
        base_rate: float = 50.0,
        session_delay_scale: float = 1.0,
    ):
        """
        base_rate: average events per second across all users
        """
        self.producer = producer
        self.base_rate = base_rate
        self.current_rate = base_rate
        self.session_delay_scale = session_delay_scale
        self.running = False
        self.flash_sale_active = False
        self.total_generated = 0
        self._event_seq = 0
        self._fast_tick_seconds = 0.2
        self.flash_sale_multiplier = 1.0
        self.max_rate = base_rate * 1.2
        self.target_lag = 5_000
        self.max_lag = 20_000
        self.flash_duration_seconds = 20.0
        self.default_flash_multiplier = 1.2

        # Maintain a pool of simulated users
        self._user_pool: List[UserSession] = [UserSession.new() for _ in range(2000)]

    def _current_lag(self) -> int:
        return sum(partition.get_lag() for partition in self.producer.partitions)

    def _effective_rate(self) -> float:
        requested_rate = self.base_rate * self.flash_sale_multiplier
        lag = self._current_lag()

        if lag >= self.max_lag:
            return max(self.base_rate * 0.5, 1.0)
        if lag <= self.target_lag:
            return min(requested_rate, self.max_rate)

        lag_ratio = (lag - self.target_lag) / max(self.max_lag - self.target_lag, 1)
        scaled_rate = requested_rate * (1.0 - 0.85 * lag_ratio)
        return max(min(scaled_rate, self.max_rate), self.base_rate * 0.5)

    async def _sleep(self, min_seconds: float, max_seconds: float):
        """Optional realism delays between user actions."""
        if self.session_delay_scale <= 0:
            return
        await asyncio.sleep(random.uniform(min_seconds, max_seconds) * self.session_delay_scale)

    def _make_event(self, event_type: str, session: UserSession, data: dict) -> Event:
        self._event_seq += 1
        return Event(
            event_id=f"e_{self._event_seq}",
            event_type=event_type,
            timestamp=time.time(),
            user_id=session.user_id,
            session_id=session.session_id,
            data={
                **data,
                "country": session.country,
                "device": session.device,
            },
            partition_key=session.user_id,  # Same user always → same partition
        )

    async def _simulate_session(self, session: UserSession):
        """
        Simulate one user's journey through the conversion funnel.
        Each step has realistic drop-off probabilities.
        """
        product = random.choice(PRODUCTS)

        # 1. Page view (always)
        await self.producer.send(self._make_event("page_view", session, {
                "page": f"/products/{product['slug']}",
        }))
        self.total_generated += 1

        # 2. Search (40% chance)
        if random.random() < 0.4:
            await self.producer.send(self._make_event("search_query", session, {
                "query": random.choice(SEARCH_QUERIES),
            }))
            self.total_generated += 1

        await self._sleep(0.05, 0.3)

        # 3. Product click (65% of viewers)
        if random.random() > 0.35:
            await self.producer.send(self._make_event("product_click", session, {
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
            }))
            self.total_generated += 1

            await self._sleep(0.1, 0.5)

            # 4. Add to cart (45% of clickers)
            if random.random() > 0.55:
                quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
                session.cart.append({**product, "quantity": quantity})
                await self.producer.send(self._make_event("add_to_cart", session, {
                    "product_name": product["name"],
                    "category": product["category"],
                    "price": product["price"],
                    "quantity": quantity,
                }))
                self.total_generated += 1

                await self._sleep(0.2, 0.8)

                # 5. Checkout start (55% of cart adders)
                if random.random() > 0.45:
                    await self.producer.send(self._make_event("checkout_start", session, {
                        "cart_size": len(session.cart),
                        "cart_value": sum(p["price"] * p.get("quantity", 1) for p in session.cart),
                    }))
                    self.total_generated += 1

                    await self._sleep(0.0, 0.1)

                    # 6. Purchase (70% of checkout starters)
                    if random.random() > 0.30:
                        discount = random.choices([0, 0.1, 0.2], weights=[0.6, 0.3, 0.1])[0]
                        amount = sum(p["price"] * p.get("quantity", 1) for p in session.cart)
                        amount = round(amount * (1 - discount), 2)
                        await self.producer.send(self._make_event("purchase", session, {
                            "product_name": product["name"],
                            "category": product["category"],
                            "amount": amount,
                            "discount": discount,
                            "items": len(session.cart),
                        }))
                        self.total_generated += 1

                        # 7. Refund (3% of purchases)
                        if random.random() < 0.03:
                            await self._sleep(0.5, 1.5)
                            await self.producer.send(self._make_event("refund", session, {
                                "product_name": product["name"],
                                "amount": amount,
                                "reason": random.choice(["defective", "wrong_size", "changed_mind", "not_as_described"]),
                            }))
                            self.total_generated += 1

        # Recycle session with some probability
        if random.random() < 0.15:
            session.session_id = f"s_{self._event_seq}"
            session.cart = []

    def _make_fast_event(self, session: UserSession) -> Event:
        product = PRODUCTS[random.randrange(len(PRODUCTS))]
        roll = random.random()
        if roll < 0.35:
            event_type = "page_view"
        elif roll < 0.47:
            event_type = "search_query"
        elif roll < 0.71:
            event_type = "product_click"
        elif roll < 0.83:
            event_type = "add_to_cart"
        elif roll < 0.91:
            event_type = "checkout_start"
        elif roll < 0.99:
            event_type = "purchase"
        else:
            event_type = "refund"

        if event_type == "page_view":
            data = {"page": f"/products/{product['slug']}"}
        elif event_type == "search_query":
            data = {"query": random.choice(SEARCH_QUERIES)}
        elif event_type == "product_click":
            data = {
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
            }
        elif event_type == "add_to_cart":
            quantity = 1 if random.random() < 0.7 else 2 if random.random() < 0.9 else 3
            data = {
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": quantity,
            }
        elif event_type == "checkout_start":
            quantity = 1 if random.random() < 0.7 else 2 if random.random() < 0.9 else 3
            data = {"cart_size": quantity, "cart_value": round(product["price"] * quantity, 2)}
        elif event_type == "purchase":
            quantity = 1 if random.random() < 0.7 else 2 if random.random() < 0.9 else 3
            discount_roll = random.random()
            discount = 0 if discount_roll < 0.6 else 0.1 if discount_roll < 0.9 else 0.2
            amount = round(product["price"] * quantity * (1 - discount), 2)
            data = {
                "product_name": product["name"],
                "category": product["category"],
                "amount": amount,
                "discount": discount,
                "items": quantity,
            }
        else:
            data = {
                "product_name": product["name"],
                "amount": round(product["price"], 2),
                "reason": random.choice(["defective", "wrong_size", "changed_mind", "not_as_described"]),
            }

        return self._make_event(event_type, session, data)

    async def _run_fast(self):
        """High-throughput mode for load testing."""
        while self.running:
            tick_started = time.perf_counter()
            self.current_rate = self._effective_rate()
            batch_size = max(1, int(self.current_rate * self._fast_tick_seconds))
            batch = []
            for _ in range(batch_size):
                session = self._user_pool[random.randrange(len(self._user_pool))]
                batch.append(self._make_fast_event(session))
            await self.producer.send_batch(batch)
            self.total_generated += len(batch)
            elapsed = time.perf_counter() - tick_started
            if elapsed < self._fast_tick_seconds:
                await asyncio.sleep(self._fast_tick_seconds - elapsed)

    async def run(self):
        """
        Main simulation loop.
        Uses Poisson arrival process: inter-arrival times are exponentially distributed.
        This naturally produces bursty, realistic traffic patterns.
        """
        self.running = True
        print(f"[Simulator] Starting at {self.base_rate} events/sec base rate")

        if self.session_delay_scale <= 0:
            await self._run_fast()
            return

        while self.running:
            # Target an event rate, not a session-start rate.
            session_rate = max(self.current_rate / self.AVG_EVENTS_PER_SESSION, 0.001)
            inter_arrival = random.expovariate(session_rate)
            await asyncio.sleep(inter_arrival)

            # Pick a random user from pool
            session = random.choice(self._user_pool)

            # Launch session simulation as background task (non-blocking)
            asyncio.create_task(self._simulate_session(session))

    async def trigger_flash_sale(self, duration_seconds: float = 20.0, multiplier: float = 1.2):
        """
        Spike traffic by multiplier for duration_seconds.
        This stress-tests backpressure handling.
        """
        print(f"[Simulator] 🔥 FLASH SALE STARTED! Rate x{multiplier} for {duration_seconds}s")
        self.flash_sale_active = True
        self.flash_sale_multiplier = multiplier
        await asyncio.sleep(duration_seconds)
        self.flash_sale_multiplier = 1.0
        self.current_rate = self.base_rate
        self.flash_sale_active = False
        print("[Simulator] Flash sale ended, rate normalized")

    async def run_flash_sale_scheduler(self):
        """Periodically trigger flash sales to stress-test the system."""
        while self.running:
            await asyncio.sleep(random.uniform(60, 120))  # Every 1-2 minutes
            if self.running:
                await self.trigger_flash_sale(
                    duration_seconds=self.flash_duration_seconds,
                    multiplier=self.default_flash_multiplier,
                )

    def stop(self):
        self.running = False

    def stats(self) -> dict:
        return {
            "total_generated": self.total_generated,
            "current_rate": self.current_rate,
            "base_rate": self.base_rate,
            "max_rate": self.max_rate,
            "target_lag": self.target_lag,
            "max_lag": self.max_lag,
            "flash_sale_active": self.flash_sale_active,
            "user_pool_size": len(self._user_pool),
        }
