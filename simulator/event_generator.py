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
import uuid
from dataclasses import dataclass
from typing import List, Optional
from broker.partition import Event
from broker.producer import Producer


# ─── Product Catalog ────────────────────────────────────────────────────────

PRODUCTS = [
    {"name": "Wireless Headphones Pro", "category": "Electronics", "price": 199.99},
    {"name": "Running Shoes Elite", "category": "Footwear", "price": 129.99},
    {"name": "Organic Coffee Blend", "category": "Food & Drink", "price": 24.99},
    {"name": "Yoga Mat Premium", "category": "Sports", "price": 49.99},
    {"name": "Smart Watch Series X", "category": "Electronics", "price": 349.99},
    {"name": "Leather Wallet", "category": "Accessories", "price": 59.99},
    {"name": "Gaming Mouse", "category": "Electronics", "price": 79.99},
    {"name": "Winter Jacket", "category": "Clothing", "price": 189.99},
    {"name": "Protein Powder Vanilla", "category": "Health", "price": 44.99},
    {"name": "Mechanical Keyboard", "category": "Electronics", "price": 159.99},
    {"name": "Sunglasses UV400", "category": "Accessories", "price": 89.99},
    {"name": "Backpack 30L", "category": "Bags", "price": 74.99},
    {"name": "Skincare Set", "category": "Beauty", "price": 69.99},
    {"name": "Novel: The Last Shore", "category": "Books", "price": 16.99},
    {"name": "Standing Desk Mat", "category": "Office", "price": 34.99},
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
            user_id=f"u_{uuid.uuid4().hex[:10]}",
            session_id=f"s_{uuid.uuid4().hex[:8]}",
            country=weighted_choice(COUNTRIES),
            device=weighted_choice(DEVICES),
            cart=[],
        )


class EventSimulator:
    """
    Simulates a stream of e-commerce events with realistic behavior.
    Base rate is configurable; flash sales temporarily spike it.
    """

    def __init__(self, producer: Producer, base_rate: float = 50.0):
        """
        base_rate: average events per second across all users
        """
        self.producer = producer
        self.base_rate = base_rate
        self.current_rate = base_rate
        self.running = False
        self.flash_sale_active = False
        self.total_generated = 0

        # Maintain a pool of simulated users
        self._user_pool: List[UserSession] = [UserSession.new() for _ in range(200)]

    def _make_event(self, event_type: str, session: UserSession, data: dict) -> Event:
        return Event(
            event_id=uuid.uuid4().hex,
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
            "page": f"/products/{product['name'].lower().replace(' ', '-')}",
        }))
        self.total_generated += 1

        # 2. Search (40% chance)
        if random.random() < 0.4:
            await self.producer.send(self._make_event("search_query", session, {
                "query": random.choice(SEARCH_QUERIES),
            }))
            self.total_generated += 1

        await asyncio.sleep(random.uniform(0.5, 3.0))

        # 3. Product click (65% of viewers)
        if random.random() > 0.35:
            await self.producer.send(self._make_event("product_click", session, {
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
            }))
            self.total_generated += 1

            await asyncio.sleep(random.uniform(1.0, 5.0))

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

                await asyncio.sleep(random.uniform(2.0, 8.0))

                # 5. Checkout start (55% of cart adders)
                if random.random() > 0.45:
                    await self.producer.send(self._make_event("checkout_start", session, {
                        "cart_size": len(session.cart),
                        "cart_value": sum(p["price"] * p.get("quantity", 1) for p in session.cart),
                    }))
                    self.total_generated += 1

                    await asyncio.sleep(random.uniform(1.0, 4.0))

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
                            await asyncio.sleep(random.uniform(5.0, 15.0))
                            await self.producer.send(self._make_event("refund", session, {
                                "product_name": product["name"],
                                "amount": amount,
                                "reason": random.choice(["defective", "wrong_size", "changed_mind", "not_as_described"]),
                            }))
                            self.total_generated += 1

        # Recycle session with some probability
        if random.random() < 0.15:
            session.session_id = f"s_{uuid.uuid4().hex[:8]}"
            session.cart = []

    async def run(self):
        """
        Main simulation loop.
        Uses Poisson arrival process: inter-arrival times are exponentially distributed.
        This naturally produces bursty, realistic traffic patterns.
        """
        self.running = True
        print(f"[Simulator] Starting at {self.base_rate} events/sec base rate")

        while self.running:
            # Poisson process: exponential inter-arrival time
            inter_arrival = random.expovariate(self.current_rate / 10)
            await asyncio.sleep(inter_arrival)

            # Pick a random user from pool
            session = random.choice(self._user_pool)

            # Launch session simulation as background task (non-blocking)
            asyncio.create_task(self._simulate_session(session))

    async def trigger_flash_sale(self, duration_seconds: float = 30.0, multiplier: float = 8.0):
        """
        Spike traffic by multiplier for duration_seconds.
        This stress-tests backpressure handling.
        """
        print(f"[Simulator] 🔥 FLASH SALE STARTED! Rate x{multiplier} for {duration_seconds}s")
        self.flash_sale_active = True
        self.current_rate = self.base_rate * multiplier
        await asyncio.sleep(duration_seconds)
        self.current_rate = self.base_rate
        self.flash_sale_active = False
        print("[Simulator] Flash sale ended, rate normalized")

    async def run_flash_sale_scheduler(self):
        """Periodically trigger flash sales to stress-test the system."""
        while self.running:
            await asyncio.sleep(random.uniform(60, 120))  # Every 1-2 minutes
            if self.running:
                await self.trigger_flash_sale(
                    duration_seconds=random.uniform(20, 40),
                    multiplier=random.uniform(5, 12),
                )

    def stop(self):
        self.running = False

    def stats(self) -> dict:
        return {
            "total_generated": self.total_generated,
            "current_rate": self.current_rate,
            "base_rate": self.base_rate,
            "flash_sale_active": self.flash_sale_active,
            "user_pool_size": len(self._user_pool),
        }
