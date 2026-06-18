import pytest
from broker.partition import Event
from processor.state_store import StateStore


def make_event(event_type="page_view", amount=None, user_id="u_test", query=None):
    data = {"country": "US", "device": "desktop"}
    if amount is not None:
        data.update({
            "amount": amount,
            "product_name": "Test Product",
            "category": "Electronics",
            "items": 1,
            "discount": 0,
        })
    if query is not None:
        data["query"] = query
    return Event(
        event_id="e_1",
        event_type=event_type,
        timestamp=1000.0,
        user_id=user_id,
        session_id="s_test",
        data=data,
    )


@pytest.fixture
def store(tmp_path):
    s = StateStore(
        wal_path=str(tmp_path / "state.wal"),
        snapshot_path=str(tmp_path / "state.snapshot.json"),
        recover_on_start=False,
    )
    yield s
    s.close(compact=False)


async def test_page_view_increments_funnel(store):
    await store.record_event(make_event("page_view"))
    assert store.funnel["page_view"] == 1
    assert store.total_events == 1


async def test_purchase_updates_revenue_and_orders(store):
    await store.record_event(make_event("purchase", amount=99.99))
    assert store.total_orders == 1
    assert store.total_revenue == pytest.approx(99.99)
    assert store.funnel["purchase"] == 1


async def test_multiple_purchases_accumulate_revenue(store):
    await store.record_event(make_event("purchase", amount=50.0))
    await store.record_event(make_event("purchase", amount=75.0))
    assert store.total_orders == 2
    assert store.total_revenue == pytest.approx(125.0)


async def test_tracks_unique_active_users(store):
    await store.record_event(make_event(user_id="u_001"))
    await store.record_event(make_event(user_id="u_002"))
    await store.record_event(make_event(user_id="u_001"))  # duplicate
    assert len(store.active_users) == 2


async def test_refund_tracking(store):
    refund = Event(
        event_id="e_refund",
        event_type="refund",
        timestamp=1001.0,
        user_id="u_test",
        session_id="s_test",
        data={"amount": 49.99, "country": "US", "device": "desktop"},
    )
    await store.record_event(refund)
    assert store.refunds == 1
    assert store.refund_amount == pytest.approx(49.99)


async def test_geo_distribution_tracked(store):
    await store.record_event(make_event())  # country=US
    await store.record_event(make_event())
    assert store.geo_distribution["US"] == 2


async def test_search_terms_tracked(store):
    await store.record_event(make_event("search_query", query="wireless headphones"))
    assert store.search_terms["wireless headphones"] == 1


async def test_record_events_batch(store):
    events = [make_event("purchase", amount=10.0) for _ in range(5)]
    await store.record_events(events)
    assert store.total_orders == 5
    assert store.total_revenue == pytest.approx(50.0)


async def test_snapshot_contains_expected_keys(store):
    snapshot = store.snapshot()
    for key in ("total_revenue", "total_orders", "funnel", "top_products", "p50_latency_ms", "p99_latency_ms"):
        assert key in snapshot


async def test_wal_recovery_restores_state(tmp_path):
    wal_path = str(tmp_path / "state.wal")
    snapshot_path = str(tmp_path / "state.snapshot.json")

    store1 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=False)
    await store1.record_event(make_event("purchase", amount=100.0))
    store1.wal.flush()
    store1.close(compact=False)

    store2 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=True)
    assert store2.total_orders == 1
    assert store2.total_revenue == pytest.approx(100.0)
    store2.close(compact=False)


async def test_snapshot_then_recovery(tmp_path):
    wal_path = str(tmp_path / "state.wal")
    snapshot_path = str(tmp_path / "state.snapshot.json")

    store1 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=False)
    await store1.record_events([make_event("purchase", amount=200.0) for _ in range(3)])
    store1.close(compact=True)  # saves snapshot, resets WAL

    store2 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=True)
    assert store2.total_orders == 3
    assert store2.total_revenue == pytest.approx(600.0)
    store2.close(compact=False)
