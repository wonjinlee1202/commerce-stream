import pytest
from broker.partition import Event
from processor.state_store import StateStore


def make_event(event_type="page_view", amount=None, user_id="u_test", query=None, event_id="e_1"):
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
        event_id=event_id,
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
    await store.record_event(make_event("purchase", amount=50.0, event_id="e_rev_1"))
    await store.record_event(make_event("purchase", amount=75.0, event_id="e_rev_2"))
    assert store.total_orders == 2
    assert store.total_revenue == pytest.approx(125.0)


async def test_tracks_unique_active_users(store):
    await store.record_event(make_event(user_id="u_001", event_id="e_usr_1"))
    await store.record_event(make_event(user_id="u_002", event_id="e_usr_2"))
    await store.record_event(make_event(user_id="u_001", event_id="e_usr_3"))  # same user, new event
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
    await store.record_event(make_event(event_id="e_geo_1"))
    await store.record_event(make_event(event_id="e_geo_2"))
    assert store.geo_distribution["US"] == 2


async def test_search_terms_tracked(store):
    await store.record_event(make_event("search_query", query="wireless headphones"))
    assert store.search_terms["wireless headphones"] == 1


async def test_record_events_batch(store):
    events = [make_event("purchase", amount=10.0, event_id=f"e_batch_{i}") for i in range(5)]
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


async def test_duplicate_event_rejected(store):
    event = make_event("purchase", amount=100.0, event_id="e_unique_purchase")
    await store.record_event(event)
    await store.record_event(event)  # same event_id — should be dropped
    assert store.total_orders == 1
    assert store.total_revenue == pytest.approx(100.0)
    assert store.duplicates_rejected == 1


async def test_unique_event_ids_all_processed(store):
    for i in range(5):
        await store.record_event(make_event("purchase", amount=10.0, event_id=f"e_purchase_{i}"))
    assert store.total_orders == 5
    assert store.duplicates_rejected == 0


async def test_duplicate_in_batch_rejected(store):
    events = [make_event("purchase", amount=50.0, event_id=f"e_batch_{i}") for i in range(3)]
    duplicate = make_event("purchase", amount=50.0, event_id="e_batch_0")  # same ID as first
    await store.record_events(events + [duplicate])
    assert store.total_orders == 3  # not 4
    assert store.total_revenue == pytest.approx(150.0)
    assert store.duplicates_rejected == 1


async def test_dedup_survives_wal_recovery(tmp_path):
    wal_path = str(tmp_path / "state.wal")
    snapshot_path = str(tmp_path / "state.snapshot.json")

    store1 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=False)
    event = make_event("purchase", amount=99.0, event_id="e_dedup_recovery")
    await store1.record_event(event)
    store1.wal.flush()
    store1.close(compact=False)

    # After recovery, the same event_id should still be known — no double-count
    store2 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=True)
    await store2.record_event(event)  # retry of the same event
    assert store2.total_orders == 1
    assert store2.duplicates_rejected == 1
    store2.close(compact=False)


async def test_compact_truncates_wal_and_saves_snapshot(tmp_path):
    wal_path = str(tmp_path / "state.wal")
    snapshot_path = str(tmp_path / "state.snapshot.json")

    store = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=False)
    await store.record_events([make_event("purchase", amount=10.0, event_id=f"e_cmp_{i}") for i in range(3)])
    store.wal.flush()

    import os
    assert os.path.getsize(wal_path) > 0

    store.compact()

    assert os.path.getsize(wal_path) == 0
    assert os.path.exists(snapshot_path)
    store.close(compact=False)


async def test_compact_state_survives_recovery(tmp_path):
    wal_path = str(tmp_path / "state.wal")
    snapshot_path = str(tmp_path / "state.snapshot.json")

    store1 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=False)
    await store1.record_events([make_event("purchase", amount=50.0, event_id=f"e_csr_{i}") for i in range(4)])
    store1.compact()
    store1.close(compact=False)

    store2 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=True)
    assert store2.total_orders == 4
    assert store2.total_revenue == pytest.approx(200.0)
    store2.close(compact=False)


async def test_snapshot_then_recovery(tmp_path):
    wal_path = str(tmp_path / "state.wal")
    snapshot_path = str(tmp_path / "state.snapshot.json")

    store1 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=False)
    await store1.record_events([make_event("purchase", amount=200.0, event_id=f"e_snap_{i}") for i in range(3)])
    store1.close(compact=True)  # saves snapshot, resets WAL

    store2 = StateStore(wal_path=wal_path, snapshot_path=snapshot_path, recover_on_start=True)
    assert store2.total_orders == 3
    assert store2.total_revenue == pytest.approx(600.0)
    store2.close(compact=False)
