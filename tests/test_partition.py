import pytest
from broker.partition import Event, Partition


def make_event(event_id="e_1", user_id="u_test"):
    return Event(
        event_id=event_id,
        event_type="page_view",
        timestamp=1000.0,
        user_id=user_id,
        session_id="s_test",
        data={"page": "/products/test"},
        partition_key=user_id,
    )


@pytest.fixture
def partition(tmp_path):
    p = Partition(partition_id=0, log_dir=str(tmp_path), max_queue_size=10)
    p.open()
    yield p
    p.close()


async def test_produce_increments_offset(partition):
    offset = await partition.produce(make_event())
    assert offset == 0
    offset = await partition.produce(make_event("e_2"))
    assert offset == 1


async def test_consume_returns_produced_event(partition):
    await partition.produce(make_event("e_abc"))
    event = await partition.consume(timeout=1.0)
    assert event is not None
    assert event.event_id == "e_abc"


async def test_consume_returns_none_on_empty_queue(partition):
    result = await partition.consume(timeout=0.05)
    assert result is None


async def test_produce_batch_returns_sequential_offsets(partition):
    events = [make_event(f"e_{i}") for i in range(5)]
    offsets = await partition.produce_batch(events)
    assert offsets == [0, 1, 2, 3, 4]
    assert partition.total_produced == 5


async def test_consume_batch_drains_multiple_events(partition):
    events = [make_event(f"e_{i}") for i in range(3)]
    await partition.produce_batch(events)
    consumed = await partition.consume_batch(max_items=10, timeout=1.0)
    assert len(consumed) == 3
    assert consumed[0].event_id == "e_0"
    assert consumed[2].event_id == "e_2"


async def test_lag_tracks_unconsumed_events(partition):
    events = [make_event(f"e_{i}") for i in range(4)]
    await partition.produce_batch(events)
    assert partition.get_lag() == 4
    await partition.consume(timeout=1.0)
    assert partition.get_lag() == 3


async def test_stats_reflect_produce_consume(partition):
    await partition.produce(make_event())
    await partition.consume(timeout=1.0)
    stats = partition.stats()
    assert stats["total_produced"] == 1
    assert stats["total_consumed"] == 1
    assert stats["partition_id"] == 0


async def test_replay_from_offset(partition):
    for i in range(5):
        await partition.produce(make_event(f"e_{i}"))
    partition._flush_pending()

    replayed = list(partition.replay_from_offset(2))
    assert len(replayed) == 3
    assert replayed[0].event_id == "e_2"
    assert replayed[2].event_id == "e_4"


async def test_replay_from_zero_returns_all(partition):
    for i in range(3):
        await partition.produce(make_event(f"e_{i}"))
    partition._flush_pending()

    replayed = list(partition.replay_from_offset(0))
    assert len(replayed) == 3


async def test_log_rotation_bounds_file_size(tmp_path):
    """Rotation discards the oldest half of entries and advances log_base_offset."""
    import os
    p = Partition(partition_id=0, log_dir=str(tmp_path), log_max_bytes=500)
    p.open()
    try:
        for i in range(40):
            await p.produce(make_event(f"e_{i}"))
        p._flush_pending()
        size_before = os.path.getsize(p.log_path)

        p._rotate_log()

        size_after = os.path.getsize(p.log_path)
        assert size_after < size_before
        assert p._log_base_offset == 20  # oldest half (offsets 0-19) dropped
    finally:
        p.close()


async def test_replay_skips_rotated_offsets(tmp_path):
    """replay_from_offset clamps to log_base_offset when earlier entries were rotated out."""
    p = Partition(partition_id=0, log_dir=str(tmp_path), log_max_bytes=500)
    p.open()
    try:
        for i in range(40):
            await p.produce(make_event(f"e_{i}"))
        p._flush_pending()
        p._rotate_log()

        # Requesting from offset 0 should only return what's still in the log
        replayed = list(p.replay_from_offset(0))
        assert all(e.event_id.startswith("e_") for e in replayed)
        # The first replayed event offset must be >= log_base_offset
        if replayed:
            first_offset = int(replayed[0].event_id.split("_")[1])
            assert first_offset >= p._log_base_offset
    finally:
        p.close()
