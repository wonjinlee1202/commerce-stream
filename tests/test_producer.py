import pytest
from broker.partition import Event, Partition
from broker.producer import Producer


def make_event(user_id="u_test", event_id="e_1"):
    return Event(
        event_id=event_id,
        event_type="page_view",
        timestamp=1000.0,
        user_id=user_id,
        session_id="s_test",
        data={},
        partition_key=user_id,
    )


@pytest.fixture
def producer(tmp_path):
    partitions = [
        Partition(partition_id=i, log_dir=str(tmp_path), max_queue_size=1000)
        for i in range(4)
    ]
    for p in partitions:
        p.open()
    prod = Producer(partitions)
    yield prod
    for p in partitions:
        p.close()


async def test_same_key_always_routes_to_same_partition(producer):
    user_id = "u_consistent_hash_test"
    partition_ids = set()
    for i in range(10):
        pid, _ = await producer.send(make_event(user_id=user_id, event_id=f"e_{i}"))
        partition_ids.add(pid)
    assert len(partition_ids) == 1


async def test_different_keys_spread_across_partitions(producer):
    partition_ids = set()
    for i in range(50):
        pid, _ = await producer.send(make_event(user_id=f"u_{i:04d}", event_id=f"e_{i}"))
        partition_ids.add(pid)
    assert len(partition_ids) > 1


async def test_send_returns_partition_id_and_offset(producer):
    pid, offset = await producer.send(make_event())
    assert 0 <= pid < 4
    assert offset == 0


async def test_total_sent_increments_on_send(producer):
    await producer.send(make_event(event_id="e_1"))
    await producer.send(make_event(event_id="e_2"))
    assert producer.total_sent == 2


async def test_send_batch_routes_all_events(producer):
    events = [make_event(user_id=f"u_{i}", event_id=f"e_{i}") for i in range(10)]
    results = await producer.send_batch(events)
    assert len(results) == 10
    assert producer.total_sent == 10


async def test_send_batch_groups_by_partition(producer):
    # Same user_id -> same partition -> all in one group
    user_id = "u_same"
    events = [make_event(user_id=user_id, event_id=f"e_{i}") for i in range(5)]
    results = await producer.send_batch(events)
    partition_ids = {pid for pid, _ in results}
    assert len(partition_ids) == 1


async def test_get_partition_stats_returns_one_per_partition(producer):
    stats = producer.get_partition_stats()
    assert len(stats) == 4
    assert all("partition_id" in s for s in stats)
