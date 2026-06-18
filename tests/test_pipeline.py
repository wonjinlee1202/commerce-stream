import pytest
from broker.partition import Event
from processor.pipeline import Pipeline


def make_event(event_type="page_view", amount=None):
    data = {}
    if amount is not None:
        data["amount"] = amount
    return Event(
        event_id="e_1",
        event_type=event_type,
        timestamp=1000.0,
        user_id="u_test",
        session_id="s_test",
        data=data,
    )


async def test_sink_receives_event():
    received = []

    async def collect(e):
        received.append(e)

    pipeline = Pipeline().sink(collect)
    event = make_event()
    await pipeline.process(event)
    assert received == [event]


async def test_filter_passes_matching_events():
    received = []

    async def collect(e):
        received.append(e)

    pipeline = Pipeline().filter(lambda e: e.event_type == "purchase").sink(collect)
    await pipeline.process(make_event("purchase"))
    await pipeline.process(make_event("page_view"))
    assert len(received) == 1
    assert received[0].event_type == "purchase"


async def test_filter_drops_non_matching_events():
    received = []

    async def collect(e):
        received.append(e)

    pipeline = Pipeline().filter(lambda e: e.event_type == "purchase").sink(collect)
    await pipeline.process(make_event("page_view"))
    assert received == []


async def test_map_transforms_value():
    results = []

    async def collect(v):
        results.append(v)

    pipeline = Pipeline().map(lambda e: e.event_type).sink(collect)
    await pipeline.process(make_event("purchase"))
    assert results == ["purchase"]


async def test_chained_filter_map_sink():
    results = []

    async def collect(v):
        results.append(v)

    pipeline = (
        Pipeline()
        .filter(lambda e: e.event_type == "purchase")
        .map(lambda e: e.data.get("amount", 0))
        .sink(collect)
    )
    await pipeline.process(make_event("purchase", amount=99.99))
    await pipeline.process(make_event("page_view"))
    assert results == [99.99]


async def test_process_batch_processes_all_events():
    received = []

    async def collect(e):
        received.append(e)

    pipeline = Pipeline().sink(collect)
    events = [make_event() for _ in range(5)]
    await pipeline.process_batch(events)
    assert len(received) == 5


async def test_process_batch_respects_filter():
    received = []

    async def collect(e):
        received.append(e)

    pipeline = Pipeline().filter(lambda e: e.event_type == "purchase").sink(collect)
    events = [make_event("purchase"), make_event("page_view"), make_event("purchase")]
    await pipeline.process_batch(events)
    assert len(received) == 2


async def test_no_sink_does_not_raise():
    pipeline = Pipeline().filter(lambda e: True)
    await pipeline.process(make_event())  # should not raise


async def test_empty_pipeline_passes_event_to_sink():
    received = []

    async def collect(e):
        received.append(e)

    pipeline = Pipeline().sink(collect)
    event = make_event()
    await pipeline.process(event)
    assert received[0] is event
