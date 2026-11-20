from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from scrapy import Request
from scrapy.settings import Settings
from scrapy.squeues import FifoMemoryQueue

from containers.crawler.crawler.site_policies import SitePolicies
from containers.crawler.crawler.value_priority_queue import ValuePriorityQueue


@pytest.fixture
def value_queue(monkeypatch):
    policies = SitePolicies({"domains": {"example.com": {"request_limits": {
        "download_slot": "valuable", "max_concurrent_requests": 2}}}})
    monkeypatch.setattr(SitePolicies, "load", lambda: policies)
    downloader = SimpleNamespace(slots={}, active=set(), get_slot_key=lambda request: urlsplit(request.url).hostname)
    downloader.slots["valuable"] = SimpleNamespace(
        active=set(), transferring=downloader.active, queue=[], concurrency=2)
    crawler = SimpleNamespace(settings=Settings(), engine=SimpleNamespace(downloader=downloader))
    queue = ValuePriorityQueue(crawler, FifoMemoryQueue, "")
    yield queue, downloader
    queue.close()


def test_high_value_dispatches_first_but_yields_when_policy_capacity_is_full(value_queue):
    queue, downloader = value_queue
    low = Request("https://other.example.net/a", priority=100)
    high = [Request(f"https://en.example.com/{i}", priority=1000) for i in range(3)]
    for req in [low, *high]:
        queue.push(req)
    for req in high[:2]:
        assert queue.pop() is req
        downloader.active.add(req)
    assert queue.pop() is low
    assert queue.pop() is None  # saturated requests remain queued
    assert len(queue) == 1
    downloader.active.remove(high[0])
    assert queue.pop() is high[2]


def test_shared_slot_counts_other_source_and_other_language_hosts(value_queue):
    queue, downloader = value_queue
    manual = Request("https://zh.example.com/manual", meta={"manual_bulk_crawl_job_id": "job"})
    downloader.active.add(manual)
    first = Request("https://en.example.com/first", priority=1000)
    second = Request("https://fr.example.com/second", priority=1000)
    queue.push(first)
    queue.push(second)
    assert set(queue.pqueues) == {"valuable"}
    assert queue.peek() is first
    assert queue.pop() is first
    downloader.active.add(first)
    assert queue.pop() is None
    downloader.active.remove(manual)
    assert queue.pop() is second


def test_equal_values_keep_work_conserving_round_robin(value_queue):
    queue, downloader = value_queue
    a = Request("https://a.example.net/a")
    b = Request("https://b.example.net/b")
    queue.push(b)
    queue.push(a)
    assert queue.pop() is a
    assert queue.pop() is b


def test_redirect_re_resolves_policy_instead_of_inheriting_cached_slot(value_queue):
    queue, downloader = value_queue
    request = Request("https://en.example.com/a")
    assert queue._policy(request)[1:] == ("valuable", 2)
    redirected = request.replace(url="https://other.example.net/b")
    assert queue._policy(redirected)[1:] == ("other.example.net", 16)


def test_invalid_hostname_does_not_abort_enqueue_or_future_work(value_queue):
    queue, downloader = value_queue
    bad = Request("https://computer_name.company.com/example")
    good = Request("https://valid.example.net/example")
    queue.push(bad)
    queue.push(good)
    assert {queue.pop(), queue.pop()} == {bad, good}
    # Host validity remains the downloader's existing responsibility.
    with pytest.raises(ValueError):
        queue.policies.resolve("computer_name.company.com")


def test_sources_share_slot_capacity_and_borrow_when_other_source_is_empty(value_queue):
    from containers.crawler.crawler.value_priority_queue import ManualValuePriorityQueue
    normal, downloader = value_queue
    manual = ManualValuePriorityQueue(normal.crawler, FifoMemoryQueue, "")
    try:
        normals = [Request(f"https://en.example.com/n{i}", priority=1000) for i in range(3)]
        manuals = [Request(f"https://zh.example.com/m{i}", meta={"manual_bulk_crawl_job_id": "job"}) for i in range(2)]
        for req in normals:normal.push(req)
        for req in manuals:manual.push(req)
        assert normal.pop() is normals[0]
        downloader.active.add(normals[0])
        assert normal.pop() is None  # reserve manual capacity while it has work
        assert manual.pop() is manuals[0]
        downloader.active.add(manuals[0])
        assert manual.pop() is None
        downloader.active.clear()
        assert manual.pop() is manuals[1]
        # Manual source drained and idle: normal can borrow the entire slot.
        assert normal.pop() is normals[1]
        downloader.active.add(normals[1])
        assert normal.pop() is normals[2]
    finally:
        manual.close()


def test_seven_request_slot_reserves_six_normal_and_one_manual(monkeypatch):
    from containers.crawler.crawler.value_priority_queue import ManualValuePriorityQueue
    policies = SitePolicies({"domains": {"example.com": {"request_limits": {
        "download_slot": "valuable", "max_concurrent_requests": 7}}}})
    monkeypatch.setattr(SitePolicies, "load", lambda: policies)
    downloader = SimpleNamespace(slots={}, active=set(), get_slot_key=lambda request: urlsplit(request.url).hostname)
    downloader.slots["valuable"] = SimpleNamespace(
        active=set(), transferring=downloader.active, queue=[], concurrency=7)
    crawler = SimpleNamespace(settings=Settings(), engine=SimpleNamespace(downloader=downloader))
    normal = ValuePriorityQueue(crawler, FifoMemoryQueue, "")
    manual = ManualValuePriorityQueue(crawler, FifoMemoryQueue, "")
    try:
        for i in range(10):
            normal.push(Request(f"https://en.example.com/n{i}", priority=1000))
            manual.push(Request(f"https://en.example.com/m{i}", meta={"manual_bulk_crawl_job_id": "job"}))
        downloader.active.add(manual.pop())
        assert manual.pop() is None
        for _ in range(6):downloader.active.add(normal.pop())
        assert len(downloader.active) == 7
        assert normal.pop() is None
    finally:
        normal.close()
        manual.close()


def test_delayed_high_priority_slot_leaves_downloader_room_for_other_domains(value_queue):
    queue, downloader = value_queue
    waiting = Request("https://en.example.com/waiting")
    downloader.active.add(waiting)
    slot = downloader.slots["valuable"]
    slot.transferring = set()
    slot.queue = [(waiting, None)]
    high = Request("https://en.example.com/high", priority=1000)
    low = Request("https://other.example.net/low", priority=1)
    queue.push(high)
    queue.push(low)
    assert queue.pop() is low
    assert queue.pop() is None
    # Once the existing waiter transfers, highest-value admission resumes.
    slot.queue.clear()
    slot.transferring.add(waiting)
    assert queue.pop() is high


def test_middleware_waiter_counts_before_downloader_slot_exists(value_queue):
    queue, downloader = value_queue
    first = Request("https://new.example.net/first")
    second = Request("https://new.example.net/second")
    queue.push(first)
    queue.push(second)
    assert queue.pop() is first
    downloader.active.add(first)
    assert queue.pop() is None
    downloader.slots["new.example.net"] = SimpleNamespace(
        active={first}, transferring={first}, queue=[], concurrency=16)
    assert queue.pop() is second


def test_admission_uses_live_downloader_concurrency_when_lower_than_policy(value_queue):
    queue, downloader = value_queue
    slot = downloader.slots["valuable"]
    slot.concurrency = 1
    first = Request("https://en.example.com/first")
    second = Request("https://en.example.com/second")
    queue.push(first)
    queue.push(second)
    assert queue.pop() is first
    downloader.active.add(first)
    assert queue.pop() is None


def test_waiters_share_one_wakeup_and_stop_waking_after_engine_closes(value_queue, monkeypatch):
    from unittest.mock import Mock
    from twisted.internet import reactor
    from twisted.internet.task import Clock
    queue, downloader = value_queue
    clock = Clock()
    monkeypatch.setattr(reactor, "callLater", clock.callLater)
    engine = queue.crawler.engine
    engine._slot = SimpleNamespace(nextcall=SimpleNamespace(schedule=Mock()))
    engine.running = True
    queue._wake_after_wait()
    queue._wake_after_wait()
    assert len(clock.getDelayedCalls()) == 1
    clock.advance(0.05)
    engine._slot.nextcall.schedule.assert_called_once()
    queue._wake_after_wait()
    schedule = engine._slot.nextcall.schedule
    engine._slot = None
    clock.advance(0.05)
    schedule.assert_called_once()


def test_live_delayed_priority_work_leaves_capacity_for_other_domains():
    import os
    from pathlib import Path
    import subprocess
    import sys
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "helpers/value_admission_runner.py")],
        cwd=root / "containers/crawler",
        env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
