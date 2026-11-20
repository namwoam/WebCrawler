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
