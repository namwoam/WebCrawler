"""Receiver backpressure and admission for consumer-driven offers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crawler.queue_consumer import QueueConsumer
from crawler.spiders.spider import HtmlSpider
from libs.ipc.demand import DemandTracker
from libs.ipc.jsonio import atomic_write_json
from unittest.mock import Mock


def test_queue_leaves_offer_on_disk_when_capacity_is_insufficient(tmp_path):
    path = tmp_path / "domain_000009.json"
    atomic_write_json(str(path), {"urls": ["https://example.test"] * 120})
    consumer = QueueConsumer(str(tmp_path))
    assert consumer.pop_domain_batches(include_domain_ids={9}, max_urls=100) == {}
    assert path.exists()
    assert len(consumer.pop_domain_batches(include_domain_ids={9}, max_urls=120)[9]) == 120
    assert not path.exists()


def test_requested_domain_can_be_admitted_above_domain_watermark(tmp_path):
    spider = HtmlSpider()
    spider.queue = QueueConsumer(str(tmp_path))
    spider.domain_low_watermark = 1
    spider._domain_pending = {1: 100}
    spider._pending_requests = 100
    spider.demand_tracker = DemandTracker()
    spider.demand_tracker.admitted(9)
    spider.demand_max_pending = 200
    spider._set_inflight_stats = lambda: None
    spider._log = lambda message: None
    atomic_write_json(str(tmp_path / "domain_000009.json"), {"urls": ["https://example.test"] * 80})
    atomic_write_json(str(tmp_path / "domain_000010.json"), {"urls": ["https://other.test"] * 80})
    reserved = spider._reserve_urls("test")
    assert len(reserved) == 80
    assert {domain_id for domain_id, url in reserved} == {9}
    assert spider._pending_requests == 180
    assert (tmp_path / "domain_000010.json").exists()


def test_requested_domain_is_not_prefetched_above_tail_watermark(tmp_path):
    spider = HtmlSpider()
    spider.queue = QueueConsumer(str(tmp_path))
    spider.domain_low_watermark = 1
    spider._domain_pending = {9: 150}
    spider._pending_requests = 150
    spider.demand_tracker = DemandTracker()
    spider.demand_tracker.admitted(9)
    spider.demand_max_pending = 200
    atomic_write_json(str(tmp_path / "domain_000009.json"), {"urls": ["https://example.test"] * 80, "priority": 1})
    assert spider._reserve_urls("test") == []
    assert (tmp_path / "domain_000009.json").exists()


def test_ready_next_batch_is_prefetched_before_domain_runs_empty(tmp_path):
    spider = HtmlSpider()
    spider.queue = QueueConsumer(str(tmp_path))
    spider.domain_low_watermark = 1
    spider._domain_pending = {9: 50}
    spider._pending_requests = 50
    spider.demand_tracker = DemandTracker()
    spider.demand_tracker.admitted(9)
    spider.demand_max_pending = 200
    spider._set_inflight_stats = lambda: None
    spider._log = lambda message: None
    atomic_write_json(str(tmp_path / "domain_000009.json"), {"urls": ["https://example.test"] * 80, "priority": 1})
    assert len(spider._reserve_urls("test")) == 80
    assert spider._pending_requests == 130


def test_periodic_refill_failure_does_not_stop_future_ticks():
    spider = HtmlSpider()
    calls = []
    def fail_once():
        calls.append(True)
        if len(calls) == 1:
            raise ValueError("invalid offer")
    spider._periodic_top_up_once = fail_once
    spider._periodic_top_up()
    spider._periodic_top_up()
    assert len(calls) == 2


def test_priority_crosses_queue_boundary_and_is_applied_to_normal_requests(tmp_path):
    spider = HtmlSpider()
    spider.queue = QueueConsumer(str(tmp_path))
    spider.demand_tracker = DemandTracker()
    path = tmp_path / "domain_000009.json"
    atomic_write_json(str(path), {"urls": ["https://example.test"], "priority": 1.25})
    spider.queue.pop_domain_batches()
    request = spider._build_request("https://example.test", 9)
    assert request.priority == 1250000
    assert request.meta["_track_domain_id"] == 9


def test_priority_admission_can_bootstrap_an_unseen_domain_above_watermark(tmp_path):
    spider = HtmlSpider()
    spider.queue = QueueConsumer(str(tmp_path))
    spider.domain_low_watermark = 1
    spider._domain_pending = {1: 100}
    spider._pending_requests = 100
    spider.demand_tracker = DemandTracker()
    spider.demand_max_pending = 500
    spider._set_inflight_stats = lambda: None
    spider._log = lambda message: None
    atomic_write_json(str(tmp_path / "domain_000009.json"), {"urls": ["https://valuable.test"] * 80, "priority": 1.5})
    atomic_write_json(str(tmp_path / "domain_000010.json"), {"urls": ["https://ordinary.test"] * 80, "priority": 0.2})
    reserved = spider._reserve_urls("test")
    assert reserved[0][0] == 9
    assert len(reserved) == 160
    assert spider._pending_requests == 260


def test_capacity_rechecks_do_not_decode_unchanged_offer(tmp_path, monkeypatch):
    from crawler import queue_consumer
    path = tmp_path / "domain_000009.json"
    atomic_write_json(str(path), {"urls": ["https://example.test"] * 120, "priority": 1})
    read = Mock(wraps=queue_consumer.read_json)
    monkeypatch.setattr(queue_consumer, "read_json", read)
    consumer = QueueConsumer(str(tmp_path))
    for _ in range(20):
        assert consumer.pop_domain_batches(max_urls=100, priority_only=True) == {}
    assert read.call_count == 1
    # Atomic replacement invalidates both the priority and count cache.
    atomic_write_json(str(path), {"urls": ["https://example.test"] * 80, "priority": 2})
    assert len(consumer.pop_domain_batches(max_urls=100, priority_only=True)[9]) == 80
    assert consumer.priorities[9] == 2


def test_blocked_valuable_offer_reserves_headroom_from_ordinary_refill(tmp_path):
    spider = HtmlSpider()
    spider.queue = QueueConsumer(str(tmp_path))
    spider.domain_low_watermark = 10
    spider._domain_pending = {1: 150}
    spider._pending_requests = 150
    spider.demand_tracker = DemandTracker()
    spider.demand_max_pending = 250
    spider._set_inflight_stats = lambda: None
    spider._log = lambda message: None
    high = tmp_path / "domain_000009.json"
    low = tmp_path / "domain_000010.json"
    atomic_write_json(str(high), {"urls": ["https://valuable.test"] * 120, "priority": 1.5})
    atomic_write_json(str(low), {"urls": ["https://ordinary.test"] * 50, "priority": 0.2})
    assert spider._reserve_urls("test") == []
    assert high.exists() and low.exists()
    spider._pending_requests = 130
    assert len(spider._reserve_urls("test")) == 120
    assert not high.exists() and low.exists()
    assert spider._pending_requests == 250


def test_demand_completions_refill_when_domain_drains_not_every_response():
    spider = HtmlSpider()
    spider.domain_low_watermark = 10
    spider._domain_pending = {9: 3}
    spider._pending_requests = 3
    spider.demand_tracker = DemandTracker()
    spider._set_inflight_stats = Mock()
    spider._maybe_top_up = Mock()
    spider._finish_owned_request("test", 9)
    spider._finish_owned_request("test", 9)
    spider._maybe_top_up.assert_not_called()
    spider._finish_owned_request("test", 9)
    spider._maybe_top_up.assert_called_once()


def test_legacy_completion_refill_is_unchanged():
    spider = HtmlSpider()
    spider.domain_low_watermark = 10
    spider._domain_pending = {9: 3}
    spider._pending_requests = 3
    spider._set_inflight_stats = Mock()
    spider._maybe_top_up = Mock()
    spider._finish_owned_request("test", 9)
    spider._maybe_top_up.assert_called_once()
