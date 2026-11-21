"""Bounded process submissions, write acknowledgements, failures, and drain."""
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from twisted.internet import reactor
from twisted.internet.defer import succeed

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crawler import pipelines


@pytest.fixture
def controlled_writer(tmp_path, monkeypatch):
    pool = Mock()
    monkeypatch.setattr(pipelines, "ProcessPoolExecutor", Mock(return_value=pool))
    jobs, notifications = [], []

    def submit(*args):
        job = Future()
        job.set_running_or_notify_cancel()
        jobs.append(job)
        return job

    pool.submit.side_effect = submit
    monkeypatch.setattr(reactor, "callFromThread", notifications.append)
    monkeypatch.setattr(pipelines, "deferToThread", lambda fn, **kw: (fn(**kw), succeed(None))[1])
    writer = pipelines.JsonPipeline(str(tmp_path), 10, writer_processes=1, on_broken=Mock())
    writer.stats = Mock()
    spider = SimpleNamespace(crawler_id=0)
    writer.open_spider(spider)
    return writer, spider, jobs, notifications, pool


def finish(jobs, notifications, index, error=None):
    if error is None:
        jobs[index].set_result(None)
    else:
        jobs[index].set_exception(error)
    notifications.pop(0)()


def test_submission_is_bounded_and_errors_reach_the_caller(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    items = [{"url": f"https://example.test/{i}"} for i in range(3)]
    results = [writer.process_item(item, spider) for item in items]
    received, errors = [], []
    for result in results:
        result.addCallbacks(received.append, lambda f: errors.append(f.type))
    assert len(jobs) == 2 and received == []
    assert getattr(spider, "items_written", 0) == 0
    finish(jobs, notifications, 0)
    assert received == items[:1] and len(jobs) == 3
    assert spider.items_written == 1
    finish(jobs, notifications, 1, OSError("write failed"))
    assert errors == [OSError]
    assert spider.items_written == 1
    finish(jobs, notifications, 2)
    assert received == [items[0], items[2]] and writer._pending == 0
    assert spider.items_written == 2
    assert writer.close_spider(spider).called
    pool.shutdown.assert_called_once_with(wait=True)


def test_cancellation_keeps_actual_writes_bounded_and_shutdown_drains(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    results = [writer.process_item({"url": f"https://example.test/{i}"}, spider) for i in range(3)]
    results[0].addErrback(lambda failure: None)
    results[0].cancel()
    assert len(jobs) == 2
    closed = writer.close_spider(spider)
    assert not closed.called
    pool.shutdown.assert_not_called()
    with pytest.raises(RuntimeError, match="closing"):
        writer.process_item({}, spider)
    finish(jobs, notifications, 0)
    assert len(jobs) == 3
    assert not results[1].called and not closed.called
    finish(jobs, notifications, 1)
    assert not closed.called
    finish(jobs, notifications, 2)
    assert results[2].called and closed.called and writer._pending == 0
    pool.shutdown.assert_called_once_with(wait=True)


def test_broken_pool_notifies_once_and_fails_waiting_writes(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    failures = []
    for i in range(3):
        writer.process_item({"url": str(i)}, spider).addErrback(lambda f: failures.append(f.type))
    finish(jobs, notifications, 0, BrokenProcessPool("worker died"))
    finish(jobs, notifications, 1, BrokenProcessPool("worker died"))
    assert failures == [BrokenProcessPool] * 3
    writer._on_broken.assert_called_once()
    assert len(jobs) == 2 and writer._pending == 0
    assert writer.close_spider(spider).called


def test_writer_failure_stops_refilling_and_closes_spider():
    from crawler.spiders.spider import HtmlSpider
    spider = HtmlSpider()
    spider.manual_bulk_crawls = SimpleNamespace(closed=False, poller=Mock(running=True))
    spider.crawler = SimpleNamespace(stats=Mock(), engine=Mock())
    spider.crawler.engine.close_spider.return_value = succeed(None)
    spider._writer_pool_broken()
    spider._writer_pool_broken()
    assert spider.manual_bulk_crawls.closed
    assert spider._reserve_urls("after_failure") == []
    spider.manual_bulk_crawls.poller.stop.assert_called_once()
    spider.crawler.engine.close_spider.assert_called_once_with(spider, reason="json_writer_pool_broken")


def test_writer_rejects_multiple_processes_that_could_reorder_appends(tmp_path):
    with pytest.raises(ValueError, match="0 or 1"):
        pipelines.JsonPipeline(str(tmp_path), 10, writer_processes=2)


def test_process_writer_preserves_output_and_recovers_from_child_death(tmp_path):
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "helpers/json_writer_runner.py"), str(tmp_path)],
        cwd=root / "containers/crawler",
        env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
