from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone
import multiprocessing
import os
from pathlib import Path

from twisted.internet.defer import Deferred, DeferredSemaphore
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from crawler.worker_lifecycle import initialize_worker

from libs.ipc.jsonio import append_jsonl
from libs.ipc.folder_reader import current_interval


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class JsonPipeline:
    """
    Writes each item as a JSONL record, acknowledging it only after persistence.

    base_dir = RESULT_DIR_TEMPLATE.format(id=crawler_id)

    date = YYYYMMDD
    time = HHMM (floored to interval)
    """
    def __init__(self, result_dir_template: str, interval_minutes: int, *, writer_processes=0,
                 on_broken=None):
        if writer_processes not in (0, 1):
            raise ValueError("JSON_WRITER_PROCESSES must be 0 or 1 to preserve record order")
        self.result_dir_template = result_dir_template
        self.interval_minutes = interval_minutes
        self.base_dir = None
        self.writer_processes = writer_processes
        self.stats = None
        self._pool = None
        self._slots = DeferredSemaphore(2)
        self._pending = 0
        self._closing = False
        self._drained = None
        self._closed = None
        self._broken = None
        self._on_broken = on_broken

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings

        tmpl = settings.get("RESULT_DIR_TEMPLATE")
        interval_minutes = int(settings.getint("INTERVAL_MINUTES", 5))

        pipe = cls(tmpl, interval_minutes,
                   writer_processes=settings.getint("JSON_WRITER_PROCESSES", 0),
                   on_broken=lambda: crawler.spider._writer_pool_broken())
        pipe.stats = crawler.stats
        return pipe

    def open_spider(self, spider):
        self.base_dir = Path(self.result_dir_template.format(id=spider.crawler_id))
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if self.writer_processes:
            self._pool = ProcessPoolExecutor(
                max_workers=1, mp_context=multiprocessing.get_context("spawn"),
                initializer=initialize_worker, initargs=(os.getpid(),),
            )

    def process_item(self, item, spider):
        if self.base_dir is None:
            raise RuntimeError("Pipeline not initialized (spider_opened not called)")
        if self._closing:
            raise RuntimeError("JSON writer is closing")
        if self._broken is not None:
            raise self._broken

        date, t = current_interval(self.interval_minutes)

        out_dir = self.base_dir / date / t
        path = out_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}.jsonl"

        rec = {
            "url": item.get("url"),
            "domain": item.get("domain"),
            "fetched_at": _now_iso(),
            "status": "ok" if item.get("content") else "fail",
            "fail_reason": item.get("fail_reason"),
            "content": item.get("content"),
            "outlinks": item.get("outlinks", []),
            "title": item.get("title"),
            "hreflang_count": item.get("hreflang_count"),
            "has_json_ld": item.get("has_json_ld"),
            "last_modified": item.get("last_modified"),
            "etag": item.get("etag"),
            "cache_control": item.get("cache_control"),
            "is_redirect": item.get("is_redirect"),
            "redirect_hop_count": item.get("redirect_hop_count"),
        }

        if not self.writer_processes:
            append_jsonl(path, rec)
            self._record_written(spider, rec)
            return item

        # Two submitted jobs for one ordered writer, matching the parser's
        # bounded process-pool pattern. Additional items remain under Scrapy's
        # pipeline/response backpressure instead of an executor's work queue.
        self._pending += 1
        if self.stats is not None:
            self.stats.set_value("json_writer/pending", self._pending)
        result = Deferred()
        work = self._slots.run(self._submit_write, path, rec)

        def finished(value):
            self._pending -= 1
            failed = isinstance(value, Failure)
            if self.stats is not None:
                self.stats.set_value("json_writer/pending", self._pending)
                self.stats.inc_value("json_writer/errors" if failed else "json_writer/written")
            if not failed:
                self._record_written(spider, rec)
            # Cancellation of the caller must not cancel the internal write or
            # release its admission permit before the writer actually finishes.
            if not result.called:
                if failed:
                    result.errback(value)
                else:
                    result.callback(item)
            if self._pending == 0 and self._drained is not None and not self._drained.called:
                self._drained.callback(None)
            return None

        work.addBoth(finished)
        return result

    @staticmethod
    def _record_written(spider, record):
        # Preserve the production heartbeat counters on the reactor, after the
        # child's acknowledgement rather than before handing work to the pool.
        spider.items_written = getattr(spider, "items_written", 0) + 1
        if record["status"] == "ok":
            spider.items_ok = getattr(spider, "items_ok", 0) + 1

    def _mark_broken(self, error):
        if self._broken is None:
            self._broken = error
            if self._on_broken is not None:
                self._on_broken()

    def _submit_write(self, path, record):
        from twisted.internet import reactor

        if self._broken is not None:
            raise self._broken
        try:
            future = self._pool.submit(append_jsonl, path, record)
        except BrokenProcessPool as error:
            self._mark_broken(error)
            raise
        result = Deferred()

        def completed():
            try:
                future.result()
            except BrokenProcessPool as error:
                self._mark_broken(error)
                result.errback(Failure())
            except BaseException:
                result.errback(Failure())
            else:
                result.callback(None)

        future.add_done_callback(lambda _: reactor.callFromThread(completed))
        return result

    def close_spider(self, spider):
        self._closing = True
        if self._pool is None:
            return None
        if self._closed is None:
            self._drained = Deferred()
            self._closed = Deferred()
            # Drain all accepted writes, then join the writer process outside
            # the reactor. Scrapy awaits this Deferred during graceful shutdown.
            self._drained.addCallback(lambda _: deferToThread(self._pool.shutdown, wait=True))
            self._drained.chainDeferred(self._closed)
            if self._pending == 0:
                self._drained.callback(None)
        return self._closed
