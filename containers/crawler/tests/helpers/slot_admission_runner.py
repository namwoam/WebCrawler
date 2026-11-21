"""Exercise delay-aware admission with real downloader slots and HTTP traffic."""
import asyncio
import json
import time


async def run():
    from scrapy import Request, Spider, signals
    from scrapy.crawler import CrawlerRunner
    from scrapy.utils.defer import maybe_deferred_to_future
    from twisted.internet import reactor
    from twisted.web.resource import Resource
    from twisted.web.server import NOT_DONE_YET, Site
    from crawler.site_policies import SitePolicies

    SitePolicies.load = classmethod(lambda cls: SitePolicies({
        "default": {"request_limits": {"max_concurrent_requests": 4}}}))
    received = []
    active = peak = 0

    class Page(Resource):
        isLeaf = True

        def render_GET(self, request):
            nonlocal active, peak
            received.append((request.path.decode(), time.monotonic()))
            active += 1
            peak = max(peak, active)

            def finish():
                nonlocal active
                active -= 1
                request.write(b"<html>ok</html>")
                request.finish()

            reactor.callLater(0.08, finish)
            return NOT_DONE_YET

    listener = reactor.listenTCP(0, Site(Page()), interface="127.0.0.1")
    url = f"http://127.0.0.1:{listener.getHost().port}"

    class Crawl(Spider):
        name = "admission-test"

        async def start(self):
            for i in range(12):
                yield Request(f"{url}/slow/{i}", meta={"download_slot": "slow"})
            for i in range(20):
                yield Request(f"{url}/fast/{i}", meta={"download_slot": "fast"})

        def parse(self, response):
            return ()

    runner = CrawlerRunner({
        "LOG_ENABLED": False, "CONCURRENT_REQUESTS": 4,
        "SCHEDULER_PRIORITY_QUEUE": "crawler.slot_admission_queue.SlotAdmissionQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "SCHEDULER_START_MEMORY_QUEUE": None,
        "SCHEDULER_START_DISK_QUEUE": None,
        "DOWNLOAD_DELAY_JITTER": 0, "RANDOMIZE_DOWNLOAD_DELAY": False,
        "DOWNLOAD_SLOTS": {"slow": {"concurrency": 4, "delay": 0.15},
                           "fast": {"concurrency": 4, "delay": 0}},
        "TELNETCONSOLE_ENABLED": False,
    })
    crawler = runner.create_crawler(Crawl)
    max_queued = 0
    sampling = True

    async def sample():
        nonlocal max_queued
        while sampling:
            engine = crawler.engine
            if engine is not None:
                max_queued = max(max_queued, *(len(s.queue) for s in engine.downloader.slots.values()), 0)
            await asyncio.sleep(0.005)

    sampler = asyncio.create_task(sample())
    try:
        await maybe_deferred_to_future(runner.crawl(crawler))
    finally:
        sampling = False
        await sampler
        await maybe_deferred_to_future(listener.stopListening())
    slow = [t for path, t in received if path.startswith("/slow/")]
    fast = [t for path, t in received if path.startswith("/fast/")]
    assert len(slow) == 12 and len(fast) == 20, received
    assert min(fast) < slow[4], "Delayed backlog blocked fast work"
    assert peak >= 2, "Admission serialized independent HTTP transfers"
    assert max_queued <= 1, max_queued
    # Server arrival timestamps can bunch up when this shared reactor is busy;
    # check the whole delayed run rather than a single pair's arrival jitter.
    assert slow[-1] - slow[0] >= 1.35, slow
    print(json.dumps({"max_queued": max_queued, "peak_transfers": peak,
                      "first_fast_seconds": round(min(fast)-min(slow), 3)}))


if __name__ == "__main__":
    from scrapy.utils.reactor import install_reactor
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    from twisted.internet.task import react
    from scrapy.utils.defer import deferred_from_coro
    react(lambda reactor: deferred_from_coro(run()))
