"""A Scrapy scheduler bridge to the shared ready-host frontier."""
import asyncio
from collections import deque
import json
import logging
import time

from scrapy import Request, signals
from scrapy.core.scheduler import BaseScheduler
from scrapy.exceptions import DontCloseSpider
from scrapy.utils.defer import deferred_from_coro
from prometheus_client import start_http_server

from .backend import AsyncBackend
from .metrics import WORKER, CAPACITY, ACTIVE_DOMAINS, AUTOTHROTTLE, sample_runtime
from .policy import routing
from . import profiling

logger = logging.getLogger(__name__)


def request_task(request):
    """Persist native Scrapy retry/redirect state, without recursively saving the lease."""
    task = request.meta['poc_task']
    policy = routing(request.url)
    if task['kind'] == 'robots':
        policy = {key: policy[key] for key in ('group', 'interval', 'concurrency')}
    meta = {key: value for key, value in request.meta.items() if not key.startswith('poc_')}
    # Scrapy's framework metadata here consists of scalar/list/dict values.
    json.dumps(meta)
    return {**task, 'url': request.url, **policy, 'request_meta': meta,
        'request_headers': {key.decode('latin1'): [v.decode('latin1') for v in values]
                            for key, values in request.headers.items()
                            if key.lower() not in (b'x-crawler-worker', b'x-crawler-node')},
        'redirects': meta.get('redirect_times', 0)}


class Frontier:
    def __init__(self, crawler):
        self.crawler = crawler
        self.backend = AsyncBackend()
        self.leases = {}
        self.background = set()
        self.stopping = False
        self.poller = None
        self.session = None
        self.loki = None
        self.profile_loop = None

    def start(self):
        import os
        from .loki import start_loki
        self.loki = start_loki()
        if profiling.ENABLED:
            self.profile_loop = asyncio.create_task(profiling.watch_loop('worker'))
        start_http_server(int(os.getenv('METRICS_PORT','9101')), addr='0.0.0.0')
        from scrapy.extensions.throttle import AutoThrottle
        AUTOTHROTTLE.set(int(self.crawler.get_extension(AutoThrottle) is not None))
        self.poller = asyncio.create_task(self.run())
        logger.warning('scrapy.worker.started worker=%s concurrency=%d autothrottle=%s',
            WORKER, CAPACITY, self.crawler.settings.getbool('AUTOTHROTTLE_ENABLED'))

    def idle(self):
        if not self.stopping:
            raise DontCloseSpider

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background.add(task)
        def finished(future):
            self.background.discard(future)
            if not future.cancelled() and future.exception():
                logger.error('frontier.background_failed', exc_info=future.exception())
        task.add_done_callback(finished)
        return task

    def finish(self, task):
        self.leases.pop(task['token'], None)

    async def release_network(self, task):
        try:
            await self.backend.release_network(task)
        except Exception:
            logger.exception('frontier.release_failed; network lease expires conservatively')

    async def requeue(self, request):
        task = request.meta['poc_task']
        try:
            for attempt in range(3):
                try:
                    await self.backend.requeue(request_task(request))
                    return
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)
        finally:
            self.finish(task)

    def admitted_request(self, task):
        spider = self.crawler.spider
        meta = {**task.get('request_meta', {}), 'poc_task': task, 'poc_admitted': True}
        if task['kind'] == 'robots':
            meta['dont_obey_robotstxt'] = True
        return Request(task['url'], headers=task.get('request_headers', {}), meta=meta,
            callback=spider.parse_robots if task['kind']=='robots' else spider.parse_page,
            errback=spider.request_failed, dont_filter=True)

    async def run(self):
        recovery = 0
        while not self.stopping:
            wait = .05
            try:
                now = time.time()
                for token, task in list(self.leases.items()):
                    if task['lease_deadline'] <= now:
                        self.leases.pop(token, None)
                ACTIVE_DOMAINS.set(len({task['group'] for task in self.leases.values()}))
                if time.monotonic() >= recovery:
                    sample_runtime(self.crawler, self.leases)
                    # Bound idle local slot state; active slots are never evicted.
                    slots=self.crawler.engine.downloader.slots
                    for key,slot in list(slots.items()):
                        if len(slots)<=self.crawler.settings.getint('WORKER_MAX_DOMAINS',30):break
                        if not slot.active and not slot.queue:
                            slot.close();slots.pop(key,None)
                    await self.backend.recover()
                    recovery = time.monotonic()+1
                if len(self.leases)<CAPACITY and not self.crawler.engine.needs_backout():
                    domains={task['group'] for task in self.leases.values()}
                    claimed=await self.backend.claim(WORKER,CAPACITY-len(self.leases),domains,self.crawler.settings.getint('WORKER_MAX_DOMAINS',30))
                    for task in claimed['tasks']:
                        self.leases[task['token']]=task
                        self.crawler.engine.crawl(self.admitted_request(task))
                    wait=0 if claimed['tasks'] else max(.02,min(.25,claimed['wait']))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('frontier.unavailable; admission paused')
                wait=1
            await asyncio.sleep(wait)

    async def close(self):
        self.stopping=True
        if self.poller:
            self.poller.cancel()
            await asyncio.gather(self.poller,return_exceptions=True)
        if self.background:
            done,pending=await asyncio.wait(self.background,timeout=10)
            for task in pending:task.cancel()
            await asyncio.gather(*pending,return_exceptions=True)
        if self.session:
            await self.session.close()
        if self.loki:
            await self.loki.stop()
        if self.profile_loop:
            self.profile_loop.cancel()
            await asyncio.gather(self.profile_loop,return_exceptions=True)
        await self.backend.client.aclose()


class DistributedScheduler(BaseScheduler):
    def __init__(self,crawler):
        self.crawler=crawler
        self.ready=deque()
        crawler.poc_frontier=Frontier(crawler)
        crawler.signals.connect(crawler.poc_frontier.start,signal=signals.engine_started)
        crawler.signals.connect(crawler.poc_frontier.idle,signal=signals.spider_idle)

    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler)

    def enqueue_request(self,request):
        if request.meta.pop('poc_admitted',False):
            self.ready.append(request)
        else:
            # Native Scrapy middleware returned a retry or redirect Request.
            self.crawler.poc_frontier.spawn(self.crawler.poc_frontier.requeue(request))
        return True

    def next_request(self):
        return self.ready.popleft() if self.ready else None

    def has_pending_requests(self):
        return bool(self.ready)

    def close(self,reason):
        return deferred_from_coro(self.crawler.poc_frontier.close())
