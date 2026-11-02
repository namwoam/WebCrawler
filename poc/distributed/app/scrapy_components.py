"""Small Scrapy extensions for shared robots state, permit validation and result storage."""
import asyncio
from collections import OrderedDict
from twisted.web.client import HTTPConnectionPool
import base64
import json
import logging
import os
import tempfile
import time
from urllib.parse import urlsplit

import aiohttp
from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler
from scrapy.downloadermiddlewares.robotstxt import RobotsTxtMiddleware
from scrapy.exceptions import DownloadTimeoutError, IgnoreRequest

from .backend import now
from .metrics import WORKER, NODE, REQUESTS, DOWNLOADS, DURATION, LABELS, emit
from .request_profile import ROBOTS_CACHE, parse_robots
from .result_upload import upload


class PermitExpired(IgnoreRequest):
    pass


class JobCancelled(IgnoreRequest):
    pass


class SharedRobotsMiddleware(RobotsTxtMiddleware):
    async def robot_parser(self,request):
        task=request.meta['poc_task']
        raw=await self.crawler.poc_frontier.backend.client.hget(ROBOTS_CACHE,task['origin_id'])
        origin=json.loads(raw or '{"state":"pending"}')
        if origin.get('state')=='pending':
            # Recovery can temporarily return a URL before its robots cache is ready.
            raise PermitExpired('Robots cache not ready; return to frontier')
        if not origin.get('has_parser',False):
            return None
        # Inherited process_request/process_request_2 enforce the actual Scrapy rules.
        from .request_profile import small_robots_parser
        body=base64.b64decode(origin['body_b64'])
        return small_robots_parser(body) if len(body)<=65536 else parse_robots(body)


class MockTraceMiddleware:
    def process_request(self,request):
        if (urlsplit(request.url).hostname or '').endswith('.poc.test'):
            request.headers['X-Crawler-Worker']=WORKER
            request.headers['X-Crawler-Node']=NODE


class BoundedConnectionPool(HTTPConnectionPool):
    """Bound idle cached connection keys while leaving native active transfers alone."""
    def __init__(self,reactor,maximum):
        super().__init__(reactor,persistent=True)
        self.maximum=maximum
        self.recent=OrderedDict()

    def _removeConnection(self,key,connection):
        timer=self._timeouts.get(connection)
        if timer is not None and timer.active():timer.cancel()
        super()._removeConnection(key,connection)
        if not self._connections[key]:
            del self._connections[key]
            self.recent.pop(key,None)

    def getConnection(self,key,endpoint):
        result=super().getConnection(key,endpoint)
        if key in self._connections and not self._connections[key]:
            del self._connections[key];self.recent.pop(key,None)
        return result

    def _putConnection(self,key,connection):
        super()._putConnection(key,connection)
        if not self._connections.get(key):return
        self.recent.pop(key,None);self.recent[key]=None
        while len(self.recent)>self.maximum:
            oldest=next(iter(self.recent))
            for cached in list(self._connections.get(oldest,())):
                self._removeConnection(oldest,cached)


class PermitHTTP11DownloadHandler(HTTP11DownloadHandler):
    def __init__(self,crawler):
        super().__init__(crawler)
        if not hasattr(crawler,'poc_connection_pool'):
            pool=BoundedConnectionPool(self._pool._reactor,crawler.settings.getint('WORKER_MAX_DOMAINS',30))
            pool.maxPersistentPerHost=self._pool.maxPersistentPerHost
            pool._factory=self._pool._factory
            crawler.poc_connection_pool=pool
        self._pool=crawler.poc_connection_pool

    async def download_request(self,request):
        frontier=self._crawler.poc_frontier
        task=request.meta['poc_task']
        # This hook runs after Scrapy's AutoThrottle/slot queue, just before HTTP.
        # Recheck the shared rate clock here; claim-time spacing cannot prevent
        # locally queued requests from starting together later.
        while True:
            checked=time.monotonic()
            permit=await frontier.backend.start_network(task)
            if permit['status']=='cancelled':
                raise JobCancelled('Job cancelled or expired')
            if permit['status']=='expired':
                raise PermitExpired('Network permit no longer valid')
            if permit['status']=='started':
                # Use server-reported remaining time, subtracting the round trip
                # and a safety margin. Stop HTTP before reservation recovery.
                budget=min(30,permit['remaining']-(time.monotonic()-checked)-5)
                if budget<=0:
                    raise PermitExpired('Network permit expired during admission')
                break
            await asyncio.sleep(min(.25,max(.001,permit['wait'])))
        status='error';started=time.monotonic();failure=None
        emit('request.start',domain=task.get('group',urlsplit(request.url).hostname),kind=task['kind'])
        with DOWNLOADS.track_inprogress():
            try:
                try:
                    async with asyncio.timeout(budget):
                        response=await super().download_request(request)
                except TimeoutError as error:
                    # Preserve native Scrapy retry handling for the outer bound.
                    raise DownloadTimeoutError('Download exceeded network reservation budget') from error
                status=str(response.status)
                return response
            except BaseException as error:
                failure=type(error).__name__
                raise
            finally:
                duration=time.monotonic()-started
                DURATION.observe(duration)
                emit('request.fail' if failure else 'request.end',
                    level=logging.WARNING if failure else logging.INFO,
                    domain=task.get('group',urlsplit(request.url).hostname),kind=task['kind'],
                    status=None if status=='error' else int(status),
                    latency_ms=duration*1000,fail_reason=failure)
                REQUESTS.labels(**LABELS,status=status,
                    outcome='error' if status=='error' or int(status)>=400 else 'success',kind=task['kind']).inc()
                frontier.spawn(frontier.release_network(task))


class ResultUploadPipeline:
    @classmethod
    def from_crawler(cls,crawler):
        instance=cls();instance.crawler=crawler;return instance

    async def process_item(self,item):
        frontier=self.crawler.poc_frontier
        task=item['task'];body=None
        try:
            body=tempfile.SpooledTemporaryFile(max_size=256*1024)
            raw=item.pop('body',None)
            if raw:
                await asyncio.to_thread(body.write,raw)
            del raw
            body.seek(0)
            if frontier.session is None:
                frontier.session=aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=64),trust_env=False)
            await upload(frontier.session,task,item['metadata'],body)
            return item
        finally:
            if body:body.close()
            frontier.finish(task)
