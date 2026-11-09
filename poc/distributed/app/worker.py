"""API-only Scrapy worker: native spider, scheduler, middleware, callbacks and pipeline."""
import base64
import json
import logging
import os

from scrapy import Spider
from scrapy.crawler import AsyncCrawlerProcess

from .backend import now
from .metrics import CAPACITY, MAX_BYTES
from .request_profile import USER_AGENT, DEFAULT_REQUEST_HEADERS, DOWNLOAD_TIMEOUT, DNS_TIMEOUT, RETRY_TIMES, REDIRECT_MAX_TIMES, ROBOTS_CACHE, robots_result
from .scrapy_components import PermitExpired, JobCancelled


class DistributedSpider(Spider):
    name='distributed_api_worker'

    async def start(self):
        # The scheduler's shared frontier supplies Requests. Idle workers remain alive.
        if False:
            yield None

    def parse_page(self,response):
        task=response.meta['poc_task']
        content_type=response.headers.get(b'Content-Type',b'').split(b';',1)[0].strip().lower()
        html=content_type in {b'text/html',b'application/xhtml+xml'}
        error=None if 200<=response.status<300 else f'HTTP {response.status}'
        if not html:
            error=error or 'NonHTML content-type'
        return dict(task=task,body=response.body if html else None,
            metadata=dict(final_url=response.url,status_code=response.status,error=error,
                          html=html,fetched_at=now()))

    async def parse_robots(self,response):
        task=response.meta['poc_task']
        metadata=dict(final_url=response.url,status_code=response.status,error=None,fetched_at=now())
        try:
            cached=robots_result(metadata,response.body)
        except Exception as error:
            cached=robots_result({**metadata,'download_exception':True,'error':type(error).__name__},b'')
        await self.crawler.poc_frontier.backend.store_robots(task,cached)
        await self.crawler.poc_frontier.backend.ack(task)
        self.crawler.poc_frontier.finish(task)

    async def request_failed(self,failure):
        request=failure.request;task=request.meta['poc_task'];frontier=self.crawler.poc_frontier
        if isinstance(failure.value,PermitExpired):
            await frontier.requeue(request)
            return None
        if isinstance(failure.value,JobCancelled):
            await frontier.backend.ack(task);frontier.finish(task)
            return None
        response=getattr(failure.value,'response',None)
        if response is not None:
            if task['kind']=='robots':
                await self.parse_robots(response)
                return None
            return self.parse_page(response)
        await frontier.release_network(task)
        metadata=dict(final_url=request.url,status_code=None,error=failure.type.__name__,
                      html=False,fetched_at=now(),download_exception=True)
        if task['kind']=='robots':
            await frontier.backend.store_robots(task,robots_result(metadata,b''))
            await frontier.backend.ack(task);frontier.finish(task)
            return None
        return dict(task=task,metadata=metadata,body=None)


def settings():
    return {
        'TWISTED_REACTOR':'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'TWISTED_REACTOR_ENABLED':True,
        'SCHEDULER':'poc.distributed.app.frontier.DistributedScheduler',
        'DOWNLOAD_HANDLERS':{
            'http':'poc.distributed.app.scrapy_components.PermitHTTP11DownloadHandler',
            'https':'poc.distributed.app.scrapy_components.PermitHTTP11DownloadHandler'},
        'TWISTED_DNS_RESOLVER':'poc.distributed.app.resolver.Resolver',
        'REACTOR_THREADPOOL_MAXSIZE':32,
        'CONCURRENT_REQUESTS':CAPACITY,
        'WORKER_MAX_DOMAINS':int(os.getenv('WORKER_MAX_DOMAINS','30')),
        'CONCURRENT_REQUESTS_PER_DOMAIN':int(os.getenv('CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN','16')),
        'SCRAPER_SLOT_MAX_ACTIVE_SIZE':int(os.getenv('CRAWLER_SCRAPER_SLOT_MAX_ACTIVE_SIZE','100000000')),
        'DOWNLOAD_DELAY':float(os.getenv('CRAWLER_DOWNLOAD_DELAY','.25')),
        'AUTOTHROTTLE_ENABLED':os.getenv('CRAWLER_USE_AUTOTHROTTLE','true').lower() in ('true','1','yes'),
        'AUTOTHROTTLE_START_DELAY':float(os.getenv('CRAWLER_AUTOTHROTTLE_START_DELAY','.5')),
        'AUTOTHROTTLE_MAX_DELAY':float(os.getenv('CRAWLER_AUTOTHROTTLE_MAX_DELAY','10')),
        'AUTOTHROTTLE_TARGET_CONCURRENCY':float(os.getenv('CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY','2')),
        'RETRY_ENABLED':True,'RETRY_TIMES':RETRY_TIMES,'RETRY_PRIORITY_ADJUST':0,
        'REDIRECT_ENABLED':True,'REDIRECT_MAX_TIMES':REDIRECT_MAX_TIMES,
        'ROBOTSTXT_OBEY':True,
        'DOWNLOADER_MIDDLEWARES':{
            'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware':None,
            'poc.distributed.app.scrapy_components.SharedRobotsMiddleware':100,
            'poc.distributed.app.scrapy_components.MockTraceMiddleware':50},
        'ITEM_PIPELINES':{'poc.distributed.app.scrapy_components.ResultUploadPipeline':500},
        'COOKIES_ENABLED':False,'TELNETCONSOLE_ENABLED':False,'HTTPPROXY_ENABLED':False,
        'DOWNLOAD_TIMEOUT':DOWNLOAD_TIMEOUT,'DNS_TIMEOUT':DNS_TIMEOUT,'DOWNLOAD_MAXSIZE':MAX_BYTES,
        'DOWNLOAD_WARNSIZE':MAX_BYTES,'DOWNLOAD_FAIL_ON_DATALOSS':True,
        'USER_AGENT':USER_AGENT,'DEFAULT_REQUEST_HEADERS':DEFAULT_REQUEST_HEADERS,'LOG_LEVEL':'WARNING',
    }


def main():
    process=AsyncCrawlerProcess(settings=settings())
    process.crawl(DistributedSpider)
    process.start()


if __name__=='__main__':
    main()
