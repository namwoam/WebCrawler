import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from scrapy import Request, Spider
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector
from scrapy.exceptions import IgnoreRequest
from twisted.internet.task import Clock

from poc.distributed.app.worker import DistributedSpider, settings
from poc.distributed.app.frontier import Frontier, request_task
from poc.distributed.app.policy import routing
from poc.distributed.app.scrapy_components import BoundedConnectionPool, SharedRobotsMiddleware
from poc.distributed.app.request_profile import robots_result


def test_native_scrapy_components_and_autothrottle_default():
    s=settings()
    assert s['AUTOTHROTTLE_ENABLED'] is True
    assert s['AUTOTHROTTLE_TARGET_CONCURRENCY']==2
    assert s['DOWNLOAD_DELAY']==.25
    assert s['WORKER_MAX_DOMAINS']==30
    assert s['ROBOTSTXT_OBEY'] is True
    assert 'scrapy.downloadermiddlewares.retry.RetryMiddleware' not in s['DOWNLOADER_MIDDLEWARES']
    assert 'scrapy.downloadermiddlewares.redirect.RedirectMiddleware' not in s['DOWNLOADER_MIDDLEWARES']
    assert s['ITEM_PIPELINES']
    assert routing('https://example.org')['interval']==1/32
    assert routing('https://example.org')['concurrency']==16
    assert routing('https://en.wikipedia.org')['group']=='en.wikipedia.org'


def test_frontier_creates_normal_scrapy_requests_not_manual_download_calls():
    spider=DistributedSpider()
    crawler=SimpleNamespace(spider=spider)
    f=Frontier(crawler)
    task={'kind':'url','url':'https://example.org/a','token':'x',**routing('https://example.org/a')}
    request=f.admitted_request(task)
    assert request.callback==spider.parse_page
    assert request.errback==spider.request_failed
    assert not request.meta.get('dont_retry')
    assert not request.meta.get('dont_redirect')
    assert not request.meta.get('dont_obey_robotstxt')
    task['kind']='robots'
    request=f.admitted_request(task)
    assert request.meta['dont_obey_robotstxt'] is True
    assert 'handle_httpstatus_all' not in request.meta  # would disable native redirects!


def test_requeued_request_preserves_native_metadata_without_recursive_lease():
    task={'kind':'url','url':'https://a.example.org','token':'x',**routing('https://a.example.org')}
    request=Request('https://b.example.org',meta={'poc_task':task,'retry_times':1,'redirect_times':7},
                    headers={'Accept-Language':'en','X-Crawler-Worker':'old'})
    new=request_task(request)
    assert new['group']=='b.example.org'
    assert new['request_meta']=={'retry_times':1,'redirect_times':7}
    assert 'X-Crawler-Worker' not in new['request_headers']


def test_idle_connection_pool_has_bounded_keys_and_timeout_cleanup():
    clock=Clock();pool=BoundedConnectionPool(clock,30);closed=[]
    for i in range(80):
        connection=SimpleNamespace(state='QUIESCENT',transport=SimpleNamespace(loseConnection=lambda i=i:closed.append(i)))
        # Twisted tracks protocols by identity; a normal object is hashable.
        class Protocol:
            state='QUIESCENT'
            def __init__(self):self.transport=connection.transport
        pool._putConnection(('https',f'{i}.example.org',443),Protocol())
    assert len(pool._connections)==30 and len(closed)==50
    assert len(pool._timeouts)==30
    clock.advance(241)
    assert not pool._connections and not pool._timeouts and not pool.recent
    assert len(closed)==80


def test_shared_robots_uses_inherited_native_rule_enforcement():
    import json
    cache=robots_result(dict(status_code=200,error=None,final_url='https://example.org/robots.txt',fetched_at='test'),
        b'User-agent: *\nDisallow: /*.pdf$\nAllow: /public/*.pdf$\n')
    crawler=SimpleNamespace(settings=Settings({**settings(),'ROBOTSTXT_OBEY':True}),spider=Spider('test'))
    crawler.stats=StatsCollector(crawler)
    crawler.poc_frontier=SimpleNamespace(backend=SimpleNamespace(client=SimpleNamespace(hget=AsyncMock(return_value=json.dumps(cache)))))
    middleware=SharedRobotsMiddleware.from_crawler(crawler)
    def request(path):return Request('https://example.org'+path,meta={'poc_task':{'origin_id':'test'}})
    asyncio.run(middleware.process_request(request('/public/a.pdf')))
    with pytest.raises(IgnoreRequest):asyncio.run(middleware.process_request(request('/private/a.pdf')))
