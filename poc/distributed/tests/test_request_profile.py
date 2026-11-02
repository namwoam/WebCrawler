import base64
from types import SimpleNamespace

import pytest
from scrapy import Request, Spider, signals
from scrapy.http import HtmlResponse, Response
from scrapy.settings import Settings
from scrapy.signalmanager import SignalManager
from scrapy.statscollectors import StatsCollector
from scrapy.spidermiddlewares.referer import RefererMiddleware
from scrapy.exceptions import DownloadTimeoutError, IgnoreRequest

from poc.distributed.app.request_profile import (robots_allowed, robots_result, USER_AGENT,
    DEFAULT_REQUEST_HEADERS, DOWNLOAD_TIMEOUT, DNS_TIMEOUT)
from poc.distributed.app.frontier import request_task
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.downloadermiddlewares.redirect import RedirectMiddleware, MetaRefreshMiddleware
from poc.distributed.app.policy import routing


def resolved(body, status=200, error=None, exception=False):
    return robots_result(dict(status_code=status,error=error,download_exception=exception,
        final_url='https://example.org/robots.txt',fetched_at='test'),body)


@pytest.mark.parametrize('status',[200,403,404,429,500,503])
def test_parse_received_body_regardless_of_http_status(status):
    origin=resolved(b'User-agent: *\nDisallow: /private\n',status)
    assert not robots_allowed(origin,'https://example.org/private')
    assert robots_allowed(origin,'https://example.org/public')


def test_failed_robots_download_has_no_parser_like_production():
    origin=resolved(b'',None,'DownloadTimeoutError',True)
    assert origin['state']=='ready' and origin['has_parser'] is False
    assert robots_allowed(origin,'https://example.org/private')
    assert origin['error']=='DownloadTimeoutError'


def test_protego_wildcards_precedence_bom_and_useragent():
    origin=resolved(b'\xef\xbb\xbfUser-agent: *\nDisallow: /*.pdf$\nAllow: /public/*.pdf$\n\nUser-agent: WebCrawlerBot\nDisallow: /\n')
    assert not robots_allowed(origin,'https://example.org/private/a.pdf')
    assert robots_allowed(origin,'https://example.org/public/a.pdf')
    assert robots_allowed(origin,'https://example.org/private/a.pdf?download=1')
    assert robots_allowed(origin,'https://example.org/other')


def test_download_failed_response_does_not_become_a_rule_denial():
    assert robots_allowed(resolved(b'User-agent: *\nDisallow: /',403,'ResponseDataLossError',True),
        'https://example.org/private')


def test_full_body_is_not_truncated_to_old_512k_limit():
    body=b'#'+b'x'*(512*1024)+b'\nUser-agent: *\nDisallow: /late\n'
    assert not robots_allowed(resolved(body),'https://example.org/late')


def test_netloc_cache_identity_matches_scrapy():
    assert routing('http://example.org/a')['origin_id']==routing('https://example.org/b')['origin_id']
    assert routing('http://example.org:8080/a')['origin_id']!=routing('http://example.org/a')['origin_id']


def test_profile_matches_verified_production_basics():
    assert 'Chrome/121.0.0.0' in USER_AGENT
    assert DEFAULT_REQUEST_HEADERS['Sec-Fetch-Mode']=='navigate'
    assert DOWNLOAD_TIMEOUT==DNS_TIMEOUT==15


@pytest.fixture
def transitions():
    settings=Settings({'RETRY_ENABLED':True,'RETRY_TIMES':1,'RETRY_PRIORITY_ADJUST':0,
        'REDIRECT_ENABLED':True,'REDIRECT_MAX_TIMES':20,'METAREFRESH_ENABLED':True})
    crawler=SimpleNamespace(settings=settings,signals=SignalManager(),spider=Spider('test'))
    crawler.stats=StatsCollector(crawler)
    crawler.spider.crawler=crawler
    crawler.spider.settings=settings
    referer=RefererMiddleware(settings)
    crawler.get_spider_middleware=lambda cls:referer
    # Reference native Scrapy middleware decisions, not the worker's control flow.
    class NativeReference:
        def __init__(self):
            self.retry=RetryMiddleware.from_crawler(crawler)
            self.redirect=RedirectMiddleware.from_crawler(crawler)
            self.refresh=MetaRefreshMiddleware.from_crawler(crawler)
        def response(self,request,response):
            request=request.replace(meta={k:v for k,v in request.meta.items() if k not in ('dont_retry','dont_redirect')})
            for middleware in (self.redirect,self.refresh,self.retry):
                value=middleware.process_response(request,response)
                if isinstance(value,Request):return value
        def exception(self,request,error):
            request=request.replace(meta={k:v for k,v in request.meta.items() if k!='dont_retry'})
            return self.retry.process_exception(request,error)
    result=NativeReference()
    crawler.signals.send_catch_log(signals.engine_started)
    return result


def test_retry_counter_is_independent_of_redirect_count(transitions):
    request=Request('https://example.org/a',meta={'dont_retry':True,'dont_redirect':True})
    redirect=transitions.response(request,Response(request.url,status=302,headers={'Location':'/b'}))
    assert redirect.meta['redirect_times']==1
    retry=transitions.response(redirect,Response(redirect.url,status=522))
    assert retry.meta['retry_times']==1
    assert transitions.response(retry,Response(retry.url,status=522)) is None
    assert transitions.exception(request,DownloadTimeoutError('test')) is not None
    assert transitions.exception(request,IgnoreRequest('too large')) is None


def test_redirect_limit_and_meta_refresh_match_scrapy(transitions):
    request=Request('https://example.org/a',meta={'redirect_times':19,'redirect_ttl':1})
    last=transitions.response(request,Response(request.url,status=301,headers={'Location':'/b'}))
    assert last.meta['redirect_times']==20
    with pytest.raises(IgnoreRequest):
        transitions.response(last,Response(last.url,status=301,headers={'Location':'/c'}))
    request=Request('https://example.org/page')
    response=HtmlResponse(request.url,body=b'<html><meta http-equiv="refresh" content="0;url=/next"></html>',encoding='utf8')
    assert transitions.response(request,response).url=='https://example.org/next'


def test_http_location_precedes_meta_refresh(transitions):
    request=Request('https://example.org/page')
    response=HtmlResponse(request.url,status=302,headers={'Location':'/http'},
        body=b'<meta http-equiv="refresh" content="0;url=/meta">',encoding='utf8')
    assert transitions.response(request,response).url=='https://example.org/http'


def test_robots_redirect_keeps_original_cache_identity(transitions):
    task={'kind':'robots','url':'https://example.org/robots.txt',**routing('https://example.org/robots.txt')}
    request=Request('https://cdn.example.org/robots.txt',meta={'redirect_times':1,'poc_task':task},headers={'X-Crawler-Worker':'test'})
    next_task=request_task(request)
    assert next_task['origin_id']==task['origin_id']
    assert next_task['group']=='cdn.example.org'
    assert 'X-Crawler-Worker' not in next_task['request_headers']


def test_429_retry_does_not_introduce_retry_after_wait(transitions):
    request=Request('https://example.org/rate-limited')
    response=Response(request.url,status=429,headers={'Retry-After':'86400'})
    retry=transitions.response(request,response)
    assert retry is not None and retry.meta['retry_times']==1
    assert transitions.response(retry,response) is None
