import asyncio
import threading
from unittest.mock import Mock

import fakeredis
import pytest
from scrapy.resolver import CachingThreadedResolver
from twisted.internet.defer import Deferred, CancelledError
from twisted.internet.error import DNSLookupError
from twisted.internet.task import Clock
from twisted.python.failure import Failure

from poc.distributed.app.resolver import Resolver
from poc.distributed.app.scan_lease import ScanLease
from poc.distributed.tests.test_retention import make_backend, publish, claim, response


def test_scan_lease_renews_across_long_scan_and_excludes_other_processes():
    async def run():
        c=fakeredis.FakeRedis(decode_responses=True)
        async with ScanLease(c,ttl_ms=150,interval=.02) as first:
            assert first.acquired
            await asyncio.sleep(.24)
            assert c.get(first.key)==first.token and not first.lost.is_set()
            async with ScanLease(c,ttl_ms=150,interval=.02) as second:
                assert not second.acquired
            assert c.get(first.key)==first.token
        assert c.get(first.key) is None and first.lost.is_set()
        c.close()
    asyncio.run(run())


def test_lost_scan_lease_stops_scan_and_never_releases_replacement_owner():
    async def run():
        c=fakeredis.FakeRedis(decode_responses=True)
        async with ScanLease(c,ttl_ms=1000,interval=.01) as lease:
            c.set(lease.key,'replacement',px=1000)
            for _ in range(100):
                if lease.lost.is_set():break
                await asyncio.sleep(.01)
            assert lease.lost.is_set()
        assert c.get(lease.key)=='replacement'
        c.close()
    asyncio.run(run())


def test_scan_error_releases_owned_lease():
    async def run():
        c=fakeredis.FakeRedis(decode_responses=True)
        with pytest.raises(RuntimeError):
            async with ScanLease(c) as lease:
                raise RuntimeError('scan failed')
        assert c.get(lease.key) is None
        c.close()
    asyncio.run(run())


def test_robots_completion_wakes_queued_pages_at_original_admission_priority():
    async def run():
        b=make_backend()
        try:
            await publish(b,urls=['https://example.org/a'])
            robots=(await claim(b))[0]
            await publish(b,'b'*32,urls=[f'https://other{i}.example.org/a' for i in range(100)])
            await b.store_robots(robots,response());await b.ack(robots)
            page=(await b.claim('test',1))['tasks'][0]
            assert page['kind']=='url' and page['group']=='example.org'
        finally:await b.client.aclose()
    asyncio.run(run())


def test_blocked_origins_are_not_rescanned_every_thirty_milliseconds():
    async def run():
        b=make_backend()
        try:
            await publish(b)
            await claim(b,1)  # Lease the robots task; its pages remain blocked.
            await b.claim('page-worker',1)
            seconds,micros=await b.client.time();now=seconds+micros/1e6
            assert await b.client.zscore('poc:ready','example.org')>now+.8
        finally:await b.client.aclose()
    asyncio.run(run())


def test_parallel_dns_lookups_share_work_without_sharing_mutable_deferred_chain(monkeypatch):
    root=Deferred();lookup=Mock(return_value=root)
    monkeypatch.setattr(CachingThreadedResolver,'getHostByName',lookup)
    resolver=Resolver(Clock(),100,15);values=[]
    resolver.getHostByName('coalesce.example').addCallback(lambda value:values.append(('a',value)))
    resolver.getHostByName('coalesce.example').addCallback(lambda value:values.append(('b',value)))
    assert lookup.call_count==1
    root.callback('203.0.113.1')
    assert values==[('a','203.0.113.1'),('b','203.0.113.1')]
    assert not resolver.inflight


def test_dns_cancellation_does_not_cancel_other_callers_and_failures_are_not_cached(monkeypatch):
    roots=[Deferred(),Deferred()];lookup=Mock(side_effect=roots)
    monkeypatch.setattr(CachingThreadedResolver,'getHostByName',lookup)
    resolver=Resolver(Clock(),100,15);errors=[]
    first=resolver.getHostByName('failed.example');second=resolver.getHostByName('failed.example')
    first.addErrback(lambda failure:errors.append(failure.type));second.addErrback(lambda failure:errors.append(failure.type))
    first.cancel();assert not roots[0].called
    roots[0].errback(Failure(DNSLookupError('temporary failure')))
    assert errors==[CancelledError,DNSLookupError]
    values=[];resolver.getHostByName('failed.example').addCallback(values.append)
    roots[1].callback('203.0.113.2')
    assert values==['203.0.113.2'] and lookup.call_count==2
