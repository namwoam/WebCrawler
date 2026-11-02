"""Execute the coordinator Lua scripts and exercise the final HTTP boundary."""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest
from scrapy import Request
from scrapy.http import Response
from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler
from scrapy.exceptions import DownloadTimeoutError

from poc.distributed.app import scripts
from poc.distributed.app.backend import AsyncBackend, tasks_for
from poc.distributed.app.scrapy_components import (
    JobCancelled, PermitExpired, PermitHTTP11DownloadHandler,
)


async def setup_backend(concurrency=2, count=3):
    backend = AsyncBackend.__new__(AsyncBackend)
    backend.client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    for name, script in [('claim', scripts.CLAIM), ('start_network', scripts.START_NETWORK),
                         ('ack', scripts.ACK), ('requeue', scripts.REQUEUE), ('recover', scripts.RECOVER)]:
        setattr(backend, '_' + name, backend.client.register_script(script))
    stamp = datetime.now(timezone.utc)
    payload = dict(urls=[f'https://example.org/{i}' for i in range(count)])
    tasks = tasks_for('job', payload)
    for task in tasks:
        task.update(interval=1, concurrency=concurrency)
    await backend.client.hset('poc:origins:v2', tasks[0]['origin_id'], json.dumps({'state': 'ready'}))
    enqueue = backend.client.register_script(scripts.ENQUEUE)
    await enqueue(args=[json.dumps(dict(id='job', created_at=stamp.isoformat(),
        expires=(stamp + timedelta(days=1)).timestamp(), tasks=tasks))])
    return backend


def test_concurrent_workers_share_http_start_clock_and_hold_capacity():
    async def run():
        b = await setup_backend()
        try:
            # Claims reserve concurrency without spending the HTTP rate budget.
            claims = await asyncio.gather(b.claim('a', 1), b.claim('b', 1))
            tasks = [claim['tasks'][0] for claim in claims]
            assert await b.client.hget('poc:host:example.org', 'next') is None
            assert (await b.claim('c', 1))['tasks'] == []
            assert all(t['network_deadline'] == t['lease_deadline'] for t in tasks)
            assert all(t['network_deadline'] - t['granted_at'] == pytest.approx(180) for t in tasks)

            # Both local Scrapy queues become ready together: only one may start.
            starts = await asyncio.gather(*(b.start_network(t) for t in tasks))
            assert sorted(s['status'] for s in starts) == ['started', 'wait']
            winner = next(i for i, s in enumerate(starts) if s['status'] == 'started')
            assert (await b.start_network(tasks[winner]))['status'] == 'expired'
            assert await b.client.zcard('poc:net:example.org') == 2

            # Advancing the rate clock does not free an ongoing download's slot.
            await b.client.hset('poc:host:example.org', 'next', 0)
            assert (await b.start_network(tasks[1 - winner]))['status'] == 'started'
            assert await b.client.zcard('poc:net:example.org') == 2
            await b.release_network(tasks[winner])
            await b.client.zadd('poc:ready', {'example.org': 0})
            await b.client.hset('poc:host:example.org', 'next', 0)
            assert len((await b.claim('c', 1))['tasks']) == 1
        finally:
            await b.client.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('invalid', ['cancelled', 'expired', 'near_expiry', 'released', 'recovered', 'job_expired'])
def test_invalid_reservation_never_gets_http_permission(invalid):
    async def run():
        b = await setup_backend()
        try:
            task = (await b.claim('a', 1))['tasks'][0]
            if invalid == 'cancelled':
                await b.client.hset('poc:job:job', 'status', 'cancelled')
            elif invalid == 'job_expired':
                await b.client.hset('poc:job:job', 'expires', 0)
            elif invalid == 'expired':
                await b.client.zadd('poc:net:example.org', {task['token']: 0})
            elif invalid == 'near_expiry':
                seconds, micros = await b.client.time()
                await b.client.zadd('poc:net:example.org', {task['token']: seconds + micros / 1e6 + 19})
            elif invalid == 'released':
                await b.release_network(task)
            else:
                await b.client.zadd('poc:lease_deadlines', {task['token']: 0})
                await b._recover()
            result = await b.start_network(task)
            assert result['status'] == ('cancelled' if invalid in ('cancelled', 'job_expired') else 'expired')
            assert await b.client.hget('poc:host:example.org', 'next') is None
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_recovered_task_can_start_with_new_token():
    async def run():
        b = await setup_backend(concurrency=1, count=1)
        try:
            task = (await b.claim('a', 1))['tasks'][0]
            assert (await b.start_network(task))['status'] == 'started'
            await b.client.zadd('poc:lease_deadlines', {task['token']: 0})
            await b._recover()
            await b.client.hset('poc:host:example.org', 'next', 0)
            await b.client.zadd('poc:ready', {'example.org': 0})
            new = (await b.claim('b', 1))['tasks'][0]
            assert new['token'] != task['token']
            assert (await b.start_network(new))['status'] == 'started'
            assert (await b.start_network(task))['status'] == 'expired'
        finally:
            await b.client.aclose()
    asyncio.run(run())


def make_handler(results):
    background = []
    frontier = SimpleNamespace(backend=SimpleNamespace(start_network=AsyncMock(side_effect=results)),
        release_network=AsyncMock(), spawn=lambda coro: background.append(asyncio.create_task(coro)))
    handler = PermitHTTP11DownloadHandler.__new__(PermitHTTP11DownloadHandler)
    handler._crawler = SimpleNamespace(poc_frontier=frontier)
    request = Request('https://example.org/a', meta={'poc_task': {'token': 'token', 'kind': 'url'}})
    return handler, frontier, request, background


def test_handler_waits_for_rate_permission_and_holds_slot_until_download_ends(monkeypatch):
    async def run():
        handler, frontier, request, background = make_handler([
            {'status': 'wait', 'wait': .001}, {'status': 'started', 'remaining': 100}])
        async def download(self, req):
            assert frontier.backend.start_network.await_count == 2
            frontier.release_network.assert_not_awaited()
            return Response(req.url, status=200)
        monkeypatch.setattr(HTTP11DownloadHandler, 'download_request', download)
        assert (await handler.download_request(request)).status == 200
        await asyncio.gather(*background)
        frontier.release_network.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('status,error', [('expired', PermitExpired), ('cancelled', JobCancelled)])
def test_handler_rejects_invalid_permits_before_http(monkeypatch, status, error):
    async def run():
        handler, _, request, _ = make_handler([{'status': status}])
        download = AsyncMock()
        monkeypatch.setattr(HTTP11DownloadHandler, 'download_request', download)
        with pytest.raises(error):
            await handler.download_request(request)
        download.assert_not_awaited()
    asyncio.run(run())


def test_download_is_cancelled_before_reservation_expiration(monkeypatch):
    async def run():
        handler, frontier, request, background = make_handler([{'status': 'started', 'remaining': 20}])
        stopped = asyncio.Event()
        async def download(self, req):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        native_timeout = asyncio.timeout
        def short_timeout(budget):
            assert 0 < budget <= 15  # At least five seconds before reservation expiry.
            return native_timeout(.01)
        monkeypatch.setattr(HTTP11DownloadHandler, 'download_request', download)
        monkeypatch.setattr(asyncio, 'timeout', short_timeout)
        with pytest.raises(DownloadTimeoutError):
            await handler.download_request(request)
        await asyncio.gather(*background)
        assert stopped.is_set()
        frontier.release_network.assert_awaited_once()
    asyncio.run(run())
