import asyncio
from datetime import datetime, timedelta, timezone
import json
import random
from types import SimpleNamespace
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest
from scrapy import Request
from scrapy.settings import Settings
from scrapy.statscollectors import StatsCollector

from poc.distributed.app import backend as backend_module, scripts
from poc.distributed.app.backend import AsyncBackend, tasks_for
from poc.distributed.app.request_profile import robots_allowed, robots_result
from poc.distributed.app.retention import ROBOTS_BODY_PREFIX, ROBOTS_CACHE, pack_robots, unpack_robots, robots_ttl
from poc.distributed.app.scrapy_components import PermitExpired, SharedRobotsMiddleware


def make_backend():
    b = AsyncBackend.__new__(AsyncBackend)
    b.client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    for name in ('claim', 'start_network', 'ack', 'requeue', 'recover', 'robots_get', 'robots_store', 'robots_invalidate'):
        setattr(b, '_' + name, b.client.register_script(getattr(scripts, name.upper())))
    return b


async def publish(b, job='a' * 32, urls=None):
    stamp = datetime.now(timezone.utc)
    tasks = tasks_for(job, dict(urls=urls or ['https://example.org/a', 'https://example.org/b']))
    for t in tasks:
        t['interval'] = 0
    payload = dict(id=job, created_at=stamp.isoformat(), expires=(stamp + timedelta(days=1)).timestamp(), tasks=tasks)
    await b.client.register_script(scripts.ENQUEUE)(args=[json.dumps(payload)])
    return payload


async def claim(b, capacity=8):
    for _ in range(10):
        for host in await b.client.zrange('poc:ready', 0, -1):
            await b.client.zadd('poc:ready', {host: 0})
        result = await b.claim('test', capacity)
        if result['tasks']:
            return result['tasks']
    return []


def response(body=b'User-agent: *\nDisallow: /a\n', status=200):
    return robots_result(dict(status_code=status, error=None, final_url='https://example.org/robots.txt', fetched_at='test'), body)


def body_key(t):
    return ROBOTS_BODY_PREFIX + t['origin_id'] + ':' + t['robots_generation']


def test_compression_preserves_native_parser_and_expiration_policy():
    value = response(b'User-agent: *\nDisallow: /a\n' + b'# comment\n' * 10000)
    packed = pack_robots(value)
    assert len(packed) < len(json.dumps(value)) / 10
    assert unpack_robots(packed) == value
    assert not robots_allowed(unpack_robots(packed), 'https://example.org/a')
    assert robots_allowed(unpack_robots(packed), 'https://example.org/b')
    for _ in range(20):
        assert 69120 <= robots_ttl(value) <= 86400
        assert 240 <= robots_ttl(response(status=503)) <= 300
        assert 240 <= robots_ttl(response(status=None)) <= 300


def test_expiry_during_active_job_refetches_once_and_applies_new_rules():
    async def run():
        b = make_backend()
        try:
            await publish(b)
            robots = (await claim(b))[0]
            assert robots['kind'] == 'robots'
            assert await b.store_robots(robots, response())
            await b.ack(robots)
            leased = (await claim(b, 1))[0]
            assert not robots_allowed(await b.get_robots(leased), 'https://example.org/a')
            # Expire both the body and the origin field while another URL is queued.
            await b.client.delete(body_key(leased))
            await b.client.execute_command('HEXPIRE', ROBOTS_CACHE, 0, 'FIELDS', 1, leased['origin_id'])
            assert await b.get_robots(leased) is None
            await asyncio.gather(b.requeue(leased), publish(b, 'b' * 32))
            pending = json.loads(await b.client.hget(ROBOTS_CACHE, leased['origin_id']))
            assert pending['state'] == 'pending'
            queued = [json.loads(v) for v in await b.client.lrange('poc:q:example.org', 0, -1)]
            assert sum(t['kind'] == 'robots' for t in queued) == 1
            refreshed = (await claim(b))[0]
            assert refreshed['kind'] == 'robots'
            assert refreshed['robots_generation'] != leased['robots_generation']
            assert await b.store_robots(refreshed, response(b'User-agent: *\nDisallow: /b\n'))
            await b.ack(refreshed)
            pages = await claim(b)
            assert len(pages) == 4 and all(t['kind'] == 'url' for t in pages)
            for t in pages:
                rules = await b.get_robots(t)
                assert robots_allowed(rules, 'https://example.org/a')
                assert not robots_allowed(rules, 'https://example.org/b')
        finally:
            await b.client.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('limit', ['bytes', 'entries'])
def test_cache_budget_evicts_only_bodies_and_eviction_refetches(limit):
    async def run():
        b = make_backend()
        try:
            await publish(b, urls=[f'https://h{i}.example.org/a' for i in range(3)])
            robots = await claim(b)
            assert len(robots) == 3 and all(t['kind'] == 'robots' for t in robots)
            value = response(random.Random(7).randbytes(2000))
            packed = pack_robots(value)
            budget = len(packed) * 2 if limit == 'bytes' else len(packed) * 10
            entries = 2 if limit == 'entries' else 10
            await b.client.set('do-not-evict', 'scheduler-state')
            for t in robots:
                assert await b._robots_store(args=[t['token'], packed, 600, budget, entries]) == 1
                await b.ack(t)
            assert await b.client.get(body_key(robots[0])) is None
            assert await b.client.get(body_key(robots[-1])) == packed
            assert await b.client.get('do-not-evict') == 'scheduler-state'
            assert await b.client.zcard('poc:robots:lru') == 2
            assert await b.client.hlen(ROBOTS_CACHE) == 2
            assert int(await b.client.get('poc:robots:bytes')) == len(packed) * 2
            pages = await claim(b)
            if len(pages) < 3:
                pages += await claim(b)
            # The evicted host gets a new robots lease; its page waits.
            assert len([t for t in pages if t['kind'] == 'robots']) == 1
            assert len([t for t in pages if t['kind'] == 'url']) == 2
            assert int(await b.client.get('poc:pending')) == 3
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_cache_maintenance_and_reads_do_not_extend_freshness():
    async def run():
        b = make_backend()
        try:
            await publish(b)
            t = (await claim(b))[0]
            packed = pack_robots(response())
            await b._robots_store(args=[t['token'], packed, 100, 100000, 10])
            await b.ack(t)
            before = await b.client.ttl(body_key(t))
            await b.get_robots(t)
            await b.client.register_script(scripts.MAINTAIN)()
            assert 0 < await b.client.ttl(body_key(t)) <= before
            assert 0 < (await b.client.execute_command('HTTL', ROBOTS_CACHE, 'FIELDS', 1, t['origin_id']))[0] <= 100
            await b.client.zadd('poc:robots:expiry', {body_key(t): 0})
            await b.client.register_script(scripts.MAINTAIN)()
            assert await b.client.get(body_key(t)) is None
            assert int(await b.client.get('poc:robots:bytes')) == 0
            assert await b.client.hlen('poc:robots:sizes') == 0
            assert await b.client.zcard('poc:robots:lru') == 0
            assert (await claim(b))[0]['kind'] == 'robots'
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_recovered_robots_lease_fences_old_writer():
    async def run():
        b = make_backend()
        try:
            await publish(b)
            old = (await claim(b))[0]
            await b.client.zadd('poc:lease_deadlines', {old['token']: 0})
            await b._recover()
            fresh = (await claim(b))[0]
            assert fresh['token'] != old['token']
            assert await b.store_robots(old, response()) is False
            assert await b.store_robots(fresh, response(b'User-agent: *\nDisallow: /b\n'))
            assert await b.store_robots(old, response()) is False
            assert robots_allowed(await b.get_robots(fresh), 'https://example.org/a')
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_corrupt_cache_is_removed_and_rescheduled_without_bypassing_rules():
    async def run():
        b = make_backend()
        try:
            await publish(b)
            robots = (await claim(b))[0]
            await b.store_robots(robots, response())
            await b.ack(robots)
            page = (await claim(b, 1))[0]
            await b.client.set(body_key(page), 'invalid gzip')
            assert await b.get_robots(page) is None
            assert int(await b.client.get('poc:robots:bytes')) == 0
            await b.requeue(page)
            assert (await claim(b))[0]['kind'] == 'robots'
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_terminal_state_expires_without_losing_deduplication_or_html_accounting():
    async def run():
        b = make_backend()
        try:
            payload = await publish(b, urls=['https://example.org/a'])
            t = (await claim(b))[0]
            await b.store_robots(t, response())
            await b.ack(t)
            page = (await claim(b))[0]
            assert (await b.client.execute_command('HTTL', 'poc:states', 'FIELDS', 1, page['id'])) == [-1]
            await b._ack(args=[page['token'], 1, 1, 42, 'test'])
            assert (await b.client.execute_command('HTTL', 'poc:states', 'FIELDS', 1, page['id']))[0] > 0
            await b.client.execute_command('HEXPIRE', 'poc:states', 0, 'FIELDS', 1, page['id'])
            assert await b._ack(args=[page['token'], 1, 1, 42, 'test']) == 0
            assert await b.client.register_script(scripts.ENQUEUE)(args=[json.dumps(payload)]) == 0
            assert await b.client.ttl('poc:job:' + payload['id']) == -1
            assert await b.client.get('poc:html_bytes') == '42'
            cancel = b.client.register_script(scripts.CANCEL)
            assert await cancel(args=[payload['id']]) == 1
            assert await cancel(args=[payload['id']]) == 0
            assert await b.client.get('poc:html_bytes') == '0'
            assert await b.client.ttl('poc:job:' + payload['id']) > 0
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_legacy_sweep_preserves_pending_and_leased_state_and_does_not_extend_ttl():
    async def run():
        b = make_backend()
        try:
            await b.client.hset('poc:states', mapping={'old-done': 'done', 'old-cancelled': 'cancelled', 'pending': 'pending', 'leased': 'leased:token'})
            await b.client.hset('poc:host:old.example.org', 'next', 10)
            maintain = b.client.register_script(scripts.MAINTAIN)
            await maintain()
            ttls = await b.client.execute_command('HTTL', 'poc:states', 'FIELDS', 4, 'old-done', 'old-cancelled', 'pending', 'leased')
            assert ttls[0] > 0 and ttls[1] > 0 and ttls[2:] == [-1, -1]
            await b.client.execute_command('HEXPIRE', 'poc:states', 10, 'FIELDS', 1, 'old-done')
            await maintain()
            assert 0 < (await b.client.execute_command('HTTL', 'poc:states', 'FIELDS', 1, 'old-done'))[0] <= 10
            assert await b.client.ttl('poc:host:old.example.org') > 0
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_missing_cache_uses_retry_path_before_http():
    async def run():
        crawler = SimpleNamespace(settings=Settings({'ROBOTSTXT_OBEY': True}), spider=SimpleNamespace())
        crawler.stats = StatsCollector(crawler)
        crawler.poc_frontier = SimpleNamespace(backend=SimpleNamespace(get_robots=AsyncMock(return_value=None)))
        middleware = SharedRobotsMiddleware.from_crawler(crawler)
        with pytest.raises(PermitExpired):
            await middleware.robot_parser(Request('https://example.org/a', meta={'poc_task': {'origin_id': 'x'}}))
    asyncio.run(run())


def test_large_insert_yields_between_eviction_batches_and_keeps_accounting():
    async def run():
        b = make_backend()
        try:
            await publish(b)
            robot = (await claim(b))[0]
            packed = pack_robots(response(random.Random(13).randbytes(2000)))
            # A large replacement may need more than 128 tiny entries evicted.
            for i in range(130):
                key = ROBOTS_BODY_PREFIX + str(i) + ':old'
                await b.client.set(key, 'x')
                await b.client.hset('poc:robots:sizes', key, 1)
                await b.client.zadd('poc:robots:lru', {key: i})
                await b.client.zadd('poc:robots:expiry', {key: 9999999999})
            await b.client.set('poc:robots:bytes', 130)
            args = [robot['token'], packed, 600, len(packed)+1, 200]
            assert await b._robots_store(args=args) == 2
            assert int(await b.client.get('poc:robots:bytes')) == 2
            assert await b._robots_store(args=args) == 1
            assert int(await b.client.get('poc:robots:bytes')) == len(packed)+1
            assert await b.client.zcard('poc:robots:lru') == 2
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_oversized_cache_insert_does_not_change_lease_or_ready_state():
    async def run():
        b = make_backend()
        try:
            await publish(b)
            robot = (await claim(b))[0]
            packed = pack_robots(response())
            with pytest.raises(Exception, match='exceeds cache byte budget'):
                await b._robots_store(args=[robot['token'], packed, 600, len(packed)-1, 10])
            assert json.loads(await b.client.hget(ROBOTS_CACHE, robot['origin_id']))['state'] == 'pending'
            assert await b.client.hget('poc:states', robot['id']) == 'leased:'+robot['token']
            assert await b.client.get('poc:robots:bytes') is None
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_ready_page_is_not_starved_by_later_cold_hosts():
    async def run():
        b = make_backend()
        try:
            await publish(b, urls=['https://example.org/a'])
            robot = (await claim(b))[0]
            await b.store_robots(robot, response())
            await b.ack(robot)
            await asyncio.sleep(.04)  # The blocked page's 30ms scheduling hint is due.
            await publish(b, 'b'*32, urls=[f'https://new{i}.example.org/a' for i in range(100)])
            tasks = (await b.claim('test', 1))['tasks']
            assert len(tasks) == 1
            assert tasks[0]['kind'] == 'url' and tasks[0]['group'] == 'example.org'
        finally:
            await b.client.aclose()
    asyncio.run(run())


def test_admission_budget_defers_job_without_partial_enqueue_and_retries():
    async def run():
        b = make_backend()
        try:
            first = await publish(b, urls=['https://example.org/a'])
            stamp = datetime.now(timezone.utc)
            second = dict(id='b'*32, created_at=stamp.isoformat(), expires=(stamp+timedelta(days=1)).timestamp(),
                tasks=tasks_for('b'*32, dict(urls=['https://other.example.org/a','https://other.example.org/b'])))
            enqueue=b.client.register_script(scripts.ENQUEUE)
            assert await enqueue(args=[json.dumps(second),2]) == -1
            assert await b.client.exists('poc:job:'+second['id']) == 0
            assert await b.client.llen('poc:q:other.example.org') == 0
            assert await b.client.hlen(ROBOTS_CACHE) == 1
            robot=(await claim(b))[0];await b.store_robots(robot,response());await b.ack(robot)
            await b.ack((await claim(b))[0])
            assert await enqueue(args=[json.dumps(second),2]) == 2
            assert await b.client.get('poc:pending') == '2'
        finally:
            await b.client.aclose()
    asyncio.run(run())
