"""Exercise real Valkey expiry/refetch/eviction on an empty, isolated DB 15."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os

assert os.environ['VALKEY_URL'].endswith('/15'), 'Use isolated DB 15'
from poc.distributed.app.backend import Backend, AsyncBackend
from poc.distributed.app.retention import ROBOTS_CACHE, ROBOTS_BODY_PREFIX, pack_robots
from poc.distributed.app.request_profile import robots_result, robots_allowed


async def verify():
    b = AsyncBackend(); sync = Backend(); c = b.client
    assert await c.dbsize() == 0, 'Test DB is not empty; refusing to modify it'
    def payload(host, count=2):
        stamp = datetime.now(timezone.utc)
        return dict(urls=[f'http://{host}/{i}' for i in range(count)],
            created_at=stamp.isoformat(), expires_at=(stamp+timedelta(days=1)).isoformat())
    async def claim():
        for _ in range(100):
            tasks = (await b.claim('retention-test', 1))['tasks']
            if tasks:
                return tasks[0]
            await asyncio.sleep(.04)
        raise AssertionError('No task admitted after cache refresh')
    def packed(body):
        return pack_robots(robots_result(dict(status_code=200, error=None,
            final_url='http://retention.poc.test/robots.txt', fetched_at='test'), body))
    async def store(task, body, ttl=600, entries=100):
        return await b._robots_store(args=[task['token'], packed(body), ttl, 1000000, entries])
    def key(task):
        return ROBOTS_BODY_PREFIX+task['origin_id']+':'+task['robots_generation']
    try:
        sync.publish('retention-a', payload('retention.poc.test'))
        robot = await claim(); assert robot['kind'] == 'robots'
        assert await store(robot, b'User-agent: *\nDisallow: /0\n', ttl=2) == 1
        await b.ack(robot)
        page = await claim(); assert page['kind'] == 'url'
        assert not robots_allowed(await b.get_robots(page), 'http://retention.poc.test/0')
        await asyncio.sleep(2.2)
        assert await b.get_robots(page) is None
        assert await c.hget(ROBOTS_CACHE, page['origin_id']) is None
        await b.requeue(page)
        sync.publish('retention-b', payload('retention.poc.test'))
        queued = [json.loads(v) for v in await c.lrange('poc:q:retention.poc.test', 0, -1)]
        assert sum(t['kind'] == 'robots' for t in queued) == 1
        refresh = await claim(); assert refresh['kind'] == 'robots'
        assert refresh['robots_generation'] != robot['robots_generation']
        assert await store(refresh, b'User-agent: *\nDisallow: /1\n') == 1
        assert await store(robot, b'User-agent: *\nDisallow: /\n') == 0
        await b.ack(refresh)
        for _ in range(4):
            t = await claim(); assert t['kind'] == 'url'
            assert robots_allowed(await b.get_robots(t), 'http://retention.poc.test/0')
            assert not robots_allowed(await b.get_robots(t), 'http://retention.poc.test/1')
            await b.ack(t)
            assert await b.ack(t) == 0
        assert await c.get('poc:pending') == '0'
        assert (await c.execute_command('HTTL','poc:states','FIELDS',1,t['id']))[0] > 0
        await c.set('protected-scheduler-state', 'keep')
        sync.publish('retention-c', payload('other.poc.test', 1))
        other = await claim(); assert other['kind'] == 'robots'
        assert await store(other, b'User-agent: *\n', entries=1) == 1
        await b.ack(other)
        assert await c.get(key(refresh)) is None
        assert await c.get('protected-scheduler-state') == 'keep'
        assert await c.zcard('poc:robots:lru') == 1
        t = await claim(); assert t['kind'] == 'url'; await b.ack(t)
        sync.publish('retention-d', payload('retention.poc.test', 1))
        again = await claim(); assert again['kind'] == 'robots'
        assert again['robots_generation'] != refresh['robots_generation']
        assert await store(again, b'User-agent: *\n') == 1
        await b.ack(again)
        await b.ack(await claim())
        assert await c.get('poc:pending') == '0' and await c.hlen('poc:leases') == 0
        print('RETENTION_OK: real TTL expiry, active-job refetch, one refresh per origin, stale-writer fencing, LRU eviction, scheduler isolation, terminal-state TTL, idempotent ACK',flush=True)
    finally:
        await c.flushdb(); await c.aclose(); sync.client.close()


if __name__ == '__main__':
    asyncio.run(verify())
