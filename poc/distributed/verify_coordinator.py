"""Run only against an empty, isolated Valkey DB 15."""
import asyncio
from datetime import datetime,timedelta,timezone
import json
import os

assert os.environ['VALKEY_URL'].endswith('/15')
os.environ['HOST_CONCURRENCY']='2'
from poc.distributed.app.backend import AsyncBackend,Backend

async def verify():
    b=AsyncBackend();sync=Backend();c=b.client
    assert await c.dbsize()==0,'Test DB is not empty; refusing to modify it'
    try:
        stamp=datetime.now(timezone.utc)
        payload=dict(urls=[f'http://atomic.poc.test/{i}' for i in range(20)],created_at=stamp.isoformat(),expires_at=(stamp+timedelta(days=1)).isoformat())
        assert sync.publish('atomic',payload)==20
        assert sync.publish('atomic',payload)==0
        r=await b.claim('robots',1);assert len(r['tasks'])==1
        robot=r['tasks'][0];assert robot['kind']=='robots'
        await b.store_robots(robot,dict(state='ready',has_parser=False,status_code=None))
        await b.ack(robot);await c.hset('poc:host:atomic.poc.test','next',0)
        responses=await asyncio.gather(*(b.claim(f'w{i}',8) for i in range(100)))
        tasks=[t for r in responses for t in r['tasks']]
        assert len(tasks)==2,len(tasks)
        assert len({t['token'] for t in tasks})==2
        for t in tasks:await b.release_network(t)
        assert await b.ack(tasks[0])==1
        assert await b.ack(tasks[0])==0
        await c.zadd('poc:lease_deadlines',{tasks[1]['token']:0})
        assert await b._recover()==1
        assert await c.hget('poc:states',tasks[1]['id'])=='pending'
        await c.hset('poc:host:atomic.poc.test',mapping=dict(next=0))
        for _ in range(100):
            result=await b.claim('drain',8)
            for t in result['tasks']:await b.ack(t)
            if int(await c.get('poc:pending') or 0)==0:break
            await asyncio.sleep(.04)
        job=await c.hgetall('poc:job:atomic')
        assert job['completed']=='20',job
        assert job['status']=='completed',job
        # Cancellation must release/discount both leased and still-queued URLs.
        assert sync.publish('cancel',payload)==20
        await c.hset('poc:host:atomic.poc.test','next',0)
        await c.zadd('poc:ready',{'atomic.poc.test':0})
        claimed=(await b.claim('cancel',8))['tasks'];assert claimed
        sync.cancel(args=['cancel'])
        for t in claimed:await b.ack(t)
        for _ in range(10):
            await asyncio.sleep(.04)
            await b.claim('cancel-drain',8)
        assert int(await c.get('poc:pending') or 0)==0
        assert await c.hlen('poc:leases')==0
        print('COORDINATOR_OK: 100 concurrent claimers, host cap, idempotent publish/ACK, crash recovery, cancellation',flush=True)
    finally:
        await c.flushdb();await c.aclose();sync.client.close()

asyncio.run(verify())
