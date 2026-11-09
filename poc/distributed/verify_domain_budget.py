"""Check bounded per-worker domain admission and shared-domain ownership on isolated DB15."""
import asyncio,json,os
from datetime import datetime,timedelta,timezone
assert os.environ['VALKEY_URL'].endswith('/15')
os.environ['HOST_QPS']='1000000';os.environ['HOST_CONCURRENCY']='16'
from poc.distributed.app.backend import Backend,AsyncBackend

async def main():
    sync=Backend();b=AsyncBackend();c=b.client
    assert await c.dbsize()==0
    try:
        stamp=datetime.now(timezone.utc)
        def payload(urls):return dict(urls=urls,created_at=stamp.isoformat(),expires_at=(stamp+timedelta(days=1)).isoformat())
        sync.publish('budget',payload([f'http://h{h:03d}.poc.test/{i}' for h in range(80) for i in range(3)]))
        owned=set();leases=[]
        for _ in range(200):
            result=await b.claim('worker-a',8,owned,30)
            for task in result['tasks']:
                if task['kind']=='robots':
                    await b.store_robots(task,dict(state='ready',has_parser=False,status_code=None))
                    await b.ack(task)
                else:
                    owned.add(task['group']);leases.append(task)
            assert len(owned)<=30
            if len(owned)==30:break
            await asyncio.sleep(.002)
        assert len(owned)==30,len(owned)
        await asyncio.sleep(.05)
        follow=(await b.claim('worker-a',8,owned,30))['tasks']
        assert follow,'Owned domains starved behind unowned domains'
        assert {t['group'] for t in follow}<=owned
        # A second worker may use the SAME domain; global timing remains shared.
        await c.flushdb()
        os.environ['HOST_QPS']='32'
        sync.publish('shared',payload(['http://shared.poc.test/a','http://shared.poc.test/b']))
        robot=(await b.claim('robots',1))['tasks'][0]
        await b.store_robots(robot,dict(state='ready',has_parser=False,status_code=None))
        await b.ack(robot)
        await asyncio.sleep(.04)
        first=(await b.claim('worker-a',1))['tasks'][0]
        assert (await b.start_network(first))['status']=='started'
        first_start=json.loads(await c.hget('poc:leases',first['token']))['network_started']
        await asyncio.sleep(.04)
        second=(await b.claim('worker-b',1))['tasks'][0]
        assert first['group']==second['group']=='shared.poc.test'
        assert first['worker']!=second['worker']
        assert (await b.start_network(second))['status']=='started'
        second_start=json.loads(await c.hget('poc:leases',second['token']))['network_started']
        assert second_start-first_start>=1/32
        print('DOMAIN_BUDGET_OK: max30 domains, no owned-domain starvation, same domain shared across workers, shared32QPS clock')
    finally:
        await c.flushdb();await c.aclose();sync.client.close()

asyncio.run(main())
