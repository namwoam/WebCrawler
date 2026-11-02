"""Read-only scheduling snapshot for throughput bottleneck analysis."""
import collections,json,time
from poc.distributed.app.backend import Backend
c=Backend().client
now=time.time()
ready=c.zrange('poc:ready',0,-1,withscores=True)
p=c.pipeline(transaction=False)
for g,due in ready:p.hgetall('poc:host:'+g);p.llen('poc:q:'+g);p.zcard('poc:net:'+g);p.lindex('poc:q:'+g,0)
values=p.execute();rows=[]
for i,(g,due) in enumerate(ready):
    host,n,net,raw=values[i*4:i*4+4];task=json.loads(raw or '{}')
    rows.append(dict(host=g,queued=n,active_network_leases=net,due_in=max(0,due-now),
        robots_interval=float(host.get('robots_interval',0)),cooldown_in=max(0,float(host.get('cooldown',0))-now),
        next_in=max(0,float(host.get('next',0))-now),head_kind=task.get('kind'),head_attempt=task.get('attempt'),
        head_delay=max(0,float(task.get('not_before',0))-now),origin_id=task.get('origin_id')))
p=c.pipeline(transaction=False)
for row in rows:p.hget('poc:origins:v2',row['origin_id'] or '')
origins=p.execute()
for row,raw in zip(rows,origins):row['origin_state']=json.loads(raw or '{}').get('state');row.pop('origin_id')
leases=[json.loads(v) for v in c.hvals('poc:leases')]
summary=dict(epoch=now,pending=int(c.get('poc:pending') or 0),stats=c.hgetall('poc:stats'),
    ready_hosts=len(rows),queued=sum(r['queued'] for r in rows),leases=len(leases),
    lease_kinds=collections.Counter(t['kind'] for t in leases),
    robots_delayed_hosts=sum(r['robots_interval']>0 for r in rows),
    cooldown_hosts=sum(r['cooldown_in']>0 for r in rows),
    blocked_origins=sum(r['origin_state']=='pending' for r in rows),
    top_queued=sorted(rows,key=lambda r:r['queued'],reverse=True)[:25],
    longest_robots_delay=sorted(rows,key=lambda r:r['robots_interval'],reverse=True)[:20],
    oldest_leases=sorted([dict(id=t['id'],kind=t['kind'],group=t['group'],age=now-t['granted_at'],attempt=t['attempt'],worker=t['worker']) for t in leases],key=lambda t:t['age'],reverse=True)[:15])
print(json.dumps(summary,indent=2))
