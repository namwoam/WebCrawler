"""Post-run result audit: exact failure counts, robots classification and final URLs."""
import base64
import collections
from datetime import datetime
import heapq,json,time
from pathlib import Path
from poc.distributed.app.backend import Backend
from poc.distributed.app.policy import routing
from poc.distributed.app.request_profile import ROBOTS_CACHE,parse_robots,USER_AGENT

root=Path('/data/manual_bulk_crawls');c=Backend().client
errors=collections.Counter();statuses=collections.Counter();totals=collections.Counter()
by_error_host=collections.defaultdict(collections.Counter);ignored=collections.defaultdict(list)
last=[];samples=collections.defaultdict(list)
jobs=[p.stem for p in (root/'pending').glob('*.json')]
for job in jobs:
 for p in (root/'results'/job/'crawler_00').glob('[0-9]*.json'):
  r=json.loads(p.read_text());error=r.get('error');totals['records']+=1
  totals['failed' if error else 'success']+=1;statuses[str(r.get('status_code'))]+=1
  totals['html_files']+=int(r.get('html_file') is not None)
  if error:
   errors[error]+=1;by_error_host[error][routing(r['url'])['group']]+=1
   if len(samples[error])<3:samples[error].append(r['url'])
   if error=='IgnoreRequest':ignored[routing(r['final_url'])['origin_id']].append(r['final_url'])
  row=dict(url=r['url'],final_url=r['final_url'],status_code=r.get('status_code'),error=error,fetched_at=r.get('fetched_at'))
  heapq.heappush(last,(p.stat().st_mtime_ns,p.name,job,row))
  if len(last)>10:heapq.heappop(last)
robot_reasons=collections.Counter();ids=list(ignored)
for start in range(0,len(ids),128):
 batch=ids[start:start+128];raws=c.hmget(ROBOTS_CACHE,batch)
 for oid,raw in zip(batch,raws):
  origin=json.loads(raw or '{}');urls=ignored[oid]
  if not origin.get('has_parser'):
   robot_reasons['IgnoreRequest_not_from_robots_rules_or_unknown_cache']+=len(urls);continue
  parser=parse_robots(base64.b64decode(origin['body_b64']))
  for url in urls:robot_reasons['robots_rules_disallow' if not parser.allowed(url,USER_AGENT) else 'other_IgnoreRequest_redirect_limit_or_filter']+=1
out=dict(epoch=time.time(),jobs=len(jobs),totals=dict(totals),error_rate=totals['failed']/max(1,totals['records']),
 errors=errors.most_common(),statuses=statuses.most_common(),ignore_request_breakdown=dict(robot_reasons),
 top_hosts={e:h.most_common(10) for e,h in by_error_host.items()},sample_urls=dict(samples),
 last_completed=[x[3] for x in sorted(last,reverse=True)])
print(json.dumps(out,ensure_ascii=False,indent=2))
