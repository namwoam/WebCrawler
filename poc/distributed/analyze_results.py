"""Read committed metadata only; no new HTTP requests or DB writes."""
import collections,json,time
from pathlib import Path
from urllib.parse import urlsplit
from poc.distributed.app.backend import Backend
c=Backend().client
root=Path('/data/manual_bulk_crawls')
errors=collections.Counter();statuses=collections.Counter();counts=collections.Counter();byerror=collections.defaultdict(collections.Counter)
failed_samples=collections.defaultdict(list)
robot_origins=collections.Counter()
from poc.distributed.app.policy import routing
jobs=[]
for manifest in (root/'pending').glob('*.json'):
    job=manifest.stem;info=c.hgetall('poc:job:'+job)
    if info.get('status')=='cancelled':continue
    jobs.append(info)
    for path in (root/'results'/job/'crawler_00').glob('[0-9]*.json'):
        try:record=json.loads(path.read_text())
        except (FileNotFoundError,json.JSONDecodeError):continue
        counts['committed_records']+=1
        error=record.get('error');host=urlsplit(record['url']).hostname
        statuses[str(record.get('status_code'))]+=1
        if error:
            errors[error]+=1;byerror[error][host]+=1;counts['failed']+=1
            if error=='RobotsDenied':robot_origins[routing(record.get('final_url') or record['url'])['origin_id']]+=1
            if len(failed_samples[error])<3:failed_samples[error].append(record['url'])
        else:counts['success']+=1
        if record.get('html_file'):counts['html_files']+=1
robot_reasons=collections.Counter()
ids=list(robot_origins)
for offset in range(0,len(ids),256):
    batch=ids[offset:offset+256]
    values=c.hmget('poc:origins:v2',batch)
    for ident,raw in zip(batch,values):
        state=json.loads(raw or '{}').get('state','missing')
        reason='robots_fetch_failed_fail_closed' if state=='denied' else 'robots_rules_disallow' if state=='ready' else state
        robot_reasons[reason]+=robot_origins[ident]
summary=dict(robot_denial_breakdown=dict(robot_reasons),epoch=time.time(),jobs=len(jobs),completed_jobs=sum(j.get('status')=='completed' for j in jobs),
    counts=dict(counts),success_rate=counts['success']/max(1,counts['committed_records']),
    errors=errors.most_common(),statuses=statuses.most_common(),
    top_error_hosts={error:hosts.most_common(12) for error,hosts in byerror.items()},samples=dict(failed_samples))
print(json.dumps(summary,indent=2))
