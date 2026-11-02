"""Submit one 100k-URL workload, record throughput and resource evidence until done."""
import collections
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.error
import urllib.request

ROOT=Path('/output');API=os.getenv('BENCH_API','http://172.16.191.1:18090')
PROM=API+'/prometheus/api/v1/query'
RUN=ROOT/'run-50';RUN.mkdir(exist_ok=True)

def request(path,payload=None):
    req=urllib.request.Request(API+path,data=json.dumps(payload).encode() if payload is not None else None,
        headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=90) as r:return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'HTTP {e.code}: {e.read().decode()}') from e

def query(expr):
    with urllib.request.urlopen(PROM+'?'+urllib.parse.urlencode(dict(query=expr)),timeout=15) as r:
        return json.load(r)['data']['result']

QUERIES={
 'http_qps':'sum(rate(crawler_http_requests_total[1m]))',
 'http_total':'sum(crawler_http_requests_total)',
 'http_status_qps':'sum by(status,kind,outcome)(rate(crawler_http_requests_total[1m]))',
 'commits_qps':'sum(rate(crawler_urls_completed_total[1m]))',
 'html_success_qps':'sum(rate(crawler_urls_completed_total{outcome="success"}[1m]))',
 'active_downloads':'sum(crawler_active_downloads)',
 'active_uploads':'sum(crawler_active_uploads)',
 'worker_cpu':'sum by(node)(rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container="worker",pod=~"crawler-worker-.*"}[1m]))',
 'worker_cpu_per_pod':'sum by(pod,node)(rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container="worker",pod=~"crawler-worker-.*"}[1m]))',
 'worker_memory':'sum by(node)(container_memory_working_set_bytes{namespace="crawler-poc",container="worker",pod=~"crawler-worker-.*"})',
 'poc_container_cpu':'sum by(node,container)(rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container!="",container!="POD"}[1m]))',
 'cpu_throttle_ratio':'sum by(container)(rate(container_cpu_cfs_throttled_periods_total{namespace="crawler-poc",container!="",container!="POD"}[1m])) / sum by(container)(rate(container_cpu_cfs_periods_total{namespace="crawler-poc",container!="",container!="POD"}[1m]))',
 'download_p95':'histogram_quantile(0.95,sum by(le)(rate(crawler_http_request_duration_seconds_bucket[1m])))',
 'upload_p95':'histogram_quantile(0.95,sum by(le)(rate(crawler_result_upload_duration_seconds_bucket[1m])))',
 'pending':'crawler_pending_urls',
 'ready_hosts':'crawler_ready_hosts',
 'cooldown_hosts':'crawler_cooldown_hosts',
 'html_bytes':'crawler_html_bytes_stored',
 'scrape_health':'up{job="crawler-poc"}',
}

def sample(jobs,started,submitted):
    statuses=[]
    for job in jobs:statuses.append(request('/v1/manual-bulk-crawls/'+job))
    sums={k:sum(s[k] for s in statuses) for k in ('total_urls','completed_urls','succeeded_urls','failed_urls','html_files')}
    data=dict(time=datetime.now(timezone.utc).isoformat(),epoch=time.time(),elapsed=time.monotonic()-started,
        submitted=submitted,counts=sums,jobs=collections.Counter(s['status'] for s in statuses),metrics={})
    for key,expr in QUERIES.items():
        try:data['metrics'][key]=query(expr)
        except Exception as e:data['metrics'][key]=dict(error=str(e))
    (RUN/'latest.json').write_text(json.dumps(data,indent=2))
    with (RUN/'samples.jsonl').open('a') as f:f.write(json.dumps(data)+'\n')
    print(json.dumps(dict(time=data['time'],elapsed=round(data['elapsed'],1),counts=sums,jobs=data['jobs'],
        http_qps=data['metrics']['http_qps'],active_downloads=data['metrics']['active_downloads'],
        active_uploads=data['metrics']['active_uploads'])),flush=True)
    return data,statuses

assert not (RUN/'jobs.json').exists(),'Run already exists; refusing to submit duplicate workload'
raw=(ROOT/'db-100k-urls.txt').read_bytes();urls=raw.decode().splitlines()
assert len(urls)==100000 and len(set(urls))==100000
started=time.monotonic();jobs=[];submitted=0
metadata=dict(started_at=datetime.now(timezone.utc).isoformat(),workers=50,urls=100000,urls_per_job=500,
    sha256=hashlib.sha256(raw).hexdigest(),worker_cpu_request=1,worker_cpu_limit=4,node_poc_cpu_limit=40,
    host_qps=2,host_concurrency=2,wikipedia_group_qps=1,transport='Scrapy 2.19.0 Twisted 26.4.0 HTTP11; image app:0.5; upload concurrency 8/worker',
    baseline_metrics={k:query(v) for k,v in QUERIES.items()})
(RUN/'metadata.json').write_text(json.dumps(metadata,indent=2))
for offset in range(0,len(urls),500):
    job=request('/v1/manual-bulk-crawls',dict(urls=urls[offset:offset+500],ttl_days=1))['job_id']
    jobs.append(job);submitted+=500
    (RUN/'jobs.json').write_text(json.dumps(dict(jobs=jobs,submitted=submitted),indent=2))
    if len(jobs)%20==0:print('SUBMITTED',submitted,'jobs',len(jobs),'elapsed',round(time.monotonic()-started,1),flush=True)
metadata['submission_seconds']=time.monotonic()-started
metadata['last_submitted_at']=datetime.now(timezone.utc).isoformat()
(RUN/'metadata.json').write_text(json.dumps(metadata,indent=2))
previous=None
while True:
    stamp=time.monotonic();data,statuses=sample(jobs,started,submitted)
    if previous:
        dt=data['epoch']-previous['epoch']
        print('WINDOW',json.dumps({k:(data['counts'][k]-previous['counts'][k])/dt for k in ('completed_urls','succeeded_urls','failed_urls','html_files')}),flush=True)
    previous=data
    if data['counts']['completed_urls']==100000:
        (RUN/'final.json').write_text(json.dumps(dict(summary=data,statuses=statuses,metadata=metadata),indent=2))
        print('BENCHMARK_COMPLETE',flush=True);break
    time.sleep(max(1,30-(time.monotonic()-stamp)))
