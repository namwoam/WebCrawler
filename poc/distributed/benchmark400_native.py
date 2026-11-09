"""400k sustained-load benchmark; count commits atomically and sample throughout submission."""
QUERIES = {'http_qps': 'sum(rate(crawler_http_requests_total[1m]))', 'http_total': 'sum(crawler_http_requests_total)', 'http_status_qps': 'sum by(status,kind,outcome)(rate(crawler_http_requests_total[1m]))', 'commits_qps': 'sum(rate(crawler_urls_completed_total[1m]))', 'html_success_qps': 'sum(rate(crawler_urls_completed_total{outcome="success"}[1m]))', 'active_downloads': 'sum(crawler_active_downloads)', 'active_uploads': 'sum(crawler_active_uploads)', 'worker_cpu': 'sum by(node)(rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container="worker",pod=~"crawler-worker-.*"}[1m]))', 'worker_cpu_per_pod': 'sum by(pod,node)(rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container="worker",pod=~"crawler-worker-.*"}[1m]))', 'worker_memory': 'sum by(node)(container_memory_working_set_bytes{namespace="crawler-poc",container="worker",pod=~"crawler-worker-.*"})', 'poc_container_cpu': 'sum by(node,container)(rate(container_cpu_usage_seconds_total{namespace="crawler-poc",container!="",container!="POD"}[1m]))', 'cpu_throttle_ratio': 'sum by(container)(rate(container_cpu_cfs_throttled_periods_total{namespace="crawler-poc",container!="",container!="POD"}[1m])) / sum by(container)(rate(container_cpu_cfs_periods_total{namespace="crawler-poc",container!="",container!="POD"}[1m]))', 'download_p95': 'histogram_quantile(0.95,sum by(le)(rate(crawler_http_request_duration_seconds_bucket[1m])))', 'upload_p95': 'histogram_quantile(0.95,sum by(le)(rate(crawler_result_upload_duration_seconds_bucket[1m])))', 'pending': 'crawler_pending_urls', 'ready_hosts': 'crawler_ready_hosts', 'cooldown_hosts': 'crawler_cooldown_hosts', 'html_bytes': 'crawler_html_bytes_stored', 'scrape_health': 'up{job="crawler-poc"}'}
import concurrent.futures
from datetime import datetime,timezone
import hashlib,json,os
from pathlib import Path
import threading,time
import urllib.parse,urllib.request,urllib.error
from prometheus_client.parser import text_string_to_metric_families

QUERIES['worker_domain_count']='max(crawler_worker_active_domains)'
QUERIES['autothrottle_workers']='sum(crawler_autothrottle_enabled)'
QUERIES['poc_memory_per_container']='sum by(node,container)(container_memory_working_set_bytes{namespace="crawler-poc",container!="",container!="POD"})'
QUERIES['profile_stage_mean']='sum by(component,stage)(rate(crawler_profile_stage_seconds_sum[30s])) / clamp_min(sum by(component,stage)(rate(crawler_profile_stage_seconds_count[30s])),0.000001)'
QUERIES['profile_stage_p95']='histogram_quantile(.95,sum by(component,stage,le)(rate(crawler_profile_stage_seconds_bucket[30s])))'
QUERIES['profile_stage_occupancy']='sum by(component,stage)(rate(crawler_profile_stage_seconds_sum[30s]))'
QUERIES['profile_stage_sum']='sum by(component,stage)(crawler_profile_stage_seconds_sum)'
QUERIES['profile_stage_count']='sum by(component,stage)(crawler_profile_stage_seconds_count)'
QUERIES['profile_stage_buckets']='sum by(component,stage,le)(crawler_profile_stage_seconds_bucket)'
QUERIES['profile_loop_p95']='histogram_quantile(.95,sum by(component,le)(rate(crawler_profile_event_loop_delay_seconds_bucket[30s])))'
QUERIES['upload_waiters']='sum(crawler_profile_upload_waiters)'
QUERIES['upload_bytes_per_second']='sum by(component)(rate(crawler_profile_bytes_total[30s]))'
QUERIES['manifest_cache_misses_per_second']='sum(rate(crawler_profile_manifest_cache_misses_total[30s]))'
QUERIES['dispatcher_scans_inflight']='sum(crawler_profile_dispatcher_scans_inflight)'
QUERIES['robots_cache_bytes']='crawler_robots_cache_bytes'
QUERIES['robots_cache_entries']='crawler_robots_cache_entries'
QUERIES['robots_cache_event_rates']='sum by(event)(rate(crawler_robots_cache_events_total[1m]))'
ROOT=Path(os.getenv('BENCH_OUTPUT','/output'))
RUN=ROOT/os.getenv('BENCH_RUN','run-400k-native-autothrottle30')
API=os.getenv('BENCH_API','http://172.16.191.1:18090')
TARGET=int(os.getenv('BENCH_URL_COUNT','400000'))
BATCH=int(os.getenv('BENCH_BATCH_SIZE','500'))
DATASET=os.getenv('BENCH_DATASET','db-400k-urls.txt')
INTERVAL=float(os.getenv('BENCH_SAMPLE_SECONDS','15'))
assert TARGET>0 and BATCH>0 and INTERVAL>=1
SUBMITTED=0;JOBS=[];COMPLETE=threading.Event();STOP=threading.Event()


def request(path,payload=None):
    req=urllib.request.Request(API+path,data=json.dumps(payload).encode() if payload is not None else None,
        headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=90) as r:return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'HTTP {e.code}: {e.read().decode()}') from e


def commits():
    with urllib.request.urlopen(API+'/metrics',timeout=20) as r:
        text=r.read().decode()
    result={'success':0,'error':0}
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name=='crawler_urls_completed_total':result[s.labels['outcome']]=int(s.value)
            elif s.name=='crawler_html_bytes_stored':result['html_bytes']=int(s.value)
            elif s.name=='crawler_pending_urls':result['pending']=int(s.value)
    return result


def query(expr):
    url=API+'/prometheus/api/v1/query?'+urllib.parse.urlencode(dict(query=expr))
    with urllib.request.urlopen(url,timeout=12) as r:return json.load(r)['data']['result']


def write(name,value):
    temporary=RUN/(name+'.tmp')
    temporary.write_text(json.dumps(value,indent=2));temporary.replace(RUN/name)


def snapshot(started,baseline,previous):
    c=commits();epoch=time.time();elapsed=time.monotonic()-started
    counts=dict(succeeded_urls=c['success']-baseline['success'],failed_urls=c['error']-baseline['error'])
    counts['completed_urls']=counts['succeeded_urls']+counts['failed_urls']
    data=dict(time=datetime.now(timezone.utc).isoformat(),epoch=epoch,elapsed=elapsed,
        submitted=SUBMITTED,job_count=len(JOBS),counts=counts,html_bytes=c['html_bytes'],
        backend_pending=c['pending'],metrics={})
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures={executor.submit(query,expr):name for name,expr in QUERIES.items()}
        for future in concurrent.futures.as_completed(futures):
            try:data['metrics'][futures[future]]=future.result()
            except Exception as error:data['metrics'][futures[future]]=dict(error=str(error))
    if previous:
        dt=epoch-previous['epoch']
        data['window_qps']={key:(value-previous['counts'][key])/dt for key,value in counts.items()}
    data['success_fraction']=counts['succeeded_urls']/max(1,counts['completed_urls'])
    write('latest.json',data)
    with (RUN/'samples.jsonl').open('a') as f:f.write(json.dumps(data)+'\n')
    print(json.dumps({key:data[key] for key in ('time','elapsed','submitted','counts','html_bytes','success_fraction')} |
        dict(window_qps=data.get('window_qps'),http_qps=data['metrics']['http_qps'],
        active_downloads=data['metrics']['active_downloads'],active_uploads=data['metrics']['active_uploads'])),flush=True)
    if SUBMITTED==TARGET and counts['completed_urls']==TARGET:COMPLETE.set()
    return data


def sampler(started,baseline):
    previous=None
    while not STOP.is_set():
        tick=time.monotonic()
        try:previous=snapshot(started,baseline,previous)
        except Exception as error:print('SAMPLE_ERROR',repr(error),flush=True)
        if COMPLETE.is_set():break
        STOP.wait(max(1,INTERVAL-(time.monotonic()-tick)))


def main():
    global SUBMITTED
    RUN.mkdir(exist_ok=True)
    assert not (RUN/'jobs.json').exists(),'Existing benchmark; refusing duplicate submission'
    raw=(ROOT/DATASET).read_bytes();urls=raw.decode().splitlines()
    assert len(urls)==TARGET and len(set(urls))==TARGET
    baseline=commits();assert baseline['html_bytes']==0,'Stored HTML was not cleared before this run'
    started=time.monotonic()
    metadata=dict(started_at=datetime.now(timezone.utc).isoformat(),started_epoch=time.time(),urls=TARGET,
        workers=int(os.getenv('BENCH_WORKERS','100')),urls_per_job=BATCH,worker_cpu_limit=4,api_cpu_limit=12,api_processes=8,
        node_poc_cpu_limit=40,pvc_gib=int(os.getenv("BENCH_PVC_GIB","250")),valkey_memory_limit_gib=float(os.getenv("BENCH_VALKEY_MEMORY_GIB","8")),valkey_maxmemory_gib=float(os.getenv("BENCH_VALKEY_MAXMEMORY_GIB","6")),image=os.getenv('BENCH_IMAGE','docker.io/crawler-poc/app:0.11'),worker_memory_limit_gib=2,worker_domain_limit=30,autothrottle=True,autothrottle_target=2,host_qps=32,host_concurrency=16,loki_enabled=os.getenv('BENCH_LOKI_ENABLED','true').lower()=='true',
        transport='Scrapy2.19.0 Twisted26.4.0 HTTP11',request_profile='Native Scrapy Spider/Scheduler/DownloaderMiddleware/Callbacks/ItemPipeline; Chrome121 headers; DNS/download15s',
        robots='production Protego, every HTTP response parsed, download failure has no parser; no Crawl-delay/Request-rate extension',
        scheduling='global per-host32QPS at HTTP handler start; concurrency16 reservations held through download; task/network deadline180s; outer download bound min(30s,remaining-5s-roundtrip);30activeDomains/worker; nativeAutoThrottle target2,minDelay.25,start.5,max10; native retries/redirects/robots get new global permits; noextraRetryAftercooldown',
        sha256=hashlib.sha256(raw).hexdigest(),baseline=baseline)
    metadata.update(dataset=DATASET,sample_seconds=INTERVAL,profiling=os.getenv('CRAWLER_PROFILE_ENABLED','').lower() in ('1','true','yes'))
    metadata.update(api_image=os.getenv('BENCH_API_IMAGE',metadata['image']),
        valkey_io_threads=int(os.getenv('BENCH_VALKEY_IO_THREADS','1')),
        valkey_cpu_limit=int(os.getenv('BENCH_VALKEY_CPU','2')))
    metadata['claim_batch_limit']=int(os.getenv('BENCH_CLAIM_BATCH_LIMIT','16'))
    metadata['retention_enabled']=os.getenv('BENCH_RETENTION_ENABLED','false').lower()=='true'
    if metadata['retention_enabled']:
        metadata['retention']=dict(robots_cache_max_bytes=int(os.getenv("BENCH_ROBOTS_CACHE_MAX_BYTES",str(3*1024**3))),robots_cache_max_entries=int(os.getenv("BENCH_ROBOTS_CACHE_MAX_ENTRIES","500000")),
            robots_ttl_max_seconds=86400,failed_robots_ttl_max_seconds=300,terminal_state_ttl_seconds=3600,max_pending_urls=100000,idle_host_ttl_seconds=600)
    if metadata['profiling']:
        metadata['profile_baseline']={key:query(QUERIES[key]) for key in ('profile_stage_sum','profile_stage_count','profile_stage_buckets')}
    write('metadata.json',metadata)
    metadata['node_memory_limit_gib']=48
    write('metadata.json',metadata)
    thread=threading.Thread(target=sampler,args=(started,baseline),daemon=True);thread.start()
    try:
        for offset in range(0,TARGET,BATCH):
            batch_urls=urls[offset:offset+BATCH]
            job=request('/v1/manual-bulk-crawls',dict(urls=batch_urls,ttl_days=1))['job_id']
            JOBS.append(job);SUBMITTED+=len(batch_urls)
            write('jobs.json',dict(jobs=JOBS,submitted=SUBMITTED))
            if len(JOBS)%100==0:print('SUBMITTED',SUBMITTED,'jobs',len(JOBS),'elapsed',round(time.monotonic()-started,1),flush=True)
        metadata['submission_seconds']=time.monotonic()-started;metadata['last_submitted_at']=datetime.now(timezone.utc).isoformat()
        write('metadata.json',metadata)
        while not COMPLETE.wait(10):pass
        thread.join(timeout=30)
        statuses=[request('/v1/manual-bulk-crawls/'+job) for job in JOBS]
        # Progress files are asynchronous. Refresh only lagging jobs rather
        # than treating scan latency as lost committed results on large runs.
        deadline=time.monotonic()+180
        while any(s['completed_urls']<s['total_urls'] for s in statuses) and time.monotonic()<deadline:
            time.sleep(2)
            for index,status in enumerate(statuses):
                if status['completed_urls']<status['total_urls']:
                    statuses[index]=request('/v1/manual-bulk-crawls/'+JOBS[index])
        assert sum(s['completed_urls'] for s in statuses)==TARGET
        write('final.json',dict(metadata=metadata,statuses=statuses,
            summary=json.loads((RUN/'latest.json').read_text())))
        print('BENCHMARK_COMPLETE',flush=True)
    finally:STOP.set()

if __name__=='__main__':main()
