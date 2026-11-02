"""Check concurrent commits for one job before live benchmarking."""
import json,time,urllib.request
API='http://crawler-api:8080/v1/manual-bulk-crawls'
def req(url,data=None,method=None):
    with urllib.request.urlopen(urllib.request.Request(url,data=json.dumps(data).encode() if data else None,
        headers={'Content-Type':'application/json'},method=method),timeout=30) as r:
        raw=r.read();return json.loads(raw) if raw else None
stamp=str(int(time.time()))
urls=[f'http://load-{h}.{stamp}.poc.test:8081/html/{i}?delay=0.02' for h in range(100) for i in range(5)]
job=req(API,dict(urls=urls,ttl_days=1))['job_id'];started=time.monotonic()
try:
    for _ in range(40):
        status=req(API+'/'+job)
        if status['status']=='completed':break
        time.sleep(2)
    assert status['completed_urls']==500 and status['succeeded_urls']==500,status
    files=req(API+'/'+job+'/results')['files']
    assert len([f for f in files if f['name'].endswith('.html')])==500
    print('CONCURRENT_COMMIT_OK',json.dumps(dict(elapsed=time.monotonic()-started,status=status)),flush=True)
finally:req(API+'/'+job,method='DELETE')
