"""Exercise the deployed public API and inspect actual cross-worker mock requests."""
import json
import time
import urllib.request
import urllib.error
from pathlib import Path

API='http://crawler-api:8080/v1/manual-bulk-crawls'

def call(url, payload=None, method=None):
    req=urllib.request.Request(url,data=json.dumps(payload).encode() if payload is not None else None,
        headers={'Content-Type':'application/json'},method=method)
    with urllib.request.urlopen(req,timeout=20) as r:
        body=r.read();return r.status,json.loads(body) if body else None

suffix=str(int(time.time()))
urls=[f'http://h{h}.{suffix}.poc.test:8081/html/{i}?delay=1.2' for i in range(6) for h in range(4)]
urls+=[f'http://h0.{suffix}.poc.test:8081/denied',f'http://h1.{suffix}.poc.test:8081/throttle',f'http://h2.{suffix}.poc.test:8081/redirect']
code,created=call(API,dict(urls=urls,ttl_days=1));assert code==200
job=created['job_id'];print('job',job,flush=True)
try:
    deadline=time.monotonic()+100
    while time.monotonic()<deadline:
        _,status=call(API+'/'+job)
        print(json.dumps(status),flush=True)
        if status['status']=='completed':break
        time.sleep(3)
    assert status['status']=='completed',status
    assert status['completed_urls']==27,status
    assert status['succeeded_urls']==26,status
    assert status['failed_urls']==1,status
    _,stats=call('http://crawler-mock:8081/__stats')
    stats['events']=[x for x in stats['events'] if suffix in x['host'] or x['host'].startswith('target.')]
    assert max(stats['peak'].values())<=16,stats['peak']
    denied=[x for x in stats['events'] if x['path']=='/denied'];assert not denied
    throttle=[x for x in stats['events'] if x['path']=='/throttle'];assert len(throttle)==2
    assert throttle[1]['start']-throttle[0]['start']>=.02
    assert any(x['host'].startswith('target.') for x in stats['events'])
    workers={x['worker'] for x in stats['events']};nodes={x['node'] for x in stats['events']}
    assert len(workers)>1,workers
    # Result list/download and cancellation retain the original API contract.
    _,listing=call(API+'/'+job+'/results');print('results',json.dumps(listing)[:600],flush=True)
    html=next(x for x in listing['files'] if x['name'].endswith('.html'))
    with urllib.request.urlopen('http://crawler-api:8080'+html['download_url'],timeout=10) as r:
        assert b'crawler mock' in r.read()
    print('INTEGRATION_OK',json.dumps(dict(job=job,status=status,workers=sorted(workers),nodes=sorted(nodes),stats=stats)),flush=True)
finally:
    assert call(API+'/'+job,method='DELETE')[0]==204
    try:call(API+'/'+job)
    except urllib.error.HTTPError as e:assert e.code==404
    else:raise AssertionError('Deleted job still visible')
