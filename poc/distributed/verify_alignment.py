"""Verify production request identity and robots semantics over real HTTP workers."""
import json,time,urllib.request
from poc.distributed.app.request_profile import USER_AGENT,DEFAULT_REQUEST_HEADERS

api='http://crawler-api:8080/v1/manual-bulk-crawls'
def call(url,data=None,method=None):
    with urllib.request.urlopen(urllib.request.Request(url,data=json.dumps(data).encode() if data else None,
        headers={'Content-Type':'application/json'},method=method),timeout=30) as r:
        raw=r.read();return json.loads(raw) if raw else None

stamp=str(int(time.time()))
cases=[('robots403','/html',True),('robots500','/html',True),('robots429','/html',True),
       ('robotsrules403','/denied',False),('rules','/denied',False),('rules','/a.pdf',False),
       ('rules','/public/a.pdf',True),('rules','/chain/0',True),('rules','/refresh',True)]
urls=[f'http://{host}.{stamp}.poc.test:8081{path}' for host,path,success in cases]
job=call(api,dict(urls=urls,ttl_days=1))['job_id'];started=time.monotonic()
try:
    for _ in range(45):
        status=call(api+'/'+job)
        if status['status']=='completed':break
        time.sleep(2)
    assert status['completed_urls']==len(cases),status
    assert status['succeeded_urls']==sum(c[2] for c in cases),status
    assert status['failed_urls']==sum(not c[2] for c in cases),status
    stats=call('http://crawler-mock:8081/__stats')
    events=[e for e in stats['events'] if stamp in e['host']]
    for e in events:
        headers={k.lower():v for k,v in e['headers'].items()}
        assert headers['user-agent']==USER_AGENT,(e['host'],headers)
        for k,v in DEFAULT_REQUEST_HEADERS.items():assert headers[k.lower()]==v,(k,headers)
    assert len([e for e in events if e['host'].startswith('robots429.') and e['path']=='/robots.txt'])==2
    robots429=[e for e in events if e['host'].startswith('robots429.')]
    robots429.sort(key=lambda e:e['start'])
    assert robots429[-1]['path']=='/html'
    assert .02 <= robots429[-1]['start']-robots429[-2]['start'] < 5
    assert time.monotonic()-started < 25,'Unexpected added Retry-After wait'
    assert len([e for e in events if e['host'].startswith('robots500.') and e['path']=='/robots.txt'])==2
    assert len([e for e in events if e['path']=='/retry503'])==2
    assert all(e['path']!='/denied' and e['path']!='/a.pdf' for e in events)
    assert any(e['path']=='/refresh' for e in events)
    assert max(stats['peak'].values())<=16,stats['peak']
    assert time.monotonic()-started<80,'Crawl-delay extension was applied unlike production'
    print('ALIGNMENT_HTTP_OK',json.dumps(dict(status=status,elapsed=time.monotonic()-started,events=events)),flush=True)
finally:call(api+'/'+job,method='DELETE')
