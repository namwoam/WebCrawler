"""Mirror the live crawler view onto the POC dashboard without editing production."""
import base64
import copy
import json
import os
from pathlib import Path
import urllib.request

PROM = {'type':'prometheus','uid':'crawler-poc-prometheus'}
LOKI = {'type':'loki','uid':'loki'}


def build_dashboard(source, target):
    dashboard = copy.deepcopy(source)
    dashboard.update(id=target['id'],uid=target['uid'],title=target['title'],
        version=target['version'],refresh='15s',editable=True,
        tags=['crawler-poc','kubernetes','loki'],time={'from':'now-30m','to':'now'},
        description='Live crawler layout for the isolated POC. Request starts, domain QPS, HTTP outcomes and exact download quantiles come from POC worker Loki events. Scrapy internals and infrastructure use POC Prometheus. HTTP metrics include robots, redirects and retries; URL completion metrics count final job outcomes. Worker Loki telemetry begins with image 0.10.')
    gauges = {6:('crawler_worker_inflight','Requests active in Scrapy slots, including their local queues.'),
        7:('crawler_worker_pending','Tasks leased by each worker until completion, including result uploads. This is ownership backlog, not just the local downloader queue.'),
        8:('crawler_worker_active_domains','Distinct hosts represented in the worker frontier; configured limit 30.'),
        9:('crawler_worker_transferring','Scrapy transferring requests, including time waiting at the shared start gate. The infrastructure section separately shows actual active HTTP downloads.'),
        10:('crawler_worker_slot_queue','Requests queued in local Scrapy downloader slots before entering the HTTP handler.')}
    for panel in dashboard['panels']:
        if panel['type']=='row':
            continue
        if panel['id'] in gauges:
            metric, description = gauges[panel['id']]
            panel['datasource'] = PROM
            panel['description'] = description
            panel['targets'] = [dict(datasource=PROM,refId='A',
                expr=f'avg by(worker) (avg_over_time({metric}{{worker=~"$worker_id"}}[1m]))',
                legendFormat='{{worker}}')]
        else:
            panel['datasource'] = LOKI
            for query in panel.get('targets',[]):
                query['datasource'] = LOKI
                query['expr'] = query['expr'].replace('service="crawler"','service="crawler-poc"')
            if panel['id']==2:
                # A minute across this corpus exceeds shared Loki's 10k series
                # limit. A five-second instant top-k stays bounded under load.
                panel.update(type='bargauge',title='QPS per domain (top 10, last 5s)',
                    description='Current top ten domains by HTTP starts per second over the last five seconds. This is a live snapshot; other request charts use one-minute windows.')
                panel.pop('interval',None)
                panel.pop('maxDataPoints',None)
                panel['fieldConfig']['defaults'].pop('custom',None)
                panel['options'] = dict(orientation='horizontal',displayMode='gradient',
                    showUnfilled=True,reduceOptions=dict(calcs=['lastNotNull'],fields='',values=False))
                panel['targets'][0].update(queryType='instant',
                    expr='topk(10, sum by(domain)(count_over_time({service="crawler-poc",worker_id=~"$worker_id",event="request.start"} | json domain="domain" [5s])) / 5)')
            if panel['id']==5:
                panel['targets'][0]['expr'] = '{service="crawler-poc",worker_id=~"$worker_id",level=~"ERROR|WARNING"}'
            if panel['id']==3:
                panel['description'] = 'HTTP attempts with status below 400 divided by all completed HTTP attempts, including network failures. Includes robots, redirects and retries; differs from final successful HTML per submitted URL.'
                selector = '{service="crawler-poc",worker_id=~"$worker_id",event="request.end"}'
                failures = '{service="crawler-poc",worker_id=~"$worker_id",event="request.fail"}'
                panel['targets'][0]['expr'] = (
                    f'(sum(rate({selector} | json | status < 400 [5m])) or vector(0)) / '
                    f'((sum(rate({selector}[5m])) or vector(0)) + '
                    f'(sum(rate({failures} | json | fail_reason !~ "HttpError.*" [5m])) or vector(0)))')
    dashboard['templating'] = {'list':[dict(name='worker_id',label='Worker',type='query',
        datasource=PROM,definition='label_values(crawler_worker_active_domains, worker)',
        query={'query':'label_values(crawler_worker_active_domains, worker)','refId':'StandardVariableQuery'},
        includeAll=True,allValue='.*',multi=True,refresh=2,sort=1,options=[],
        current={'selected':False,'text':'All','value':'$__all'})]}
    dashboard['panels'].append(dict(id=900,type='row',title='POC infrastructure and URL outcomes',
        collapsed=False,gridPos=dict(x=0,y=46,w=24,h=1)))
    already_mirrored = any(p['id']==900 for p in target['panels'])
    for old in target['panels']:
        if already_mirrored and old['id']<1000:
            continue
        panel = copy.deepcopy(old)
        if not already_mirrored:
            panel['id'] += 1000
            panel['gridPos']['y'] += 47
        dashboard['panels'].append(panel)
    return dashboard


def main():
    base = os.getenv('GRAFANA_URL','http://172.16.191.2:3000').rstrip('/')
    auth = 'Basic '+base64.b64encode((os.getenv('GRAFANA_USER','admin')+':'+os.environ['GRAFANA_PASSWORD']).encode()).decode()
    def request(path, payload=None):
        req = urllib.request.Request(base+path,data=json.dumps(payload).encode() if payload is not None else None,
            headers={'Authorization':auth,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=30) as response:
            return json.load(response)
    source = request('/api/dashboards/uid/crawler-live')
    target = request('/api/dashboards/uid/crawler-poc')
    output = Path(os.getenv('GRAFANA_ARTIFACTS','/tmp/crawler-poc-dashboard'))
    output.mkdir(parents=True,exist_ok=True)
    (output/'source-before.json').write_text(json.dumps(source,indent=2))
    (output/'target-before.json').write_text(json.dumps(target,indent=2))
    dashboard = build_dashboard(source['dashboard'],target['dashboard'])
    (output/'target-proposed.json').write_text(json.dumps(dashboard,indent=2))
    reply = request('/api/dashboards/db',dict(dashboard=dashboard,
        folderUid=target['meta'].get('folderUid',''),overwrite=False,
        message='Mirror live crawler panels using isolated POC Loki events and Prometheus metrics'))
    saved = request('/api/dashboards/uid/crawler-poc')
    assert saved['dashboard']['version']==reply['version']
    assert request('/api/dashboards/uid/crawler-live')['dashboard']==source['dashboard']
    (output/'target-after.json').write_text(json.dumps(saved,indent=2))
    print(json.dumps(dict(url=base+reply['url'],version=reply['version'],
        panels=len(saved['dashboard']['panels']),source_unchanged=True)))


if __name__=='__main__':
    main()
