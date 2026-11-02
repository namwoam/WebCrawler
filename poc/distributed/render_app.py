"""Render the API-compatible distributed crawler deployment."""
import json
import hashlib
import os
from pathlib import Path

NS = 'crawler-poc'
IMAGE = os.getenv('POC_IMAGE', 'docker.io/crawler-poc/app:0.11')
items = []

def add(kind, name, spec, api='v1'):
    items.append(dict(apiVersion=api, kind=kind, metadata=dict(name=name, namespace=NS,
        labels={'app.kubernetes.io/part-of':NS}), spec=spec))

def service(name, port):
    add('Service',name,dict(selector=dict(app=name),ports=[dict(port=port,targetPort=port)]))

def deploy(name, container, replicas=1, volume=False, worker=False):
    pod = dict(containers=[container], terminationGracePeriodSeconds=120)
    meta = dict(labels=dict(app=name),annotations={'prometheus.io/scrape':'true'})
    if volume:
        pod['volumes']=[dict(name='data',persistentVolumeClaim=dict(claimName='crawler-poc-html'))]
        pod['initContainers']=[dict(name='prepare',image=IMAGE,imagePullPolicy='IfNotPresent',
            command=['python','-c',"from pathlib import Path; [(Path('/data')/d).mkdir(exist_ok=True) for d in ('crawl','valkey')]"],
            volumeMounts=[dict(name='data',mountPath='/data')],
            resources=dict(requests=dict(cpu='50m',memory='32Mi'),limits=dict(cpu='250m',memory='64Mi')))]
    if worker:
        pod['topologySpreadConstraints']=[dict(maxSkew=1,minDomains=3,topologyKey='kubernetes.io/hostname',
            whenUnsatisfiable='DoNotSchedule',labelSelector=dict(matchLabels=dict(app=name)))]
        pod['volumes']=[dict(name='tmp',emptyDir=dict(sizeLimit='2Gi'))]
        container['volumeMounts']=[dict(name='tmp',mountPath='/tmp')]
    add('Deployment',name,dict(replicas=replicas,strategy=dict(type='Recreate') if volume else dict(type='RollingUpdate',rollingUpdate=dict(maxSurge=0,maxUnavailable=1)),
        selector=dict(matchLabels=dict(app=name)),template=dict(metadata=meta,spec=pod)), 'apps/v1')

def resources(cpu,mem,limit_cpu,limit_mem):
    return dict(requests=dict(cpu=cpu,memory=mem),limits=dict(cpu=limit_cpu,memory=limit_mem))

common=[dict(name='VALKEY_URL',value='redis://crawler-valkey:6379/0'),
        dict(name='WORKER_TOKEN',valueFrom=dict(secretKeyRef=dict(name='crawler-poc-worker',key='token')))]

deploy('crawler-valkey',dict(name='valkey',image='valkey/valkey:9.1.3',
    command=['valkey-server','--bind','0.0.0.0','--protected-mode','no','--appendonly','yes',
             '--appendfsync','everysec','--dir','/data','--maxmemory','3072mb','--maxmemory-policy','noeviction'],
    resources=resources('500m','512Mi','2','4Gi'),ports=[dict(name='valkey',containerPort=6379)],
    volumeMounts=[dict(name='data',mountPath='/data',subPath='valkey')],
    readinessProbe=dict(exec=dict(command=['valkey-cli','ping']),initialDelaySeconds=2)),volume=True)
service('crawler-valkey',6379)

deploy('crawler-api',dict(name='api',image=IMAGE,imagePullPolicy='IfNotPresent',
    command=['python','-m','uvicorn','poc.distributed.app.api:create_app','--factory','--host','0.0.0.0','--port','8080','--workers','8','--no-access-log'],
    env=common+[dict(name='CRAWL_DATA',value='/data/manual_bulk_crawls')],
    resources=resources('4','1Gi','12','4Gi'),ports=[dict(name='metrics',containerPort=8080)],
    volumeMounts=[dict(name='data',mountPath='/data',subPath='crawl')],
    readinessProbe=dict(httpGet=dict(path='/healthz',port=8080),initialDelaySeconds=3)),volume=True)
service('crawler-api',8080)

deploy('crawler-worker',dict(name='worker',image=IMAGE,imagePullPolicy='IfNotPresent',
    env=common+[dict(name='POD_NAME',valueFrom=dict(fieldRef=dict(fieldPath='metadata.name'))),
                dict(name='NODE_NAME',valueFrom=dict(fieldRef=dict(fieldPath='spec.nodeName'))),
                dict(name='CRAWLER_API_URL',value='http://crawler-api:8080'),
                dict(name='LOKI_URL',value=os.getenv('LOKI_URL','http://172.16.191.2:3100')),
                dict(name='MOCK_DNS_TARGET',value='crawler-mock.crawler-poc.svc.cluster.local'),
                dict(name='CRAWLER_CONCURRENT_REQUESTS',value='256'),
                dict(name='WORKER_MAX_DOMAINS',value='30'),
                dict(name='CRAWLER_USE_AUTOTHROTTLE',value='true')],
    resources=resources('1','512Mi','4','2Gi'),ports=[dict(name='metrics',containerPort=9101)],
    readinessProbe=dict(httpGet=dict(path='/',port=9101),initialDelaySeconds=3)),replicas=int(os.getenv('WORKERS','100')),worker=True)

mock_source=Path(__file__).with_name('app').joinpath('mock.py').read_text()
items.append(dict(apiVersion='v1',kind='ConfigMap',metadata=dict(name='crawler-mock-source',namespace=NS),data={'mock.py':mock_source}))
deploy('crawler-mock',dict(name='mock',image=IMAGE,imagePullPolicy='IfNotPresent',
    command=['python','-m','poc.distributed.app.mock'],
    resources=resources('100m','64Mi','1','256Mi'),ports=[dict(name='http',containerPort=8081)]))
mock_pod=items[-1]['spec']['template']
mock_pod['metadata']['annotations']['poc.mock-source-sha256']=hashlib.sha256(mock_source.encode()).hexdigest()
mock_pod['spec']['volumes']=[dict(name='source',configMap=dict(name='crawler-mock-source'))]
mock_pod['spec']['containers'][0]['volumeMounts']=[dict(name='source',mountPath='/app/poc/distributed/app/mock.py',subPath='mock.py')]
service('crawler-mock',8081)
Path(__file__).with_name('k8s').joinpath('app.json').write_text(json.dumps(dict(apiVersion='v1',kind='List',items=items),indent=2))
print('Rendered app deployments')
