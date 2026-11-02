"""Bounded read-only DB extraction; deterministic selection, no production writes."""
import collections
import hashlib
import json
import os
from pathlib import Path
import random
import time
from urllib.parse import urlsplit

import psycopg2
from libs.urls import validate_url
from poc.distributed.app.policy import routing

out=Path('/output');out.mkdir(exist_ok=True)
TARGET=int(os.getenv('SAMPLE_URL_COUNT','400000'))
NAME=os.getenv('DATASET_NAME','db-400k')
urls=[];seen=set();hosts=collections.Counter();groups=collections.Counter();errors=[]
started=time.time()
conn = psycopg2.connect(host='172.16.191.2',port=5433,user='yugabyte',dbname='crawlerdb',application_name='crawler-poc-readonly-sample')
conn.set_session(readonly=True,autocommit=True)
with conn.cursor() as cur:
    cur.execute("SET statement_timeout='15s'")
    # Domain-based DB shards are sampled round-robin to avoid one shard's heavy hosts.
    for limit,offset in [(1200,0),(4000,1200),(8000,5200),(16000,13200)]:
        for shard in range(256):
            if len(urls)>=TARGET: break
            try:
                cur.execute(f'SELECT url FROM url_state_current_{shard:03d} WHERE should_crawl=true LIMIT %s OFFSET %s',(limit,offset))
                rows=cur.fetchall()
            except psycopg2.Error as e:
                conn.rollback();errors.append(dict(shard=shard,error=type(e).__name__));continue
            for (raw,) in rows:
                if len(urls)>=TARGET:break
                # Avoid expensive canonicalization for hosts already at the cap.
                try:
                    raw_host=(urlsplit(raw).hostname or '').lower().rstrip('.')
                except ValueError:
                    continue
                raw_group='wikipedia.org' if raw_host=='wikipedia.org' or raw_host.endswith('.wikipedia.org') else raw_host
                if raw_host.isascii() and groups[raw_group]>=20:continue
                try:
                    url=validate_url(raw)
                    if validate_url(url)!=url:continue
                except ValueError:continue
                host=urlsplit(url).hostname
                group=routing(url)["group"]
                if url in seen or groups[group]>=20:continue
                seen.add(url);hosts[host]+=1;groups[group]+=1;urls.append(url)
            if shard%16==0:
                print(json.dumps(dict(shard=shard,urls=len(urls),hosts=len(hosts),elapsed=time.time()-started)),flush=True)
        if len(urls)>=TARGET:break
random.Random(20261102).shuffle(urls)
raw=('\n'.join(urls)+'\n').encode();(out/(NAME+'-urls.txt')).write_bytes(raw)
summary=dict(count=len(urls),hosts=len(hosts),max_urls_per_host=max(hosts.values(),default=0),politeness_groups=len(groups),max_urls_per_group=max(groups.values(),default=0),
    sha256=hashlib.sha256(raw).hexdigest(),elapsed=time.time()-started,errors=errors,
    source='172.16.191.2:5433/crawlerdb/url_state_current_000..255; should_crawl',
    top_hosts=hosts.most_common(20))
(out/(NAME+'-sample.json')).write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
if len(urls)!=TARGET:raise SystemExit('Insufficient sample')
