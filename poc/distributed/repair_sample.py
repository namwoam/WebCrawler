"""Verify canonical URLs against API validation; replace rejected rows from the DB."""
from collections import Counter
import hashlib,json
from pathlib import Path
from urllib.parse import urlsplit
import psycopg2
from libs.urls import validate_url
from poc.distributed.app.policy import routing
root=Path('/output');path=root/'db-100k-urls.txt'
old=path.read_bytes();(root/'db-100k-urls.initial.txt').write_bytes(old)
urls=[];seen=set();groups=Counter();removed=[]
def add(raw):
    try:
        url=validate_url(raw)
        if validate_url(url)!=url:return False
    except ValueError:return False
    group=routing(url)['group']
    if url in seen or groups[group]>=20:return False
    seen.add(url);groups[group]+=1;urls.append(url);return True
for index,raw in enumerate(old.decode().splitlines()):
    if not add(raw):removed.append(dict(index=index,url=raw))
conn=psycopg2.connect(host='172.16.191.2',port=5433,user='yugabyte',dbname='crawlerdb',application_name='crawler-poc-readonly-replacements')
conn.set_session(readonly=True,autocommit=True)
with conn.cursor() as cur:
    cur.execute('SET statement_timeout=15000')
    for shard in range(256):
        if len(urls)==100000:break
        cur.execute(f'SELECT url FROM url_state_current_{shard:03d} WHERE should_crawl=true LIMIT 2000 OFFSET 10000')
        for (raw,) in cur.fetchall():
            if len(urls)==100000:break
            add(raw)
conn.close()
assert len(urls)==100000 and len(seen)==100000
new=('\n'.join(urls)+'\n').encode();path.write_bytes(new)
hosts=Counter(urlsplit(u).hostname for u in urls)
summary=json.loads((root/'db-100k-sample.json').read_text())
summary.update(count=len(urls),hosts=len(hosts),politeness_groups=len(groups),max_urls_per_host=max(hosts.values()),max_urls_per_group=max(groups.values()),sha256=hashlib.sha256(new).hexdigest(),validation_replacements=removed,validation='Canonical output validated a second time against the same public API URL validator')
(root/'db-100k-sample.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
