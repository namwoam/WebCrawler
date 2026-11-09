"""Read-only URL sampling with host exclusions for cold-host crawler benchmarks."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
import time
from urllib.parse import urlsplit

import psycopg2
from libs.urls import validate_url


def hostname(url):
    return (urlsplit(url).hostname or '').lower().rstrip('.').encode('idna').decode('ascii')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=Path('/output'))
    parser.add_argument('--name',default='db-fresh-hosts-100k-20261108')
    parser.add_argument('--exclude-hosts',type=Path,required=True)
    parser.add_argument('--previous-dataset',type=Path,action='append',default=[])
    parser.add_argument('--count',type=int,default=100000)
    parser.add_argument('--max-per-host',type=int,default=5)
    parser.add_argument('--offset',type=int,default=30000)
    parser.add_argument('--rows',type=int,default=5000)
    parser.add_argument('--passes',type=int,default=5)
    parser.add_argument('--max-per-shard',type=int,default=1000)
    args=parser.parse_args()
    assert args.count>0 and args.max_per_host>0 and args.offset>=0 and args.rows>0 and args.passes>0 and args.max_per_shard>0
    destination=args.output/(args.name+'-urls.txt')
    assert not destination.exists(),'Dataset already exists; refusing overwrite'
    args.output.mkdir(parents=True,exist_ok=True)
    cached={h.strip().lower().rstrip('.') for h in args.exclude_hosts.read_text().splitlines() if h.strip()}
    previous=set()
    previous_hashes={}
    for path in args.previous_dataset:
        raw=path.read_bytes();previous_hashes[path.name]=hashlib.sha256(raw).hexdigest()
        for url in raw.decode().splitlines():
            try:previous.add(hostname(url))
            except ValueError:pass
    excluded=cached|previous
    selected=[];seen=set();hosts=Counter();rejections=Counter();errors=[];shards=Counter()
    started=time.monotonic();offset=args.offset;scanned=0
    print(json.dumps(dict(event='sampling_started',excluded_hosts=len(excluded),cached_hosts=len(cached),previous_hosts=len(previous))),flush=True)
    connection=psycopg2.connect(host='172.16.191.2',port=5433,user='yugabyte',dbname='crawlerdb',
        application_name='crawler-poc-fresh-host-readonly',connect_timeout=10)
    connection.set_session(readonly=True,autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout='15s'")
            for round_id in range(args.passes):
                limit=min(40000,args.rows*(2**round_id))
                for shard in range(256):
                    if len(selected)>=args.count:break
                    try:
                        cursor.execute(f'SELECT url FROM url_state_current_{shard:03d} WHERE should_crawl=true LIMIT %s OFFSET %s',(limit,offset))
                        rows=cursor.fetchall()
                    except psycopg2.Error as error:
                        errors.append(dict(shard=shard,offset=offset,error=type(error).__name__))
                        continue
                    accepted=0
                    for (raw,) in rows:
                        scanned+=1
                        try:raw_host=hostname(raw)
                        except ValueError:
                            rejections['invalid_host']+=1;continue
                        if raw_host in excluded:
                            rejections['excluded_host']+=1;continue
                        if hosts[raw_host]>=args.max_per_host:
                            rejections['host_cap']+=1;continue
                        try:
                            url=validate_url(raw)
                            if validate_url(url)!=url:raise ValueError('unstable canonical URL')
                            host=hostname(url)
                        except ValueError:
                            rejections['invalid_url']+=1;continue
                        if host in excluded or hosts[host]>=args.max_per_host or url in seen:
                            rejections['canonical_duplicate_or_excluded']+=1;continue
                        selected.append(url);seen.add(url);hosts[host]+=1;shards[shard]+=1;accepted+=1
                        if len(selected)>=args.count or accepted>=args.max_per_shard:break
                    if shard%8==0:
                        print(json.dumps(dict(event='sampling_progress',round=round_id,shard=shard,offset=offset,
                            selected=len(selected),hosts=len(hosts),scanned=scanned,elapsed=time.monotonic()-started)),flush=True)
                    time.sleep(.02)
                if len(selected)>=args.count:break
                offset+=limit
    finally:
        connection.close()
    random.Random(20261108).shuffle(selected)
    encoded=('\n'.join(selected)+'\n').encode()
    summary=dict(count=len(selected),distinct_hosts=len(hosts),max_urls_per_host=max(hosts.values(),default=0),
        overlap_with_cached_hosts=len(set(hosts)&cached),overlap_with_previous_hosts=len(set(hosts)&previous),
        excluded_host_count=len(excluded),cached_host_count=len(cached),previous_host_count=len(previous),
        previous_datasets=previous_hashes,excluded_hosts_sha256=hashlib.sha256(args.exclude_hosts.read_bytes()).hexdigest(),
        sha256=hashlib.sha256(encoded).hexdigest(),elapsed=time.monotonic()-started,scanned_rows=scanned,
        rejections=dict(rejections),errors=errors,shard_counts=dict(shards),
        created_at=datetime.now(timezone.utc).isoformat(),source='172.16.191.2:5433/crawlerdb/url_state_current_000..255; should_crawl; read-only',read_only=True,
        validation='Canonicalized twice with the public API URL validator; normalized hostname exclusions')
    (args.output/(args.name+'-sample.json')).write_text(json.dumps(summary,indent=2))
    if len(selected)!=args.count:
        (args.output/(args.name+'-partial.txt')).write_bytes(encoded)
        raise SystemExit(f'Insufficient fresh sample: {len(selected)} of {args.count}; see partial output')
    assert len(seen)==args.count and not (set(hosts)&excluded)
    temporary=destination.with_suffix('.tmp');temporary.write_bytes(encoded);temporary.replace(destination)
    print(json.dumps(summary),flush=True)


if __name__=='__main__':
    main()
