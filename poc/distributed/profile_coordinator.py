"""Read-only Valkey command latency, main-thread CPU, memory, and persistence samples."""
import argparse
import json
import os
from pathlib import Path
import time
import shutil
import sys
import traceback

# This observer is not an API worker and must not create multiprocess metric files.
os.environ.pop('PROMETHEUS_MULTIPROC_DIR',None)
os.environ['CRAWLER_PROFILE_ENABLED']='false'
from poc.distributed.app.backend import Backend


def process_and_network():
    lines=Path('/proc/net/snmp').read_text().splitlines()
    tcp={}
    for header,values in zip(lines[::2],lines[1::2]):
        if header.startswith('Tcp:'):
            tcp=dict(zip(header.split()[1:],map(int,values.split()[1:])))
    processes=[]
    for path in Path('/proc').glob('[0-9]*'):
        try:
            if not (path/'comm').read_text().startswith('python') or int(path.name)==os.getpid():continue
            fields=(path/'stat').read_text().rsplit(')',1)[1].split()
            processes.append(dict(pid=int(path.name),utime_ticks=int(fields[11]),stime_ticks=int(fields[12]),
                threads=int(fields[17]),start_ticks=int(fields[19])))
        except (OSError,ValueError):pass
    return dict(api_tcp=tcp,api_processes=processes,clock_ticks=os.sysconf('SC_CLK_TCK'))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--seconds',type=float,default=900)
    parser.add_argument('--interval',type=float,default=5)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
    output=args.output.open('a') if args.output else sys.stdout
    client=Backend().client
    started=time.monotonic()
    try:
        while time.monotonic()-started<args.seconds:
            tick=time.monotonic()
            try:
                snapshot=dict(epoch=time.time(),pending=client.get('poc:pending'),
                    leases=client.hlen('poc:leases'),ready_hosts=client.zcard('poc:ready'),
                    cpu=client.info('cpu'),commands=client.info('commandstats'),
                    command_latency=client.info('latencystats'),memory=client.info('memory'),
                    persistence=client.info('persistence'),errors=client.info('errorstats'))
                snapshot.update(process_and_network())
                storage=shutil.disk_usage('/data')
                filesystem=os.statvfs('/data')
                snapshot['storage']=dict(total_bytes=storage.total,used_bytes=storage.used,
                    available_bytes=storage.free,available_inodes=filesystem.f_favail)
                snapshot['robots_cache']=dict(bytes=int(client.get('poc:robots:bytes') or 0),
                    entries=client.zcard('poc:robots:lru'),events=client.hgetall('poc:robots:stats'))
                snapshot['slowlog']=[dict(id=row['id'],time=row['start_time'],duration_us=row['duration'],
                    command_name=row['command'].split()[0].decode(errors='replace') if isinstance(row['command'],bytes) else row['command'].split()[0])
                    for row in client.slowlog_get(5)]
                print(json.dumps(snapshot),file=output,flush=True)
            except Exception:
                traceback.print_exc(file=sys.stderr)
            time.sleep(max(.1,args.interval-(time.monotonic()-tick)))
    finally:
        client.close()
        if args.output:output.close()


if __name__=='__main__':
    main()
