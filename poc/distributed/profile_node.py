"""Read-only physical storage and NIC sampling on a POC host."""
import argparse
import json
import os
from pathlib import Path
import time


def disks():
    rows={}
    for line in Path('/proc/diskstats').read_text().splitlines():
        fields=line.split()
        rows[fields[2]]=[int(x) for x in fields[3:]]
    return rows


def network():
    rows={}
    for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
        name,fields=line.split(':',1)
        rows[name.strip()]=[int(x) for x in fields.split()]
    return rows


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--data-path',required=True)
    parser.add_argument('--seconds',type=float,default=900)
    parser.add_argument('--interval',type=float,default=5)
    args=parser.parse_args()
    names=set()
    def add_device(path):
        stat=os.stat(path)
        device=Path('/sys/dev/block')/f'{os.major(stat.st_dev)}:{os.minor(stat.st_dev)}'
        add_block(device.resolve())
    def add_block(device):
        if device.name in names:return
        names.add(device.name)
        for slave in (device/'slaves').glob('*'):
            add_block(slave.resolve())
        parent=device.parent
        if (device/'partition').exists():
            add_block(parent)
        backing=device/'loop/backing_file'
        if backing.exists():
            try:add_device('/'+backing.read_text().strip().lstrip('/'))
            except OSError:pass
    add_device(args.data_path)
    # The loop filesystem may be mounted only inside the nested K3s namespace.
    for path in Path('/sys/class/block').glob('loop*'):
        try:
            if 'crawler-poc' in (path/'loop/backing_file').read_text():
                add_block(path.resolve())
        except OSError:pass
    nics={}
    for path in Path('/sys/class/net').glob('*'):
        try:
            if (path/'device').exists():nics[path.name]=int((path/'speed').read_text())
        except OSError:pass
    started=time.monotonic();previous=started;d0=disks();n0=network()
    while time.monotonic()-started<args.seconds:
        time.sleep(args.interval)
        now=time.monotonic();dt=now-previous;d1=disks();n1=network()
        result=dict(epoch=time.time(),seconds=dt,disks={},nics={})
        for name in names:
            if name not in d0 or name not in d1:continue
            delta=[b-a for a,b in zip(d0[name],d1[name])];ios=delta[0]+delta[4]
            result['disks'][name]=dict(read_iops=delta[0]/dt,write_iops=delta[4]/dt,
                read_MB_s=delta[2]*512/dt/1e6,write_MB_s=delta[6]*512/dt/1e6,
                average_io_ms=(delta[3]+delta[7])/max(1,ios),busy_fraction=delta[9]/(1000*dt),
                average_queue=delta[10]/(1000*dt),flushes_s=delta[15]/dt if len(delta)>15 else None)
        for name,speed in nics.items():
            if name not in n0 or name not in n1:continue
            delta=[b-a for a,b in zip(n0[name],n1[name])]
            result['nics'][name]=dict(speed_mbps=speed,rx_MB_s=delta[0]/dt/1e6,
                tx_MB_s=delta[8]/dt/1e6,rx_drops=delta[3],tx_drops=delta[11])
        print(json.dumps(result),flush=True)
        d0,n0,previous=d1,n1,now


if __name__=='__main__':
    main()
