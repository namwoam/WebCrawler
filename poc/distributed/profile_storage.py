"""Read-only physical NIC and POC backing-store counters; run on node4."""
from pathlib import Path
import json,os,time

def disks():
    out={}
    for line in Path('/proc/diskstats').read_text().splitlines():
        fields=line.split();out[fields[2]]=[int(x) for x in fields[3:]]
    return out

def network():
    out={}
    for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
        name,fields=line.split(':',1);out[name.strip()]=[int(x) for x in fields.split()]
    return out

nics={}
for p in Path('/sys/class/net').glob('*'):
    try:
        if (p/'device').exists():nics[p.name]=int((p/'speed').read_text())
    except OSError:pass
loops=[]
for p in Path('/sys/block').glob('loop*'):
    try:
        if 'crawler-poc' in (p/'loop/backing_file').read_text():loops.append(p.name)
    except OSError:pass
st=os.stat('/data');device=Path('/sys/dev/block')/f'{os.major(st.st_dev)}:{os.minor(st.st_dev)}'
backing=device.resolve().name
names=set(loops+[backing]+[p.name for p in (device/'slaves').glob('*')])
start=time.time();d0=disks();n0=network();time.sleep(5);d1=disks();n1=network();dt=time.time()-start
out=dict(epoch=time.time(),seconds=dt,disks={},nics={})
for name in names:
    if name not in d1 or name not in d0:continue
    delta=[b-a for a,b in zip(d0[name],d1[name])]
    io=delta[0]+delta[4]
    out['disks'][name]=dict(read_iops=delta[0]/dt,write_iops=delta[4]/dt,
        read_MB_s=delta[2]*512/dt/1e6,write_MB_s=delta[6]*512/dt/1e6,
        average_io_ms=(delta[3]+delta[7])/max(io,1),busy_fraction=delta[9]/(dt*1000),average_queue=delta[10]/(dt*1000),
        flushes_s=delta[15]/dt if len(delta)>15 else None)
for name,speed in nics.items():
    delta=[b-a for a,b in zip(n0[name],n1[name])]
    out['nics'][name]=dict(speed_mbps=speed,rx_MB_s=delta[0]/dt/1e6,tx_MB_s=delta[8]/dt/1e6,
        rx_drops=delta[3],tx_drops=delta[11],rx_errors=delta[2],tx_errors=delta[10])
print(json.dumps(out,indent=2))
