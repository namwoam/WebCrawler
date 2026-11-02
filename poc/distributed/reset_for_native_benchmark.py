"""Remove prior benchmark results after preserving counts; run only with workers stopped."""
import json,shutil,time
from pathlib import Path
from poc.distributed.app.api import DistributedService,ROOT
s=DistributedService();c=s.backend.client
jobs=[p.stem for p in s.pending_dir.glob('*.json')]
assert set(jobs)<=set(ALLOWED_JOBS),'Unexpected job found; refusing cleanup'
p=c.pipeline(transaction=False)
for job in jobs:p.hgetall('poc:job:'+job)
info=p.execute()
before=shutil.disk_usage(ROOT)
counts={k:sum(int(j.get(k,0)) for j in info) for k in ('total','completed','succeeded','failed','html_files','html_bytes')}
for job in jobs:s.delete(job)
# Earlier archived raw results are also explicitly retired by this cleanup.
if (ROOT/'benchmarks').exists():shutil.rmtree(ROOT/'benchmarks')
assert not list(s.pending_dir.glob('*.json'))
assert not list(ROOT.rglob('*.html'))
keys=list(c.scan_iter(match='poc:*'))
for offset in range(0,len(keys),500):c.unlink(*keys[offset:offset+500])
c.set('poc:html_bytes',0)
after=shutil.disk_usage(ROOT)
print(json.dumps(dict(epoch=time.time(),retired_jobs=len(jobs),old_counts=counts,
    volume_used_before=before.used,volume_used_after=after.used,volume_free_after=after.free,
    remaining_html_files=0),indent=2))
