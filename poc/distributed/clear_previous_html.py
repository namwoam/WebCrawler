"""Retire the prior benchmark and remove HTML only; preserve metadata/evidence."""
import json
from pathlib import Path
import shutil
import time
from poc.distributed.app.api import DistributedService, ROOT

service=DistributedService();client=service.backend.client
# Worker deployment must be fully stopped before running this maintenance script.
archive=ROOT/'benchmarks'/('100k-before-request-alignment-'+str(int(time.time())))
archive.mkdir(parents=True,exist_ok=False)
statuses=[]
for path in list(service.pending_dir.glob('*.json')):
    job=path.stem
    if job not in ALLOWED_JOBS:raise RuntimeError('Unexpected active job; refusing to retire it: '+job)
    with service.lock(job):
        statuses.append(dict(job_id=job,**client.hgetall('poc:job:'+job)))
        client.register_script(__import__('poc.distributed.app.scripts',fromlist=['CANCEL']).CANCEL)(args=[job])
        dest=archive/'pending';dest.mkdir(exist_ok=True)
        path.rename(dest/path.name)
        for partition in service.pending_dir.glob('crawler_*'):
            source=partition/path.name
            if source.exists():
                destination=dest/partition.name;destination.mkdir(exist_ok=True)
                source.rename(destination/source.name)
        result=service.result_dir/job
        if result.exists():
            dest=archive/'results';dest.mkdir(exist_ok=True)
            result.rename(dest/job)
(archive/'job-statuses.json').write_text(json.dumps(statuses,indent=2))
before=shutil.disk_usage(ROOT);removed=0;payload_bytes=0;scratch=0
for pattern in ('*.html','.upload-*'):
    for path in ROOT.rglob(pattern):
        if not path.is_file() and not path.is_symlink():continue
        if pattern=='*.html':removed+=1;payload_bytes+=path.stat().st_size
        else:scratch+=1
        path.unlink()
remaining=sum(1 for _ in ROOT.rglob('*.html'))
assert remaining==0
# All old manifests are retired and workers are stopped. Reset only POC keys.
# Keep valid outbound Retry-After cooldowns across benchmark resets.
keys=[key for key in client.scan_iter(match='poc:*')
      if not key.startswith('poc:host:') and key != 'poc:cooldowns']
for offset in range(0,len(keys),500):client.unlink(*keys[offset:offset+500])
client.set('poc:html_bytes',0)
after=shutil.disk_usage(ROOT)
summary=dict(retired_jobs=len(statuses),metadata_archive=str(archive),deleted_html_files=removed,
    deleted_html_bytes=payload_bytes,deleted_upload_scratch=scratch,remaining_html_files=remaining,
    volume_used_before=before.used,volume_used_after=after.used,volume_free_after=after.free,
    recorded_at=time.time())
(archive/'html-cleanup.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
