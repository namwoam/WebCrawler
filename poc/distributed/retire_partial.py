"""Archive evidence, then retire only jobs from the failed partial submission."""
import json,time,urllib.request
from pathlib import Path
root=Path('/output');run=root/'run-50';jobs=json.loads((run/'jobs.json').read_text())['jobs']
api='http://172.16.191.1:18090/v1/manual-bulk-crawls/'
statuses=[]
for job in jobs:
    with urllib.request.urlopen(api+job,timeout=60) as r:statuses.append(json.load(r))
(run/'partial-statuses.json').write_text(json.dumps(statuses,indent=2))
print('PARTIAL',len(jobs),'jobs',sum(s['total_urls'] for s in statuses),'URLs',sum(s['completed_urls'] for s in statuses),'completed',flush=True)
for job in jobs:
    with urllib.request.urlopen(urllib.request.Request(api+job,method='DELETE'),timeout=60) as r:assert r.status==204
run.rename(root/('invalid-partial-50-'+str(int(time.time()))))
print('PARTIAL_RETIRED',flush=True)
