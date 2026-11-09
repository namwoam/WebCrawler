import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from poc.distributed.app import profiling


def test_profiled_upload_slot_is_released_on_failure_and_cancelled_wait(monkeypatch):
    monkeypatch.setattr(profiling,'ENABLED',True)
    async def run():
        semaphore=asyncio.Semaphore(1)
        with pytest.raises(RuntimeError):
            async with profiling.upload_slot(semaphore):
                raise RuntimeError('upload failed')
        await semaphore.acquire()
        async def waiting():
            async with profiling.upload_slot(semaphore):
                pytest.fail('Cancelled waiter acquired a slot')
        task=asyncio.create_task(waiting())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        semaphore.release()
        assert semaphore._value==1
        assert profiling.WAITERS._value.get()==0
    asyncio.run(run())


def test_metadata_profile_preserves_json_and_atomic_replacement(tmp_path,monkeypatch):
    calls=[]
    monkeypatch.setattr(profiling.os,'fsync',lambda fd:calls.append(fd))
    target=tmp_path/'result.json'
    target.write_text('old')
    value={'url':'https://example.org/台灣','error':None}
    profiling.write_metadata(target,value)
    assert json.loads(target.read_text())==value
    assert len(calls)==1
    assert not target.with_suffix('.json.tmp').exists()


def test_api_stage_metrics_aggregate_multiple_processes(tmp_path):
    env={**os.environ,'PROMETHEUS_MULTIPROC_DIR':str(tmp_path),'CRAWLER_PROFILE_ENABLED':'true'}
    command='from poc.distributed.app.profiling import observe; observe("api","test_commit",.125)'
    for _ in range(2):
        subprocess.run([sys.executable,'-c',command],env=env,check=True,capture_output=True)
    code='''from prometheus_client import CollectorRegistry,multiprocess
r=CollectorRegistry();multiprocess.MultiProcessCollector(r)
assert r.get_sample_value('crawler_profile_stage_seconds_count',{'component':'api','stage':'test_commit'})==2
assert r.get_sample_value('crawler_profile_stage_seconds_sum',{'component':'api','stage':'test_commit'})==.25
'''
    subprocess.run([sys.executable,'-c',code],env=env,check=True,capture_output=True)


def test_profiled_result_route_preserves_request_injection_and_auth(monkeypatch):
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from poc.distributed.app import api
    monkeypatch.setattr(api,'DistributedService',lambda:SimpleNamespace(lease=lambda token:None))
    monkeypatch.setenv('WORKER_TOKEN','test-worker-token')
    client=TestClient(api.create_app(),raise_server_exceptions=True)
    try:
        assert client.post('/_worker/results/token').status_code==403
        response=client.post('/_worker/results/token',headers={'Authorization':'Bearer test-worker-token'})
        assert response.status_code==200
        assert response.json()=={'accepted':False,'reason':'lease_finished'}
    finally:
        client.close()


def test_operational_snapshots_do_not_accumulate_in_multiprocess_mode(tmp_path):
    env={**os.environ,'PROMETHEUS_MULTIPROC_DIR':str(tmp_path),'CRAWLER_PROFILE_ENABLED':'true'}
    code='''from types import SimpleNamespace
from prometheus_client import CollectorRegistry,generate_latest,Counter
from prometheus_client.parser import text_string_to_metric_families
from poc.distributed.app.api import OperationalMetrics,ProfileMetrics
# Simulate old exporter entries left in a process metrics directory.
Counter('crawler_urls_completed','old', ['outcome']).labels('success').inc(999)
client=SimpleNamespace(hgetall=lambda key:{'completed_success':42,'completed_error':7},
    get=lambda key:0,zcard=lambda key:0,zcount=lambda *args:0)
for _ in range(3):
 r=CollectorRegistry();r.register(ProfileMetrics());r.register(OperationalMetrics(client))
 samples=[s for f in text_string_to_metric_families(generate_latest(r).decode()) for s in f.samples if s.name=='crawler_urls_completed_total']
 assert len(samples)==2
 assert {s.labels['outcome']:s.value for s in samples}=={'success':42,'error':7}
'''
    subprocess.run([sys.executable,'-c',code],env=env,check=True,capture_output=True)
