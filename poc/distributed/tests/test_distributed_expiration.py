import json
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from poc.distributed.app import api


def service(tmp_path,monkeypatch):
    backend=SimpleNamespace(cancel=Mock(),maintain=Mock())
    monkeypatch.setattr(api,'ROOT',tmp_path)
    monkeypatch.setattr(api,'Backend',lambda:backend)
    return api.DistributedService(),backend


def test_expired_manifest_is_actually_retired_by_dispatcher(tmp_path,monkeypatch):
    s,b=service(tmp_path,monkeypatch)
    job=s.submit(['https://example.org/a'])
    path=s.pending_dir/(job+'.json');payload=json.loads(path.read_text());payload['expires_at']='2000-01-01T00:00:00+00:00'
    path.write_text(json.dumps(payload))
    s.scan()
    assert not path.exists()
    assert not (s.result_dir/job).exists()
    b.cancel.assert_called_once_with(args=[job])


def test_unknown_delete_does_not_cancel_unrelated_coordinator_state(tmp_path,monkeypatch):
    s,b=service(tmp_path,monkeypatch)
    with pytest.raises(FileNotFoundError):s.delete('f'*32)
    b.cancel.assert_not_called()


def test_full_frontier_keeps_durable_outbox_for_later_retry(tmp_path,monkeypatch):
    s,b=service(tmp_path,monkeypatch)
    b.client=SimpleNamespace(exists=Mock(return_value=False))
    b.publish=Mock(return_value=-1)
    s.progress=Mock()
    job=s.submit(['https://example.org/a'])
    s.scan()
    assert (s.pending_dir/(job+'.json')).exists()
    b.publish.assert_called_once()
    s.progress.assert_not_called()
    b.publish.return_value=1
    s.scan()
    s.progress.assert_called_once_with(job)
