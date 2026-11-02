from types import SimpleNamespace
from poc.distributed.app.metrics import sample_runtime


def test_runtime_separates_slot_queue_transfers_and_owned_tasks():
    slot = SimpleNamespace(active=[1,2,3], transferring=[1], queue=[2,3])
    crawler = SimpleNamespace(engine=SimpleNamespace(downloader=SimpleNamespace(slots={'host':slot})))
    # Owned tasks can include results uploading outside the downloader.
    assert sample_runtime(crawler, {'a':{},'b':{},'c':{},'upload':{}}) == dict(
        inflight=3, transferring=1, slot_queue=2, pending=4)
