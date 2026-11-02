import os
import socket
import json
import logging
from prometheus_client import Counter, Gauge, Histogram
WORKER = os.getenv("POD_NAME", socket.gethostname())
NODE = os.getenv("NODE_NAME", "local")
CAPACITY = int(os.getenv("CRAWLER_CONCURRENT_REQUESTS", "256"))
MAX_BYTES = int(os.getenv("MAX_DOWNLOAD_BYTES", str(10 * 1024 * 1024)))
API = os.getenv("CRAWLER_API_URL", "http://crawler-api:8080")

LABELS = {"worker": WORKER, "node": NODE}
REQUESTS = Counter("crawler_http_requests", "Actual outbound HTTP attempts", ["worker", "node", "status", "outcome", "kind"])
DOWNLOADS = Gauge("crawler_active_downloads", "Actual active downloads", ["worker", "node"]).labels(**LABELS)
UPLOADS = Gauge("crawler_active_uploads", "Active result uploads", ["worker", "node"]).labels(**LABELS)
DURATION = Histogram("crawler_http_request_duration_seconds", "Full response download time", ["worker", "node"], buckets=(.05,.1,.25,.5,1,2,5,10,20,30)).labels(**LABELS)
UPLOAD_TIME = Histogram("crawler_result_upload_duration_seconds", "Result upload and durable commit", ["worker", "node"], buckets=(.01,.025,.05,.1,.25,.5,1,2,5,10,30,60)).labels(**LABELS)



ACTIVE_DOMAINS = Gauge("crawler_worker_active_domains", "Domains currently held by this worker", ["worker","node"]).labels(**LABELS)
AUTOTHROTTLE = Gauge("crawler_autothrottle_enabled", "Native Scrapy AutoThrottle extension is enabled", ["worker","node"]).labels(**LABELS)

INFLIGHT = Gauge('crawler_worker_inflight', 'Requests active in Scrapy downloader slots', ['worker','node']).labels(**LABELS)
PENDING = Gauge('crawler_worker_pending', 'Leased tasks awaiting completion, including result uploads', ['worker','node']).labels(**LABELS)
TRANSFERRING = Gauge('crawler_worker_transferring', 'Scrapy transferring requests, including network admission waits', ['worker','node']).labels(**LABELS)
SLOT_QUEUE = Gauge('crawler_worker_slot_queue', 'Requests queued inside Scrapy downloader slots', ['worker','node']).labels(**LABELS)

# Domain names belong in log bodies, rather than unbounded Prometheus labels.
telemetry = logging.getLogger('crawler.poc.telemetry')
telemetry.setLevel(logging.INFO)
telemetry.propagate = False
if not telemetry.handlers:
    telemetry.addHandler(logging.StreamHandler())


def emit(event, *, level=logging.INFO, **fields):
    telemetry.log(level, json.dumps(dict(service='crawler-poc', worker_id=WORKER,
        node=NODE, event=event, level=logging.getLevelName(level), **fields)))


def sample_runtime(crawler, leases):
    slots = crawler.engine.downloader.slots.values()
    values = dict(inflight=0, transferring=0, slot_queue=0, pending=len(leases))
    for slot in slots:
        values['inflight'] += len(slot.active)
        values['transferring'] += len(slot.transferring)
        values['slot_queue'] += len(slot.queue)
    for key, gauge in [('inflight', INFLIGHT), ('pending', PENDING),
                       ('transferring', TRANSFERRING), ('slot_queue', SLOT_QUEUE)]:
        gauge.set(values[key])
    return values
