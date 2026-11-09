"""Reuse the public API unchanged; add a durable outbox and internal result receiver."""
import asyncio
import base64
import contextlib
import fcntl
from functools import lru_cache
import re
import json
import logging
import os
from pathlib import Path
import tempfile
import time

from fastapi import HTTPException, Request, Response
from starlette.requests import ClientDisconnect
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from containers.api.app import create_app as original_app
from containers.api.service import ManualBulkCrawlService
from libs.ipc.manual_bulk_crawl import is_expired, write_file, write_json
from .backend import Backend, now
from . import profiling
from .retention import ROBOTS_MAX_BYTES, ROBOTS_MAX_ENTRIES, MAX_PENDING_URLS
from .scan_lease import ScanLease

logger = logging.getLogger("distributed.api")
ROOT = Path(os.getenv("CRAWL_DATA", "/data/manual_bulk_crawls"))
MAX_BODY = int(os.getenv("MAX_DOWNLOAD_BYTES", str(10 * 1024 * 1024)))


class OperationalMetrics:
    """Expose Valkey snapshots without incrementing multiprocess counters on scrapes."""
    def __init__(self, client):
        self.client=client

    def collect(self):
        client=self.client
        values=client.hgetall('poc:stats')
        completed=CounterMetricFamily('crawler_urls_completed','Completed job URLs',labels=['outcome'])
        completed.add_metric(['success'],int(values.get('completed_success',0)))
        completed.add_metric(['error'],int(values.get('completed_error',0)))
        yield completed
        cache=CounterMetricFamily('crawler_robots_cache_events','Robots cache events',labels=['event'])
        cache_values=client.hgetall('poc:robots:stats')
        for event in ('hits','misses','stored','refetches','evicted','expired','corrupt'):
            cache.add_metric([event],int(cache_values.get(event,0)))
        yield cache
        for name, description, value in (
            ('crawler_robots_cache_bytes','Compressed robots cache payload bytes',int(client.get('poc:robots:bytes') or 0)),
            ('crawler_robots_cache_entries','Robots cache entries, including uncollected expirations',client.zcard('poc:robots:lru')),
            ('crawler_robots_cache_max_bytes','Robots cache payload byte budget',ROBOTS_MAX_BYTES),
            ('crawler_robots_cache_max_entries','Robots cache entry budget',ROBOTS_MAX_ENTRIES),
            ('crawler_pending_url_limit','Maximum URLs admitted to the in-memory frontier',MAX_PENDING_URLS),
            ('crawler_pending_urls','URLs not yet committed',max(0,int(client.get('poc:pending') or 0))),
            ('crawler_ready_hosts','Hosts with queued URLs',client.zcard('poc:ready')),
            ('crawler_cooldown_hosts','Hosts in cooldown',client.zcount('poc:cooldowns',time.time(),'+inf')),
            ('crawler_html_bytes_stored','Current HTML payload bytes',max(0,int(client.get('poc:html_bytes') or 0)))):
            yield GaugeMetricFamily(name,description,value=value)


class ProfileMetrics:
    def collect(self):
        from prometheus_client import multiprocess
        for metric in multiprocess.MultiProcessCollector(None).collect():
            if metric.name.startswith('crawler_profile_'):
                yield metric


@lru_cache(maxsize=512)
def immutable_manifest(path, mtime_ns, size):
    with profiling.stage('api','manifest_read_decode'):
        if profiling.ENABLED:
            profiling.MANIFEST_MISSES.inc()
        return json.loads(path.read_text(encoding="utf-8"))


class DistributedService(ManualBulkCrawlService):
    def __init__(self):
        super().__init__({"pending_dir": str(ROOT / "pending"), "result_dir": str(ROOT / "results")},
                         route_urls=lambda urls: [0] * len(urls))
        self.backend = Backend()
        (ROOT / "locks").mkdir(parents=True, exist_ok=True)

    def _manifest(self, job):
        if not re.fullmatch(r"[a-f0-9]{32}", job):
            raise FileNotFoundError(job)
        path = self.pending_dir / (job + ".json")
        stat = path.stat()
        payload = immutable_manifest(path, stat.st_mtime_ns, stat.st_size)
        if is_expired(payload):
            raise FileNotFoundError(job)
        return path, payload

    @contextlib.contextmanager
    def lock(self, job, shared=False):
        started=time.perf_counter()
        with (ROOT / "locks" / (job + ".lock")).open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
            profiling.observe('api','result_lock_acquire' if '.' in job else 'job_lock_acquire',time.perf_counter()-started)
            yield

    def delete(self, job):
        if not re.fullmatch(r"[a-f0-9]{32}",job):
            raise FileNotFoundError(job)
        with self.lock(job):
            if not (self.pending_dir/(job+".json")).exists():
                raise FileNotFoundError(job)
            self.backend.cancel(args=[job])
            super().delete(job)

    def progress(self, job):
        with profiling.stage('api','progress_valkey'):
            values = self.backend.client.hgetall("poc:job:" + job)
        if not values.get("started_at") or values.get("status") == "cancelled":
            return
        progress = {"started_at": values["started_at"], "completed_at": values.get("completed_at"),
                    **{dest: int(values.get(source, 0)) for dest, source in {
                        "completed_urls": "completed", "succeeded_urls": "succeeded",
                        "failed_urls": "failed", "html_files": "html_files"}.items()}}
        path = self.result_dir / job / "crawler_00" / "progress.json"
        try:
            with profiling.stage('api','progress_read'):
                unchanged=path.exists() and json.loads(path.read_text()) == progress
            if unchanged:
                return
            with profiling.stage('api','progress_total'):
                profiling.write_metadata(path,progress,kind='progress')
        except FileNotFoundError:
            pass

    def scan(self,stop=None):
        if stop is not None and stop.is_set():return
        self.backend.maintain()
        for path in self.pending_dir.glob("*.json"):
            if stop is not None and stop.is_set():return
            try:
                stat = path.stat()
                payload = immutable_manifest(path, stat.st_mtime_ns, stat.st_size)
                if is_expired(payload):
                    self.delete(path.stem)
                    continue
                if not self.backend.client.exists("poc:job:" + path.stem):
                    if self.backend.publish(path.stem, payload)<0:
                        continue  # Durable outbox retries once frontier capacity opens.
                with self.lock(path.stem):
                    self.progress(path.stem)
            except FileNotFoundError:
                continue

    def lease(self, token):
        value = self.backend.client.hget("poc:leases", token)
        return json.loads(value) if value else None

    def commit(self, task, metadata, temporary):
        job, index = task["job_id"], task["index"]
        with self.lock(job, shared=True), self.lock(f"{job}.{index % 64}"):
            with profiling.stage('api','commit_manifest'):
                _, payload = self._manifest(job)
            with profiling.stage('api','valkey_validate'):
                owned=self.backend.client.hget("poc:states", task["id"]) == "leased:" + task["token"]
            if not owned:
                return False
            directory = self.result_dir / job / "crawler_00"
            filename = f"{index:05d}.html" if temporary is not None else None
            size = temporary.stat().st_size if temporary else 0
            if temporary:
                with profiling.stage('api','html_rename'):
                    os.replace(temporary, directory / filename)
            record = {"url": payload["urls"][index], "final_url": metadata["final_url"],
                      "status_code": metadata.get("status_code"), "error": metadata.get("error"),
                      "html_file": filename, "fetched_at": metadata["fetched_at"]}
            with profiling.stage('api','metadata_total'):
                profiling.write_metadata(directory / f"{index:05d}.json", record)
            with profiling.stage('api','valkey_ack'):
                accepted = self.backend.ack(args=[task["token"], int(not record["error"]),
                    int(filename is not None), size, now()])
            return bool(accepted)


def create_app():
    service = DistributedService()
    app = original_app(service=service)
    token = os.environ["WORKER_TOKEN"]

    async def dispatcher():
        while True:
            try:
                async with ScanLease(service.backend.client) as lease:
                    if lease.acquired:
                        with profiling.SCANS.track_inprogress():
                            await profiling.to_thread('api','dispatcher_scan',service.scan,lease.lost)
            except Exception:
                logger.exception("distributed.outbox_scan_failed")
            await asyncio.sleep(1)

    @app.on_event("startup")
    async def startup():
        app.state.dispatcher = asyncio.create_task(dispatcher())
        if profiling.ENABLED:
            app.state.profile_loop = asyncio.create_task(profiling.watch_loop('api'))

    @app.on_event("shutdown")
    async def shutdown():
        app.state.dispatcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await app.state.dispatcher
        if profiling.ENABLED:
            app.state.profile_loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await app.state.profile_loop
        service.backend.client.close()

    @app.post("/_worker/results/{lease}")
    @profiling.timed_async('api','result_total')
    async def result(lease: str, request: Request):
        if request.headers.get("authorization") != "Bearer " + token:
            raise HTTPException(403)
        task = await profiling.to_thread('api','lease_lookup',service.lease,lease)
        if task is None:
            return {"accepted": False, "reason": "lease_finished"}
        try:
            metadata = json.loads(base64.b64decode(request.headers["x-result-meta"]))
        except (ValueError, KeyError):
            raise HTTPException(400) from None
        temporary = None
        handle = None
        try:
            await profiling.to_thread('api','pre_upload_manifest',service._manifest,task["job_id"])
            if metadata.get("html"):
                directory = service.result_dir / task["job_id"] / "crawler_00"
                with profiling.stage('api','html_open'):
                    fd, name = tempfile.mkstemp(prefix=".upload-", dir=directory)
                    temporary = Path(name)
                    handle = os.fdopen(fd, "wb")
                size = 0
                with profiling.stage('api','body_receive_total'):
                    async for chunk in request.stream():
                        size += len(chunk)
                        if size > MAX_BODY:
                            raise HTTPException(413)
                        if profiling.ENABLED:
                            profiling.BYTES.labels('api').inc(len(chunk))
                        await profiling.to_thread('api','body_write',handle.write,chunk)
                await profiling.to_thread('api','html_flush',handle.flush)
                await profiling.to_thread('api','html_fsync',os.fsync,handle.fileno())
                handle.close(); handle = None
            else:
                async for _ in request.stream():
                    pass
            accepted = await profiling.to_thread('api','commit_total',service.commit,task,metadata,temporary)
            return {"accepted": accepted}
        except ClientDisconnect:
            logger.warning("distributed.upload_disconnected lease=%s", lease)
            return Response(status_code=499)
        except FileNotFoundError:
            return {"accepted": False, "reason": "job_deleted"}
        finally:
            if handle:
                handle.close()
            if temporary:
                temporary.unlink(missing_ok=True)

    @app.get("/metrics")
    def metrics():
        registry = CollectorRegistry()
        if os.getenv('PROMETHEUS_MULTIPROC_DIR'):
            registry.register(ProfileMetrics())
        registry.register(OperationalMetrics(service.backend.client))
        return Response(generate_latest(registry), media_type="text/plain; version=0.0.4")

    return app
