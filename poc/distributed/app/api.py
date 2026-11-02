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
from prometheus_client import CollectorRegistry, Gauge, Counter, generate_latest

from containers.api.app import create_app as original_app
from containers.api.service import ManualBulkCrawlService
from libs.ipc.manual_bulk_crawl import is_expired, write_file, write_json
from .backend import Backend, now

logger = logging.getLogger("distributed.api")
ROOT = Path(os.getenv("CRAWL_DATA", "/data/manual_bulk_crawls"))
MAX_BODY = int(os.getenv("MAX_DOWNLOAD_BYTES", str(10 * 1024 * 1024)))


@lru_cache(maxsize=512)
def immutable_manifest(path, mtime_ns, size):
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
        with (ROOT / "locks" / (job + ".lock")).open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
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
        values = self.backend.client.hgetall("poc:job:" + job)
        if not values.get("started_at") or values.get("status") == "cancelled":
            return
        progress = {"started_at": values["started_at"], "completed_at": values.get("completed_at"),
                    **{dest: int(values.get(source, 0)) for dest, source in {
                        "completed_urls": "completed", "succeeded_urls": "succeeded",
                        "failed_urls": "failed", "html_files": "html_files"}.items()}}
        path = self.result_dir / job / "crawler_00" / "progress.json"
        try:
            if path.exists() and json.loads(path.read_text()) == progress:
                return
            write_json(path, progress)
        except FileNotFoundError:
            pass

    def scan(self):
        for path in self.pending_dir.glob("*.json"):
            try:
                stat = path.stat()
                payload = immutable_manifest(path, stat.st_mtime_ns, stat.st_size)
                if is_expired(payload):
                    self.delete(path.stem)
                    continue
                if not self.backend.client.exists("poc:job:" + path.stem):
                    self.backend.publish(path.stem, payload)
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
            _, payload = self._manifest(job)
            if self.backend.client.hget("poc:states", task["id"]) != "leased:" + task["token"]:
                return False
            directory = self.result_dir / job / "crawler_00"
            filename = f"{index:05d}.html" if temporary is not None else None
            size = temporary.stat().st_size if temporary else 0
            if temporary:
                os.replace(temporary, directory / filename)
            record = {"url": payload["urls"][index], "final_url": metadata["final_url"],
                      "status_code": metadata.get("status_code"), "error": metadata.get("error"),
                      "html_file": filename, "fetched_at": metadata["fetched_at"]}
            write_json(directory / f"{index:05d}.json", record)
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
                # Only one API process scans manifests/progress in each interval.
                leader = await asyncio.to_thread(service.backend.client.set,
                    "poc:dispatcher-lock", "1", nx=True, ex=1)
                if leader:
                    await asyncio.to_thread(service.scan)
            except Exception:
                logger.exception("distributed.outbox_scan_failed")
            await asyncio.sleep(1)

    @app.on_event("startup")
    async def startup():
        app.state.dispatcher = asyncio.create_task(dispatcher())

    @app.on_event("shutdown")
    async def shutdown():
        app.state.dispatcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await app.state.dispatcher
        service.backend.client.close()

    @app.post("/_worker/results/{lease}")
    async def result(lease: str, request: Request):
        if request.headers.get("authorization") != "Bearer " + token:
            raise HTTPException(403)
        task = await asyncio.to_thread(service.lease, lease)
        if task is None:
            return {"accepted": False, "reason": "lease_finished"}
        try:
            metadata = json.loads(base64.b64decode(request.headers["x-result-meta"]))
        except (ValueError, KeyError):
            raise HTTPException(400) from None
        temporary = None
        handle = None
        try:
            await asyncio.to_thread(service._manifest, task["job_id"])
            if metadata.get("html"):
                directory = service.result_dir / task["job_id"] / "crawler_00"
                fd, name = tempfile.mkstemp(prefix=".upload-", dir=directory)
                temporary = Path(name)
                handle = os.fdopen(fd, "wb")
                size = 0
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > MAX_BODY:
                        raise HTTPException(413)
                    await asyncio.to_thread(handle.write, chunk)
                await asyncio.to_thread(handle.flush)
                await asyncio.to_thread(os.fsync, handle.fileno())
                handle.close(); handle = None
            else:
                async for _ in request.stream():
                    pass
            accepted = await asyncio.to_thread(service.commit, task, metadata, temporary)
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
        client = service.backend.client
        values = client.hgetall("poc:stats")
        complete = Counter("crawler_urls_completed", "Completed job URLs", ["outcome"], registry=registry)
        complete.labels("success").inc(int(values.get("completed_success", 0)))
        complete.labels("error").inc(int(values.get("completed_error", 0)))
        Gauge("crawler_pending_urls", "URLs not yet committed", registry=registry).set(max(0, int(client.get("poc:pending") or 0)))
        Gauge("crawler_ready_hosts", "Hosts with queued URLs", registry=registry).set(client.zcard("poc:ready"))
        Gauge("crawler_cooldown_hosts", "Hosts in cooldown", registry=registry).set(client.zcount("poc:cooldowns", time.time(), "+inf"))
        Gauge("crawler_html_bytes_stored", "Current HTML payload bytes", registry=registry).set(max(0, int(client.get("poc:html_bytes") or 0)))
        return Response(generate_latest(registry), media_type="text/plain; version=0.0.4")

    return app
