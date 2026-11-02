"""Internal result transport only; all external downloads belong to Scrapy."""
import asyncio
import base64
import json
import os
import aiohttp
from .metrics import UPLOADS, UPLOAD_TIME
API = os.getenv("CRAWLER_API_URL", "http://crawler-api:8080")
TOKEN = os.environ.get("WORKER_TOKEN", "")
UPLOAD_SEMAPHORE = asyncio.Semaphore(int(os.getenv("UPLOAD_CONCURRENCY", "8")))


async def upload(session, task, metadata, body):
    # Complete within the task lease even if the result receiver is unavailable.
    async with asyncio.timeout(70), UPLOAD_SEMAPHORE:
        await upload_attempts(session, task, metadata, body)


async def upload_attempts(session, task, metadata, body):
    async def chunks():
        while True:
            chunk = await asyncio.to_thread(body.read, 65536)
            if not chunk:
                break
            yield chunk

    headers = {"Authorization": "Bearer " + TOKEN,
               "X-Result-Meta": base64.b64encode(json.dumps(metadata).encode()).decode(),
               "Content-Type": "application/octet-stream"}
    for attempt in range(4):
        body.seek(0)
        try:
            with UPLOADS.track_inprogress(), UPLOAD_TIME.time():
                async with session.post(API + "/_worker/results/" + task["token"],
                    data=chunks() if metadata["html"] else b"", headers=headers,
                    timeout=aiohttp.ClientTimeout(total=45)) as response:
                    if response.status == 200:
                        await response.read()
                        return
                    response.raise_for_status()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            if attempt == 3:
                raise
            await asyncio.sleep(min(4, 2**attempt))
