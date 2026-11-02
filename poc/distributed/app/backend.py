import json
import os
import uuid
from datetime import datetime, timezone

import valkey
import valkey.asyncio

from . import scripts
from .policy import routing

VALKEY_URL = os.getenv("VALKEY_URL", "redis://crawler-valkey:6379/0")


def now():
    return datetime.now(timezone.utc).isoformat()


def tasks_for(job_id, payload):
    return [{"id": f"{job_id}:{index:05d}", "kind": "url", "job_id": job_id, "index": index,
             "url": url, "original_url": url, "attempt": 0, "redirects": 0, **routing(url)}
            for index, url in enumerate(payload["urls"])]


class Backend:
    def __init__(self):
        self.client = valkey.Valkey.from_url(VALKEY_URL, decode_responses=True,
                                         socket_timeout=5, socket_connect_timeout=5)
        self.enqueue = self.client.register_script(scripts.ENQUEUE)
        self.ack = self.client.register_script(scripts.ACK)
        self.cancel = self.client.register_script(scripts.CANCEL)

    def publish(self, job_id, payload):
        request = {"id": job_id, "created_at": payload["created_at"],
                   "expires": datetime.fromisoformat(payload["expires_at"]).timestamp(),
                   "tasks": tasks_for(job_id, payload)}
        return self.enqueue(args=[json.dumps(request)])


class AsyncBackend:
    def __init__(self):
        self.client = valkey.asyncio.Valkey.from_url(VALKEY_URL, decode_responses=True,
            socket_timeout=5, socket_connect_timeout=5, max_connections=256)
        self._claim = self.client.register_script(scripts.CLAIM)
        self._start_network = self.client.register_script(scripts.START_NETWORK)
        self._ack = self.client.register_script(scripts.ACK)
        self._requeue = self.client.register_script(scripts.REQUEUE)
        self._recover = self.client.register_script(scripts.RECOVER)

    async def claim(self, worker, capacity, domains=(), max_domains=30):
        data = json.loads(await self._claim(args=[min(8, capacity), uuid.uuid4().hex, worker, now(), json.dumps(list(domains)), max_domains]))
        data["tasks"] = data["tasks"] if isinstance(data["tasks"], list) else []
        return data

    async def release_network(self, task):
        await self.client.zrem("poc:net:" + task["group"], task["token"])

    async def start_network(self, task):
        return json.loads(await self._start_network(args=[task['token']]))

    async def ack(self, task):
        return await self._ack(args=[task["token"], 1, 0, 0, now()])

    async def requeue(self, task, delay=0):
        t = dict(task)
        for key in ["token", "worker", "granted_at", "network_deadline", "lease_deadline", "network_started", "skip"]:
            t.pop(key, None)
        return await self._requeue(args=[task["token"], json.dumps(t), datetime.now(timezone.utc).timestamp() + delay])

    async def recover(self):
        if await self.client.set("poc:recovery-lock", "1", nx=True, ex=1):
            return await self._recover()
