import json
import asyncio
import os
import uuid
from datetime import datetime, timezone

import valkey
import valkey.asyncio

from . import scripts
from .policy import routing
from .profiling import stage
from .retention import ROBOTS_CACHE, ROBOTS_BODY_PREFIX, ROBOTS_MAX_BYTES, ROBOTS_MAX_ENTRIES, pack_robots, unpack_robots, robots_ttl

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
        self.maintain = self.client.register_script(scripts.MAINTAIN)

    def publish(self, job_id, payload):
        request = {"id": job_id, "created_at": payload["created_at"],
                   "expires": datetime.fromisoformat(payload["expires_at"]).timestamp(),
                   "tasks": tasks_for(job_id, payload)}
        return self.enqueue(args=[json.dumps(request)])

    def robots_snapshot(self, origins):
        """Read-only audit; eviction/expiry means historical rules are unknown."""
        if not origins:
            return []
        metadata=self.client.hmget(ROBOTS_CACHE,origins)
        pipeline=self.client.pipeline(transaction=False)
        for ident,raw in zip(origins,metadata):
            generation=json.loads(raw or '{}').get('generation','missing')
            pipeline.get(ROBOTS_BODY_PREFIX+ident+':'+generation)
        result=[]
        for raw in pipeline.execute():
            try:
                result.append(unpack_robots(raw) if raw else None)
            except (ValueError,OSError,EOFError,KeyError):
                result.append(None)
        return result


class AsyncBackend:
    def __init__(self):
        self.client = valkey.asyncio.Valkey.from_url(VALKEY_URL, decode_responses=True,
            socket_timeout=5, socket_connect_timeout=5, max_connections=256)
        self._claim = self.client.register_script(scripts.CLAIM)
        self._start_network = self.client.register_script(scripts.START_NETWORK)
        self._ack = self.client.register_script(scripts.ACK)
        self._requeue = self.client.register_script(scripts.REQUEUE)
        self._recover = self.client.register_script(scripts.RECOVER)
        self._robots_store = self.client.register_script(scripts.ROBOTS_STORE)
        self._robots_get = self.client.register_script(scripts.ROBOTS_GET)
        self._robots_invalidate = self.client.register_script(scripts.ROBOTS_INVALIDATE)

    async def get_robots(self, task):
        generation = task.get('robots_generation')
        if not generation:
            return None
        value = await self._robots_get(args=[task['origin_id'], generation])
        if value is None:
            return None
        try:
            return await asyncio.to_thread(unpack_robots, value)
        except (ValueError, OSError, EOFError, KeyError):
            await self._robots_invalidate(args=[task['origin_id'], generation, value])
            return None

    async def store_robots(self, task, value):
        packed = await asyncio.to_thread(pack_robots, value)
        ttl = robots_ttl(value)
        # Each Lua invocation evicts at most 128 cache entries. Yield between
        # batches so large inserts do not monopolize the scheduler's thread.
        while True:
            result = await self._robots_store(args=[task['token'], packed, ttl,
                ROBOTS_MAX_BYTES, ROBOTS_MAX_ENTRIES])
            if result != 2:
                return bool(result)
            await asyncio.sleep(.01)

    async def claim(self, worker, capacity, domains=(), max_domains=30):
        with stage('worker','valkey_claim'):
            data = json.loads(await self._claim(args=[min(16, capacity), uuid.uuid4().hex, worker, now(), json.dumps(list(domains)), max_domains]))
        data["tasks"] = data["tasks"] if isinstance(data["tasks"], list) else []
        return data

    async def release_network(self, task):
        await self.client.zrem("poc:net:" + task["group"], task["token"])

    async def start_network(self, task):
        with stage('worker','valkey_start_network'):
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
