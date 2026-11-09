"""Renewable ownership for the API outbox scanner across Uvicorn processes."""
import asyncio
import contextlib
import logging
import threading
import uuid

logger=logging.getLogger(__name__)
RENEW="if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end return 0"
RELEASE="if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0"


class ScanLease:
    def __init__(self,client,ttl_ms=30000,interval=10):
        self.client=client;self.ttl_ms=ttl_ms;self.interval=interval
        self.key='poc:dispatcher-lock';self.token=uuid.uuid4().hex
        self.lost=threading.Event();self.acquired=False;self.task=None

    async def __aenter__(self):
        self.acquired=bool(await asyncio.to_thread(self.client.set,self.key,self.token,nx=True,px=self.ttl_ms))
        if self.acquired:self.task=asyncio.create_task(self.renew())
        return self

    async def renew(self):
        try:
            while True:
                await asyncio.sleep(self.interval)
                owned=await asyncio.to_thread(self.client.eval,RENEW,1,self.key,self.token,self.ttl_ms)
                if not owned:
                    self.lost.set();return
        except Exception:
            self.lost.set()
            logger.exception('distributed.scanner_lease_lost')

    async def __aexit__(self,*exc):
        self.lost.set()  # A cancelled to_thread scan stops before its next job.
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):await self.task
        if self.acquired:
            try:
                await asyncio.to_thread(self.client.eval,RELEASE,1,self.key,self.token)
            except Exception:
                logger.exception('distributed.scanner_lease_release_failed')
