"""Bounded, asynchronous worker log delivery; HTTP fetching never waits on Loki."""
import asyncio
from collections import defaultdict, deque
import json
import logging
import os
import time

import aiohttp
from prometheus_client import Counter

from .metrics import WORKER, NODE, telemetry

DROPPED = Counter('crawler_loki_dropped_logs', 'Worker log records dropped from the bounded queue', ['worker','node']).labels(worker=WORKER,node=NODE)
FAILURES = Counter('crawler_loki_push_failures', 'Failed worker log batches', ['worker','node']).labels(worker=WORKER,node=NODE)


class LokiReporter(logging.Handler):
    def __init__(self, url, capacity=10000):
        super().__init__(logging.INFO)
        self.url = url.rstrip('/') + '/loki/api/v1/push'
        self.records = deque(maxlen=capacity)
        self.stopping = False
        self.task = None

    def emit(self, record):
        if record.name != telemetry.name and record.levelno < logging.WARNING:
            return
        try:
            message = record.getMessage()
            try:
                data = json.loads(message)
                if not isinstance(data, dict):
                    data = {'message': message}
            except ValueError:
                data = {'message': message}
            if record.exc_info:
                data['exception'] = logging.Formatter().formatException(record.exc_info)
            data.update(service='crawler-poc', worker_id=WORKER, node=NODE,
                        level=record.levelname, logger=record.name)
            line = json.dumps(data)
            if len(line.encode()) > 16384:
                line = json.dumps(dict(service='crawler-poc', worker_id=WORKER,
                    node=NODE, level=record.levelname, message=message[:2000], truncated=True))
            if len(self.records) == self.records.maxlen:
                DROPPED.inc()
            self.records.append((record.levelname, str(time.time_ns()), line))
        except Exception:
            self.handleError(record)

    @staticmethod
    def payload(records):
        streams = defaultdict(list)
        for level, timestamp, line in records:
            event = json.loads(line).get('event','log')
            if event not in ('request.start','request.end','request.fail'):
                event = 'log'
            streams[level,event].append([timestamp, line])
        return {'streams': [dict(stream=dict(service='crawler-poc', worker_id=WORKER,
            node=NODE, level=level, event=event), values=sorted(values,key=lambda v:int(v[0])))
            for (level,event), values in streams.items()]}

    def start(self):
        logging.getLogger().addHandler(self)
        telemetry.addHandler(self)
        self.task = asyncio.create_task(self.run())

    async def run(self):
        pending = []
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False) as session:
            while not self.stopping or self.records or pending:
                if not pending:
                    pending = [self.records.popleft() for _ in range(min(1000,len(self.records)))]
                if not pending:
                    await asyncio.sleep(1)
                    continue
                try:
                    async with session.post(self.url,json=self.payload(pending)) as response:
                        response.raise_for_status()
                        await response.read()
                    pending = []
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                    FAILURES.inc()
                    await asyncio.sleep(2)

    async def stop(self):
        logging.getLogger().removeHandler(self)
        telemetry.removeHandler(self)
        self.stopping = True
        if self.task:
            try:
                await asyncio.wait_for(self.task,timeout=10)
            except asyncio.TimeoutError:
                pass
        self.close()


def start_loki():
    url = os.getenv('LOKI_URL')
    if not url:
        return None
    reporter = LokiReporter(url)
    reporter.start()
    return reporter
