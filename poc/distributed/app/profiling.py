"""Optional wall-time profiling, shared across API processes through Prometheus."""
import asyncio
from contextlib import contextmanager, asynccontextmanager
from functools import wraps
import json
import os
import time

from prometheus_client import Counter, Gauge, Histogram

ENABLED = os.getenv('CRAWLER_PROFILE_ENABLED','').lower() in ('1','true','yes')
BUCKETS = (.0001,.00025,.0005,.001,.0025,.005,.01,.025,.05,.1,.25,.5,1,2,5,10,20,30,60,120)
STAGES = Histogram('crawler_profile_stage_seconds',
    'Wall time per operation, including failures; nested stages overlap',
    ['component','stage'],buckets=BUCKETS)
LOOP_DELAY = Histogram('crawler_profile_event_loop_delay_seconds',
    'Event loop scheduling delay beyond a 250ms sleep', ['component'],buckets=BUCKETS)
WAITERS = Gauge('crawler_profile_upload_waiters','Results waiting for a worker upload slot')
BYTES = Counter('crawler_profile_bytes','Bytes transferred through the upload path',['component'])
MANIFEST_MISSES = Counter('crawler_profile_manifest_cache_misses','Manifest read/decode cache misses')
SCANS = Gauge('crawler_profile_dispatcher_scans_inflight','API dispatcher scans queued or executing',multiprocess_mode='livesum')


def observe(component, name, seconds):
    if ENABLED:
        STAGES.labels(component,name).observe(seconds)


@contextmanager
def stage(component, name):
    if not ENABLED:
        yield
        return
    started=time.perf_counter()
    try:
        yield
    finally:
        observe(component,name,time.perf_counter()-started)


def timed_async(component, name):
    def decorate(function):
        @wraps(function)
        async def wrapped(*args,**kwargs):
            with stage(component,name):
                return await function(*args,**kwargs)
        return wrapped
    return decorate


async def to_thread(component, name, function, *args):
    if not ENABLED:
        return await asyncio.to_thread(function,*args)
    queued=time.perf_counter()
    def execute():
        observe(component,name+'_executor_wait',time.perf_counter()-queued)
        with stage(component,name):
            return function(*args)
    return await asyncio.to_thread(execute)


@asynccontextmanager
async def upload_slot(semaphore):
    if not ENABLED:
        async with semaphore:
            yield
        return
    WAITERS.inc()
    try:
        with stage('worker','upload_slot_wait'):
            await semaphore.acquire()
    finally:
        WAITERS.dec()
    try:
        yield
    finally:
        semaphore.release()


def write_metadata(path, value, kind='metadata'):
    # Match libs.ipc.manual_bulk_crawl.write_json's persistence and replacement.
    temporary=path.with_suffix('.json.tmp')
    with stage('api',kind+'_encode'):
        encoded=json.dumps(value,ensure_ascii=False).encode('utf-8')
    with stage('api',kind+'_open'):
        handle=temporary.open('wb')
    with handle:
        with stage('api',kind+'_write_flush'):
            handle.write(encoded)
            handle.flush()
        with stage('api',kind+'_fsync'):
            os.fsync(handle.fileno())
    with stage('api',kind+'_rename'):
        os.replace(temporary,path)


async def watch_loop(component):
    while True:
        started=time.perf_counter()
        await asyncio.sleep(.25)
        LOOP_DELAY.labels(component).observe(max(0,time.perf_counter()-started-.25))
