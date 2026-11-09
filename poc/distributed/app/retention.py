"""Bounded robots cache and terminal scheduler-state retention."""
import base64
import gzip
import json
import os
import random

ROBOTS_CACHE = 'poc:origins:v3'
ROBOTS_BODY_PREFIX = 'poc:robots:body:'
ROBOTS_TTL = min(86400, max(1, int(os.getenv('ROBOTS_CACHE_TTL_SECONDS', '86400'))))
ROBOTS_FAILURE_TTL = min(300, max(1, int(os.getenv('ROBOTS_FAILURE_TTL_SECONDS', '300'))))
ROBOTS_MAX_BYTES = int(os.getenv('ROBOTS_CACHE_MAX_BYTES', str(3 * 1024**3)))
ROBOTS_MAX_ENTRIES = int(os.getenv('ROBOTS_CACHE_MAX_ENTRIES', '500000'))
STATE_RETENTION = max(360, int(os.getenv('TASK_STATE_RETENTION_SECONDS', '3600')))
JOB_RETENTION_GRACE = max(360, int(os.getenv('JOB_RETENTION_GRACE_SECONDS', '3600')))
IDLE_HOST_TTL = max(360, int(os.getenv('IDLE_HOST_TTL_SECONDS', '600')))
MAX_PENDING_URLS = int(os.getenv('MAX_PENDING_URLS', '100000'))
if ROBOTS_MAX_BYTES < 32 * 1024**2 or ROBOTS_MAX_ENTRIES < 1:
    raise ValueError('Robots cache requires at least 32 MiB and one entry')
if MAX_PENDING_URLS < 10000:
    raise ValueError('Pending URL budget must fit the maximum public API batch of 10000')


def robots_ttl(value):
    failed = not value.get('has_parser') or (value.get('status_code') or 0) >= 500
    maximum = ROBOTS_FAILURE_TTL if failed else ROBOTS_TTL
    # Jitter only shortens freshness, including when a host is accessed frequently.
    return max(1, int(maximum * random.uniform(.8, 1)))


def pack_robots(value):
    raw = json.dumps(value, separators=(',', ':')).encode()
    return base64.b64encode(gzip.compress(raw, compresslevel=1)).decode('ascii')


def unpack_robots(value):
    result = json.loads(gzip.decompress(base64.b64decode(value, validate=True)))
    if not isinstance(result, dict) or not isinstance(result.get('has_parser'), bool):
        raise ValueError('Invalid cached robots response')
    if result['has_parser']:
        if not isinstance(result.get('body_b64'), str):
            raise ValueError('Invalid cached robots body')
        base64.b64decode(result['body_b64'], validate=True)
    return result
