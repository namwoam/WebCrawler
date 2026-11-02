import hashlib
import os
from urllib.parse import urlsplit


def routing(url):
    parts = urlsplit(url)
    host = (parts.hostname or "").lower().rstrip(".")
    group = host
    origin = f"{parts.scheme}://{parts.netloc}"
    return {"group": group, "origin": origin,
            "origin_id": hashlib.sha256(parts.netloc.encode()).hexdigest()[:24],
            "interval": 1 / float(os.getenv("HOST_QPS", "32")),
            "concurrency": int(os.getenv("HOST_CONCURRENCY", "16"))}
