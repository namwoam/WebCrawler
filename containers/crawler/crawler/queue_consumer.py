from __future__ import annotations

import re
import math
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from libs.ipc.jsonio import read_json

_DOMAIN_FILE_RE = re.compile(r"^domain_(\d+)\.json$")


@dataclass(frozen=True)
class _DomainQueueFile:
    path: Path
    domain_id: int
    mtime_ns: int


def _parse_domain_queue_file(path: Path) -> _DomainQueueFile | None:
    m = _DOMAIN_FILE_RE.match(path.name)
    if not m:
        return None

    try:
        ns = path.stat().st_mtime_ns
    except OSError:
        ns = (1 << 63) - 1

    return _DomainQueueFile(
        path=path,
        domain_id=int(m.group(1)),
        mtime_ns=ns,
    )


def _domain_json_sort_key(entry: _DomainQueueFile) -> tuple[int, int]:
    """
    Order queue files for consumption: oldest mtime first, then by domain_id.
    Filename ordering is not a reliable proxy for queue age.
    Id-only ordering starves high domain_ids when many low-id files keep arriving.
    """
    return (entry.mtime_ns, entry.domain_id)


class QueueConsumer:
    """
    Per-domain queue consumer:
      - list domain_*.json files
      - read then DELETE immediately
      - return {domain_id: [urls]}
    """

    def __init__(self, queue_dir: str):
        self.queue_dir = Path(queue_dir)
        self.priorities = OrderedDict()
        self._priority_cache = {}

    def pop_domain_batches(
        self,
        limit: int = 0,
        exclude_domain_ids: set[int] | None = None,
        include_domain_ids: set[int] | None = None,
        max_urls: int | None = None,
        priority_only: bool = False,
    ) -> dict[int, list[str]]:
        """
        Read up to `limit` domain files (0 = all available).
        Files whose domain_id is in `exclude_domain_ids` are skipped
        (left on disk for a future pop).
        Returns {domain_id: [url, ...]}.
        Each consumed file is deleted immediately after reading.
        """
        if not self.queue_dir.exists():
            return {}

        candidates = sorted(
            [
                entry
                for f in self.queue_dir.iterdir()
                if (entry := _parse_domain_queue_file(f)) is not None
            ],
            key=_domain_json_sort_key,
        )
        if not candidates:
            return {}

        if priority_only:
            live_ids = {entry.domain_id for entry in candidates}
            self._priority_cache = {key: value for key, value in self._priority_cache.items() if key in live_ids}
            ranked = []
            for entry in candidates:
                cached = self._priority_cache.get(entry.domain_id)
                if cached is None or cached[0] != entry.mtime_ns:
                    try:
                        value = read_json(entry.path).get("priority", 0)
                        value = float(value) if isinstance(value, (int, float)) else 0
                        cached = (entry.mtime_ns, value if math.isfinite(value) else 0)
                        self._priority_cache[entry.domain_id] = cached
                    except (OSError, ValueError, TypeError, AttributeError):
                        continue
                if cached[1] > 0:
                    ranked.append((cached[1], entry))
            candidates = [entry for priority, entry in sorted(
                ranked, key=lambda pair: (-pair[0], pair[1].mtime_ns, pair[1].domain_id))]

        # Filter out domains that the caller already has in-flight.
        if exclude_domain_ids:
            candidates = [
                entry for entry in candidates if entry.domain_id not in exclude_domain_ids
            ]
        if include_domain_ids is not None:
            candidates = [entry for entry in candidates if entry.domain_id in include_domain_ids]

        if not candidates:
            return {}

        if limit > 0:
            candidates = candidates[:limit]

        result: dict[int, list[str]] = {}
        for entry in candidates:
            p = entry.path
            domain_id = entry.domain_id

            try:
                data = read_json(p)
            except Exception:
                continue
            urls = data.get("urls")
            if isinstance(urls, list) and max_urls is not None and len(urls) > max_urls:
                # Leave the whole offer on disk until the consumer has capacity.
                continue
            try:
                p.unlink()
            except FileNotFoundError:
                continue

            if not isinstance(urls, list) or not urls:
                continue

            result[domain_id] = [str(u) for u in urls]
            value = data.get("priority", 0)
            self.priorities.pop(domain_id, None)
            if isinstance(value, (int, float)) and math.isfinite(value) and value > 0:
                self.priorities[domain_id] = min(float(value), 1000)
            while len(self.priorities) > 2000:
                self.priorities.popitem(last=False)
            if max_urls is not None:
                max_urls -= len(urls)

        return result

    def pop_batch(self) -> list[str]:
        """
        Legacy compatibility: pop a single domain file and return flat URL list.
        """
        batches = self.pop_domain_batches(limit=1)
        if not batches:
            return []
        return next(iter(batches.values()))
