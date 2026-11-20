"""Consumer-owned, expiring demand. No downloader or site-policy dependencies."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from pathlib import Path

from .jsonio import atomic_write_json, read_json

DEMAND_FILE = "demand.snapshot"


@dataclass
class _ObservedDomain:
    started: float
    touched: float
    completed: int = 0
    rate: float = 0.0


class DemandTracker:
    """Estimate replenishment from drain rate while a domain has work.

    Idle time is excluded: starving a consumer must not lower its next request.
    State is process-local and bounded; a restart bootstraps through ordinary offers.
    """

    def __init__(self, horizon_sec=180, max_domain_urls=2000, max_domains=2000):
        if min(horizon_sec, max_domain_urls, max_domains) <= 0:
            raise ValueError("Demand tracker budgets must be positive")
        self.horizon_sec = horizon_sec
        self.max_domain_urls = max_domain_urls
        self.max_domains = max_domains
        self.domains: dict[int, _ObservedDomain] = {}

    def admitted(self, domain_id: int, now: float | None = None):
        now = time.monotonic() if now is None else now
        previous = self.domains.pop(domain_id, None)
        self.domains[domain_id] = _ObservedDomain(now, now, rate=previous.rate if previous else 0)
        while len(self.domains) > self.max_domains:
            del self.domains[next(iter(self.domains))]

    def finished(self, domain_id: int, now: float | None = None):
        now = time.monotonic() if now is None else now
        state = self.domains.get(domain_id)
        if state is not None:
            state.completed += 1
            state.touched = now
            state.rate = state.completed / max(1.0, now - state.started)

    def snapshot(self, pending: dict[int, int], capacity: int, *, now=None, wall_time=None):
        now = time.monotonic() if now is None else now
        demands = {}
        targets = {}
        for domain_id, state in list(self.domains.items()):
            if now - state.touched > 600 and domain_id not in pending:
                del self.domains[domain_id]
                continue
            target = min(self.max_domain_urls, max(100, math.ceil(state.rate * self.horizon_sec)))
            targets[str(domain_id)] = target
            wanted = max(0, target - pending.get(domain_id, 0))
            if wanted:
                demands[str(domain_id)] = wanted
        return {"version": 1, "generated_at": time.time() if wall_time is None else wall_time,
                "capacity": max(0, capacity), "domains": demands,
                "inventory": {str(key): value for key, value in pending.items()}, "targets": targets}

    def publish(self, queue_dir: str, pending: dict[int, int], capacity: int):
        atomic_write_json(str(Path(queue_dir) / DEMAND_FILE), self.snapshot(pending, capacity))


def read_demand(queue_dir: str, *, max_age_sec=30, max_domains=2000, now=None):
    """Invalid/stale feedback disables the fast path, leaving legacy refill intact."""
    try:
        doc = read_json(Path(queue_dir) / DEMAND_FILE)
        age = (time.time() if now is None else now) - float(doc["generated_at"])
        if doc["version"] != 1 or not math.isfinite(age) or not 0 <= age <= max_age_sec:
            return 0, {}
        capacity = doc["capacity"]
        raw = doc["domains"]
        if type(capacity) is not int or capacity < 0 or not isinstance(raw, dict) or len(raw) > max_domains:
            return 0, {}
        domains = {int(key): value for key, value in raw.items()
                   if type(value) is int and 0 < value <= 1000000 and int(key) >= 0}
        return capacity, domains
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        return 0, {}
