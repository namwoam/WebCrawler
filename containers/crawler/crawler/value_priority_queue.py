"""Dispatch valuable normal work first when its policy slot has capacity."""
from collections import Counter
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit

from scrapy.pqueues import DownloaderAwarePriorityQueue, _path_safe

from .site_policies import SitePolicies
from .dispatch_defaults import MANUAL_SHARE_PERCENT


class ValuePriorityQueue(DownloaderAwarePriorityQueue):
    """Keep policy-saturated domains out of the downloader waiting room.

    This is a crawler-owned dispatch policy: the offerer provides a value only.
    Manual source scheduling and downloader rate/concurrency enforcement remain
    in their existing components. Slot occupancy includes requests waiting in
    middleware, not only those that have reached the network slot.
    """

    source = "normal"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.policies = SitePolicies.load()
        if not hasattr(self.crawler, "_inventory_source_queues"):
            self.crawler._inventory_source_queues = {"normal": [], "manual": []}
        self.crawler._inventory_source_queues[self.source].append(self)

    def _policy(self, request):
        cached = request.meta.get("_inventory_slot_policy")
        if cached is not None and cached[0] == request.url:
            return cached
        hostname = urlsplit(request.url).hostname
        if not hostname:
            slot = self._downloader_interface.get_slot_key(request)
            cap = self.policies.config.default.request_limits.max_concurrent_requests
        else:
            try:
                policy = self.policies.resolve(hostname)
            except ValueError:
                # Enqueue must remain total for URLs Scrapy can construct. The
                # existing downloader middleware rejects invalid hostnames and
                # routes them through the normal errback/result pipeline.
                slot = self._downloader_interface.get_slot_key(request)
                cap = self.policies.config.default.request_limits.max_concurrent_requests
            else:
                slot = policy.request_limits.download_slot or self._downloader_interface.get_slot_key(request)
                cap = policy.request_limits.max_concurrent_requests
        cached = (request.url, slot, cap)
        request.meta["_inventory_slot_policy"] = cached
        return cached

    def _slot(self, request):
        return self._policy(request)[1]

    def push(self, request):
        slot = self._slot(request)
        if slot not in self.pqueues:
            self.pqueues[slot] = self.pqfactory(slot)
        self.pqueues[slot].push(request)

    def _wake_after_wait(self):
        # A queued download starting does not wake Scrapy's engine. Keep one
        # crawler-wide timer so admission can refill a slot without waiting for
        # an entire response or the engine's five-second idle heartbeat.
        engine = self.crawler.engine
        if (getattr(engine, "_slot", None) is None
                or getattr(self.crawler, "_inventory_admission_wakeup", None) is not None):
            return
        from twisted.internet import reactor

        def wake():
            self.crawler._inventory_admission_wakeup = None
            if getattr(engine, "_slot", None) is not None and getattr(engine, "running", False):
                engine._slot.nextcall.schedule()

        self.crawler._inventory_admission_wakeup = reactor.callLater(0.05, wake)

    def _choose(self):
        if not self.pqueues:
            return None
        downloader = self.crawler.engine.downloader
        occupied = Counter((self._slot(request), "manual" if "manual_bulk_crawl_job_id" in request.meta else "normal")
                           for request in getattr(downloader, "active", ()))
        choices = []
        for key, queue in self.pqueues.items():
            try:
                head = queue.peek()
            except NotImplementedError:
                # Configured FIFO backends implement peek. Retain compatibility
                # with another optional backend without silently dropping work.
                return super()._next_slot(self._downloader_interface.stats(self.pqueues), update_state=False)
            if head is None:
                continue
            _, actual, cap = self._policy(head)
            own_active = occupied[actual, self.source]
            other_source = "manual" if self.source == "normal" else "normal"
            other_active = occupied[actual, other_source]
            active = own_active + other_active
            slot = downloader.slots.get(actual)
            active = max(active, len(slot.active) if slot is not None else 0)
            # Admit at most one request waiting to transfer per policy slot,
            # including middleware waiters from either source. Downloader delay
            # and AutoThrottle remain responsible for actual network timing.
            transferring = len(slot.transferring) if slot is not None else 0
            waiting = active - transferring
            if slot is not None:
                cap = min(cap, slot.concurrency)
                waiting = max(waiting, len(slot.queue))
            if waiting >= 1:
                self._wake_after_wait()
                continue
            dispatch_policy = getattr(self.crawler, "_inventory_dispatch_policy", "weighted")
            other_waiting = other_active > 0 or any(
                actual in q.pqueues for q in self.crawler._inventory_source_queues[other_source])
            if cap > 1 and other_waiting and dispatch_policy in ("weighted", "balanced"):
                share = getattr(self.crawler, "_inventory_manual_share", MANUAL_SHARE_PERCENT)
                manual_cap = min(cap - 1, max(1, cap * share // 100))
                source_cap = manual_cap if self.source == "manual" else cap - manual_cap
                if own_active >= source_cap:
                    continue
            if active < cap:
                choices.append((head.priority, active, key))
        if not choices:
            return None
        best_priority = max(item[0] for item in choices)
        stats = [(active, key) for priority, active, key in choices if priority == best_priority]
        return super()._next_slot(stats, update_state=False)

    def peek(self):
        slot = self._choose()
        return None if slot is None else self.pqueues[slot].peek()

    def pop(self):
        slot = self._choose()
        if slot is None:
            return None
        self._last_selected_slot = slot
        queue = self.pqueues[slot]
        request = queue.pop()
        if not queue:
            del self.pqueues[slot]
            queue.close()
            if self.key:
                with suppress(OSError):
                    Path(self.key, _path_safe(slot)).rmdir()
        return request


class ManualValuePriorityQueue(ValuePriorityQueue):
    """The same slot admission boundary for the existing manual source."""
    source = "manual"
