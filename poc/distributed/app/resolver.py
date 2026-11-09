"""Keep production DNS behavior; map only reserved test domains for integration tests."""
import os
from scrapy.resolver import CachingThreadedResolver
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

class Resolver(CachingThreadedResolver):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.inflight={}

    def getHostByName(self, name, timeout=()):
        target = os.getenv("MOCK_DNS_TARGET")
        name=target if target and name.endswith('.poc.test') else name
        def cancelled(waiter):
            waiters=self.inflight.get(name,[])
            if waiter in waiters:waiters.remove(waiter)
        waiter=Deferred(cancelled)
        if name in self.inflight:
            self.inflight[name].append(waiter)
            return waiter
        self.inflight[name]=[waiter]
        def resolved(value):
            for child in self.inflight.pop(name,[]):
                if not child.called:child.callback(value)
            return value
        def failed(failure):
            for child in self.inflight.pop(name,[]):
                if not child.called:child.errback(failure)
            return None  # Failure belongs to the independent callers now.
        try:
            lookup=super().getHostByName(name,timeout)
        except Exception:
            failed(Failure())
        else:
            lookup.addCallbacks(resolved,failed)
        return waiter
