"""Keep production DNS behavior; map only reserved test domains for integration tests."""
import os
from scrapy.resolver import CachingThreadedResolver

class Resolver(CachingThreadedResolver):
    def getHostByName(self, name, timeout=()):
        target = os.getenv("MOCK_DNS_TARGET")
        return super().getHostByName(target if target and name.endswith(".poc.test") else name, timeout)
