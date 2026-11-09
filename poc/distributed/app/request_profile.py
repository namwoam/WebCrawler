"""Request behavior verified against the deployed production Scrapy 2.19 crawler."""
import base64
from functools import lru_cache
import os

from scrapy.robotstxt import ProtegoRobotParser
from .retention import ROBOTS_CACHE

USER_AGENT = os.getenv('CRAWLER_USER_AGENT',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/121.0.0.0 Safari/537.36')
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none', 'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}
DOWNLOAD_TIMEOUT = 15
DNS_TIMEOUT = 15
RETRY_TIMES = 1
REDIRECT_MAX_TIMES = 20


def parse_robots(body):
    return ProtegoRobotParser(body, None)


@lru_cache(maxsize=30)
def small_robots_parser(body):
    return parse_robots(body)


def robots_allowed(origin, url):
    # Exactly the production middleware's _robots_error behavior: no parser.
    if not origin.get('has_parser', False):
        return True
    body = base64.b64decode(origin['body_b64'])
    parser = small_robots_parser(body) if len(body) <= 65536 else parse_robots(body)
    return parser.allowed(url, USER_AGENT)


def robots_result(metadata, body):
    """Scrapy parses every received robots response, including non-2xx bodies."""
    has_parser = metadata['status_code'] is not None and not metadata.get('download_exception')
    if has_parser:
        parse_robots(body)  # Parser exceptions follow the same no-parser path.
    return dict(state='ready', has_parser=has_parser,
        body_b64=base64.b64encode(body).decode() if has_parser else '',
        status_code=metadata['status_code'], error=metadata.get('error'),
        fetched_url=metadata['final_url'], fetched_at=metadata['fetched_at'],
        policy='production-scrapy-2.19-protego-v2')
