## 2026-10-26~2026-11-2 (+Midterm Report)

### Business Impact

- Delivered on-demand bulk crawling with progress tracking, result downloads, cancellation, and retention through merged [PR #84](https://github.com/NTU-CSIE5376/WebCrawler/pull/84).
- [PR #87](https://github.com/NTU-CSIE5376/WebCrawler/pull/87), pending merge, demonstrated **5.56× Wikipedia throughput** and **98.45% successful pages** in its 10k-URL benchmark.
- Validated distributed crawling with 400k real URLs: approximately **417 completed URLs/s overall**, **63.65% success**, and **zero worker restarts**. Failure analysis identified access restrictions, DNS failures, robots rules, and broken links as major issues.

### Technical Detail

- Added atomic job submission, configurable manual/normal scheduling, URL validation, and TTL-based cleanup.
- Introduced declarative site policies, AutoThrottle settings, and site-specific middleware in PR #87.
- Built a three-node Kubernetes POC with **50 native Scrapy workers**, AutoThrottle enabled, **30 active domains per worker**, and Valkey enforcing a shared **32 QPS ceiling per domain**.
- Integrated Prometheus/Grafana monitoring and expanded HTML storage to a **200Gi PVC**; cleared previous results before rerunning the benchmark.
