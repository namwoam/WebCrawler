# Crawler Performance, Distributed Framework, and Ingestion Progress Report

**Reporting date:** November 8, 2026 (Asia/Taipei)
**Scope:** Current production baseline, distributed crawler proof of concept (POC), sustained one-million-URL evaluation, related pull requests, and remaining deployment blockers.

## 1. Executive overview and business impact

The current production crawler provides an initial baseline of approximately **500 HTTP request attempts per second**, as reported from current operation. The distributed crawler remains a **separate experimental deployment**; its benchmark results do not represent an improvement already delivered to production.

The main result is a completed **1,000,000-URL run using 100 workers across three POC nodes**. All submitted URLs reached terminal accounting, with **504,468 successes and 495,532 failures**. During an explicitly defined approximately 35-minute interior observation window, the POC averaged **1,280.6 HTTP attempts/s, 407.8 completed input URLs/s, and 202.0 successful URLs/s**. Including startup and the slow final tail, successful throughput was approximately **165.2 URLs/s**.

Against the current production baseline of approximately 500 HTTP attempts/s, the POC's interior-window HTTP rate is approximately **2.56×** as high. This is a comparison of observed request activity under different workloads, not a controlled production speedup claim. The production baseline has no accompanying matched dataset, success rate, worker-resource allocation, or measurement window in this report. We therefore cannot conclude that production would obtain 2.56× more useful pages by switching implementations.

The work also produced merged site-policy and ingestion improvements, a reproducible benchmark workflow, bounded cache retention, shared request admission, lease-based task recovery, and integrated monitoring. The one-million-URL experiment completed without worker restarts or new Valkey OOM errors, but exposed a Valkey persistence failure and coordinator contention. These remain material blockers to production readiness and sustained useful-throughput improvement.

## 2. Contribution and pull-request status

Statuses were checked against GitHub on the reporting date. Merge timestamps below use UTC.

| Contribution | Status | Main contribution | Evidence and limitations |
| --- | --- | --- | --- |
| [PR #84](https://github.com/NTU-CSIE5376/WebCrawler/pull/84) | Merged, as recorded in the earlier report | On-demand bulk crawling, progress, downloads, cancellation, and retention | Established the user-facing workflow used by subsequent crawler work |
| [PR #87](https://github.com/NTU-CSIE5376/WebCrawler/pull/87) | **Merged**, November 7, 17:30 UTC | Declarative site policies, shared Wikipedia concurrency, AutoThrottle tuning, middleware, and scheduling efficiency | Fresh Wikipedia experiments support the selected target; results concern that site and workload |
| [PR #124](https://github.com/NTU-CSIE5376/WebCrawler/pull/124) | **Merged**, November 7, 12:58 UTC | Ingest records by shard and retry connection failures before COMMIT | Isolated playback and production canaries; durable recovery of exhausted/ambiguous batches remains incomplete |
| [PR #130](https://github.com/NTU-CSIE5376/WebCrawler/pull/130) | **Open** | Shared domain exclusions and measured ESPN/Yahoo policies | Sustained single-crawler site tests; no claim of merged deployment or distributed mixed-host validation |
| Distributed framework and latest retention/profiling changes | Experimental POC; latest workspace changes uncommitted | Distributed scheduling, leases, network admission, storage, monitoring, and bounded state | One-million-URL baseline completed on POC image `0.16`; additional performance changes await deployment and comparison |

### 2.1 Site-policy contribution: PR #87

The merged policy framework replaces scattered domain settings with validated YAML defaults and registered-domain/exact-host overrides. Policies select pacing, concurrency, headers, and middleware. Wikipedia uses an AutoThrottle target of eight with a shared hard concurrency cap of eight; other sites retain the global target of two. Wikipedia 429 responses pause the affected slot for at least ten seconds or a longer `Retry-After`, while unrelated domains continue.

Four experiments each used 5,000 fresh Wikipedia URLs, one crawler, two CPUs, and 4 GiB RAM. All retained a nominal 15 QPS limit and the same hard concurrency cap. Recorded rates include startup, retries, and cooldowns. [PR #87](https://github.com/NTU-CSIE5376/WebCrawler/pull/87)

| AutoThrottle target | Elapsed | HTTP attempts/s | Successful pages | Observed / terminal HTTP 429s |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 27m 58s | 3.00 | 4,942 / 5,000 | 0 / 0 |
| 4 | 14m 25s | 5.83 | 4,943 / 5,000 | 0 / 0 |
| **8, selected** | **13m 23s** | **6.32** | **4,926 / 5,000** | **15 / 0** |
| 10 | 13m 39s | 6.18 | 4,920 / 5,000 | 19 / 0 |

Target eight improved successful-page throughput by approximately 7.4% over target four. Target ten provided no measured benefit. These observations justify a bounded site-specific setting rather than increasing global concurrency indiscriminately. The older 10k Wikipedia result in the historical section describes a separate experiment.

### 2.2 Ingestion contribution: PR #124

The ingestor groups discovery and result records by shard within each folder before forming the existing 2,000-record batches. It preserves discovery-first processing, folder-level deduplication, and checkpoints. Connection retries use up to three transaction-execution attempts with fresh sessions and exponential waits starting at 0.2 seconds. COMMIT remains outside the retry boundary because replay after an ambiguous acknowledgement can duplicate additive counters. [PR #124](https://github.com/NTU-CSIE5376/WebCrawler/pull/124)

Eight isolated trials replayed 72,214 captured records, with two trials per implementation and initial state:

| Initial state | Baseline mean | Candidate mean | Baseline SQL executions | Candidate SQL executions |
| --- | ---: | ---: | ---: | ---: |
| Empty URL state | 28.735 s | 22.003 s | 993 | 363 |
| Pre-seeded URL state | 16.500 s | 16.510 s | 783 | 312 |

Empty-state elapsed time fell approximately 23%, and SQL execution counts fell approximately 60–63%. Pre-seeded elapsed time was effectively unchanged. Commits increased from 16 to 81 per replay, and larger operations increased client-observed SQL p95. These results establish reduced repeated work, not uniform latency or production throughput improvement.

A separate retry-enabled 30-minute production canary processed 60 minutes of queued data, reducing backlog by 30 minutes. Including completion of its final folder, it processed **2,490,256 records across 1,381 batches**, approximately **1,359 records/s**. It recorded zero terminal batch errors and zero connection retries. Fault injection in the isolated database validated retry behavior; the production canary did not exercise actual retry recovery. A remaining correctness limitation is the existing batch-error handler, which can advance a folder checkpoint after retry exhaustion or ambiguous COMMIT. Durable failed-batch recovery is still needed. [PR #124](https://github.com/NTU-CSIE5376/WebCrawler/pull/124)

### 2.3 Domain exclusions and useful throughput: PR #130

The open PR adds a shared `allow` policy checked before robots fetching or request-budget acquisition, including ordinary jobs, manual jobs, retries, and redirects. It excludes six deployment-restricted domains: Instagram, Facebook, Reddit, X, IMDb, and Sofascore. It also adds explicit crawler identity and stateless request policies for ESPN and Yahoo, with ESPN account/personalization path filtering. These changes preserve requested URLs and query parameters. [PR #130](https://github.com/NTU-CSIE5376/WebCrawler/pull/130)

Fresh 10,000-URL tests used one crawler, two CPUs, 4 GiB RAM, production download middleware, AutoThrottle, parsing, outlink extraction, and output writes:

| Site | Successful content | Duration | Useful content/s | HTTP attempts/s | Observed 429s |
| --- | ---: | ---: | ---: | ---: | ---: |
| ESPN | 9,826 / 10,000 | 377.75 s | 26.01 | 27.72 | 0 |
| Yahoo | 9,964 / 10,000 | 1,109.36 s | 8.98 | 9.10 | 0 |

First-half and second-half useful rates were 26.05/25.98 for ESPN and 9.06/8.91 for Yahoo. ESPN success requires nonempty HTTP-200 HTML; Yahoo additionally requires a final `www.yahoo.com` URL and at least 200 visible article characters. These definitions differ from generic POC URL success, so their percentages cannot be directly substituted for mixed-host success. The rebased PR records **407 passing crawler tests**. It remains open. [PR #130](https://github.com/NTU-CSIE5376/WebCrawler/pull/130)

## 3. Distributed crawler architecture and design

### 3.1 End-to-end execution

The API accepts a manual bulk-crawl job, validates its URLs, and writes a durable job manifest/outbox. Dispatchers publish admitted work into Valkey. Workers use native Scrapy scheduling, download middleware, robots interpretation, retries, redirects, and result pipelines. Page results upload to the API, which persists files and acknowledges completion through Valkey. Prometheus and Loki provide complementary metrics and request evidence.

```text
Client / benchmark feeder
          |
          v
Bulk-crawl API --> durable job manifest / outbox
          |                    |
          +------ dispatcher --+
                       |
                       v
              Valkey shared frontier
          host queues + ready-host index
          task states + leases + robots cache
                       |
                  atomic CLAIM
                       |
                       v
             Native Scrapy workers
            robots / local scheduling
                       |
         validate shared rate permission
            immediately before HTTP
                       |
                       v
               External HTTP target
                       |
           release network reservation
                       |
                       v
              Upload HTML + metadata
                       |
                       v
            API persists files --> ACK
                       |
                       v
             Job progress / download

Prometheus + Loki observe workers, API, coordinator, and storage.
```

The POC uses nodes `172.16.191.1`, `.3`, and `.4`. Production workloads on `.2` are separate. Reading the production URL database for benchmark sampling does not mean the distributed crawler is deployed in production.

### 3.2 Frontier data structures and atomicity

| Valkey structure | Type | Purpose |
| --- | --- | --- |
| `poc:q:<hostname>` | List | Pending serialized tasks for a host |
| `poc:ready` | Sorted set | Hosts indexed by the next scheduling-check time |
| `poc:host:<hostname>` | Hash | Authoritative next HTTP authorization time |
| `poc:net:<hostname>` | Sorted set | Shared network reservations indexed by expiration |
| `poc:states` | Hash | Pending, leased-token, or terminal task state |
| `poc:leases` | Hash | Lease token, worker, task, and deadlines |
| `poc:lease_deadlines` | Sorted set | Expiration index for abandoned-task recovery |
| `poc:job:<job_id>` | Hash | Job state, expiration, and completion counters |
| `poc:origins:v3` and robots-body keys | Hash and expiring strings | Small readiness metadata and separately bounded cached rules |

Lua scripts make enqueue, claim, requeue, recovery, robots ownership, and acknowledgement transitions atomic on the Valkey primary. `poc:ready` provides scheduling hints; scripts still validate robots readiness, ownership, job state, delays, and capacity. A ready score alone never authorizes HTTP.

### 3.3 Leases, actual request pacing, and recovery

A successful claim assigns a unique token and a 180-second task lease. Network reservations also have bounded deadlines. The worker validates or acquires the host's rate permission immediately before the HTTP handler starts, after local Scrapy queueing. The shared POC host limit is 32 authorizations/s with at most 16 reserved network slots. Retries, redirects, and robots requests also pass through distributed admission.

The network slot remains reserved until the download finishes or is cancelled; it is then released while result upload can continue. Downloads are bounded by both Scrapy timeouts and the remaining reservation lifetime. This addresses the earlier risk that a permit could expire while an HTTP transfer remained active.

Recovery returns expired owned tasks to the frontier and gives a subsequent worker a new token. A stale worker cannot acknowledge through an invalidated token. Execution remains **at least once**: a crash between file persistence and Valkey acknowledgement can lead to another fetch. Filesystem writes and ACK are separate operations, and the implementation does not provide an exactly-once transaction across them. Deadline passage makes a lease recoverable; ACK currently checks ownership rather than independently rejecting every past-deadline token. See [lease.md](lease.md) for the detailed lifecycle.

## 4. Sustained one-million-URL experiment

### 4.1 Objective and configuration

The experiment tested whether bounded state could process a larger, predominantly cold-host workload without repeating earlier memory exhaustion. It also profiled the gap between high HTTP activity and useful page completions.

| Setting | Deployed one-million-URL baseline |
| --- | --- |
| POC application image | `docker.io/crawler-poc/app:0.16` |
| Workers | 100 across three nodes |
| Per-worker limits | 4 CPUs, 2 GiB RAM, 30 active domains, up to 256 owned tasks |
| API | Eight processes, 12 CPUs, 4 GiB RAM |
| Outer POC node limits | 40 CPUs and 48 GiB RAM per node |
| Valkey | Version 9.1.3; four I/O threads; four CPUs |
| Valkey memory | 8 GiB container, 6 GiB `maxmemory`, `noeviction` |
| Persistence | AOF enabled, `appendfsync everysec`; automatic RDB snapshots also enabled |
| Robots cache | 100,000 entries; 2 GiB encoded compressed-body budget |
| Pending frontier | At most 100,000 URLs; additional accepted work remains in the outbox |
| Result storage | 250 GiB PVC, backed by a 300 GiB filesystem pool |
| Native transport | Scrapy 2.19.0 / Twisted 26.4.0 HTTP/1.1 |
| Local adaptive behavior | AutoThrottle target two; native retries, redirects, and robots behavior retained |
| Monitoring | Approximately 15-second benchmark samples, Prometheus metrics, Loki request logs, and coordinator profiling |

Worker CPU limits are ceilings, not dedicated allocations; summing them would overstate available node CPU.

### 4.2 Dataset preparation and capacity checks

The input contains **1,000,000 unique URLs across 875,326 normalized hosts**, with at most five URLs per host. Sampling used read-only `should_crawl=true` URL-state shards. Hosts from previous datasets and the inspected cache exclusion list were removed to reduce reuse. This avoids a deliberately warmed benchmark, although external CDNs, DNS infrastructure, or upstream caches cannot be assumed cold.

The input file is `db-fresh-hosts-1m-20261108-urls.txt`, 46,667,140 bytes, with SHA-256:

```text
a013438721607fae318d0f77c7f18d3f2342eb4514201b521c3204479ebb44ad
```

Capacity was checked before starting. The PVC/project quota was expanded from 200 to 250 GiB and verified against the actual filesystem. A conservative estimate based on the largest measured 100k output used 132.65 GiB for one million URLs, a 30% allowance, 12 GiB for persistence, and 20 GiB reserve: approximately 204.45 GiB required. Authorized cleanup of old results freed approximately 30.86 GiB. Immediately before the fresh run, the volume had approximately **249.76 GiB free and 19.28 million free inodes**.

The workload was submitted as **1,000 jobs of 1,000 URLs**. Submission began at **2026-11-08 13:14:38 UTC**. All job records were audited after completion; the last job completed approximately 3,053.20 seconds after the benchmark start. The final periodic sample arrived at 3,060.42 seconds.

### 4.3 Final accounting and full-run performance

| Result | Measurement |
| --- | ---: |
| Submitted / completed URLs | **1,000,000 / 1,000,000** |
| Successful URLs | **504,468** |
| Failed URLs | **495,532** |
| Success fraction | **50.4468%** |
| Time to last job completion | **3,053.20 s, approximately 50m 53s** |
| Full-run completed URLs/s | **327.5** |
| Full-run successful URLs/s | **165.2** |
| Observed HTTP counter increase | **3,164,806 attempts** |
| HTTP rate over periodic sample interval | **1,034.2 attempts/s** |
| Stored HTML files | **653,481** |
| Stored HTML body bytes | **77,177,137,525 bytes, approximately 71.88 GiB** |
| Pending URLs / active leases at final audit | **0 / 0** |
| Worker restarts / replacements | **0 / 0** |
| New Valkey OOM errors | **0** |
| Free result-volume space at later audit | **Approximately 171.17 GiB** |

Full-run URL rates use benchmark start to the last job's completion. The HTTP rate uses the first and last available counter samples, approximately 3,060.09 seconds apart; it is not divided by the shorter job-completion interval. HTTP counter observations include robots, redirects, retries, and failed requests. Stored HTML includes error-response bodies and therefore exceeds the successful-URL count.

### 4.4 Sustained interior-window performance

To distinguish sustained processing from startup and final slow URLs, this report additionally evaluates samples within **300–2,400 seconds after start**. The available sample endpoints are **301.39 and 2,386.69 seconds**, giving **2,085.30 seconds, approximately 34m 45s**, of observation. This is a descriptive interior-window analysis, not a separately randomized benchmark or an assertion that every minute achieved its average.

| Interior-window measurement | Result |
| --- | ---: |
| Completed input URLs | 850,357 |
| Successful input URLs | 421,270 |
| Failed input URLs | 429,087 |
| Completed URLs/s | **407.8** |
| Successful URLs/s | **202.0** |
| Failed URLs/s | **205.8** |
| HTTP attempts/s | **1,280.6** |

This sustained interval confirms the observation that approximately 1.2k HTTP attempts/s can coexist with approximately 200 successful pages/s. It also shows why reporting only a peak request rate or only the full-run average would omit relevant behavior.

### 4.5 Comparison with the initial production baseline

Here, **initial performance means the current production system before distributed deployment**. The earlier 400k POC is historical evidence, not the production baseline.

| System and measurement | HTTP attempts/s | Completed URLs/s | Successful URLs/s | Deployment interpretation |
| --- | ---: | ---: | ---: | --- |
| Current production, user-observed baseline | **~500** | Not supplied | Not supplied | Existing system; distributed framework not deployed |
| Distributed POC, sustained 1M interior window | **1,280.6** | **407.8** | **202.0** | Experimental 100-worker deployment |
| Distributed POC, whole-run observations | **1,034.2** | **327.5** | **165.2** | Includes startup and final tail; HTTP and job endpoints differ slightly |

The observed HTTP activity ratios are approximately **2.56×** for the sustained interval and **2.07×** for the full-run HTTP interval relative to ~500. These ratios do not establish equal-resource scaling, equal input quality, improved success rate, or production-ready capacity. A matched production/POC dataset and resource budget are required to measure useful-throughput improvement fairly.

## 5. Retention, memory, and 24/7 operation

The old unbounded robots cache occupied approximately 4 GiB. Workers were quiesced for migration, the legacy namespace was retired after moving all processes to the new format, and memory dropped sharply. That drop reflects cache removal, not a claim that all later workloads require only that small amount of memory.

The deployed cache separates small readiness metadata from encoded compressed bodies. Successful entries expire after approximately **19.2–24 hours**; failed fetches and HTTP 5xx entries expire after **4–5 minutes**. Reads do not extend freshness. Entry and byte caps trigger cache-specific LRU eviction without evicting pending tasks or leases. Generation and lease-token checks reject stale cache writers. Missing rules cause one deduplicated refetch, and dependent pages wait.

Terminal task tombstones expire after one hour; idle host clocks expire after ten idle minutes. Pending and leased task ownership does not expire merely to free memory. Job metadata is retained until the API retires result files/manifests and adjusts storage accounting. The 100k pending-URL cap prevents the entire million-URL workload from entering Valkey simultaneously.

During the run the cache reached its 100k-entry ceiling. A later audit showed 964,186 evictions, 43,701 expirations, and approximately 1.073 million stored cache responses. These event counters include redirects/refetch behavior and should not be interpreted as unique input hosts. Valkey memory at the later audit was approximately **887 MiB allocated and 1.19 GiB RSS**. Its lifetime peak includes earlier workloads and is unsuitable as the one-million-run peak.

The completed experiment supports bounded processing for this finite workload. It does **not** establish indefinite 24/7 sustainability. At the observed output volume and runtime, uninterrupted comparable traffic would produce HTML on the order of **2 TB/day**, before metadata and replication. Production needs an explicit result-retention or object-storage lifecycle, capacity monitoring, and cleanup verification. A 250 GiB local volume is a benchmark provision, not sufficient long-term storage by itself.

## 6. Bottleneck analysis: HTTP activity versus successful output

### 6.1 Request composition and external failures

A live one-minute diagnostic window recorded approximately **587.6 robots starts/s and 600.3 page starts/s**, totaling **1,187.8 HTTP starts/s**. Successful URL commits were approximately **188.5/s**, close to page HTTP-200 activity of **184.6/s** plus other successful responses. Thus, the observed gap is largely explained by request composition and failed or intermediate attempts; it does not indicate that hundreds of successful pages per second disappeared in upload.

Page activity included approximately 94 redirects/s and 244.6 network failures/s. Network failures included approximately 156.4 name-resolution failures/s and 62.5 download timeouts/s. Page status activity included approximately 23.2 HTTP 403s/s, 30.0 HTTP 429s/s, and 18.5 HTTP 404s/s. These are live-window event rates, not the complete one-million-run terminal failure distribution. Backend failures also occurred, so external failures cannot explain every unsuccessful URL without a complete final error classification.

### 6.2 Shared coordinator contention

Valkey's main execution thread was active for approximately **99.9%** of a profiled interval. It handled approximately **4,472 Lua evaluations/s**, **64,172 HGETs/s**, and **10,742 queue pops/s**. Frequent polling of robots-blocked hosts created considerable scheduler work relative to useful pages.

Client-observed coordinator calls took approximately 109–115 ms on average across claim, network admission, lease lookup, validation, and ACK. Four I/O threads assist networking but do not make shared Lua execution parallel. Increasing workers alone can increase contention at this serial coordination point.

The profile also found **eight overlapping dispatcher scans**: the deployed scanner lock could expire after one second while a scan was still running. Additional API processes could therefore duplicate scans instead of increasing useful dispatch capacity.

### 6.3 Upload and disk findings

The same diagnostic window showed approximately 247 active uploads against a configured aggregate capacity of 800, one queued upload waiter, and mean slot wait around 1.88 ms. Upload HTTP time averaged approximately 471 ms, while API result handling averaged approximately 431 ms. HTML and metadata fsync averaged approximately 4.16 and 4.42 ms respectively.

These observations prioritize coordinator efficiency and request outcomes over introducing an upload message queue immediately. Upload latency remains measurable and should continue to be profiled, but this window does not show a saturated upload-slot budget or fsync dominating request completion.

## 7. Validation and evidence

Validation combined local regression tests, isolated real-Valkey checks, native HTTP integration, Kubernetes health observations, and post-run accounting:

- The deployed retention implementation passed **59 local tests**, plus isolated DB-15 checks for retention, coordinator behavior with 100 concurrent claimers, and domain budgeting.
- A native HTTP/robots alignment test completed nine inputs with six expected successes and three expected denials.
- The subsequent local efficiency candidate passed **66 tests**, including renewed scanner ownership, wake-up ordering, blocked-host backoff, and DNS lookup coalescing. These passing tests do not establish a deployed performance improvement.
- The benchmark completed all 1,000 jobs; final direct coordinator accounting matched one million terminal input URLs with no pending work or leases.
- All 100 workers remained ready with zero restarts/replacements. API and Valkey pods also did not restart during the run.

Primary local evidence is under `artifacts/distributed-poc/`:

| Evidence | Purpose |
| --- | --- |
| `run-1m-retention-20261108/metadata.json` | Exact image, resources, dataset identity, retention settings, and start time |
| `run-1m-retention-20261108/samples.jsonl` | Periodic counters used for full-run and sustained-window calculations |
| `run-1m-retention-20261108/final.json` | Benchmark recorder's final observation |
| `run-1m-retention-20261108/job-final.json` | Direct job totals, pending/leases, storage, memory, persistence, and error counters |
| `run-1m-retention-20261108/success-bottleneck-live.json` | Live request composition and stage profiling |
| `run-1m-retention-20261108/watch.jsonl` and pod snapshots | Worker health and sampled retention checks |
| `run-1m-retention-20261108/valkey-full-run.log` | Persistence failure and subsequent recovery evidence |
| `capacity-1m-20261108/` | Storage-capacity checks and preparation |

These experiment artifacts are ignored by Git and must be retained or exported separately if a grader requires raw evidence from another checkout. The report records the key results independently of those files. Implementation references include [scripts.py](poc/distributed/app/scripts.py), [retention.py](poc/distributed/app/retention.py), [api.py](poc/distributed/app/api.py), [scrapy_components.py](poc/distributed/app/scrapy_components.py), and [benchmark400_native.py](poc/distributed/benchmark400_native.py).

## 8. Current blockers and limitations

| Priority | Blocker | Evidence / consequence | Required next step |
| --- | --- | --- | --- |
| High | Valkey persistence child crash | RDB background child terminated with SIGSEGV at approximately 13:22:22 UTC; 10,789 new `MISCONF` errors were recorded | Identify and reproduce the persistence failure; validate a mitigation while retaining explicit AOF durability guarantees |
| High | Infrastructure errors may become URL failures | Coordination failures can reach task failure handling; their contribution to final failures is not yet separated | Audit final errors and requeue transient coordination failures without incorrectly recording permanent site failure |
| High | Serial coordinator saturation | Approximately 99.9% main-thread activity and heavy blocked-host polling | Deploy and compare bounded scans, larger claim batches, and immediate robots wake-up behavior |
| High | Useful-throughput gap | Approximately 202 successful URLs/s during 1,281 HTTP attempts/s sustained activity | Reduce redundant robots/DNS work, classify final failures, and evaluate domain policies without weakening success criteria |
| High | 24/7 output growth | Approximately 77.18 GB of HTML in one finite run | Establish storage retention/object lifecycle and verify cleanup under ongoing load |
| Medium | Excess cache churn | 100k-entry cap against 875k input hosts; approximately 964k evictions at later audit | Validate the prepared 500k-entry / 3 GiB cache budget within actual memory and persistence headroom |
| Medium | Profiler stream interruption | Long-running coordinator exec stream ended with unexpected EOF | Use a detached file-backed observer and display data age explicitly |
| Medium | Deployment and comparison incomplete | Distributed framework remains POC; latest candidate not deployed | Run the candidate with comparable input/resources before production rollout |
| Medium | Ingestor failed-batch recovery | Retry exhaustion or ambiguous COMMIT can reach a handler that advances checkpoints | Add durable recovery that avoids duplicating additive counters |

The persistence crash did not restart the Valkey parent. A subsequent RDB save succeeded, and the later audit reported successful AOF writes and rewrites. This recovery does not erase the incident or prove crash/power-loss resilience. The one-million run must not be described as free of backend errors. AOF `everysec` also allows a recent-write loss window; no power-outage recovery experiment was performed here.

The coordinator profiler's raw INFO stream stopped before the run ended. Prometheus benchmark samples continued, worker-health monitoring remained live, and final accounting was read directly. Consequently, exact continuous Valkey allocated-memory maxima and uninterrupted raw INFO coverage are not claimed.

Different input distributions, host overlap, cache state, worker counts, and observation windows limit historical comparisons. In particular, scaling from 50 to 100 workers is not an isolated experiment on worker count once other settings change. A 51-minute finite run is stronger evidence than a brief peak, but remains shorter than a multi-day reliability test.

## 9. Prepared improvements and deployment plan

The next local candidate, image configuration `0.17`, prepares the following changes. **They were not deployed in the reported one-million-URL run:**

1. Raise the robots cache from 100k entries / 2 GiB to **500k entries / 3 GiB**, retaining Valkey's 8 GiB container and 6 GiB `maxmemory`. The body budget excludes key, index, allocator, client, and persistence overhead.
2. Replace the one-second scanner lock with a **token-owned 30-second lease renewed every ten seconds**, stopping a scan after loss of ownership.
3. Bound each claim's candidate examination to 64 and increase the maximum returned batch from eight to 16, subject to worker/domain/host capacity.
4. Back off robots-blocked hosts to one-second checks and wake waiting pages immediately at their original scheduling priority when rules become ready.
5. Coalesce concurrent DNS lookups for the same name while preserving independent cancellation and retryable failures; do not cache a transient failure as a permanent negative result.
6. Make coordinator profiling file-backed and tolerant of individual sampling errors.

Before production deployment, the candidate needs real-Valkey integration checks, native HTTP alignment, a comparable one-million-URL run, persistence-failure handling, and a storage lifecycle. The next evaluation should report HTTP attempts, terminal completions, successful pages, backend failures, cache churn, latency, resource use, and the full slow tail together. Success means more useful pages under explicit correctness and resource limits, rather than a higher request counter alone.

## 10. Earlier reporting period and historical results

The following section preserves the earlier midterm record. Its worker counts, storage size, and benchmark figures describe that period rather than the latest configuration. PR #87's status has been corrected to merged.

## 2026-10-26~2026-11-2 (+Midterm Report)

### Business Impact

- Delivered on-demand bulk crawling with progress tracking, result downloads, cancellation, and retention through merged [PR #84](https://github.com/NTU-CSIE5376/WebCrawler/pull/84).
- [PR #87](https://github.com/NTU-CSIE5376/WebCrawler/pull/87), now merged, demonstrated **5.56× Wikipedia throughput** and **98.45% successful pages** in its 10k-URL benchmark.
- Validated distributed crawling with 400k real URLs: approximately **417 completed URLs/s overall**, **63.65% success**, and **zero worker restarts**. Failure analysis identified access restrictions, DNS failures, robots rules, and broken links as major issues.

### Technical Detail

- Added atomic job submission, configurable manual/normal scheduling, URL validation, and TTL-based cleanup.
- Introduced declarative site policies, AutoThrottle settings, and site-specific middleware in PR #87.
- Built a three-node Kubernetes POC with **50 native Scrapy workers**, AutoThrottle enabled, **30 active domains per worker**, and Valkey enforcing a shared **32 QPS ceiling per domain**.
- Integrated Prometheus/Grafana monitoring and expanded HTML storage to a **200Gi PVC**; cleared previous results before rerunning the benchmark.
