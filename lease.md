# Distributed crawler lease process

A lease gives a worker temporary ownership of a crawl task. It allows the crawler to recover work when a worker crashes or cannot finish uploading its result. This document describes the implementation in `poc/distributed`.

## Two deadlines

Each successful claim creates a unique token and two deadlines, calculated using Valkey's server time.

| Reservation | Duration from claim | Purpose | Normal release |
| --- | --- | --- | --- |
| Network reservation | Up to 180 seconds | Reserves a slot in the host's shared concurrency budget, including local queueing | After the HTTP download finishes |
| Task lease | 180 seconds | Tracks ownership through downloading and result upload | When completion is acknowledged |

Releasing a network reservation does not complete the task. The worker can upload its result while another request uses the freed host capacity. The current implementation uses fixed deadlines without lease renewal. Rate permission is granted separately, immediately before invoking the HTTP handler.

## Lifecycle

```text
                      +-------------------------+
                      | Task pending in Valkey  |
                      +------------+------------+
                                   |
                      CLAIM validates admission
                                   |
                                   v
                      +-------------------------+
                      | Leased to Worker A      |
                      | Unique token            |
                      | Network deadline: +180s |
                      | Task deadline: +180s    |
                      +------------+------------+
                                   |
                 +-----------------+------------------+
                 |                                    |
                 v                                    v
       +---------------------+             +-------------------------+
       | Download finishes   |             | Worker crashes or fails |
       | Release net permit  |             | to complete the task    |
       +----------+----------+             +------------+------------+
                  |                                     |
                  v                         Task lease deadline passes
       +---------------------+                          |
       | Upload result       |                          v
       | API validates token |             +-------------------------+
       | Save files, then ACK|             | RECOVER returns task    |
       +----------+----------+             | to the pending queue    |
                  |                        +------------+------------+
                  v                                     |
       +---------------------+                          v
       | Done                |             +-------------------------+
       | Remove lease state  |             | Another worker claims   |
       | Update job counters |             | it with a new token     |
       +---------------------+             +-------------------------+
```

Before downloading, a worker calls `START_NETWORK` after Scrapy's local queue. It waits for the host's shared rate clock, then starts HTTP. Retries, redirects, and reservations that become too old before downloading take an additional path: `leased -> REQUEUE -> pending -> CLAIM`. They obtain a new lease and new rate permission before the next attempt.

## Claiming a task

The worker calls the `CLAIM` Lua script through `AsyncBackend.claim()`. The script selects a host from `poc:ready` and checks the task state, job status and expiration, robots readiness, retry delay, host admission interval, and available host network capacity.

For an admitted task, the script atomically:

1. Creates a token from the claim's UUID and loop index.
2. Records the worker identity and increments the attempt number.
3. Sets `network_deadline = now + 180` and `lease_deadline = now + 180`.
4. Reserves network capacity when required. Claiming does not advance the host's rate clock.
5. Changes the task state from `pending` to `leased:<token>`.
6. Stores the leased task and indexes its expiration deadline.

Atomic execution on one Valkey primary prevents two claims from leasing the same pending task at the same time.

## Downloading and completing a task

The custom Scrapy download handler calls the atomic `START_NETWORK` Lua script immediately before HTTP execution, after local downloader queueing. Using Valkey's server clock, the script checks token ownership, job cancellation and expiration, reservation presence, and both deadlines. If fewer than 20 seconds remain, the worker returns the request to the frontier for another claim.

If the host's next allowed start is in the future, the worker waits and rechecks. On success, the script advances the host's next start time by the configured interval and records `network_started` on the lease. Each token can authorize only one HTTP start. Retries and redirects must obtain new tokens.

The handler bounds the complete download to the smaller of 30 seconds or the server-reported remaining reservation lifetime, minus the admission round trip and a five-second safety margin. It cancels the download before releasing capacity. The normal Scrapy download timeout remains enabled as well. This avoids releasing the concurrency slot solely because an earlier 60-second permit expired while the worker was still downloading.

The gate spaces authorizations at the HTTP handler boundary. DNS, connection establishment, network latency, and process scheduling can still affect when requests reach the remote server; this is not an exact arrival-time guarantee.

After downloading, the worker releases the network permit. For page tasks, the result pipeline uploads HTML and metadata to `POST /_worker/results/{token}` using worker authentication.

The API looks up the lease and checks that the task state still matches `leased:<token>`. Under result locks, it writes the result files and calls the `ACK` Lua script. `ACK` verifies token ownership again, marks the task `done`, removes lease and network entries, and updates job counters. The job becomes `completed` when all its URL tasks are acknowledged.

Robots tasks follow the same lease mechanism, but workers save their robots cache entry directly in Valkey and acknowledge them without uploading page results.

## Recovering abandoned work

Workers attempt recovery approximately once per second. A shared, one-second `poc:recovery-lock` coordinates recovery calls across workers. Each `RECOVER` execution processes up to 128 expired task leases.

For each expired lease, the script checks whether the task still belongs to that token. If it does, the script changes the state to `pending`, appends the task to its host queue, and makes the host eligible for scheduling. It then removes the old lease, deadline index entry, and network permit. A subsequent claim assigns a new token.

Deadline passage makes a lease eligible for recovery; it does not automatically delete the lease. The current `ACK` script checks token ownership rather than comparing the deadline with the current time. An acknowledgement can therefore succeed after the nominal deadline if recovery has not yet invalidated that token.

`CLAIM` removes expired network reservations from the selected host's network set before checking capacity. Expiration itself does not terminate an HTTP request; the worker's bounded download is responsible for stopping local HTTP work before that deadline. Crash recovery still depends on expired reservations and task leases.

## Retry and redirect handling

Native Scrapy retries and redirects pass through the distributed scheduler. `REQUEUE` verifies the current lease token, removes the old reservations, changes the task to `pending`, and appends the updated request to the destination host queue. Request metadata preserves Scrapy's retry and redirect state.

A redirect to a new host must pass that host's admission checks and robots handling before downloading. Requeueing preserves the unfinished job URL count; completion accounting happens through `ACK`.

## Valkey structures

| Key | Type | Role in leasing |
| --- | --- | --- |
| `poc:q:<hostname>` | List | Pending task JSON records |
| `poc:ready` | Sorted set | Hosts scored by their next scheduling check time |
| `poc:host:<hostname>` | Hash | Host's next allowed HTTP start authorization time |
| `poc:net:<hostname>` | Sorted set | Network tokens scored by expiration time |
| `poc:states` | Hash | Task ID mapped to its current state and lease token |
| `poc:leases` | Hash | Token mapped to task JSON, worker, and deadlines |
| `poc:lease_deadlines` | Sorted set | Task tokens scored by recovery deadline |
| `poc:job:<job_id>` | Hash | Job status, expiration, timestamps, and counters |
| `poc:recovery-lock` | Expiring string | Coordinates recovery attempts |

New ready entries use the current server timestamp so newly submitted hosts do not overtake already-ready pages and cause cache churn. The score in `poc:ready` is a scheduling hint. `CLAIM` checks host capacity before reserving a slot; `START_NETWORK` enforces the authoritative rate clock immediately before HTTP.

## Guarantees and limits

- **At least once execution:** a failed worker's task can be reassigned, so a URL may be fetched more than once.
- **Token ownership checks:** after recovery or requeueing invalidates a token, stale acknowledgements cannot update completion counters through that token.
- **Separate storage and acknowledgement:** filesystem writes and the Valkey acknowledgement are separate operations. A failure between them can cause another fetch and result replacement; they do not form an exactly once transaction.
- **Cancellation is cooperative:** the download handler checks cancellation before HTTP execution. Cancellation does not immediately stop every download already in progress.
- **Recovery requires a running worker and Valkey:** deadline passage alone does not execute the recovery script.

## Cache and state retention

Robots responses are compressed into separate `poc:robots:body:<origin>:<generation>` keys. Successful responses expire after 19.2–24 hours; failed fetches and HTTP 5xx responses use a 4–5 minute TTL. Reads do not extend freshness. The cache has a 3 GiB compressed-payload budget and a 500,000-entry limit; its LRU eviction removes only robots-body keys. Byte accounting is conservative until expired entries are collected in bounded batches. Valkey itself retains `noeviction` for scheduler state.

`poc:origins:v3` contains small, per-origin readiness fields. Ready fields expire with their bodies and are removed when the matching cached generation is evicted; pending refresh ownership is preserved. Missing or evicted bodies cause an atomic, deduplicated robots refetch in `ENQUEUE`, `CLAIM`, or `REQUEUE`; queued pages wait for the replacement. A cache miss between claim and middleware returns the URL to the frontier. Generation and lease ownership checks reject late writes from recovered robots workers. Pending robots metadata and pending/leased task states do not expire.

Completed/cancelled task tombstones expire after one hour, exceeding the 180-second lease and recovery window. Expired tombstones cannot make a task pending again: job metadata prevents republication and stale tokens no longer have leases. Job metadata remains until the API retires the manifest/results and discounts HTML bytes, then its cancellation tombstone expires after one hour. Idle host rate clocks expire after ten idle minutes, renewed only by actual HTTP admission. A bounded maintenance pass adds retention to legacy terminal states and host clocks without extending existing TTLs.

Deployment settings: `ROBOTS_CACHE_TTL_SECONDS=86400`, `ROBOTS_FAILURE_TTL_SECONDS=300`, `ROBOTS_CACHE_MAX_BYTES=3221225472`, `ROBOTS_CACHE_MAX_ENTRIES=500000`, `TASK_STATE_RETENTION_SECONDS=3600`, `JOB_RETENTION_GRACE_SECONDS=3600`, `IDLE_HOST_TTL_SECONDS=600`, `MAX_PENDING_URLS=100000`. Terminal retention has a 360-second minimum. The in-memory frontier admits at most 100,000 pending URLs; further accepted jobs stay in the durable filesystem outbox and are retried without partially enqueueing a job. The main Valkey remains at an 8 GiB container limit and 6 GiB `maxmemory`; the cache payload budget excludes key/index/allocator overhead. Cache size, entry budgets and events are exported as `crawler_robots_cache_*` metrics.

For the v2 → v3 upgrade, quiesce workers and verify no active leases or pending URLs before replacing API/workers together. Retire the old `poc:origins:v2` cache only after all processes use v3. This deliberately starts a cold cache and preserves job results and scheduler ownership. `verify_retention.py` verifies real expiry/refetch/eviction on an empty isolated Valkey DB 15; `verify_alignment.py` checks native HTTP and robots behavior on the POC mock target.

## Dispatcher and admission efficiency

The API scanner uses a token-owned 30-second lease, renewed every ten seconds and released only by its owner. A lost lease stops the scan before its next job. Robots-blocked hosts are rechecked at one-second intervals and woken immediately at their original admission priority when rules arrive, including redirected robots fetches. Each claim examines at most 64 queue candidates and returns up to 16 tasks, subject to worker capacity, the 30-domain limit and existing per-host network reservations. Concurrent DNS lookups for the same name share one native resolver operation, with independent cancellation; DNS failures remain retryable and are not negatively cached.

## Implementation references

- [Atomic claim, acknowledgement, requeue, and recovery scripts](poc/distributed/app/scripts.py)
- [Valkey backend and recovery lock](poc/distributed/app/backend.py)
- [Worker admission loop and distributed Scrapy scheduler](poc/distributed/app/frontier.py)
- [Network permit validation and result pipeline](poc/distributed/app/scrapy_components.py)
- [Result receiver and commit handling](poc/distributed/app/api.py)
- [Result upload transport](poc/distributed/app/result_upload.py)
