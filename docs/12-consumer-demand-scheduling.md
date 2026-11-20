# Consumer-driven offer allocation

The offerer previously issued at most 100 URLs per domain, skipped refilling
above its queue-domain watermark, and slept 60 seconds after completing a pass.
In a read-only production check on November 20, 2026, recent refill passes spent
19–37 seconds peeking and another 17–40 seconds claiming/publishing. A domain
that can drain six URLs per second consumes a 100-URL batch in about 17 seconds.
Other domains' ready files can therefore keep that domain starved.

The new path is opt-in. It keeps consumer internals behind an IPC boundary:
the offerer reads neither crawler site policies nor QPS, concurrency, throttle,
or hostname rules. Within-domain claim ordering, scoring, rate enforcement,
manual-source selection policy, and the background ranker are retained. The opt-in
normal dispatch queue now consumes offer priority. A claim also rechecks
the domain's pause state before modifying URL rows.

## Demand contract

Each consumer atomically writes `demand.snapshot` in its own queue directory:

```json
{"version":1,"generated_at":1795190000,"capacity":5000,"domains":{"123":900}}
```

`capacity` is the additional inventory the consumer can accept in total.
Each domain value is its desired additional URL count, after subtracting its
current inventory. These are inventory requests, not permission to exceed a
request rate. Feedback expires after 30 seconds; malformed feedback falls back
to ordinary scheduling. No more than 2,000 domain entries are accepted. The
filename does not match the ready-offer pattern `domain_*.json`.

The crawler adapter estimates normal-work drain rate from completions while a
domain has a batch. Idle time after draining is excluded so supply starvation
does not reduce future demand. Its target is 180 seconds of observed work, with
a 100-URL bootstrap floor and 2,000-URL maximum. State is bounded to 2,000 domains
and inactive entries expire after ten minutes. Restarting loses these estimates;
priority-aware bootstrap offers initialize them again. This adapter has no
site-specific rules. The snapshot also includes bounded `inventory` and
`targets` maps for diagnostics; the offerer does not interpret crawler internals.

The crawler publishes every periodic top-up (normally five seconds). It can
admit valuable ready offers, including newly observed domains, above the ordinary
domain watermark. It may prefetch another batch for an active domain when that
domain has at most 100 pending URLs. It
caches file priorities by mtime without retaining decoded URL lists. Ready offers
remain on disk until the complete file fits within its 32,000 pending-URL limit.
This is a count bound, not an RSS limit. Existing container memory limits remain
necessary. A queued offer and an active batch may coexist for a domain.

## Allocation

For requested domains with positive effective value, maximize

`sum(value[i] * x[i])`

`value` is the highest refreshed URL score in the bounded usable frontier;
domain score bootstraps an unscored frontier. A refreshed zero remains zero.
This retains manual URL priorities above the model's [0, 1] range. Bounded
per-shard LATERAL probes return at most the requested/per-domain-cap number of
eligible rows, rather than counting a domain's entire database frontier.

subject to integer `x[i] >= 0`, binary `y[i]`,
`x[i] <= min(eligible_available[i], requested[i], per_domain_cap) * y[i]`,
`sum(x) <= min(consumer_capacity, offerer_url_budget)`, and
`sum(y) <= min(demand_domain_slots, remaining_queue_slots)`.

The default allocator uses simulated annealing initialized with the score-ordered
greedy allocation. It retains the best feasible plan, limits search to 20 ms or
2,000 iterations, and skips the search when the greedy solution is already
optimal (the URL budget is filled, all domains fit, or demands are equal).
The time budget is checked between iterations, not a hard real-time guarantee.
It uses no numerical solver and no crawler policies.

The optional SciPy HiGHS-backed `milp` solver has a one-second solve limit and a
1% relative-gap target. Any returned incumbent is checked for feasible URL and
domain limits and compared with a feasible greedy baseline, retaining the
higher-value plan. Its fallback is the bounded annealing result, which never
discards the greedy baseline. Neither heuristic is necessarily optimal under
a domain-slot constraint. Candidate lookup is restricted to the reported
IDs, this offerer's shard range, positive value, and unpaused domains. Each
claim retains URL score ordering, eligibility checks, and `SKIP LOCKED` handling.
There are no domain-name exceptions or new indexes.

Already-ready domain files are excluded and never overwritten by replenishment.
Global bootstrap refill runs afterwards to discover new domains. With the flag
enabled, it checks remaining file slots above the old low watermark and examines
at least 200 candidates per shard (subject to the configured peek hard cap).
A one-row eligible URL priority probe replaces the old existence-only probe.
Results are reranked by effective value within this bounded candidate window;
this is not a global maximum over every URL/domain in the database.

With fresh consumer headroom, bootstrap offers can use the 2,000-URL cap;
without feedback they retain the conservative 100-URL starting size. Bootstrap
and demand offers share the 10,000-URL cycle budget. Consumer admission remains
bounded even when an unrequested bootstrap offer stays queued on disk.
The optimizer only
controls supplemental demand allocation; it does not claim global fairness or
optimal end-to-end throughput. Exhausted or locked frontiers can return fewer
URLs than planned. The allocation is not re-solved after each partial claim.
With demand enabled, polling uses a start-to-start interval instead of adding
a full sleep after every completed cycle. A slow database pass can still delay
the next demand cycle beyond the inventory horizon.

## Crawler dispatch

Offer payloads carry a numeric `priority`. The crawler maps it to normal request
priority and uses a policy-slot-aware priority queue. Valuable ready work gets
available capacity first; domains at their concurrency cap stay queued while
other domains use spare capacity. Local occupancy includes requests waiting in
middleware and manual-source requests. Requests sharing a policy slot share its
admission count. Existing downloader delays and shared cross-process concurrency
leases still enforce their configured limits. With weighted source selection,
the normal and manual queues share policy-slot admission: when both have work,
the manual allocation uses the configured share (at least one slot), and normal
work uses the remainder. Either source can borrow spare slots when the other
has no work. Balanced selection uses equal shares; the explicit source-first
policies retain their precedence.

This branch builds on `namwoam:perf/link-processing` (PR #142), using its bounded
HTML parser process pool and shutdown handling. It does not add a second parser
pool implementation or change crawler site-policy limits.

The canary logs headers-to-body time, response size, normal parse duration, and
slot delay/occupancy to distinguish supply starvation from downloader/CPU limits.
`pre_response_ms` remains a residual; it must not be interpreted as pure queue
wait because it can include body transfer and middleware processing.

## Activation and validation

Both flags default off:

- Set `offerer.demand_refill_enabled: true` in control YAML. Budgets are
  `demand_url_budget: 10000`, `demand_per_domain_cap: 2000`, and
  `demand_domain_slots: 16`, per offerer per pass.
- `demand_allocation_method` selects `annealing` (default), `greedy`, or `mip`.
- Set crawler environment `IPC_DEMAND_ENABLED=true`. Consumer targets live in
  crawler settings as `IPC_DEMAND_HORIZON_SEC`, `IPC_DEMAND_MAX_DOMAIN_URLS`, and
  `IPC_DEMAND_MAX_PENDING_URLS`.

Deploy the consumer adapter first, then enable one matching offerer. Disabling
the offerer flag restores its previous behavior; disabling the consumer flag
restores its prior admission policy. Old demand files expire automatically.
Production activation is limited to an isolated crawler 0 / offerer 0 canary;
the repository defaults remain off. The temporary programs use a copy of the
production checkout plus the changed files, retaining the separately deployed
ingestor/ranker fixes. A host watchdog restores the original worker programs
after the measurement window. Other worker PIDs and production source files
are checked for changes.

Tests compare small integer allocations to exhaustive optimal solutions, check
stale/malformed feedback, preserve existing queue files, exercise failure
fallbacks and receiver inventory bounds, and simulate replenishment of a
six-URL/sec consumer with 120-second supply cycles. Simulation is not measured
production improvement.

`containers/scheduler_control` owns its tests, `Taskfile.yml`, and one
`requirements.txt`. Docker and `task install` use that same file. CI includes
the service in its matrix and invokes `task test`, including the former root
offerer/ranker tests and approved-configuration checks. No separate CI test
step or test-only requirements file is needed.

For a production canary, compare 15–30-minute normal-source request starts and
completions by registered domain, time with no normal inventory, successful
HTML pages per second, offer/claim latency, errors/429s, pending requests, and
RSS. Separate manual traffic and retries from normal throughput. Inspect
`offer.demand` for allocated URL volume and `offer.allocation_fallback` for solver
fallbacks. The earlier 15-minute log sample contained 513 Wikipedia request-end
events across logged traffic (about 0.57/sec); it was not a controlled baseline
or an isolated normal-source measurement. Repeat the baseline immediately
before the canary because cluster rebalancing and manual load can change it.

Solver reference: https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.milp.html
