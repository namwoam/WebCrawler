# Lease deadlines and per-host admission: pseudocode

The current implementation assigns a deadline **per claimed task/token**, not one deadline per domain. Each host has a set of tokens with individual expiration times. The lease duration is fixed at **180 seconds**; QPS, latency, queue length, and worker count do not change that duration. There is no task-lease renewal.

Here, “domain” means the normalized URL hostname: lowercase with a trailing dot removed. For example, `www.example.com` and `api.example.com` have separate budgets in this POC. They are not combined into one registered-domain budget.

## Formulas

```text
claim_time       = Valkey.TIME()             # server time, seconds
lease_deadline   = claim_time + 180          # ownership through result upload
network_deadline = claim_time + 180          # upper bound on network reservation

interval         = 1 / HOST_QPS             # default: 1/32 = 0.03125 seconds
host_next_start  = authorization_time + interval

remaining = min(lease_deadline,
                network_deadline,
                host_reservation_score) - Valkey.TIME()
```

All tokens created by one CLAIM Lua execution use that execution's server timestamp. Subsequent claims get their own timestamps. Host rate-clock TTL and robots-cache TTL are separate from task deadlines.

## 1. Claim a task and reserve host capacity

This pseudocode summarizes the successful admission path. Each atomic block represents a Lua script; scheduling details and invalid-task cleanup are abbreviated.

```text
ATOMIC CLAIM(worker, capacity):
    now = Valkey.TIME()

    for each eligible candidate, subject to scan and batch limits:
        task = candidate_from_ready_host_queue()
        host = task.group

        remove reservations from network[host] with deadline <= now

        if task is not pending OR job is cancelled/expired:
            perform invalid-task cleanup
            continue

        ensure robots readiness for page task
        skip_http = task has an explicit denied robots state

        if robots pending OR task.not_before > now:
            return task to queue and schedule another check
            continue

        if NOT skip_http:
            if host_next_start[host] > now:
                return task to queue and schedule another check
                continue
            if number_of(network[host]) >= task.concurrency:
                return task to queue and schedule another check
                continue

        token = unique_claim_id + candidate_index
        task.token = token
        task.worker = worker
        task.attempt += 1
        task.network_started = unset
        task.granted_at = now
        task.network_deadline = now + 180
        task.lease_deadline = now + 180

        if NOT skip_http:
            network[host].add(token, score=task.network_deadline)

        states[task.id] = "leased:" + token
        leases[token] = task
        lease_deadlines.add(token, score=task.lease_deadline)
        return task to worker

    # Claim reserves capacity but does NOT advance host_next_start.
```

## 2. Authorize immediately before HTTP

```text
ATOMIC START_NETWORK(token):
    now = Valkey.TIME()
    task = leases[token]

    if task missing OR states[task.id] != "leased:" + token:
        return EXPIRED
    if task's job is cancelled or expired:
        return CANCELLED

    reservation = network[task.group].score(token)
    if reservation missing:
        return EXPIRED

    remaining = min(task.lease_deadline,
                    task.network_deadline,
                    reservation) - now

    if remaining < 20 seconds OR task.network_started is set:
        return EXPIRED

    if host_next_start[task.group] > now:
        return WAIT(host_next_start[task.group] - now)

    host_next_start[task.group] = now + task.interval
    expire host clock after max(idle_host_TTL,
                               ceil(task.interval + 180)) seconds
    task.network_started = now
    leases[token] = task
    return STARTED(remaining)

WORKER_DOWNLOAD(task):
    repeat:
        checked = monotonic_time()
        permission = START_NETWORK(task.token)

        if permission is WAIT:
            asynchronously sleep a short interval, then recheck
        else if permission is EXPIRED:
            attempt token-checked REQUEUE for a fresh claim
            stop this attempt
        else if permission is CANCELLED:
            stop this attempt
        else:
            round_trip = monotonic_time() - checked
            timeout_budget = min(30 seconds,
                                 permission.remaining - round_trip - 5 seconds)
            start native HTTP with this outer timeout bound
            # Native DNS/download timeouts also remain active.
            wait for completion, or cancel and stop the HTTP operation
            release network[task.group][task.token]
            break

    upload result while task lease remains owned
    persist files, then token-check and ACK
```

Network permission is consumed once per token. Retries and redirects requeue and obtain another claim/token. Handler authorization is paced centrally; DNS, connection establishment, and scheduling still affect arrival times at the remote server.

## 3. Recover expired ownership

```text
ATOMIC RECOVER():
    now = Valkey.TIME()
    expired = first 128 tokens with lease_deadline <= now

    for token in expired:
        task = leases[token]
        if task exists:
            if states[task.id] == "leased:" + token:
                states[task.id] = "pending"
                queue[task.group].append(task)
                mark host ready if it has no ready-index entry
            remove token from network[task.group]

        remove leases[token]
        remove lease_deadlines[token]

    # A later claim assigns a NEW token and NEW now + 180 deadlines.
```

Recovery requires a running worker to invoke it. Passing the deadline does not itself run a callback, stop HTTP, or delete the lease. The worker's bounded download stops local network work; recovery invalidates abandoned ownership. ACK currently checks token ownership, so it can succeed after the nominal deadline if recovery has not invalidated the token yet. Once invalidated, a stale token cannot acknowledge completion.

## Example: two tasks on the same host

```text
Task A claimed at t=100.000 -> lease/network deadline = 280.000
Task B claimed at t=100.010 -> lease/network deadline = 280.010

A authorized at t=100.020 -> host_next_start = 100.05125
B checks at t=100.030    -> WAIT 0.02125 seconds
B authorized at t=100.052 -> host_next_start = 100.08325

A download ends at t=101 -> release A's network reservation
                            A's task lease remains until ACK/recovery
B download ends at t=102 -> release B's network reservation

A result persisted and ACKed at t=103 -> remove A's task lease
```

Neither waiting for rate permission nor uploading extends the original deadline. A task delayed until fewer than 20 seconds remain is returned for a fresh claim before starting HTTP. A task delayed beyond its deadline during upload can be recovered and executed again; the framework provides at-least-once execution.

## Implementation references

- [CLAIM, START_NETWORK, ACK, and RECOVER](poc/distributed/app/scripts.py)
- [Hostname grouping and request interval](poc/distributed/app/policy.py)
- [HTTP timeout and reservation release](poc/distributed/app/scrapy_components.py)
- [Detailed lease lifecycle](lease.md)
