"""Bounded value/demand allocation, independent of the consumer implementation."""
from __future__ import annotations

import logging
import math
import random
import time

logger = logging.getLogger("offerer")


def _anneal(rows, caps, baseline, *, url_budget, domain_slots, time_budget_sec=0.02,
            max_iterations=2000, seed=0):
    """Search domain subsets; greedy URL allocation is exact within a subset.

    Keep the best feasible solution, even when accepting a worse intermediate
    state to escape a local maximum. Both time and iteration limits apply.
    """
    if (sum(baseline.values()) == url_budget or len(baseline) == len(rows)
            or len(set(caps)) == 1):
        # The unconstrained score-ordered solution already fits the slot limit.
        return baseline
    deadline = time.monotonic() + time_budget_sec
    scale = max(c.allocation_value for c in rows)
    weights = [c.allocation_value / scale for c in rows]
    active = [i for i, c in enumerate(rows) if c.domain_id in baseline]
    active_set = set(active)
    inactive = [i for i in range(len(rows)) if i not in active_set]
    if not inactive or not active:
        return baseline

    def evaluate(indices):
        remaining, value = url_budget, 0.0
        amounts = {}
        for index in sorted(indices):
            count = min(caps[index], remaining)
            if count:
                amounts[rows[index].domain_id] = count
                value += weights[index] * count
                remaining -= count
        return value, amounts

    current_value, best = evaluate(active)
    best_value = current_value
    temperature = max(current_value / max(1, domain_slots) * 0.15, 1e-9)
    rng = random.Random(seed)
    for step in range(max_iterations):
        if time.monotonic() >= deadline:
            break
        outgoing = rng.randrange(len(active))
        incoming = rng.randrange(len(inactive))
        proposal = active.copy()
        proposal[outgoing] = inactive[incoming]
        value, amounts = evaluate(proposal)
        delta = value - current_value
        heat = temperature * (0.001 ** (step / max_iterations))
        if delta >= 0 or rng.random() < math.exp(delta / heat):
            inactive[incoming], active[outgoing] = active[outgoing], inactive[incoming]
            current_value = value
            if value > best_value:
                best_value, best = value, amounts
    return best


def allocate(candidates, demand, *, url_budget, domain_slots, per_domain_cap, method="annealing"):
    """Maximize sum(value_i * x_i), with integer URL counts and binary slots.

    0 <= x_i <= min(demand_i, cap) * y_i; sum(x) <= budget;
    sum(y) <= slots. Only positive-value domains enter this supplemental path;
    ordinary refill remains responsible for exploration and cold starts.
    """
    if method not in ("greedy", "annealing", "mip"):
        raise ValueError("Allocation method must be greedy, annealing or mip")
    unique = {}
    for candidate in candidates:
        if math.isfinite(candidate.allocation_value) and candidate.allocation_value > 0:
            unique.setdefault(candidate.domain_id, candidate)
    rows = sorted((c for c in unique.values() if demand.get(c.domain_id, 0) > 0),
                  key=lambda c: (-c.allocation_value, c.domain_id))
    if not rows or url_budget <= 0 or domain_slots <= 0 or per_domain_cap <= 0:
        return {}
    caps = [min(demand[c.domain_id], per_domain_cap) for c in rows]
    # Keep a feasible baseline even if a time-limited incumbent is poor.
    fallback = {}
    remaining = url_budget
    for candidate, cap in zip(rows, caps):
        amount = min(cap, remaining)
        if amount > 0:
            fallback[candidate.domain_id] = amount
            remaining -= amount
        if len(fallback) >= domain_slots or remaining <= 0:
            break
    if method == "greedy":
        return fallback
    if method == "annealing":
        return _anneal(rows, caps, fallback, url_budget=url_budget, domain_slots=domain_slots)
    fallback = _anneal(rows, caps, fallback, url_budget=url_budget, domain_slots=domain_slots)
    try:
        import numpy as np
        from scipy.optimize import Bounds, LinearConstraint, milp
        from scipy.sparse import lil_matrix

        n = len(rows)
        matrix = lil_matrix((n + 2, n * 2))
        matrix[0, :n] = 1
        matrix[1, n:] = 1
        for i, cap in enumerate(caps):
            matrix[i + 2, i] = 1
            matrix[i + 2, n + i] = -cap
        scale = max(c.allocation_value for c in rows)
        result = milp(
            c=[-c.allocation_value / scale for c in rows] + [0] * n,
            integrality=np.ones(n * 2),
            bounds=Bounds(np.zeros(n * 2), caps + [1] * n),
            constraints=LinearConstraint(matrix.tocsc(), -np.inf, [url_budget, domain_slots] + [0] * n),
            options={"time_limit": 1.0, "mip_rel_gap": 0.01},
        )
        if result.x is not None:
            amounts = [int(round(v)) for v in result.x[:n]]
            # Never publish an infeasible incumbent, including numerical errors.
            if (all(0 <= x <= cap for x, cap in zip(amounts, caps))
                    and sum(amounts) <= url_budget
                    and sum(x > 0 for x in amounts) <= domain_slots):
                plan = {c.domain_id: x for c, x in zip(rows, amounts) if x}
                value = lambda p: sum(c.allocation_value * p.get(c.domain_id, 0) for c in rows)
                return plan if value(plan) >= value(fallback) else fallback
        logger.warning("offer.allocation_fallback", extra={"event": "offer.allocation_fallback",
                                                        "solver_status": result.status})
    except ImportError:
        logger.warning("offer.allocation_fallback", extra={"event": "offer.allocation_fallback",
                                                        "reason": "solver_unavailable"})
    # Deterministic feasible fallback, not claimed to be optimal with a slot cap.
    return fallback
