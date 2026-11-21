"""Scheduling constraints and IPC behavior; no production or database access."""
import itertools
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from containers.scheduler_control.offerer.allocation import allocate
from containers.scheduler_control.offerer.selection.base import CandidateDomain
from libs.ipc.demand import DemandTracker, read_demand
from libs.ipc.jsonio import atomic_write_json
from containers.scheduler_control.tests.test_offerer_service_global_refill import _make_service, _StubGlobalStrategy


def test_annealing_improves_slot_constrained_plan_and_keeps_feasibility():
    rows = [CandidateDomain(1, 0, 10), CandidateDomain(2, 0, 9), CandidateDomain(3, 0, 2)]
    demand = {1: 1, 2: 5, 3: 3}
    greedy = allocate(rows, demand, url_budget=5, domain_slots=1, per_domain_cap=5, method="greedy")
    annealed = allocate(rows, demand, url_budget=5, domain_slots=1, per_domain_cap=5,
                        method="annealing")
    assert greedy == {1: 1}
    assert annealed == {2: 5}


def test_annealing_never_worsens_greedy_across_varied_inputs():
    import random
    rng = random.Random(13)
    for _ in range(30):
        rows = [CandidateDomain(i, 0, rng.random() * 10) for i in range(40)]
        demand = {i: rng.randint(1, 100) for i in range(40)}
        args = dict(url_budget=rng.randint(50, 500), domain_slots=rng.randint(1, 8), per_domain_cap=80)
        greedy = allocate(rows, demand, method="greedy", **args)
        annealed = allocate(rows, demand, method="annealing", **args)
        value = lambda plan: sum(row.domain_score * plan.get(row.domain_id, 0) for row in rows)
        assert value(annealed) >= value(greedy) - 1e-9
        assert sum(annealed.values()) <= args["url_budget"]
        assert len(annealed) <= args["domain_slots"]
        assert all(0 < v <= min(demand[key], 80) for key, v in annealed.items())


def test_annealing_time_limit_can_return_greedy_immediately():
    from containers.scheduler_control.offerer.allocation import _anneal
    rows = [CandidateDomain(1, 0, 10), CandidateDomain(2, 0, 9)]
    assert _anneal(rows, [1, 5], {1: 1}, url_budget=5, domain_slots=1,
                   time_budget_sec=0) == {1: 1}


def test_demand_candidates_drop_empty_frontiers_with_bounded_shard_probes():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from containers.scheduler_control.offerer.selection.golden_discovery_ranker_v1_strategy import GoldenDiscoveryRankerV1Strategy
    session = MagicMock()
    session.__enter__.return_value = session
    session.execute.side_effect = [
        SimpleNamespace(fetchall=lambda: [SimpleNamespace(domain_id=1, shard_id=0, domain_score=2),
                                          SimpleNamespace(domain_id=2, shard_id=0, domain_score=1),
                                          SimpleNamespace(domain_id=3, shard_id=1, domain_score=1)]),
        SimpleNamespace(fetchall=lambda: [SimpleNamespace(domain_id=2, available_urls=3, url_score=1.5)]),
        SimpleNamespace(fetchall=lambda: []),
    ]
    selector = GoldenDiscoveryRankerV1Strategy(Session=lambda: session)
    assert selector.demand_candidates({1: 100, 2: 50, 3: 10}, 0, 1) == [CandidateDomain(2, 0, 1, 3, 1.5)]
    first, second, third = session.execute.call_args_list
    assert first.args[1] == {"domain_ids": [1, 2, 3], "start": 0, "end": 1}
    assert second.args[1]["domain_ids"] == [1, 2]
    assert third.args[1]["domain_ids"] == [3]
    assert second.args[1]["url_caps"] == [100, 50]
    assert third.args[1]["url_caps"] == [10]
    assert "LIMIT requested.url_cap" in str(second.args[0])


def test_url_priority_and_usable_frontier_bound_allocations():
    rows = [CandidateDomain(1, 0, 0.8, 100, 0.2), CandidateDomain(2, 0, 0.8, 300, 1.1)]
    assert allocate(rows, {1: 100, 2: 300}, url_budget=300, domain_slots=1,
                    per_domain_cap=2000) == {2: 300}
    assert CandidateDomain(1, 0, 0.8, url_score=0).allocation_value == 0
    assert CandidateDomain(1, 0, 0.8, url_score=None).allocation_value == 0.8


def test_integer_allocation_matches_exhaustive_optimum():
    # A short high-value domain can lose to a larger domain under a slot limit.
    # Picking domains just by score is not optimal here.
    candidates = [CandidateDomain(1, 0, 10), CandidateDomain(2, 0, 9), CandidateDomain(3, 0, 2)]
    demand = {1: 1, 2: 5, 3: 3}
    for slots in (1, 2, 3):
        for budget in range(1, 8):
            plan = allocate(candidates, demand, url_budget=budget, domain_slots=slots,
                            per_domain_cap=5, method="mip")
            feasible = [xs for xs in itertools.product(range(2), range(6), range(4))
                        if sum(xs) <= budget and sum(x > 0 for x in xs) <= slots]
            optimum = max(sum(x * c.domain_score for x, c in zip(xs, candidates)) for xs in feasible)
            assert sum(plan.get(c.domain_id, 0) * c.domain_score for c in candidates) == optimum


def test_bad_scores_duplicates_zero_budgets_and_caps():
    rows = [CandidateDomain(1, 0, 5), CandidateDomain(1, 0, 5),
            CandidateDomain(2, 0, float("nan")), CandidateDomain(3, 0, -1)]
    assert allocate(rows, {1: 20, 2: 20, 3: 20}, url_budget=10, domain_slots=3,
                    per_domain_cap=4) == {1: 4}
    assert allocate(rows, {1: 20}, url_budget=0, domain_slots=1, per_domain_cap=4) == {}


def test_no_solver_fallback_is_feasible():
    with patch.dict("sys.modules", {"scipy.optimize": None}):
        plan = allocate([CandidateDomain(1, 0, 5), CandidateDomain(2, 0, 4)],
                        {1: 20, 2: 20}, url_budget=9, domain_slots=1, per_domain_cap=4, method="mip")
    assert plan == {1: 4}


def test_tracker_excludes_starvation_time_and_bounds_state():
    tracker = DemandTracker(horizon_sec=180, max_domain_urls=2000, max_domains=2)
    tracker.admitted(10, now=0)
    for i in range(100):
        tracker.finished(10, now=(i + 1) / 6)
    snapshot = tracker.snapshot({}, 5000, now=200, wall_time=1000)
    assert snapshot["domains"]["10"] == 1080
    assert tracker.snapshot({10: 1000}, 2000, now=201)["domains"]["10"] == 80
    tracker.admitted(11, now=201)
    tracker.admitted(12, now=202)
    assert len(tracker.domains) == 2
    assert 10 not in tracker.domains


def test_expired_and_invalid_feedback_is_ignored():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "demand.snapshot"
        for doc in ({"version": 1, "generated_at": 90, "capacity": 50, "domains": {"1": 20}},
                    {"version": 2, "generated_at": 110, "capacity": 50, "domains": {}},
                    {"version": 1, "generated_at": float("nan"), "capacity": 50, "domains": {}}):
            atomic_write_json(str(path), doc)
            assert read_demand(tmp, now=125, max_age_sec=30) == (0, {})
        path.write_text("broken")
        assert read_demand(tmp) == (0, {})
        atomic_write_json(str(path), {"version": 1, "generated_at": 120, "capacity": 50,
                                      "domains": {"1": 20, "2": -1}})
        assert read_demand(tmp, now=125) == (50, {1: 20})


def test_demand_bypasses_queue_watermark_without_overwriting_ready_offers():
    selector = _StubGlobalStrategy(per_domain_urls={9: ["https://example.test/a"] * 700})
    selector.demand_candidates = lambda ids, start, end: [CandidateDomain(i, 0, 10) for i in ids]
    with tempfile.TemporaryDirectory() as tmp:
        svc = _make_service(tmp, selector, demand_refill_enabled=True, max_domain_files=4,
                            low_watermark_domains=1, demand_per_domain_cap=1000)
        queue = Path(tmp) / "queue_00"
        queue.mkdir()
        atomic_write_json(str(queue / "domain_000001.json"), {"urls": ["existing"]})
        tracker = DemandTracker()
        tracker.admitted(9, now=0)
        for i in range(100):
            tracker.finished(9, now=(i + 1) / 6)
        atomic_write_json(str(queue / "demand.snapshot"), tracker.snapshot({}, 700, now=20))
        result = svc._refill_once_if_needed()
        assert result["action"] == "refill_empty"  # discovery remains active above the domain watermark
        assert len(selector.peek_calls) == 1
        assert selector.claim_calls[0]["per_domain_cap"] == 700
        assert len(json.loads((queue / "domain_000009.json").read_text())["urls"]) == 700
        assert json.loads((queue / "domain_000001.json").read_text())["urls"] == ["existing"]
        svc._refill_once_if_needed()
        assert len(selector.claim_calls) == 1


def test_demand_and_bootstrap_share_the_cycle_url_budget():
    selector = _StubGlobalStrategy(candidates=[CandidateDomain(10, 0, 1.1)],
                                   per_domain_urls={9: ["https://example.test"] * 700,
                                                    10: ["https://other.test"] * 1000})
    selector.demand_candidates = lambda ids, start, end: [CandidateDomain(9, 0, 1)]
    original = selector.claim_domain_urls
    selector.claim_domain_urls = lambda **kw: original(**kw)[:kw["per_domain_cap"]]
    with tempfile.TemporaryDirectory() as tmp:
        svc = _make_service(tmp, selector, demand_refill_enabled=True, demand_url_budget=1000,
                            demand_per_domain_cap=2000)
        tracker = DemandTracker()
        tracker.admitted(9, now=0)
        for i in range(100):
            tracker.finished(9, now=(i + 1) / 6)
        atomic_write_json(str(Path(tmp) / "queue_00/demand.snapshot"), tracker.snapshot({}, 700, now=20))
        svc._refill_once_if_needed()
        assert [call["per_domain_cap"] for call in selector.claim_calls] == [700, 300]


def test_offerer_publishes_value_order_without_sending_scores_to_crawler():
    selector = _StubGlobalStrategy(per_domain_urls={
        1: ["https://first.test/a", "https://first.test/b"],
        2: ["https://second.test/a", "https://second.test/b"],
    })
    selector.demand_candidates = lambda ids, start, end: [
        CandidateDomain(1, 0, 0.9, 2, 0.2), CandidateDomain(2, 0, 0.1, 2, 1.1)]
    with tempfile.TemporaryDirectory() as tmp:
        svc = _make_service(tmp, selector, demand_refill_enabled=True)
        queue = Path(tmp) / "queue_00"
        tracker = DemandTracker()
        tracker.admitted(1)
        tracker.admitted(2)
        tracker.publish(str(queue), {}, 100)
        with patch.object(svc, "_write_domain_file", wraps=svc._write_domain_file) as publish:
            svc._refill_once_if_needed()
        assert [call.args[1] for call in publish.call_args_list] == [2, 1]
        for domain_id, urls in selector.per_domain_urls.items():
            payload = json.loads((queue / f"domain_{domain_id:06d}.json").read_text())
            assert set(payload) == {"generated_at", "domain_id", "urls"}
            assert payload["urls"] == urls


def test_demand_failure_does_not_block_normal_refill():
    selector = _StubGlobalStrategy(candidates=[CandidateDomain(1, 0, 10)],
                                   per_domain_urls={1: ["https://example.test"]})
    def fail(*args):
        raise RuntimeError("lookup failed")
    selector.demand_candidates = fail
    with tempfile.TemporaryDirectory() as tmp:
        svc = _make_service(tmp, selector, demand_refill_enabled=True)
        queue = Path(tmp) / "queue_00"
        tracker = DemandTracker()
        tracker.admitted(1)
        tracker.publish(str(queue), {}, 100)
        assert svc._refill_once_if_needed()["action"] == "refill"


def test_simulated_fast_domain_recovers_from_hundred_url_supply_gaps():
    """Discrete-time supply test, not a measurement of network/database QPS.

    Model a six-URL/sec consumer and a slow 120-second offer cycle. The ordinary
    hundred-URL batches starve it; measured demand must ramp without a QPS input.
    """
    def run(adaptive):
        tracker = DemandTracker()
        pending, queued, completed = 100, 0, 0
        tracker.admitted(1, now=0)
        last_window = 0
        for second in range(1, 3601):
            if not pending and queued:
                pending, queued = queued, 0
                tracker.admitted(1, now=second - 1)
            amount = min(6, pending)
            pending -= amount
            completed += amount
            if second > 3000:
                last_window += amount
            for _ in range(amount):
                tracker.finished(1, now=second)
            if second % 120 == 0 and not queued:
                wanted = tracker.snapshot({1: pending} if pending else {}, 4000, now=second)["domains"]
                queued = (allocate([CandidateDomain(1, 0, 10)], {1: wanted.get("1", 0)},
                                   url_budget=4000, domain_slots=1, per_domain_cap=2000).get(1, 0)
                          if adaptive else 100)
        return completed, last_window / 600

    old_total, old_qps = run(False)
    new_total, new_qps = run(True)
    assert old_qps < 1
    assert new_qps >= 5.5
    assert new_total > old_total * 5
