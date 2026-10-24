from __future__ import annotations

import random
from datetime import datetime, timedelta

import pytest

from helpers.corp_production_calc import MS_PER_DAY, process_corp_production_and_workforce, split_line_orders

T0 = datetime(2026, 10, 19)

# Two recipes on one line: 4 GRN from 1 H2O in 12 hours, 2 BEA from 1 H2O in 8 hours.
RECIPES = {
    "GRN": {"duration": MS_PER_DAY / 2, "inputs": [{"ticker": "H2O", "factor": 1.0}],
            "outputs": [{"ticker": "GRN", "factor": 4.0}]},
    "BEA": {"duration": MS_PER_DAY / 3, "inputs": [{"ticker": "H2O", "factor": 1.0}],
            "outputs": [{"ticker": "BEA", "factor": 2.0}]},
}


def _order(ticker: str, recurring: bool, n: int, completion: datetime | None = None) -> dict:
    recipe = RECIPES[ticker]
    return {
        "order_id": f"o{n}",
        "recurring": recurring,
        "created": (T0 + timedelta(seconds=n)).isoformat(),
        "completion": completion.isoformat() if completion else None,
        "duration": recipe["duration"],
        "production_recipe": dict(recipe),
    }


def _flow(orders: list[dict], capacity: int = 5) -> dict:
    prod = [{
        "player_name": "TEST",
        "location_name": "XX-000a",
        "is_accurate": True,
        "production_lines": [{"capacity": capacity, "production_orders": orders}],
    }]
    return process_corp_production_and_workforce(prod, [])


def test_split_keeps_queued_copies_as_the_cycle():
    running = [_order("GRN", True, i, T0) for i in range(5)]
    queued = [_order("GRN", True, 5 + i) for i in range(5)]
    split = split_line_orders(running + queued + [_order("BEA", False, 20)])
    assert split["cycle"] == queued
    assert split["recurring_active"] == running
    assert split["batch_active"] == []
    assert [o["order_id"] for o in split["batch_queued"]] == ["o20"]


def test_split_counts_recurring_orders_with_no_copy_queued_once():
    running = [_order("GRN", True, i, T0) for i in range(5)]
    batch = _order("BEA", False, 9, T0)
    split = split_line_orders(running + [batch])
    assert split["cycle"] == []
    assert split["recurring_active"] == []
    assert split["batch_active"] == [batch] + running


def test_looping_line_rate():
    flow = _flow([_order("GRN", True, i, T0) for i in range(5)] + [_order("GRN", True, 5 + i) for i in range(5)])
    # 5 slots, each making 4 GRN twice a day
    assert flow["GRN"]["prod_total"] == pytest.approx(40.0)
    assert flow["H2O"]["cons_total"] == pytest.approx(10.0)
    assert flow["GRN"]["batch_prod_active"] == 0.0


def test_line_left_to_wind_down_has_no_rate():
    # The player deleted the queued copies; the five running orders finish once and stop.
    flow = _flow([_order("GRN", True, i, T0) for i in range(5)])
    assert flow["GRN"]["prod_total"] == 0.0
    assert flow["GRN"]["batch_prod_active"] == pytest.approx(20.0)
    assert flow["H2O"]["batch_cons_active"] == pytest.approx(5.0)


# --- The game's rule, simulated -------------------------------------------------------------------
# "When a recurring order moves from the pending queue to the active queue, the same order is
# re-added to the bottom of the pending queue." (APEX handbook FAQ)

def _simulate(cycle: list[str], capacity: int, days: float, stop_requeue_at: float | None = None):
    """Run a line under the handbook rule. Yields (time, running, queued) after every event,
    and returns the output made per ticker."""
    seq = iter(range(10**9))
    queued = [(t, next(seq)) for t in cycle]
    running: list[tuple[str, int, float]] = []  # ticker, id, finish time (ms)
    made: dict[str, float] = {}
    states = []
    now = 0.0
    end = days * MS_PER_DAY
    while True:
        requeue = stop_requeue_at is None or now < stop_requeue_at * MS_PER_DAY
        if not requeue and stop_requeue_at is not None:
            queued = []  # the player deletes the queued copies
        while len(running) < capacity and queued:
            ticker, _ = queued.pop(0)
            running.append((ticker, next(seq), now + RECIPES[ticker]["duration"]))
            if requeue:
                queued.append((ticker, next(seq)))
        states.append((now, list(running), list(queued)))
        if not running:
            break
        running.sort(key=lambda r: r[2])
        ticker, _, finish = running.pop(0)
        if finish > end:
            break
        now = finish
        made[ticker] = made.get(ticker, 0.0) + RECIPES[ticker]["outputs"][0]["factor"]
    return states, made


def _snapshot_orders(running, queued) -> list[dict]:
    orders = [_order(t, True, i, T0 + timedelta(milliseconds=f)) for t, i, f in running]
    orders += [_order(t, True, i) for t, i in queued]
    return orders


@pytest.mark.parametrize("cycle,capacity", [
    (["GRN"], 5),                       # one recurring order, five slots
    (["GRN"] * 5, 5),                   # one order per slot
    (["GRN", "GRN", "BEA"], 4),         # mixed recipes
    (["GRN"] * 3 + ["BEA"] * 4, 3),     # more orders than slots
])
def test_rate_matches_the_simulated_line_at_any_moment(cycle, capacity):
    days = 60
    states, made = _simulate(cycle, capacity, days)
    rng = random.Random(0)
    for _, running, queued in rng.sample(states[len(states) // 4:], 25):
        assert queued, "a looping line always has a copy queued"
        flow = _flow(_snapshot_orders(running, queued), capacity)
        for ticker, total in made.items():
            assert flow[ticker]["prod_total"] == pytest.approx(total / days, rel=0.02)


def test_wound_down_line_counts_only_what_is_still_running():
    states, made = _simulate(["GRN"], 5, days=10, stop_requeue_at=5.0)
    _, running, queued = next(s for s in states if s[0] >= 5.0 * MS_PER_DAY)
    assert not queued
    flow = _flow(_snapshot_orders(running, queued), 5)
    assert flow["GRN"]["prod_total"] == 0.0
    assert flow["GRN"]["batch_prod_active"] == pytest.approx(4.0 * len(running))
