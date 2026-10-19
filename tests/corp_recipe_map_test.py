from __future__ import annotations

import pytest

from helpers.corp_production_calc import process_corp_production_and_workforce
from repositories.corp_production_repo import build_recipe_map

DAY = 86_400_000.0


def _rows(lines: list[str], duration: float) -> tuple[list, list, list]:
    """Recipe rows as stored: the same template once per production line."""
    core = [{"recipe_id": "GRN-T", "line_id": line, "duration": duration} for line in lines]
    inputs = [{"recipe_id": "GRN-T", "line_id": line, "ticker": "H2O", "factor": 1.0} for line in lines]
    outputs = [{"recipe_id": "GRN-T", "line_id": line, "ticker": "GRN", "factor": 4.0} for line in lines]
    return core, inputs, outputs


def test_recipe_map_keeps_each_line_apart():
    recipe_map = build_recipe_map(*_rows(["L1", "L2", "L3"], DAY / 2))
    assert set(recipe_map) == {("GRN-T", "L1"), ("GRN-T", "L2"), ("GRN-T", "L3")}
    assert recipe_map[("GRN-T", "L1")]["outputs"] == [{"ticker": "GRN", "factor": 4.0}]
    assert recipe_map[("GRN-T", "L1")]["inputs"] == [{"ticker": "H2O", "factor": 1.0}]


def test_batch_order_counts_its_own_recipe_once():
    # Other members' lines run the same template; their rows must not add to this order.
    recipe_map = build_recipe_map(*_rows(["L1", "L2", "L3"], DAY / 2))
    order = {"order_id": "o1", "recurring": False, "created": None, "completion": "2026-10-19T12:00:00",
             "duration": DAY / 2, "production_recipe": recipe_map[("GRN-T", "L1")]}
    prod = [{"player_name": "TEST", "location_name": "XX-000a", "is_accurate": True,
             "production_lines": [{"capacity": 1, "production_orders": [order]}]}]
    flow = process_corp_production_and_workforce(prod, [])
    assert flow["GRN"]["batch_prod_active"] == pytest.approx(4.0)
    assert flow["H2O"]["batch_cons_active"] == pytest.approx(1.0)
