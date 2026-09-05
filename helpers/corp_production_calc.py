import logging
import json
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List

MS_PER_DAY = 1000 * 60 * 60 * 24.0


def process_corp_production_and_workforce(prod_records: List[Any], wf_records: List[Any]) -> Dict[str, Any]:
    """
    1. Iterates Production Data.
    2. LOGIC: Mimics the original single-user endpoint EXACTLY.
       - Calculates 'line_unscaled_flow' first.
       - Applies 'daily_cycles' at the end of the line processing.
       - Uses strictly Template Orders for calculation.
       - Deduplicates and tracks recipes across all active/template orders for all users.
    """

    corp_data = defaultdict(
        lambda: {
            "prod_total": 0.0,
            "prod_acc": 0.0,
            "prod_est": 0.0,
            "cons_total": 0.0,
            "cons_acc": 0.0,
            "cons_est": 0.0,
            "producers": defaultdict(lambda: {"acc": 0.0, "est": 0.0, "batch_active": 0.0, "batch_queued": 0.0}),
            "batch_prod_active": 0.0,
            "batch_prod_queued": 0.0,
            "consumers": defaultdict(lambda: {"acc": 0.0, "est": 0.0, "batch_active": 0.0, "batch_queued": 0.0}),
            "batch_cons_active": 0.0,
            "batch_cons_queued": 0.0,
            "user_recipe_inputs": defaultdict(float),
            "user_recipes_used": defaultdict(lambda: {
                "daily_output": 0.0,
                "daily_cycles": 0.0,
                "building": "",
                "output_amount": 1.0,
                "inputs": defaultdict(float),
                "outputs": defaultdict(float),
                "users": defaultdict(lambda: {"daily_output": 0.0, "daily_cycles": 0.0}),
            }),
        }
    )

    # ------------------------------------------------------------------
    # 1. PROCESS PRODUCTION LINES
    # ------------------------------------------------------------------
    for rec in prod_records:
        player = rec["player_name"]
        loc = rec["location_name"]
        is_subscription_accurate = rec["is_accurate"]
        lines = rec["production_lines"]

        for line in lines:
            orders = line.get("production_orders", [])
            capacity = int(line.get("capacity", 0))

            if not orders or capacity <= 0:
                continue

            # Added Non-recurring calculations
            batch_active_orders = [o for o in orders if not o.get("recurring") and o.get("completion")]
            batch_queued_orders = [o for o in orders if not o.get("recurring") and not o.get("completion")]

            for batch_order, is_active in [(o, True) for o in batch_active_orders] + [(o, False) for o in batch_queued_orders]:
                b_recipe = batch_order.get("production_recipe") or {}
                b_order_dur = float(batch_order.get("duration") or 0)
                b_recipe_dur = float(b_recipe.get("duration") or 0)
                b_multiplier = (b_order_dur / b_recipe_dur) if b_recipe_dur > 0 else 1.0

                for out_factor in (b_recipe.get("outputs") or []):
                    out_ticker = out_factor.get("ticker") or ""
                    if out_ticker:
                        qty = float(out_factor.get("factor", 0.0)) * b_multiplier
                        producer_entry = corp_data[out_ticker]["producers"][(loc, player)]
                        if is_active:
                            corp_data[out_ticker]["batch_prod_active"] += qty
                            producer_entry["batch_active"] += qty
                        else:
                            corp_data[out_ticker]["batch_prod_queued"] += qty
                            producer_entry["batch_queued"] += qty
                
                for in_factor in (b_recipe.get("inputs") or []):
                    in_ticker = in_factor.get("ticker") or ""
                    if in_ticker:
                        qty = float(in_factor.get("factor", 0.0)) * b_multiplier
                        consumer_entry = corp_data[in_ticker]["consumers"][(loc, player)]
                        if is_active:
                            corp_data[in_ticker]["batch_cons_active"] += qty
                            consumer_entry["batch_active"] += qty
                        else:
                            corp_data[in_ticker]["batch_cons_queued"] += qty
                            consumer_entry["batch_queued"] += qty

            active_orders = [o for o in orders if o.get("recurring") and o.get("completion")]
            template_orders = [o for o in orders if o.get("recurring") and not o.get("completion")]

            if not template_orders:
                continue

            # Sorting
            active_orders.sort(
                key=lambda o: datetime.fromisoformat(o["completion"]) if o.get("completion") else datetime.max
            )
            template_orders.sort(
                key=lambda o: datetime.fromisoformat(o["created"]) if o.get("created") else datetime.max
            )

            # Queue construction
            queue = active_orders + template_orders
            queue = queue[:capacity]

            # Initialize Line Unscaled Flow
            line_unscaled_flow = defaultdict(float)

            # Calculate Total MS from Template Orders
            total_ms = sum((float(o.get("duration") or 0)) for o in template_orders)

            if total_ms <= 0:
                continue

            daily_cycles = (capacity * MS_PER_DAY) / total_ms

            # Iterate Template Orders to build Unscaled Flow
            for order in template_orders:
                recipe = order.get("production_recipe", {})
                if not recipe:
                    continue

                inputs = recipe.get("inputs") or []
                outputs = recipe.get("outputs") or []

                order_dur = float(order.get("duration") or 0)
                recipe_dur = float(recipe.get("duration") or 0)

                if recipe_dur == 0:
                    continue

                duration_multiplier = order_dur / recipe_dur
                effective_cycles = duration_multiplier * daily_cycles

                # Deduplicate unique input and output factors by ticker to prevent reference duplicate inflation
                unique_inputs: Dict[str, float] = {}
                for factor in inputs:
                    t = factor.get("ticker")
                    if t:
                        unique_inputs[t] = float(factor.get("factor", 0.0))

                unique_outputs: Dict[str, float] = {}
                for factor in outputs:
                    t = factor.get("ticker")
                    if t:
                        unique_outputs[t] = float(factor.get("factor", 0.0))

                # Sum Inputs (Negative Flow)
                for ticker, factor_val in unique_inputs.items():
                    flow = -factor_val * duration_multiplier
                    line_unscaled_flow[ticker] += flow

                # Sum Outputs (Positive Flow)
                for ticker, factor_val in unique_outputs.items():
                    flow = factor_val * duration_multiplier
                    line_unscaled_flow[ticker] += flow

                # Canonical key representing the recipe uniquely
                building_ticker = recipe.get("building") or recipe.get("reactor_id") or ""
                inp_sig = "-".join(sorted([f"{val}x{tick}" for tick, val in unique_inputs.items()]))
                out_sig = "-".join(sorted([f"{val}x{tick}" for tick, val in unique_outputs.items()]))
                recipe_key = f"{building_ticker}:{inp_sig}=>{out_sig}"

                # 1. Register for output materials (producers)
                for out_ticker, out_val in unique_outputs.items():
                    out_daily = out_val * effective_cycles
                    if out_daily <= 0:
                        continue

                    rec_entry = corp_data[out_ticker]["user_recipes_used"][recipe_key]
                    rec_entry["building"] = building_ticker
                    rec_entry["daily_output"] += out_daily
                    rec_entry["daily_cycles"] += effective_cycles
                    rec_entry["output_amount"] = out_val

                    for in_ticker, in_val in unique_inputs.items():
                        rec_entry["inputs"][in_ticker] = in_val
                        corp_data[out_ticker]["user_recipe_inputs"][in_ticker] += in_val * effective_cycles

                    for o_ticker, o_val in unique_outputs.items():
                        rec_entry["outputs"][o_ticker] = o_val

                    user_rec = rec_entry["users"][(player, loc)]
                    user_rec["daily_output"] += out_daily
                    user_rec["daily_cycles"] += effective_cycles

                # 2. Register for input materials (consumers)
                for inp_ticker, inp_val in unique_inputs.items():
                    inp_daily = inp_val * effective_cycles
                    if inp_daily <= 0:
                        continue

                    rec_entry = corp_data[inp_ticker]["user_recipes_used"][recipe_key]
                    rec_entry["building"] = building_ticker
                    rec_entry["daily_output"] += inp_daily
                    rec_entry["daily_cycles"] += effective_cycles
                    rec_entry["output_amount"] = inp_val

                    for in_ticker, in_val in unique_inputs.items():
                        rec_entry["inputs"][in_ticker] = in_val

                    for o_ticker, o_val in unique_outputs.items():
                        rec_entry["outputs"][o_ticker] = o_val

                    user_rec = rec_entry["users"][(player, loc)]
                    user_rec["daily_output"] += inp_daily
                    user_rec["daily_cycles"] += effective_cycles

            # --- APPLY SCALING AND AGGREGATE TO CORP ---
            for ticker, unscaled_flow in line_unscaled_flow.items():
                daily_flow = unscaled_flow * daily_cycles

                if daily_flow > 0:
                    # PRODUCTION
                    corp_data[ticker]["prod_total"] += daily_flow
                    target = corp_data[ticker]["producers"][(loc, player)]

                    if is_subscription_accurate:
                        corp_data[ticker]["prod_acc"] += daily_flow
                        target["acc"] += daily_flow
                    else:
                        corp_data[ticker]["prod_est"] += daily_flow
                        target["est"] += daily_flow

                elif daily_flow < 0:
                    # CONSUMPTION
                    abs_flow = abs(daily_flow)
                    corp_data[ticker]["cons_total"] += abs_flow
                    target = corp_data[ticker]["consumers"][(loc, player)]

                    if is_subscription_accurate:
                        corp_data[ticker]["cons_acc"] += abs_flow
                        target["acc"] += abs_flow
                    else:
                        corp_data[ticker]["cons_est"] += abs_flow
                        target["est"] += abs_flow

    # ------------------------------------------------------------------
    # 2. PROCESS WORKFORCE NEEDS
    # ------------------------------------------------------------------
    for rec in wf_records:
        player = rec["player_name"]
        loc = rec["location_name"]
        is_subscription_accurate = rec["is_accurate"]

        needs = json.loads(rec["needs"]) if isinstance(rec["needs"], str) else rec["needs"]

        for need in needs:
            ticker = need["ticker"]
            amount = float(need["unitsperinterval"])

            if amount == 0:
                continue

            # Workforce is always consumption
            corp_data[ticker]["cons_total"] += amount
            target = corp_data[ticker]["consumers"][(loc, player)]

            if is_subscription_accurate:
                corp_data[ticker]["cons_acc"] += amount
                target["acc"] += amount
            else:
                corp_data[ticker]["cons_est"] += amount
                target["est"] += amount

    return corp_data