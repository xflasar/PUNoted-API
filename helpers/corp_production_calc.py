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
    """

    corp_data = defaultdict(
        lambda: {
            "prod_total": 0.0,
            "prod_acc": 0.0,
            "prod_est": 0.0,
            "cons_total": 0.0,
            "cons_acc": 0.0,
            "cons_est": 0.0,
            "producers": defaultdict(lambda: {"acc": 0.0, "est": 0.0}),
            "consumers": defaultdict(lambda: {"acc": 0.0, "est": 0.0}),
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

            # --- ORIGINAL LOGIC START ---
            active_orders = [o for o in orders if o.get("completion")]
            template_orders = [o for o in orders if not o.get("completion")]

            # Sorting
            active_orders.sort(
                key=lambda o: datetime.fromisoformat(o["completion"]) if o.get("completion") else datetime.max
            )
            template_orders.sort(
                key=lambda o: datetime.fromisoformat(o["created"]) if o.get("created") else datetime.max
            )

            # Queue construction (used for determining capacity usage if needed, though snippet relies on template_orders)
            queue = active_orders + template_orders
            queue = queue[:capacity]  # Mimicking: queue = queue[: int(line["capacity"])]

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

                # Input/Output lists
                inputs = recipe.get("inputs") or []
                outputs = recipe.get("outputs") or []

                order_dur = float(order.get("duration") or 0)
                recipe_dur = float(recipe.get("duration") or 0)

                if recipe_dur == 0:
                    continue

                duration_multiplier = order_dur / recipe_dur

                # Sum Inputs (Negative Flow)
                for factor in inputs:
                    ticker = factor["ticker"]
                    # flow = -factor["factor"] * duration_multiplier
                    flow = -factor["factor"] * duration_multiplier
                    line_unscaled_flow[ticker] += flow

                # Sum Outputs (Positive Flow)
                for factor in outputs:
                    ticker = factor["ticker"]
                    # flow = factor["factor"] * duration_multiplier
                    flow = factor["factor"] * duration_multiplier
                    line_unscaled_flow[ticker] += flow

            # --- APPLY SCALING AND AGGREGATE TO CORP ---
            # Mimicking: for ticker, unscaled_flow in line_unscaled_flow.items():
            #                line["daily_flow"][ticker] = unscaled_flow * daily_cycles

            for ticker, unscaled_flow in line_unscaled_flow.items():
                daily_flow = unscaled_flow * daily_cycles

                # Determine if this is Production (+) or Consumption (-)
                # The single-user endpoint splits them by sign here.

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
                    # CONSUMPTION (Flip sign to positive for tracking)
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
