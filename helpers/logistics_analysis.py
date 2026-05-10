# AI GENERATED CODE Help Well not really but yeah

from collections import defaultdict
from typing import Any, Dict, List

import orjson

from app.core.ai_client import query_local_ai_json

MS_PER_DAY = 24 * 60 * 60 * 1000

def calculate_site_production_flow(
    site_data: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Calculates the daily production and consumption flow for a single site
    GROSS (separately), preventing local consumption from hiding production.
    """

    # 1. Use separate dicts to prevent netting
    gross_production = defaultdict(float)
    gross_consumption = defaultdict(float)

    # Pre-fetch material info map (id -> weight/vol)
    # We build this dynamically from the lines to handle lookups later
    material_info_map = {}

    for line in site_data.get("production_lines", []):
        # Initialize defaults
        line["queue"] = []
        line["daily_flow"] = {}

        orders = line.get("production_orders", [])
        if not orders or line.get("capacity", 0) <= 0:
            continue

        active_orders = [o for o in orders if o.get("completion")]
        template_orders = [o for o in orders if not o.get("completion")]

        # Robust Sorting
        active_orders.sort(key=lambda o: o["completion"])
        template_orders.sort(key=lambda o: o["created"])

        queue = active_orders + template_orders
        line["queue"] = queue[: int(line["capacity"])]

        if not template_orders:
            continue

        # --- CALCULATE LINE FLOW ---
        line_net_flow = defaultdict(float)

        total_ms = sum((float(o.get("duration") or 0)) for o in template_orders)
        if total_ms <= 0:
            continue

        daily_cycles = (float(line["capacity"]) * MS_PER_DAY) / total_ms

        for order in template_orders:
            recipe = order.get("production_recipe", {})
            recipe_duration = float(recipe.get("duration") or 0)

            if recipe_duration == 0:
                continue

            order_duration = float(order.get("duration") or 0)
            duration_multiplier = order_duration / recipe_duration

            # Inputs (Consumption)
            for p_input in recipe.get("inputs", []):
                ticker = p_input.get("ticker")
                if ticker:
                    factor = -float(p_input.get("factor", 0)) * duration_multiplier
                    line_net_flow[ticker] += factor

                    # Capture metadata for later
                    if ticker not in material_info_map:
                        material_info_map[ticker] = {"weight": p_input.get("weight", 0), "volume": p_input.get("volume", 0)}

            # Outputs (Production)
            for p_output in recipe.get("outputs", []):
                ticker = p_output.get("ticker")
                if ticker:
                    factor = float(p_output.get("factor", 0)) * duration_multiplier
                    line_net_flow[ticker] += factor

                    if ticker not in material_info_map:
                        material_info_map[ticker] = {"weight": p_output.get("weight", 0), "volume": p_output.get("volume", 0)}

        # --- AGGREGATE TO SITE (THE FIX) ---
        for ticker, unscaled_flow in line_net_flow.items():
            final_flow = unscaled_flow * daily_cycles

            # Save per-line stats (Net is fine here for UI)
            line["daily_flow"][ticker] = final_flow

            # Aggregate to Site (Split +/- to prevent netting)
            if final_flow > 0:
                gross_production[ticker] += final_flow
            elif final_flow < 0:
                gross_consumption[ticker] += abs(final_flow)

    # --- FORMAT OUTPUT ---
    output_production = []
    output_consumption = []

    # Helper to format dict
    def format_entry(ticker, amount):
        props = material_info_map.get(ticker, {})
        return {
            "materialTicker": ticker,
            "amount": amount,
            "tonnage": amount * float(props.get("weight", 0)),
            "volume": amount * float(props.get("volume", 0))
        }

    for ticker, amount in gross_production.items():
        output_production.append(format_entry(ticker, amount))

    for ticker, amount in gross_consumption.items():
        output_consumption.append(format_entry(ticker, amount))

    return {
        "dailyProduction": output_production,
        "dailyConsumption": output_consumption,
    }

async def generate_ai_logistics_strategy(
    summary_data: Dict[str, Any],
    recommendations: List[Dict[str, Any]],
    ships: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Takes the mathematical bottlenecks and manually generated recommendations,
    then asks the AI to prioritize them and assign specific ships.
    """

    # 1. Filter Context (Don't send irrelevant data to save tokens)
    # We only care about idle ships with capacity
    available_ships = [
        {
            "id": s["id"],
            "name": s["name"],
            "location": s["locationId"],
            "capacity": s["shipStorage"]["maxTonnage"] - s["shipStorage"]["currentTonnage"]
        }
        for s in ships
        if s["status"] == "idle" and s.get("shipStorage")
    ]

    if not summary_data.get("bottlenecks") and not recommendations:
        return {
            "strategy_name": "Maintenance Mode",
            "reasoning": "No critical bottlenecks detected.",
            "orders": []
        }

    # 2. Construct the Prompt
    system_prompt = """
    You are a Logistics Officer for a space corporation.
    Your goal is to prevent production halts by assigning ships to transport materials.
    
    RULES:
    1. Prioritize 'critical' bottlenecks (days_to_depletion < 1).
    2. Only assign ships that are in the 'available_ships' list.
    3. Do not assign a ship if its capacity is too small for the load.
    4. Return a JSON object with a strategy summary and a list of specific orders.
    """

    user_payload = {
        "current_bottlenecks": summary_data.get("bottlenecks", []),
        "heuristic_recommendations": recommendations, # The math-based suggestions
        "available_ships": available_ships
    }

    prompt = f"""
    Analyze the situation and assign ships.
    
    SITUATION DATA:
    {orjson.dumps(user_payload).decode()}
    
    REQUIRED JSON FORMAT:
    {{
        "strategy_name": "Title of the plan",
        "reasoning": "Brief explanation of priorities",
        "orders": [
            {{
                "ship_id": "ID from available_ships",
                "source_site_id": "ID from recommendations",
                "target_site_id": "ID from bottlenecks",
                "material": "ticker",
                "amount": 100
            }}
        ]
    }}
    """

    # 3. Call AI
    ai_result = await query_local_ai_json(prompt, system_message=system_prompt)

    if not ai_result:
        # Fallback if AI fails: Return the heuristic recommendations wrapped in the format
        return {
            "strategy_name": "Automated Heuristic Plan (AI Offline)",
            "reasoning": "AI service unavailable. Using standard mathematical models.",
            "orders": [] # Frontend handles raw recommendations in this case
        }

    return ai_result

def calculate_logistics_summary_and_recommendations(sites_data: List[Dict[str, Any]], ships_data: List[Dict[str, Any]]):
    system_net_balance = defaultdict(float)
    required_transport_tonnage, required_transport_volume = 0, 0

    for site in sites_data:
        for prod in site.get("dailyProduction", []):
            system_net_balance[prod["materialTicker"]] += prod["amount"]
        for cons in site.get("dailyConsumption", []):
            system_net_balance[cons["materialTicker"]] -= cons["amount"]
            required_transport_tonnage += cons.get("tonnage", 0)
            required_transport_volume += cons.get("volume", 0)

    bottlenecks = []
    for site in sites_data:
        inventory = {item["ticker"]: item["amount"] for item in site.get("storage_items", [])}
        for cons_data in site.get("dailyConsumption", []):
            material = cons_data["materialTicker"]
            prod_amount = next(
                (p.get("amount", 0) for p in site.get("dailyProduction", []) if p["materialTicker"] == material),
                0,
            )
            net_change = prod_amount - cons_data.get("amount", 0)

            if net_change < 0:
                current_amount = inventory.get(material, 0)
                days_to_depletion = current_amount / abs(net_change) if net_change != 0 else float("inf")
                if days_to_depletion < 3:
                    bottlenecks.append(
                        {
                            "type": "material_shortage",
                            "siteId": site["id"],
                            "siteName": site.get("name"),
                            "materialTicker": material,
                            "details": f"{site.get('name')} will run out of {material} in {days_to_depletion:.1f} days.",
                            "dailyNeed": abs(net_change),
                            "dailyTonnage": cons_data.get("tonnage", 0),
                            "dailyVolume": cons_data.get("volume", 0),
                            "currentAmount": current_amount,
                        }
                    )

    total_ship_tonnage = sum(
        s["shipStorage"]["maxTonnage"] for s in ships_data if s.get("status") == "idle" and s.get("shipStorage")
    )
    total_ship_volume = sum(
        s["shipStorage"]["maxVolume"] for s in ships_data if s.get("status") == "idle" and s.get("shipStorage")
    )
    tonnage_ratio = total_ship_tonnage / required_transport_tonnage if required_transport_tonnage > 0 else float("inf")
    volume_ratio = total_ship_volume / required_transport_volume if required_transport_volume > 0 else float("inf")

    sufficiency = "sufficient"
    if tonnage_ratio < 1 or volume_ratio < 1:
        sufficiency = "insufficient"
    elif tonnage_ratio < 1.2 or volume_ratio < 1.2:
        sufficiency = "barely_sufficient"

    transport_analysis = {
        "totalShipCapacityTonnage": total_ship_tonnage,
        "requiredTransportTonnage": required_transport_tonnage,
        "totalShipCapacityVolume": total_ship_volume,
        "requiredTransportVolume": required_transport_volume,
        "sufficiencyStatus": sufficiency,
    }

    recommendations = []
    rec_id_counter = 1
    for bottleneck in sorted(
        bottlenecks,
        key=lambda b: (b["currentAmount"] / b["dailyNeed"]) if b["dailyNeed"] > 0 else float("inf"),
    ):
        material_needed = bottleneck["materialTicker"]
        best_source, max_surplus = None, 0
        for site in sites_data:
            if site["id"] == bottleneck["siteId"]:
                continue
            inventory = {item["ticker"]: item["amount"] for item in site.get("storage_items", [])}
            surplus = inventory.get(material_needed, 0)
            if surplus > max_surplus:
                max_surplus, best_source = surplus, site
        if best_source:
            amount_to_ship = min(bottleneck["dailyNeed"] * 3, max_surplus)
            recommendations.append(
                {
                    "id": f"rec-{rec_id_counter}",
                    "fromId": best_source["id"],
                    "fromName": best_source["name"],
                    "toId": bottleneck["siteId"],
                    "toName": bottleneck["siteName"],
                    "materialTicker": material_needed,
                    "amount": round(amount_to_ship),
                    "priority": "critical" if (bottleneck["currentAmount"] / bottleneck["dailyNeed"] < 1) else "high",
                    "reason": f"Prevent production halt at {bottleneck['siteName']}",
                }
            )
            rec_id_counter += 1

    summary_result = {
        "systemNetBalance": [{"materialTicker": k, "netAmount": v} for k, v in system_net_balance.items()],
        "transportAnalysis": transport_analysis,
        "bottlenecks": bottlenecks,
    }
    return summary_result, recommendations
