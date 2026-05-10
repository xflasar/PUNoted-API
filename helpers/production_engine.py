# AI GENERATED CODE Help Well not really but yeah

import logging
import math
from collections import defaultdict
from typing import Any, Dict, List

import asyncpg

from helpers.production_engine_helpers.distribute_infrastructure import distribute_infrastructure
from helpers.production_engine_helpers.get_total_demand import get_total_demand

logger = logging.getLogger("production_engine")

PERMIT_CAPS = [500, 750, 1000]
MS_PER_DAY = 86400000

CX_SYSTEMS = {
    "AI1": "8ecf9670ba070d78cfb5537e8d9f1b6c", "CI1": "92029ff27c1abe932bd2c61ee4c492c7", "CI2": "a4ba8b12739da65efc2b518703652ee1",
    "NC1": "49b6615d39ccba05752b3be77b2ebf36", "NC2": "afda9bea7f948f4a066a8882cdfa9055", "IC1": "f2f57766ebaca9d69efae41ccf4d8853"
}

# Materials required for planetary environmental conditions
ENV_MATS = ['MCG', 'AEF', 'SEA', 'HSE', 'MGC', 'BL', 'INS', 'TSH']

async def find_optimal_production_chain(
    conn: asyncpg.Connection,
    ship_parts: List[str],
    overrides: Dict[str, str] = None,
    hub_ticker: str = "AI1",
    allowed_tiers: List[int] = [1, 3]
) -> Dict[str, Any]:
    hub_sys_id = CX_SYSTEMS.get(hub_ticker, "f2f57766ebaca9d69efae41ccf4d8853")

    # 0. Get BOM Demand
    calc_result = await get_total_demand(conn, ship_parts, overrides)
    if calc_result["status"] == "ambiguous":
        return calc_result

    total_demand = calc_result["total_demand"]
    raw_demand = calc_result["raw_demand"]
    resolved_recipes = calc_result["resolved_recipes"]

    final_products = set(ship_parts)

    # 1. Fetch Building Metadata & Build Materials
    b_rows = await conn.fetch("""
        SELECT b.buildingid, b.ticker, b.area, b.type, b.expertisecategory as cat,
               wc.workforcelevel, wc.capacity
        FROM buildings b
        LEFT JOIN building_workforce_capacities wc ON wc.buildingid = b.buildingid
    """)

    # Fetch Standard Construction Recipes
    bbm_rows = await conn.fetch("""
        SELECT b.ticker as b_ticker, m.ticker as m_ticker, bbm.amount 
        FROM building_build_materials bbm
        JOIN buildings b ON b.buildingid = bbm.buildingid
        JOIN materials m ON m.materialid = bbm.materialid
    """)

    building_recipes = defaultdict(dict)
    construction_materials = set(ENV_MATS) # Start with Environmental Mats
    for r in bbm_rows:
        building_recipes[r['b_ticker']][r['m_ticker']] = float(r['amount'])
        construction_materials.add(r['m_ticker'])

    ticker_to_meta = {}
    building_footprints = {}

    for r in b_rows:
        t = r['ticker']
        if t not in ticker_to_meta:
            ticker_to_meta[t] = {
                "reactor": r['buildingid'],
                "area": r['area'], "type": r['type'],
                "cat": (r['cat'] or "MANUFACTURING").upper(),
                "workforce": [],
                "base_cost": 0.0 # Standard Prefab Cost (without environment)
            }
        if r['workforcelevel']:
            ticker_to_meta[t]['workforce'].append({"level": r['workforcelevel'], "amount": float(r['capacity'])})
        building_footprints[t] = r['area'] + 15 + 12

    res_rows = await conn.fetch("""
        SELECT pr.planetid, pr.factor as raw_conc, pr.type as res_type, m.ticker
        FROM planet_resources pr 
        JOIN materials m ON m.materialid = pr.materialid 
        WHERE m.ticker = ANY($1) AND pr.factor > 0
    """, list(raw_demand.keys()))

    # 2. Market Check (BOM + All Construction Materials)
    all_tickers = list(set(list(total_demand.keys()) + list(raw_demand.keys()) + list(construction_materials)))

    search_patterns = [f"{t}.%" for t in all_tickers]
    market_rows = await conn.fetch("""
        SELECT cb.ticker AS full_ticker, cb.askprice AS buy_price, cb.askamount AS supply 
        FROM cx_brokers cb
        INNER JOIN commodity_exchanges ce ON ce.id = cb.exchangeid
        WHERE ce.code = $1 AND cb.ticker LIKE ANY($2)
    """, hub_ticker, search_patterns)

    market_map = {r['full_ticker'].split('.')[0]: {"price": float(r['buy_price'] or 0), "supply": int(r['supply'] or 0)} for r in market_rows}

    # 3. Calculate BASE Building Costs (Prefabs only)
    for b_ticker, meta in ticker_to_meta.items():
        recipe = building_recipes.get(b_ticker, {})
        cost = 0.0
        for mat, amt in recipe.items():
            price = market_map.get(mat, {'price': 0})['price']
            cost += (amt * price)
        meta['base_cost'] = cost

    # 4. Refine Demand (Market Buy Logic)
    refined_total_demand = dict(total_demand)
    refined_raw_demand = dict(raw_demand)
    market_purchase_list = {}
    total_purchase_cost = 0

    PRICE_THRESHOLD = 300.0
    MICRO_SITE_THRESHOLD = 300.0
    MICRO_SITE_PRICE_CAP = 500.0

    # A. Check RAW Materials
    for ticker, qty in list(refined_raw_demand.items()):
        if ticker in final_products: continue
        m_data = market_map.get(ticker)

        actual_area_needed = 9999
        mat_res = [r for r in res_rows if r['ticker'] == ticker]
        if mat_res:
            best_conc = max([r['raw_conc'] for r in mat_res])
            res_type = mat_res[0]['res_type']
            reactor = "COL" if res_type == "GASEOUS" else ("RIG" if res_type == "LIQUID" else "EXT")
            daily_yield = (best_conc * 100) * (0.6 if reactor == "COL" else 0.7)
            needed_b = math.ceil(qty / daily_yield)
            actual_area_needed = needed_b * building_footprints.get(reactor, 127)
        continue

        if m_data:
            if actual_area_needed < MICRO_SITE_THRESHOLD and m_data['supply'] >= qty and m_data['price'] <= MICRO_SITE_PRICE_CAP:
                cost = m_data['price'] * qty
                market_purchase_list[ticker] = {"qty": qty, "cost": cost, "reason": f"Micro-Site ({round(actual_area_needed)} area)"}
                total_purchase_cost += cost
                #del refined_raw_demand[ticker]
                #continue

            supply_weeks = m_data['supply'] / (qty or 1)
            if supply_weeks >= 1.0 and m_data['price'] <= PRICE_THRESHOLD:
                cost = m_data['price'] * qty
                market_purchase_list[ticker] = {"qty": qty, "cost": cost, "reason": f"Deep Supply ({round(supply_weeks, 1)}x)"}
                total_purchase_cost += cost
                #del refined_raw_demand[ticker]

    # B. Check MANUFACTURED Goods
    for ticker, qty in list(refined_total_demand.items()):
        if ticker in final_products: continue
        continue

        m_data = market_map.get(ticker)
        if m_data and m_data['supply'] >= qty and m_data['price'] <= PRICE_THRESHOLD:
            cost = m_data['price'] * qty
            market_purchase_list[ticker] = {"qty": qty, "cost": cost, "reason": "Cheap Manufactured"}
            total_purchase_cost += cost
            del refined_total_demand[ticker]

            rec = resolved_recipes.get(ticker)
            if rec:
                runs = qty / rec['out_q']
                for inp in rec['inputs']:
                    if inp['ticker'] in refined_raw_demand:
                        refined_raw_demand[inp['ticker']] -= (inp['amount'] * runs)
                        if refined_raw_demand[inp['ticker']] <= 0: del refined_raw_demand[inp['ticker']]

    # 5. Finalize Requirements
    id_to_ticker = {r['buildingid']: r['ticker'] for r in b_rows}
    building_reqs = defaultdict(int)
    for t, qty in refined_total_demand.items():
        rec = resolved_recipes.get(t)
        if not rec: continue

        calc_qty = qty
        if t in final_products:
             daily_output_one_building = (MS_PER_DAY / rec['duration']) * rec['out_q']
             calc_qty = daily_output_one_building

        resolved_ticker = id_to_ticker.get(rec['reactor'], rec['reactor'])
        building_reqs[(resolved_ticker, t)] = math.ceil(calc_qty / ((MS_PER_DAY / rec['duration']) * rec['out_q']))

    global_reqs = {
        "building_requirements": building_reqs,
        "buildings_meta": ticker_to_meta,
        "total_demand": refined_total_demand,
        "raw_demand": refined_raw_demand,
        "recipes": resolved_recipes,
        "market_prices": market_map # Pass prices to Distributor for dynamic calc
    }

    # 6. Allocation
    sites = await distribute_infrastructure(conn, global_reqs, hub_sys_id, allowed_tiers)

    # 7. UI Data & Final Totals
    sys_rows = await conn.fetch("SELECT systemid, name, positionx as x, positiony as y FROM systems")
    conn_rows = await conn.fetch("SELECT systemidorigin, systemiddestination FROM system_connections")

    total_infra_cost = sum(s.get('total_build_cost', 0) for s in sites)

    for s in sites:
         s['permit_cap'] = next((cap for cap in sorted([500, 750, 1000]) if cap >= s['area_used']), 1000)

    return {
        "status": "success", "sites": sites, "market_purchases": market_purchase_list,
        "summary": {
            "total_area": sum(s['area_used'] for s in sites),
            "market_cost_ica": total_purchase_cost,
            "infrastructure_cost_ica": total_infra_cost,
            "total_project_cost": total_purchase_cost + total_infra_cost,
            "total_permits": sum(1 if s.get('permit_cap', 1000) <= 500 else (2 if s.get('permit_cap', 1000) <= 750 else 3) for s in sites),
            "total_demand": total_demand, "raw_needed": refined_raw_demand
        },
        "galaxy_data": {
            "systems": [dict(r) for r in sys_rows],
            "connections": [dict(r) for r in conn_rows],
            "hub_id": hub_sys_id
        }
    }
