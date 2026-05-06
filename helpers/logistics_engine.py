# AI GENERATED CODE Help Well not really but yeah

import asyncio
import logging
import json
import networkx as nx
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import asyncpg
from datetime import datetime, timezone
import math

from app.core.ai_client import query_local_ai_json
from helpers.production_lines import get_production_data_nested
from helpers.logistics_analysis import calculate_site_production_flow

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
MS_PER_HOUR = 3600 * 1000
DEFAULT_STL_FUEL_COST = 8500  
DEFAULT_FTL_FUEL_COST = 3600

# Thresholds
MIN_BATCH_VOLUME = 500  
MIN_BATCH_WEIGHT = 500  
STORAGE_SAFETY_BUFFER = 0.15 

# Fuel Tanks
TANK_CAPACITY = {
    'TINY': {'FTL': 500, 'STL': 2000},
    'LCB':  {'FTL': 1500, 'STL': 4000},
    'WCB':  {'FTL': 3000, 'STL': 8000},
    'VCB':  {'FTL': 3000, 'STL': 8000},
    'HCB':  {'FTL': 6000, 'STL': 15000}
}

async def run_logistics_pipeline(db, user_id: str):
    async with db.pool.acquire() as conn:
        fleet = await fetch_fleet_status(conn, user_id)
        galaxy_graph = await fetch_system_map(conn)
        workforce_needs = await fetch_workforce_needs(conn, user_id)
        sites = await fetch_site_profiles(conn, user_id, workforce_needs)
        warehouses = await fetch_user_warehouses(conn, user_id)
        cx_data = await fetch_cx_data(conn)
        mat_stats = await fetch_material_stats(conn)
        flight_history, global_avgs = await fetch_flight_history_stats(conn)

    # 1. Generate Demand/Supply List
    tasks, sources = analyze_logistics_state(sites, warehouses, cx_data, mat_stats, fleet)

    # 2. Add Relocation Tasks (Ships stuck in empty systems)
    relocation_tasks = generate_return_to_hub_tasks(fleet, warehouses)
    if relocation_tasks:
        tasks.extend(relocation_tasks)

    if not tasks:
        return {
            "success": True, 
            "status": "Logistics Optimized.", 
            "data": {"director_commentary": "Global supply chain balanced. No actions required.", "orders": []}
        }

    # 3. GLOBAL FLEET OPTIMIZATION
    route_plan, unserved_tasks = optimize_routes_global(
        tasks, fleet, galaxy_graph, mat_stats, flight_history, global_avgs, warehouses, sources
    )

    fleet_advice = generate_fleet_advice(unserved_tasks, fleet)
    ai_response = await generate_ai_master_plan(route_plan, fleet_advice)
    
    return {"success": True, "data": ai_response}


# ==========================================
# STEP 3: GLOBAL ROUTE OPTIMIZER (The Brain)
# ==========================================

def optimize_routes_global(tasks, fleet, galaxy_graph, mat_stats, flight_history, global_avgs, warehouses, sources):
    """
    Assigns tasks to the entire fleet simultaneously based on a weighted scoring matrix.
    Sorts final routes using Nearest Neighbor logic to prevent zig-zagging.
    Enforces strict STL vs FTL capability rules.
    """
    
    # --- A. SETUP SIMULATION STATE ---
    sim_ships = []
    now = datetime.now(timezone.utc)
    
    for s in fleet:
        # Timezone Fix
        arrival = s.get('flight_arrival')
        if arrival and arrival.tzinfo is None: arrival = arrival.replace(tzinfo=timezone.utc)
        
        is_busy = s['status'] == 'FLIGHT' or (s['flightid'] and arrival and arrival > now)
        
        # Calculate Effective Location & Availability
        location = s['system_id']
        avail_mins = 0
        
        if is_busy:
            location = s.get('flight_dest_sys', s['system_id'])
            if arrival:
                avail_mins = max(1, (arrival - now).total_seconds() / 60)
            else:
                avail_mins = 60 
        
        # KEY FIX: Use defaultdict(int) to prevent KeyError during subtraction
        manifest = defaultdict(int)
        for i in s.get('inventory', []):
            manifest[i['ticker']] = i['amount']

        sim_ships.append({
            "id": s['shipid'],
            "data": s,
            "loc": location,
            "busy_for": int(avail_mins),
            "free_weight": (s['weightcapacity'] or 0) - (s.get('weightload') or 0),
            "free_vol": (s['volumecapacity'] or 0) - (s.get('volumeload') or 0),
            "stops": [],
            "cargo_manifest": manifest, 
            "score_log": []
        })

    # Index Sources
    source_map = defaultdict(list)
    
    # 1. Static Sources
    for src in sources:
        source_map[src['material']].append(src)
        
    # 2. Mobile Sources (Ships with cargo)
    for sim in sim_ships:
        for mat, amt in sim['cargo_manifest'].items():
            if amt > 0:
                source_map[mat].append({
                    "source_type": "SHIP_INVENTORY",
                    "source_id": sim['id'],
                    "name": "Onboard",
                    "system_id": sim['loc'], 
                    "material": mat,
                    "amount": amt
                })

    solved_tasks = set()
    
    # --- B. THE ASSIGNMENT LOOP ---
    while True:
        best_assignment = None
        best_score = -float('inf')
        
        pending_pulls = [t for t in tasks if t['type'] == 'PULL' and id(t) not in solved_tasks]
        pending_relocs = [t for t in tasks if t['type'] == 'RELOCATE' and id(t) not in solved_tasks]
        
        if not pending_pulls and not pending_relocs: break

        # 1. EVALUATE RELOCATIONS
        for task in pending_relocs:
            sim = next((s for s in sim_ships if s['id'] == task['ship_id']), None)
            if sim and not sim['stops']: 
                # [STRICT CONSTRAINT] STL Ships cannot relocate between systems
                if not sim['data']['is_ftl'] and sim['loc'] != task['target_system']:
                    continue

                best_assignment = (sim, task, None, 10000)
                break
        if best_assignment:
            sim, task, _, _ = best_assignment
            sim['stops'].append({"action": "FLY", "location": task['target_name'], "system": task['target_system'], "reason": "Relocation"})
            sim['loc'] = task['target_system']
            solved_tasks.add(id(task))
            continue

        # 2. EVALUATE SUPPLY JOBS
        for task in pending_pulls:
            mat = task['material']
            needed = task['amount_needed']
            
            potential_sources = source_map.get(mat, [])
            if not potential_sources: continue 

            for sim in sim_ships:
                s_stats = mat_stats.get(mat, {'weight': 1, 'volume': 1})
                max_w = sim['free_weight'] / s_stats['weight']
                max_v = sim['free_vol'] / s_stats['volume']
                max_carry = int(min(max_w, max_v))
                
                # Exception: If source is THIS ship, we don't need free space, we just need the item
                is_onboard_source = any(src['source_type'] == 'SHIP_INVENTORY' and src['source_id'] == sim['id'] for src in potential_sources)
                if not is_onboard_source and max_carry <= 0: continue 

                for src in potential_sources:
                    if src['system_id'] == task['target_system'] and src['source_type'] != 'SHIP_INVENTORY':
                        continue 

                    # [STRICT CONSTRAINT] STL / FTL Logic
                    if not sim['data']['is_ftl']:
                        # 1. STL Ship must BE at the source system (cannot jump to get it)
                        if sim['loc'] != src['system_id']: continue
                        # 2. Source system must BE the target system (cannot jump to deliver)
                        if src['system_id'] != task['target_system']: continue

                    dist_to_src = get_jumps(galaxy_graph, sim['loc'], src['system_id'])
                    dist_to_dst = get_jumps(galaxy_graph, src['system_id'], task['target_system'])
                    total_dist = dist_to_src + dist_to_dst
                    
                    score = 100 - (total_dist * 5) - (sim['busy_for'] * 0.5)
                    
                    if src['source_type'] == 'SHIP_INVENTORY' and src['source_id'] == sim['id']:
                        score += 500
                        dist_to_src = 0
                        # If onboard, we can deliver even if "free space" is 0 (we are emptying it)
                        max_carry = src['amount'] 
                    
                    if sim['loc'] == src['system_id']: score += 20
                    
                    if score > best_score:
                        best_score = score
                        best_assignment = (sim, task, src, max_carry)

        # 3. APPLY BEST ASSIGNMENT
        if best_assignment:
            sim, task, src, max_carry = best_assignment
            amount = min(needed, src['amount'], max_carry)
            
            if amount > 0:
                s_stats = mat_stats.get(task['material'], {'weight': 1, 'volume': 1})

                if src['source_type'] != 'SHIP_INVENTORY':
                    sim['stops'].append({
                        "action": "PICKUP",
                        "location": src['name'],
                        "system": src['system_id'],
                        "material": task['material'],
                        "amount": amount,
                        "reason": f"Source for {task['target_name']}"
                    })
                    src['amount'] -= amount
                    # Reduce capacity only if picking up new stuff
                    sim['free_weight'] -= amount * s_stats['weight']
                    sim['free_vol'] -= amount * s_stats['volume']
                else:
                    # It's onboard, deduct from manifest logic
                    # SAFE SUBTRACTION with default int
                    sim['cargo_manifest'][task['material']] -= amount
                    # We are freeing up space!
                    sim['free_weight'] += amount * s_stats['weight']
                    sim['free_vol'] += amount * s_stats['volume']

                sim['stops'].append({
                    "action": "UNLOAD",
                    "location": task['target_name'],
                    "system": task['target_system'],
                    "material": task['material'],
                    "amount": amount,
                    "reason": "Supply Fulfillment"
                })

                sim['loc'] = task['target_system'] 
                
                task['amount_needed'] -= amount
                if task['amount_needed'] <= 0:
                    solved_tasks.add(id(task))
            else:
                break
        else:
            break

    # --- C. ROUTE CONSTRUCTION & SMOOTHING ---
    final_routes = []
    
    for sim in sim_ships:
        if not sim['stops'] and not sim['busy_for']: continue
        
        route = {
            "ship_id": sim['id'], "ship_name": sim['data']['name'], 
            "strategy": "Global Opt.",
            "status": "ACTIVE" if sim['data']['status'] == 'FLIGHT' else "READY",
            "start_delay_mins": sim['busy_for'],
            "stops": [],
            "cargo_weight": sim['data'].get('weightload', 0),
            "cargo_vol": sim['data'].get('volumeload', 0),
            "fuel_rem_ftl": sim['data'].get('fuel_rem_ftl', 0),
            "fuel_rem_stl": sim['data'].get('fuel_rem_stl', 0)
        }

        current_sys = sim['data']['system_id']
        if sim['busy_for'] > 0:
            route['stops'].append({
                "action": "FINISH_FLIGHT",
                "location": f"Transit -> {sim['loc']}", 
                "est_time": f"{sim['busy_for']}m"
            })
            current_sys = sim['loc']

        ordered_stops = optimize_stop_sequence(sim['stops'], current_sys, galaxy_graph)
        
        for stop in ordered_stops:
            if stop['system'] != current_sys:
                t, c, f_f, f_s = estimate_flight_metrics(current_sys, stop['system'], galaxy_graph, flight_history, global_avgs)
                route['stops'].append({
                    "action": "FLY",
                    "location": get_base_name(stop['location']),
                    "reason": "Transit",
                    "est_time": f"{int(t/60000)}m",
                    "fuel_est": f"FTL:{int(f_f)} STL:{int(f_s)}"
                })
                current_sys = stop['system']
            
            route['stops'].append(stop)
            
            if 'amount' in stop and 'material' in stop:
                s_stats = mat_stats.get(stop['material'], {'weight':1, 'volume':1})
                if stop['action'] == 'PICKUP':
                    route['cargo_weight'] += stop['amount'] * s_stats['weight']
                    route['cargo_vol'] += stop['amount'] * s_stats['volume']
                elif stop['action'] == 'UNLOAD':
                    route['cargo_weight'] -= stop['amount'] * s_stats['weight']
                    route['cargo_vol'] -= stop['amount'] * s_stats['volume']

        route['stops'] = consolidate_stops_aggregated(route['stops'])
        if route['stops'] or sim['busy_for'] > 0:
            final_routes.append(route)

    unserved = [t for t in tasks if id(t) not in solved_tasks and t['severity'] != 'PASSIVE']
    return final_routes, unserved


# ==========================================
# HELPERS
# ==========================================

def get_jumps(G, start, end):
    if start == end: return 0
    try:
        if G.has_node(start) and G.has_node(end):
            return nx.shortest_path_length(G, start, end)
    except: pass
    return 99 # Unreachable penalty

def optimize_stop_sequence(stops, start_sys, G):
    """
    Organizes a list of stops to minimize travel.
    Preserves order of Pick -> Drop for the same item roughly by keeping pairs close,
    but optimizes System A -> System B travel.
    """
    if not stops: return []
    
    # 1. Group by System
    sys_groups = defaultdict(list)
    for s in stops:
        sys_groups[s['system']].append(s)
        
    # 2. Sort Systems by Nearest Neighbor
    sorted_systems = []
    current = start_sys
    remaining_systems = list(sys_groups.keys())
    
    while remaining_systems:
        # Find closest system
        closest = None
        min_dist = 9999
        
        for sys in remaining_systems:
            d = get_jumps(G, current, sys)
            if d < min_dist:
                min_dist = d
                closest = sys
        
        if closest:
            sorted_systems.append(closest)
            remaining_systems.remove(closest)
            current = closest
        else:
            # Graph disconnect? Just create remaining
            sorted_systems.extend(remaining_systems)
            break
            
    # 3. Flatten
    final_order = []
    for sys in sorted_systems:
        # Inside a system, prioritize Unloads (free up space) then Pickups
        actions = sys_groups[sys]
        actions.sort(key=lambda x: 0 if x['action'] == 'UNLOAD' else 1)
        final_order.extend(actions)
        
    return final_order

async def fetch_user_warehouses(conn: asyncpg.Connection, user_id: str) -> List[Dict]:
    query = """
    SELECT w.warehouseid, s.name as station_name, s.systemid,
        st.storageid, st.weightcapacity, st.volumecapacity, st.weightload, st.volumeload,
        COALESCE(json_agg(json_build_object('ticker', m.ticker, 'amount', si.quantity)) 
                 FILTER (WHERE m.ticker IS NOT NULL), '[]') as inventory
    FROM warehouses w
    INNER JOIN stations s ON w.warehouseid = s.warehouseid
    LEFT JOIN storages st ON w.warehouseid = st.addressableid
    LEFT JOIN storage_items si ON st.storageid = si.storageid
    LEFT JOIN materials m ON si.materialid = m.materialid
    INNER JOIN users u ON st.userid = u.userdataid
    WHERE u.accountid = $1
    GROUP BY w.warehouseid, s.name, s.systemid, st.storageid
    """
    rows = await conn.fetch(query, user_id)
    return [{"id": r['warehouseid'], "name": r['station_name'], "system_id": r['systemid'], "is_hub": True, "storage": json.loads(r['inventory']) if isinstance(r['inventory'], str) else r['inventory'], "siteStorage": {"maxVol": r['volumecapacity'], "currVol": r['volumeload'], "maxWgt": r['weightcapacity'], "currWgt": r['weightload']}} for r in rows]

async def fetch_workforce_needs(conn: asyncpg.Connection, user_id: str) -> Dict[str, List[Dict]]:
    query = """
    SELECT wf.siteid, wf.level, wf.population, needs_data.needs
    FROM workforces wf
    INNER JOIN sites s ON s.siteid = wf.siteid
    INNER JOIN users u ON u.userdataid = s.userid
    INNER JOIN LATERAL (
        SELECT jsonb_agg(jsonb_build_object('ticker', m.ticker, 'unitsper100', wfn.unitsper100)) AS needs
        FROM workforce_needs wfn
        INNER JOIN materials m ON m.materialid = wfn.materialid
        WHERE wfn.workforceid = wf.workforceid
        GROUP BY wf.workforceid
    ) AS needs_data ON TRUE
    WHERE u.accountid = $1
    """
    rows = await conn.fetch(query, user_id)
    workforce_by_site = defaultdict(list)
    for r in rows:
        needs = json.loads(r['needs']) if isinstance(r['needs'], str) else r['needs']
        workforce_by_site[r['siteid']].append({"population": r['population'], "needs": needs})
    return workforce_by_site

async def fetch_site_profiles(conn: asyncpg.Connection, user_id: str, workforce_map: Dict) -> List[Dict]:
    raw_data = await get_production_data_nested(conn, user_id, include_logistics_data=True)
    site_systems = {row['siteid']: row['addresssystemid'] for row in await conn.fetch("SELECT s.siteid, s.addresssystemid FROM sites s JOIN users u ON s.userid = u.userdataid WHERE u.accountid = $1", user_id)}
    processed = []
    for sid, data in raw_data.items():
        flow = calculate_site_production_flow(data)
        daily_cons = {i['materialTicker']: i['amount'] for i in flow['dailyConsumption']}
        if sid in workforce_map:
            for tier in workforce_map[sid]:
                pop = tier['population']
                for need in tier['needs']:
                    mat = need['ticker']
                    rate = need.get('unitsper100', 0)
                    daily_amount = (pop / 100.0) * rate
                    daily_cons[mat] = daily_cons.get(mat, 0) + daily_amount
        processed.append({"id": sid, "name": f"{data['planet_name']} (Site Storage)", "system_id": site_systems.get(sid), "is_hub": False, "inputs": list(daily_cons.keys()), "outputs": [i['materialTicker'] for i in flow['dailyProduction']], "storage": data.get("storage_items", []), "consumption_rates": daily_cons, "production_rates": {i['materialTicker']: i['amount'] for i in flow['dailyProduction']}, "siteStorage": data.get("siteStorage")})
        if data.get("warehouseStorage"):
            processed.append({"id": f"{sid}_WH", "name": f"{data['planet_name']} (Warehouse)", "system_id": site_systems.get(sid), "is_hub": False, "inputs": [], "outputs": [], "storage": data.get("warehouse_items", []), "consumption_rates": {}, "production_rates": {}, "siteStorage": data.get("warehouseStorage")})
    return processed

async def fetch_fleet_status(conn: asyncpg.Connection, user_id: str) -> List[Dict]:
    query = """
    SELECT s.shipid, COALESCE(s.name, s.registration, 'Ship-' || s.shipid) as name, s.registration, s.status, s.type, s.addresssystemid as system_id, s.flightid, s.reactorpower, st.storageid, st.weightcapacity, st.volumecapacity, st.weightload, st.volumeload, f.destinationsystemid as flight_dest_sys, f.departuretimestamp as flight_arrival, COALESCE(json_agg(json_build_object('ticker', m.ticker, 'amount', si.quantity)) FILTER (WHERE m.ticker IS NOT NULL), '[]') as inventory, CASE WHEN st.weightcapacity >= 5000 AND st.volumecapacity >= 5000 THEN 'HCB' WHEN st.weightcapacity >= 3000 AND st.volumecapacity >= 1000 THEN 'WCB' WHEN st.weightcapacity >= 1000 AND st.volumecapacity >= 3000 THEN 'VCB' WHEN st.weightcapacity >= 2000 THEN 'LCB' ELSE 'TINY' END as cargo_model, CASE WHEN s.reactorpower > 0 THEN TRUE ELSE FALSE END as is_ftl
    FROM ships s
    LEFT JOIN storages st ON s.idshipstore = st.storageid
    LEFT JOIN storage_items si ON st.storageid = si.storageid
    LEFT JOIN materials m ON si.materialid = m.materialid
    LEFT JOIN ship_flights f ON s.flightid = f.id
    INNER JOIN users u ON s.userid = u.userdataid
    WHERE u.accountid = $1
    GROUP BY s.shipid, s.name, s.registration, s.status, s.type, s.addresssystemid, s.flightid, s.reactorpower, st.storageid, st.weightcapacity, st.volumecapacity, st.weightload, st.volumeload, f.destinationsystemid, f.departuretimestamp
    """
    rows = await conn.fetch(query, user_id)
    results = []
    for r in rows:
        d = dict(r)
        d['inventory'] = json.loads(d['inventory']) if isinstance(d['inventory'], str) else d['inventory']
        results.append(d)
    return results

async def fetch_cx_data(conn: asyncpg.Connection) -> List[Dict]:
    rows = await conn.fetch("SELECT ex.name, ex.systemid, ex.code, br.ticker, br.askprice, br.askamount FROM cx_brokers br JOIN commodity_exchanges ex ON br.exchangeid = ex.id WHERE br.askamount > 0")
    return [{"source_type": "MARKET", "source_id": f"cx-{r['code']}", "name": r['name'], "system_id": r['systemid'], "material": r['ticker'], "amount": r['askamount'], "price": float(r['askprice'])} for r in rows]

async def fetch_material_stats(conn: asyncpg.Connection) -> Dict[str, Dict]:
    rows = await conn.fetch("SELECT ticker, weight, volume FROM materials")
    return {r['ticker']: {'weight': float(r['weight']), 'volume': float(r['volume'])} for r in rows}

async def fetch_flight_history_stats(conn: asyncpg.Connection) -> Tuple[Dict, Dict]:
    query = """
    SELECT originsystemid, destinationsystemid, AVG(EXTRACT(EPOCH FROM (departuretimestamp - arrivaltimestamp)) * 1000) as avg_ms, AVG(stltotalconsumption) as avg_stl, AVG(ftltotalconsumption) as avg_ftl
    FROM ship_flights WHERE aborted = FALSE AND arrivaltimestamp IS NOT NULL AND departuretimestamp IS NOT NULL AND originsystemid IS NOT NULL AND destinationsystemid IS NOT NULL GROUP BY originsystemid, destinationsystemid
    """
    rows = await conn.fetch(query)
    history = {}
    for r in rows: history[(r['originsystemid'], r['destinationsystemid'])] = {'avg_ms': float(r['avg_ms']), 'avg_stl_fuel': float(r['avg_stl']), 'avg_ftl_fuel': float(r['avg_ftl'])}
    g_row = await conn.fetchrow("SELECT AVG(stltotalconsumption) as global_stl, AVG(ftltotalconsumption) as global_ftl FROM ship_flights WHERE aborted = FALSE AND arrivaltimestamp IS NOT NULL")
    return history, {'stl_per_jump': float(g_row['global_stl'] or 120.0), 'ftl_per_jump': float(g_row['global_ftl'] or 60.0)}

async def fetch_system_map(conn: asyncpg.Connection) -> nx.Graph:
    rows = await conn.fetch("SELECT systemidorigin, systemiddestination FROM system_connections")
    G = nx.Graph()
    for r in rows: G.add_edge(r['systemidorigin'], r['systemiddestination'])
    return G

def analyze_logistics_state(sites, warehouses, cx_data, mat_stats, fleet):
    tasks = []   
    sources = [] 
    for wh in warehouses:
        for item in wh['storage']: sources.append({"source_type": "WAREHOUSE", "source_id": wh['id'], "name": wh['name'], "system_id": wh['system_id'], "material": item['ticker'], "amount": item['amount']})
    sources.extend(cx_data)
    for site in sites:
        inv_map = {i['ticker']: (i['amount'] or 0) for i in site['storage']}
        site_cap = site.get('siteStorage', {})
        max_vol, curr_vol = site_cap.get('maxVolume') or 0, site_cap.get('currentVolume') or 0
        safe_max_vol = max_vol * (1.0 - STORAGE_SAFETY_BUFFER)
        safe_free_vol = max(0, safe_max_vol - curr_vol)
        is_extraction = len(site['inputs']) == 0 and len(site['outputs']) > 0
        pull_target_days = 30 if is_extraction else 14
        for ticker in set(site['consumption_rates'].keys()) | set(site['production_rates'].keys()) | set(inv_map.keys()):
            prod_rate, cons_rate, current_amt = site['production_rates'].get(ticker, 0), site['consumption_rates'].get(ticker, 0), inv_map.get(ticker) or 0 
            net_flow = prod_rate - cons_rate
            if net_flow >= 0:
                excess_amt = current_amt - int(cons_rate * 1.0)
                if excess_amt > 0:
                    stats = mat_stats.get(ticker, {'weight':1, 'volume':1})
                    storage_pct = curr_vol / max_vol if max_vol else 0
                    if is_extraction:
                        severity = "CRITICAL" if storage_pct > 0.90 else ("HIGH" if (excess_amt * stats['weight'] > 4000) else "PASSIVE")
                    else:
                        severity = "CRITICAL" if storage_pct > (1.0 - STORAGE_SAFETY_BUFFER) else ("HIGH" if (excess_amt * stats['weight'] > MIN_BATCH_WEIGHT) else "PASSIVE")
                    target = next((h for h in warehouses if h['system_id'] == site['system_id']), warehouses[0] if warehouses else None)
                    if target: tasks.append({"type": "PUSH", "origin_site_id": site['id'], "origin_name": site['name'], "origin_system": site.get("system_id"), "target_site_id": target['id'], "target_name": target['name'], "target_system": target['system_id'], "material": ticker, "severity": severity, "amount_needed": excess_amt, "deadline_text": severity})
                    sources.append({"source_type": "STORAGE", "source_id": site['id'], "name": site['name'], "system_id": site.get("system_id"), "material": ticker, "amount": excess_amt})
            elif net_flow < 0:
                drain, days_left = abs(net_flow), (current_amt / abs(net_flow) if abs(net_flow) > 0 else 999)
                if days_left < pull_target_days:
                    needed = int(drain * pull_target_days) - current_amt
                    mat_s = mat_stats.get(ticker, {'weight':1, 'volume':1})
                    if mat_s['volume'] > 0: needed = min(needed, int(safe_free_vol / mat_s['volume']))
                    if needed > 0: tasks.append({"type": "PULL", "target_site_id": site['id'], "target_name": site['name'], "target_system": site.get("system_id"), "material": ticker, "severity": "CRITICAL" if days_left < 3 else ("HIGH" if days_left < 10 else "MAINTENANCE"), "amount_needed": needed, "deadline_hours": int(days_left * 24), "deadline_text": f"Empty in {days_left:.1f} days"})
    return tasks, sources

def generate_return_to_hub_tasks(fleet, warehouses):
    tasks = []
    idle_ftls = [s for s in fleet if s['is_ftl'] and s['flightid'] is None and s['status'] != 'FLIGHT']
    hub_ids = [w['system_id'] for w in warehouses]
    for ship in idle_ftls:
        if ship['system_id'] not in hub_ids:
            target = warehouses[0] if warehouses else None
            if target: tasks.append({"type": "RELOCATE", "ship_id": ship['shipid'], "target_name": target['name'], "target_system": target['system_id'], "severity": "OPTIMIZATION", "deadline_text": "ASAP"})
    return tasks

def calculate_max_cargo_avail(ship, curr_w, curr_v, mat_stats, mat):
    s = mat_stats.get(mat, {'weight': 1, 'volume': 1})
    free_w, free_v = (ship['weightcapacity'] or 0) - curr_w, (ship['volumecapacity'] or 0) - curr_v
    if free_w <= 0 or free_v <= 0: return 0
    return int(min(free_w / s['weight'], free_v / s['volume'])) if s['weight'] > 0 and s['volume'] > 0 else 0

def consolidate_stops_aggregated(stops):
    if not stops: return []
    merged_flat = []
    prev = stops[0]
    for curr in stops[1:]:
        if (prev['action'] == curr['action'] and prev['location'] == curr['location'] and prev.get('material') == curr.get('material') and prev['action'] in ['PICKUP', 'UNLOAD']):
            prev['amount'] += curr['amount']
        else:
            merged_flat.append(prev)
            prev = curr
    merged_flat.append(prev)
    final_grouped, current_group = [], None
    for s in merged_flat:
        if (current_group and s['action'] in ['PICKUP', 'UNLOAD'] and current_group['action'] == s['action'] and current_group['location'] == s['location']):
            current_group['items'].append({"material": s['material'], "amount": s['amount'], "reason": s.get('reason', '')})
        else:
            if current_group: final_grouped.append(current_group); current_group = None
            if s['action'] in ['PICKUP', 'UNLOAD']: current_group = {"action": s['action'], "location": s['location'], "items": [{"material": s['material'], "amount": s['amount'], "reason": s.get('reason', '')}]}
            else: final_grouped.append(s)
    if current_group: final_grouped.append(current_group)
    return final_grouped

def estimate_flight_metrics(origin, dest, graph, history, global_avgs):
    if not origin or not dest: return (0, 9999999, 0, 0)
    if origin == dest: return (1800000, 500, 0, 100) 
    if (origin, dest) in history:
        stats = history[(origin, dest)]
        return (stats['avg_ms'], (stats['avg_stl_fuel'] * 5) + stats['avg_ftl_fuel'], stats['avg_ftl_fuel'], stats['avg_stl_fuel'])
    if not nx.has_path(graph, origin, dest): raise nx.NetworkXNoPath
    jumps = nx.shortest_path_length(graph, origin, dest)
    est_ftl, est_stl = jumps * global_avgs.get('ftl_per_jump', 60), jumps * global_avgs.get('stl_per_jump', 120)
    return (jumps * 4 * MS_PER_HOUR, (est_stl * 5) + est_ftl, est_ftl, est_stl)

def get_base_name(full_name: str) -> str: return full_name.split(' (')[0]

def generate_fleet_advice(unserved_tasks, fleet):
    if not unserved_tasks: return []
    return [f"CRITICAL: {sum(1 for t in unserved_tasks if t['type']=='PULL')} tasks unserved. Fleet Capacity Exceeded."]

async def generate_ai_master_plan(route_plans, advice):
    context_routes = []
    for r in route_plans:
        stops_desc = []
        for s in r['stops']:
            if 'items' in s: 
                lines = [f"{i['amount']} {i['material']} ({i.get('reason','?')})" for i in s['items']]
                stops_desc.append(f"{s['action']} @ {s['location']}: " + ", ".join(lines))
            elif s['action'] == 'FLY': stops_desc.append(f"FLY -> {s['location']} ({s.get('est_time')}) Fuel: {s.get('fuel_est')}")
            else: stops_desc.append(f"{s['action']} @ {s['location']}")
        context_routes.append({"ship": r['ship_name'], "route": stops_desc})
    prompt = f"""You are the Logistics Director. Provide a concise plan. DO NOT GENERATE FAKE ORDERS. Use only the data below. PLAN: {json.dumps(context_routes, indent=2)} ALERTS: {json.dumps(advice, indent=2)} OUTPUT JSON: {{ "director_commentary": "...", "fleet_recommendations": ["..."] }}"""
    ai_res = await query_local_ai_json(prompt)
    if not ai_res: ai_res = {}
    ai_res["orders"] = route_plans 
    return ai_res