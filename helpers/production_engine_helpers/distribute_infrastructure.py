# FULL AI GENERATED CODE Well not really but yeah

import logging
import time
from collections import defaultdict, deque
import math
from typing import Dict, Tuple, List
import networkx as nx
import asyncpg
import itertools

logger = logging.getLogger("production_engine")

BASE_RUNTIMES_H = {"RIG": 4.8, "COL": 6.0, "EXT": 12.0}
STORAGE_AREA = 15 
GLOBAL_HARD_CAP = 500 
MAX_JUMPS_FROM_HUB = 12

async def get_galaxy_cache(conn: asyncpg.Connection) -> Tuple[nx.Graph, Dict[str, Dict[str, int]]]:
    rows = await conn.fetch("SELECT systemidorigin, systemiddestination FROM system_connections")
    G = nx.Graph()
    G.add_edges_from([(r['systemidorigin'], r['systemiddestination']) for r in rows])
    dist_matrix = dict(nx.all_pairs_shortest_path_length(G))
    return G, dist_matrix

async def distribute_infrastructure(conn, global_reqs, hub_sys_id, allowed_tiers):
    total_start = time.perf_counter()
    user_limit = GLOBAL_HARD_CAP
    
    # 1. SETUP DATA
    initial_build_queue = global_reqs['building_requirements'].copy()
    initial_raw_demand = global_reqs['raw_demand'].copy()
    initial_total_demand = global_reqs['total_demand'].copy()
    
    b_meta = global_reqs['buildings_meta']
    recipes = global_reqs['recipes']
    market_prices = global_reqs['market_prices'] 
    
    G, dist_matrix = await get_galaxy_cache(conn)
    hub_dists = dist_matrix.get(hub_sys_id.strip(), {})

    def get_dist(sys_a, sys_b):
        if sys_a == sys_b: return 0
        if sys_a in dist_matrix and sys_b in dist_matrix[sys_a]: return dist_matrix[sys_a][sys_b]
        return 999 

    # 2. PLANET DATA
    p_rows = await conn.fetch("""
        SELECT p.planetid, p.name, p.systemid, p.cogc, p.populationid,
               ppd.gravity, ppd.pressure, ppd.temperature, ppd.fertility, ppd.surface,
               pp.nextpopulationpioneer as pio, pp.nextpopulationsettler as set,
               pp.nextpopulationtechnician as tec, pp.nextpopulationengineer as eng,
               pp.nextpopulationscientist as sci
        FROM planets p
        LEFT JOIN planet_populations pp ON p.populationid = pp.populationid
        INNER JOIN planet_physical_data ppd ON p.planetid = ppd.planetid
    """)
    
    planet_meta = {}
    
    def get_env_cost_fast(p, area):
        if 'unit_env_cost' not in p:
            cost = 0.0
            is_gaseous = not p.get('surface')
            if is_gaseous: cost += (1.0 / 3.0) * market_prices.get('AEF', {'price': 0})['price']
            else: cost += 4.0 * market_prices.get('MCG', {'price': 0})['price']

            press = float(p.get('pressure') or 0)
            if press < 0.25: cost += 1.0 * market_prices.get('SEA', {'price': 0})['price']
            
            temp = float(p.get('temperature') or 20.0)
            if temp < -25.0: cost += 10.0 * market_prices.get('INS', {'price': 0})['price']
            
            p['unit_env_cost'] = cost 
            
            fixed = 0.0
            if press > 2.0: fixed += market_prices.get('HSE', {'price': 0})['price']
            grav = float(p.get('gravity') or 1.0)
            if grav < 0.25: fixed += market_prices.get('MGC', {'price': 0})['price']
            elif grav > 2.5: fixed += market_prices.get('BL', {'price': 0})['price']
            if temp > 75.0: fixed += market_prices.get('TSH', {'price': 0})['price']
            p['fixed_env_cost'] = fixed

        return (area * p['unit_env_cost']) + p['fixed_env_cost']

    for r in p_rows:
        p = dict(r)
        dist_to_hub = hub_dists.get(p['systemid'], 999)
        if dist_to_hub > MAX_JUMPS_FROM_HUB: continue
        p['dist'] = dist_to_hub
        get_env_cost_fast(p, 100) 
        
        static_score = 1000.0
        if p.get('populationid'): static_score += 1500
        static_score -= (get_env_cost_fast(p, 100.0) / 5.0)
        static_score -= (p['dist'] * 50) 
        p['static_score'] = static_score
        planet_meta[p['planetid']] = p

    all_planets_sorted = sorted(planet_meta.values(), key=lambda x: x['static_score'], reverse=True)

    # --- 3. HELPERS ---
    hab_meta = {t: m for t, m in b_meta.items() if m['type'] == 'HABITATION'}
    storage_ticker = next((t for t, m in b_meta.items() if m['type'] == 'STORAGE'), 'STL')

    # Pre-sort habs by density for the optimizer
    sorted_habs = []
    for t, m in hab_meta.items():
        total_cap = sum(abs(w['amount']) for w in m.get('workforce', []) if abs(w['amount']) > 0)
        area = m.get('area', 100)
        if total_cap > 0:
            sorted_habs.append({"ticker": t, "area": area, "cap": total_cap, "density": total_cap/area})
    sorted_habs.sort(key=lambda x: x['density'], reverse=True)

    # Pre-calculate efficiency for Habitation Optimization (Phase 9)
    hab_options = []
    for t, m in hab_meta.items():
        capacity = sum(abs(w['amount']) for w in m.get('workforce', []) if abs(w['amount']) > 0)
        if capacity > 0:
            hab_options.append({
                "ticker": t, "area": m.get('area', 100), "cap": capacity, 
                "supply": {w['level']: abs(w['amount']) for w in m.get('workforce', [])}
            })
    hab_options.sort(key=lambda x: x['cap'] / x['area'], reverse=True) 

    def calculate_min_housing_area(workforce_needed):
        """Estimate minimum area needed for housing."""
        if workforce_needed <= 0: return 0
        area_needed = 0
        remaining = workforce_needed
        for hab in sorted_habs:
            if remaining <= 0: break
            count = math.floor(remaining / hab['cap'])
            if count > 0:
                area_needed += (count * hab['area'])
                remaining -= (count * hab['cap'])
        if remaining > 0:
            # Add one more of the most efficient
            area_needed += sorted_habs[0]['area'] 
        return area_needed

    def get_optimized_habitation_layout(workforce_demand):
        """Precise mix of buildings for final commit."""
        if not any(v > 0 for v in workforce_demand.values()): return 0, []
        needed = workforce_demand.copy()
        buildings = []
        total_area = 0
        while any(v > 0 for v in needed.values()):
            best_choice = None
            best_score = -1
            for opt in hab_options:
                useful = 0
                for lvl, amt in opt['supply'].items(): useful += min(amt, needed.get(lvl, 0))
                if useful == 0: continue
                score = useful / opt['area']
                if score > best_score:
                    best_score = score
                    best_choice = opt
            if not best_choice: break
            buildings.append(best_choice['ticker'])
            total_area += best_choice['area']
            for lvl, amt in best_choice['supply'].items():
                if lvl in needed: needed[lvl] = max(0, needed[lvl] - amt)
        
        counts = defaultdict(int)
        for t in buildings: counts[t] += 1
        result_list = [{"ticker": t, "count": c} for t, c in counts.items()]
        return total_area, result_list

    def get_duration(rec):
        """Safely extracts duration in ms, handling int/dict."""
        if not rec: return 60000
        raw = rec.get('duration')
        if isinstance(raw, int): return max(1, raw)
        if isinstance(raw, dict): return max(1, raw.get('millis', 60000))
        return max(1, rec.get('duration_ms', 60000))

    # --- 4. SITE MANAGEMENT ---
    sites = []
    used_planet_ids = set()
    material_sources = defaultdict(list)

    def create_site(planet_data, cat):
        s_data = b_meta.get(storage_ticker, {'area': 15})
        s_area = s_data['area']
        base_c = s_data.get('base_cost', 0)
        env_c = get_env_cost_fast(planet_data, s_area)
        return {
            "name": planet_data['name'], 
            "system": planet_data['systemid'], 
            "planetid": planet_data['planetid'], 
            "area_factories": s_area,
            "area_used": s_area,      
            "workforce_demand": defaultdict(int),
            "total_build_cost": base_c + env_c,
            "buildings": [{
                "ticker": storage_ticker, "count": 1, "produces": "STORAGE", 
                "rate": 0, "unit_cost": base_c+env_c, "total_cost": base_c+env_c
            }], 
            "dist": planet_data['dist'], 
            "category": cat
        }

    # --- 5. THE "ADDER" ---
    def try_add_building_optimized(site, ticker, count, produces=None, rate=0):
        if ticker in ['FRM', 'ORC']:
            p_data = planet_meta[site['planetid']]
            if float(p_data.get('fertility') or -1.0) < 0: return 0

        b_data = b_meta.get(ticker, {'area': 100})
        b_area = b_data['area'] * count
        
        # Calculate NEW Total Workforce needed
        wf_added = sum(abs(w['amount']) for w in b_data.get('workforce', [])) * count
        current_wf_total = sum(site['workforce_demand'].values())
        new_wf_total = current_wf_total + wf_added
        
        # Calculate Required Housing Area
        hab_area_needed = calculate_min_housing_area(new_wf_total)
        
        total_projected_area = site['area_factories'] + b_area + hab_area_needed
        
        if total_projected_area <= user_limit:
            site['area_factories'] += b_area
            site['area_used'] = total_projected_area
            for wf in b_data.get('workforce', []):
                site['workforce_demand'][wf['level']] += (abs(wf['amount']) * count)
            
            base_c = b_meta.get(ticker, {}).get('base_cost', 0)
            env_c = get_env_cost_fast(planet_meta[site['planetid']], b_data['area'])
            unit_c = base_c + env_c
            
            site['buildings'].append({
                "ticker": ticker, "count": count, "produces": produces, 
                "rate": rate, "unit_cost": unit_c, "total_cost": unit_c * count
            })
            site['total_build_cost'] += (unit_c * count)
            return count
        return 0

    # --- 6. SCORING ---
    def calculate_dynamic_score(p, target_cat, existing_sites):
        score = p['static_score']
        cogc_str = str(p.get('cogc') or "").upper().replace("ADVERTISING_", "")
        
        if cogc_str == target_cat: score += 3000

        if existing_sites:
            min_dist = 9999
            check_list = existing_sites[-5:] if len(existing_sites) > 5 else existing_sites
            for s in check_list:
                d = get_dist(p['systemid'], s['system'])
                if d < min_dist: min_dist = d
            score -= (min_dist * 500) 
        return score

    # --- 7. EXTRACTION SETUP ---
    res_rows = await conn.fetch("""
        SELECT pr.planetid, pr.factor as raw_conc, pr.type as res_type, m.ticker
        FROM planet_resources pr 
        JOIN materials m ON m.materialid = pr.materialid 
        WHERE m.ticker = ANY($1) AND pr.factor > 0
    """, list(initial_raw_demand.keys()))
    res_lookup = defaultdict(list)
    for r in res_rows: 
        if r['planetid'] in planet_meta: res_lookup[r['ticker']].append(dict(r))

    for mat, total_needed in initial_raw_demand.items():
        potential = res_lookup.get(mat, [])
        if not potential: continue
        
        while total_needed > 0.1:
            candidates = []
            for rp in potential:
                 if rp['planetid'] in used_planet_ids: continue
                 p = planet_meta.get(rp['planetid'])
                 score = calculate_dynamic_score(p, "EXTRACTION", sites) + (rp['raw_conc']*5000)
                 candidates.append({**rp, **p, "score": score})
            candidates.sort(key=lambda x: x['score'], reverse=True)
            if not candidates: break
            
            best = candidates[0]
            reactor = "COL" if best['res_type'] == "GASEOUS" else ("RIG" if best['res_type'] == "LIQUID" else "EXT")
            daily_base = (best['raw_conc'] * 100) * (0.6 if reactor == "COL" else 0.7)
            
            # Using get_duration helper even though extraction duration is standard, for safety
            dur = 60000 
            # Note: Extraction buildings don't have standard recipes usually, their output is conc based.
            
            site = create_site(planet_meta[best['planetid']], "EXTRACTION")
            sites.append(site)
            used_planet_ids.add(best['planetid'])
            
            needed_count = math.ceil(total_needed / daily_base) if daily_base > 0 else 1
            
            # Rate for extraction: Count * Daily Yield
            added = try_add_building_optimized(site, reactor, needed_count, mat, daily_base * needed_count)
            if added == 0:
                for _ in range(needed_count):
                    if try_add_building_optimized(site, reactor, 1, mat, daily_base):
                        added += 1
                    else: break
            
            if added > 0:
                total_needed -= (added * daily_base)
                material_sources[mat].append(site['planetid'])
            else:
                break 

    # --- 8. DEFICIT LOOP ---
    current_queue = initial_build_queue.copy()
    
    for iteration in range(3): 
        if not current_queue: break
        
        # 1. Group by Category
        industry_queues = defaultdict(list)
        for key, count in current_queue.items():
            if count <= 0: continue
            reactor, output = key
            cat = b_meta.get(reactor, {}).get('cat', 'MANUFACTURING')
            industry_queues[cat].append({"reactor": reactor, "output": output, "count": count})

        sort_priority = {
          'RESOURCE_EXTRACTION': 0, 
          'METALLURGY': 1
        }

        industry_queues = sorted(
          industry_queues.items(), 
          key=lambda x: sort_priority.get(x[0], 50) # 50 is default for unlisted cats like ELECTRONICS
        )

        # 2. Process
        for cat, items in industry_queues:
            target_site_cat = f"MANUFACTURING: {cat}"
            items.sort(key=lambda x: x['count'], reverse=True)
            queue = deque(items)
            
            while queue:
                item = queue.popleft()
                needed = item['count']
                
                rec = recipes.get(item['output'])
                item_rate_single = 0
                if rec:
                    # Safe output amount
                    out_amt = rec.get('out_q', 1)
                    # Safe duration
                    dur = rec.get('duration', 60000)
                    cycles = 86400000 / dur
                    item_rate_single = cycles * out_amt

                # A. Existing
                for s in sites:
                    if needed <= 0: break
                    if s['category'] == "MANUFACTURING" or s['category'] == target_site_cat:
                        added = try_add_building_optimized(s, item['reactor'], needed, item['output'], item_rate_single * needed)
                        if added == 0:
                             added = try_add_building_optimized(s, item['reactor'], 1, item['output'], item_rate_single)
                        if added > 0:
                            needed -= added
                            if s['category'] == "MANUFACTURING": s['category'] = target_site_cat

                # B. New
                while needed > 0:
                    candidates = []
                    for p in all_planets_sorted:
                        if p['planetid'] in used_planet_ids: continue
                        s_score = calculate_dynamic_score(p, target_site_cat, sites)
                        candidates.append({**p, "score": s_score})
                    
                    candidates.sort(key=lambda x: x['score'], reverse=True)
                    if not candidates: break
                    
                    target = candidates[0]
                    site = create_site(target, target_site_cat)
                    sites.append(site)
                    used_planet_ids.add(target['planetid'])
                    
                    added = try_add_building_optimized(site, item['reactor'], needed, item['output'], item_rate_single * needed)
                    if added == 0:
                        added = try_add_building_optimized(site, item['reactor'], 1, item['output'], item_rate_single)
                    
                    if added > 0: needed -= added
                    else: break 

        # 3. Recalculate Deficit
        production = defaultdict(float)
        consumption = defaultdict(float)
        
        for s in sites:
            for b in s['buildings']:
                prod = b.get('produces')
                rate = b.get('rate', 0)
                if prod and rate > 0:
                    production[prod] += rate
                    rec = recipes.get(prod)
                    if rec:
                        out_amt = rec.get('out_q', 1)
                        dur = rec.get('duration', 60000)
                        cycles = rate / out_amt
                        for inp in rec.get('inputs', []):
                            consumption[inp['ticker']] += (cycles * inp['amount'])
        
        current_queue = {}
        all_mats = set(production.keys()) | set(consumption.keys())
        for m in all_mats:
            net = production[m] - consumption[m]
            if net < -0.1:
                # Find producer
                producer_ticker = None
                for (reac, out), cnt in initial_build_queue.items():
                    if out == m: 
                        producer_ticker = reac
                        break
                
                # If intermediate not in initial queue, search recipes
                if not producer_ticker:
                    for r_key, r_val in recipes.items():
                        # r_key might be the product ticker or recipe ID
                        # Assuming recipes dict is {product_ticker: recipe_data} based on previous usage
                        # If keys are products:
                        if r_key == m:
                            # We need to guess reactor. B_meta scan?
                            # For simplicity, default to most common reactor for this category or look at B_meta
                            # Fallback: Assume we have built at least one before?
                            # Or just scan b_meta for who produces 'm'
                            # This part is tricky without a reverse map. 
                            # Let's try to find if we built it already.
                            for s in sites:
                                for b in s['buildings']:
                                    if b.get('produces') == m:
                                        producer_ticker = b['ticker']
                                        break
                                if producer_ticker: break
                
                if producer_ticker:
                    rec = recipes.get(m)
                    if rec:
                        out_amt = rec.get('out_q', 1)
                        dur = rec.get('duration', 60000)
                        daily_per_b = (86400000 / dur) * out_amt
                        needed_b = math.ceil(abs(net) / daily_per_b)
                        current_queue[(producer_ticker, m)] = needed_b

    # --- 9. FINALIZATION (Habitation Commit) ---
    for s in sites:
        hab_area, hab_layout = get_optimized_habitation_layout(s['workforce_demand'])
        
        # Remove old placeholder habs if any
        s['buildings'] = [b for b in s['buildings'] if b.get('produces') != "HABITATION"]
        
        for hab_entry in hab_layout:
            h_ticker = hab_entry['ticker']
            h_count = hab_entry['count']
            h_area = b_meta.get(h_ticker, {'area': 100})['area']
            h_base = b_meta.get(h_ticker, {}).get('base_cost', 0)
            h_env = get_env_cost_fast(planet_meta[s['planetid']], h_area)
            h_tot = (h_base + h_env) * h_count
            
            s['buildings'].append({
                "ticker": h_ticker, "count": h_count, "produces": "HABITATION",
                "rate": 0, "unit_cost": h_base+h_env, "total_cost": h_tot
            })
            s['total_build_cost'] += h_tot
        
        # Stats
        s['permit_cap'] = user_limit
        fin_sup = defaultdict(float)
        for b in s['buildings']:
            h = hab_meta.get(b['ticker'])
            if h:
                 for wf in h.get('workforce', []):
                     if abs(wf['amount']) > 0: fin_sup[wf['level']] += abs(wf['amount']) * b['count']

        s['sustainability'] = {}
        for l, n in s['workforce_demand'].items():
            p = fin_sup.get(l, 0)
            status = "OK" if p >= n - 0.1 else "DEFICIT"
            s['sustainability'][l] = {
                "required": round(n, 1), "provided": round(p, 1),
                "waste": f"+{round(max(0, p-n), 1)}", "status": status
            }

    # --- 10. REPORT ---
    production = defaultdict(float)
    consumption = defaultdict(float)
    for s in sites:
        for b in s['buildings']:
            prod = b.get('produces')
            rate = b.get('rate', 0)
            if prod and rate > 0:
                production[prod] += rate
                rec = recipes.get(prod)
                if rec:
                    out_amt = rec.get('out_q', 1)
                    cycles = rate / out_amt
                    for inp in rec.get('inputs', []):
                        consumption[inp['ticker']] += (cycles * inp['amount'])

    report = []
    all_mats = set(production.keys()) | set(consumption.keys())
    for m in sorted(all_mats):
        p = production.get(m, 0)
        c = consumption.get(m, 0)
        report.append({"ticker": m, "produced": round(p, 2), "consumed": round(c, 2), "delta": round(p-c, 2)})

    if sites: sites[0]['material_report'] = report
    return sites