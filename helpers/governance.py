import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


async def get_governance_overview(db, planet_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Fetches hierarchical governance data:
    Planet -> Buildings -> (Upkeep, Upgrade Costs, Contributions).
    """

    query = """
        SELECT 
            pop.populationid,
            COALESCE(p.name, p.naturalid) AS planet_name,
            (pop.nextpopulationengineer + pop.nextpopulationpioneer + pop.nextpopulationscientist + pop.nextpopulationsettler + pop.nextpopulationtechnician) AS population,
            pop.nextpopulationpioneer AS pioneers,
            pop.nextpopulationengineer AS engineers,
            pop.nextpopulationscientist AS scientists,
            pop.nextpopulationsettler AS settlers,
            pop.nextpopulationtechnician AS technicians,
            pi.projectid,
            pi.ticker AS building_ticker,
            
            -- Upkeep Info
            piu.materialid AS upkeep_material_id,
            piu.amount AS upkeep_amount,
            piu.currentamount AS upkeep_currentamount,
            piu.stored AS upkeep_stored,
            piu.storecapacity AS upkeep_storecapacity,
            
            to_timestamp(piu.nexttick::double precision / 1000) AT TIME ZONE 'UTC' AS upkeep_nexttick,
            
            mpiu.ticker AS upkeep_resource_ticker,
            
            -- Upgrade Cost Info
            piuc.materialid AS upgrade_cost_material_id,
            piuc.amount AS upgrade_cost_amount,
            piuc.currentamount AS upgrade_cost_currentamount,
            mpiuc.ticker AS upgrade_cost_resource_ticker,
            
            -- Contribution Info
            pic.contributorid AS contribution_contributor_id,
            pic.contributorname AS contribution_contributorname,
            pic.contributorcode AS contribution_contributorcode,
            pic.amount AS contribution_amount,
            
            to_timestamp(pic.timestamp::double precision / 1000) AT TIME ZONE 'UTC' AS contribution_timestamp,
            
            pic.materialid AS contribution_material_id,
            mpic.ticker AS contribution_resource_ticker,

            p.cogc AS cogc

        FROM planet_infrastructures pi
        JOIN planet_populations pop ON pi.populationid = pop.populationid
        JOIN planets p ON pop.populationid = p.populationid
        
        -- Upkeeps (Left Join)
        LEFT JOIN planet_infrastructure_upkeeps piu ON pi.projectid = piu.projectid
        LEFT JOIN materials mpiu ON piu.materialid = mpiu.materialid
        
        -- Upgrade Costs (Left Join)
        LEFT JOIN planet_infrastructure_upgrade_costs piuc ON pi.projectid = piuc.projectid
        LEFT JOIN materials mpiuc ON piuc.materialid = mpiuc.materialid
        
        -- Contributions (Left Join)
        LEFT JOIN planet_infrastructure_contributions pic ON pi.projectid = pic.projectid
        LEFT JOIN materials mpic ON pic.materialid = mpic.materialid
    """

    args = []

    if planet_ids:
        query += """
        WHERE p.planetid = ANY($1) 
          AND pop.simulationperiod = (
              SELECT MAX(simulationperiod) 
              FROM planet_populations 
              WHERE populationid = pop.populationid
          )
        """
        args.append(planet_ids)

    query += " ORDER BY planet_name, pi.ticker;"

    try:
        async with db.pool.acquire() as con:
            rows = await con.fetch(query, *args)

        planets_map = {}

        for row in rows:
            pop_id = str(row["populationid"])
            proj_id = str(row["projectid"])

            if pop_id not in planets_map:
                planets_map[pop_id] = {
                    "id": pop_id,
                    "name": row["planet_name"] or "Unknown Planet",
                    "population": row["population"] or 0,
                    "pioneers": row["pioneers"] or 0,
                    "engineers": row["engineers"] or 0,
                    "scientists": row["scientists"] or 0,
                    "settlers": row["settlers"] or 0,
                    "technicians": row["technicians"] or 0,
                    "buildings_map": {},
                    "cogc": row["cogc"],
                    "cogcUpkeep": [],
                }

            if proj_id and proj_id not in planets_map[pop_id]["buildings_map"]:
                ticker = row["building_ticker"] or "UNKNOWN"
                planets_map[pop_id]["buildings_map"][proj_id] = {
                    "id": proj_id,
                    "ticker": ticker,
                    "name": ticker,
                    "upkeep": [],
                    "upgradeCosts": [],
                    "contributions": [],
                    "_seen_upkeeps": set(),
                    "_seen_costs": set(),
                    "_seen_contribs": set(),
                }

            building = planets_map[pop_id]["buildings_map"][proj_id]

            # Upkeep
            if row["upkeep_resource_ticker"]:
                u_key = row["upkeep_material_id"]
                if u_key not in building["_seen_upkeeps"]:
                    building["upkeep"].append(
                        {
                            "ticker": row["upkeep_resource_ticker"],
                            "amount": float(row["upkeep_amount"] or 0),
                            "currentAmount": float(row["upkeep_currentamount"] or 0),
                            "stored": float(row["upkeep_stored"] or 0),
                            "capacity": float(row["upkeep_storecapacity"] or 0),
                            "nextTick": row["upkeep_nexttick"],
                            "enabled": True,
                        }
                    )
                    building["_seen_upkeeps"].add(u_key)

            # Upgrade Cost
            if row["upgrade_cost_resource_ticker"]:
                c_key = row["upgrade_cost_material_id"]
                if c_key not in building["_seen_costs"]:
                    building["upgradeCosts"].append(
                        {
                            "ticker": row["upgrade_cost_resource_ticker"],
                            "amount": float(row["upgrade_cost_amount"] or 0),
                            "currentAmount": float(row["upgrade_cost_currentamount"] or 0),
                        }
                    )
                    building["_seen_costs"].add(c_key)

            # Contributions
            if row["contribution_resource_ticker"]:
                contrib_key = (
                    row["contribution_contributor_id"],
                    row["contribution_material_id"],
                    row["contribution_timestamp"],
                )
                if contrib_key not in building["_seen_contribs"]:
                    building["contributions"].append(
                        {
                            "contributorName": row["contribution_contributorname"],
                            "contributorCode": row["contribution_contributorcode"],
                            "amount": float(row["contribution_amount"] or 0),
                            "ticker": row["contribution_resource_ticker"],
                            "timestamp": row["contribution_timestamp"],
                        }
                    )
                    building["_seen_contribs"].add(contrib_key)

        results = []
        for p in planets_map.values():
            buildings_list = []
            for b in p["buildings_map"].values():
                del b["_seen_upkeeps"]
                del b["_seen_costs"]
                del b["_seen_contribs"]
                buildings_list.append(b)

            results.append(
                {
                    "id": p["id"],
                    "name": p["name"],
                    "population": {
                        "total": p["population"],
                        "pioneers": p["pioneers"],
                        "engineers": p["engineers"],
                        "scientists": p["scientists"],
                        "settlers": p["settlers"],
                        "technicians": p["technicians"],
                    }
                    if "pioneers" in p
                    else p["population"],
                    "buildings": buildings_list,
                    "cogc": p["cogc"].replace("ADVERTISING_", "") if p["cogc"] else None,
                    "cogcUpkeep": [],
                }
            )

        return {"status": "success", "data": results}

    except Exception as e:
        logger.error(f"Failed to fetch governance overview: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
