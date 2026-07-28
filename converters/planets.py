# converters/planets.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


def convert_planets_data(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts raw planet data into separate lists for multiple tables.
    Accepts raw_data where 'payload' is either a single planet dict or a list of planet dicts.
    """
    # Initialize aggregated lists for DB insertion
    all_planets = []
    all_resources = []
    all_build_options = []
    all_projects = []
    all_production_fees = []
    all_orbit_data = []
    all_celestial_bodies = []
    all_physical_data = []

    # 1. Normalize Payload
    payload = raw_data.get("payload")

    if not payload:
        return {}

    items_to_process = []

    # Logic: If payload is a list, process it. If it's a dict, wrap it in a list.
    if isinstance(payload, list):
        items_to_process = payload
    elif isinstance(payload, dict):
        items_to_process = [payload]

    for item in items_to_process:
        # --- Safe Data Extraction ---
        planet_id = item.get("planetId")

        physical_data = item.get("data") or {}
        country = item.get("country") or {}

        # Skip invalid records without an ID
        if not planet_id:
            continue

        # --- 1. Orbit Data ---
        orbit_raw = physical_data.get("orbit")
        if isinstance(orbit_raw, str):
            try:
                orbit_raw = json.loads(orbit_raw.replace("'", '"'))
            except:
                orbit_raw = {}
        orbit_raw = orbit_raw or {}

        all_orbit_data.append({
            "planetid": planet_id,
            "orbitindex": physical_data.get("orbitIndex"),
            "semimajoraxis": orbit_raw.get("semiMajorAxis"),
            "eccentricity": orbit_raw.get("eccentricity"),
            "inclination": orbit_raw.get("inclination"),
            "rightascension": orbit_raw.get("rightAscension"),
            "periapsis": orbit_raw.get("periapsis"),
        })

        # --- 2. Naming Date ---
        naming_date = None
        n_date = item.get("namingDate")
        if isinstance(n_date, dict) and "timestamp" in n_date:
            try:
                naming_date = datetime.fromtimestamp(n_date["timestamp"] / 1000)
            except (ValueError, TypeError):
                pass

        # --- 3. System ID Extraction ---
        address_lines = item.get("address", {}).get("lines", [])

        system_id = next((
            (l.get("entity") or {}).get("id")
            for l in address_lines if l.get("type") == "SYSTEM"
        ), None)

        # --- 4. Main Planet Record ---
        namer_username = (item.get("namer") or {}).get("username")

        all_planets.append({
            "planetid": planet_id,
            "naturalid": item.get("naturalId"),
            "name": item.get("name"),
            "namer": namer_username,
            "namingdate": naming_date,
            "nameable": item.get("nameable"),
            "systemid": system_id,
            "sunlight": physical_data.get("sunlight"),
            "surface": physical_data.get("surface"),
            "temperature": physical_data.get("temperature"),
            "plots": physical_data.get("plots"),
            "fertility": physical_data.get("fertility"),
            "populationid": item.get("populationId"),
            "admincenterid": item.get("adminCenterId"),
            "countrycode": country.get("code"),
            "countryname": country.get("name"),
            "mass": physical_data.get("mass"),
            "cogc": item.get("cogcProgramType"),
            "xata_updatedat": datetime.utcnow()
        })

        # --- 5. Resources ---
        for r in physical_data.get("resources", []):
            all_resources.append({
                "planetid": planet_id,
                "materialid": r.get("materialId"),
                "type": r.get("type"),
                "factor": r.get("factor", 0.0),
            })

        # Build physical data for planets
        all_physical_data.append({
            "planetId": planet_id,
            "fertility": physical_data.get("fertility"),
            "gravity": physical_data.get("gravity"),
            "magneticField": physical_data.get("magneticField"),
            "mass": physical_data.get("mass"),
            "massEarth": physical_data.get("massEarth"),
            "pressure": physical_data.get("pressure"),
            "radiation": physical_data.get("radiation"),
            "radius": physical_data.get("radius"),
            "surface": physical_data.get("surface"),
            "sunlight": physical_data.get("sunlight"),
            "temperature": physical_data.get("temperature")
        })


        # --- 6. Build Options ---
        for opt in item.get("buildOptions", {}).get("options", []):
            bill_of_material = json.dumps(opt.get("billOfMaterial", {}))
            all_build_options.append({
                "planetid": planet_id,
                "sitetype": opt.get("siteType"),
                "billofmaterial": bill_of_material,
            })

        # --- 7. Projects ---
        for p in item.get("projects", []):
            all_projects.append({
                "planetid": planet_id,
                "type": p.get("type"),
                "entityid": p.get("entityId"),
            })

        # --- 8. Production Fees ---
        local_rules = item.get("localRules", {})
        fees_container = local_rules.get("productionFees", {}) if local_rules else {}
        fees_list = fees_container.get("fees", []) if fees_container else []

        for f in fees_list:
            fee_obj = f.get("fee") or {}
            all_production_fees.append({
                "planetid": planet_id,
                "category": f.get("category"),
                "workforcelevel": f.get("workforceLevel"),
                "feeamount": fee_obj.get("amount", 0),
                "feecurrency": fee_obj.get("currency"),
            })

        # --- 9. Celestial Bodies ---
        for cbody in item.get("celestialBodies", []):
            c_address = cbody.get("address", {}).get("lines", [])

            c_system_id = None
            found_planet_id = None
            c_sat_id = None

            # 1. Single pass extraction
            for line in c_address:
                ent = line.get("entity") or {}
                etype = line.get("type")

                if etype == "SYSTEM":
                    c_system_id = ent.get("id")
                elif etype == "PLANET":
                    found_planet_id = ent.get("id")
                elif etype in ("SATELLITE", "MOON"):
                    c_sat_id = ent.get("id")

            # 2. Safe Fallbacks
            c_planet_id = found_planet_id or planet_id
            c_orbit = cbody.get("orbit") or {}
            natural_id = cbody.get("naturalId")

            # 3. Bulletproof ID mapping
            # We grab the ID you found, but keep the hashlib fallback JUST IN CASE
            # the game sends a broken record in the future.
            cb_id = cbody.get("id")
            if not cb_id:
                if not natural_id:
                    continue # Skip corrupted records completely
                cb_id = hashlib.md5(f"celestial_{natural_id}".encode()).hexdigest()

            # 4. Append as a Tuple for executemany
            all_celestial_bodies.append({
                "id": cb_id,
                "planetid": c_planet_id,
                "systemid": c_system_id,
                "satelliteid": c_sat_id,
                "name": cbody.get("name"),
                "naturalid": natural_id,
                "semimajoraxis": c_orbit.get("semiMajorAxis", 0),
                "eccentricity": c_orbit.get("eccentricity", 0),
                "inclination": c_orbit.get("inclination", 0),
                "rightascension": c_orbit.get("rightAscension", 0),
                "periapsis": c_orbit.get("periapsis", 0)
            })

    return {
        "planets": all_planets,
        "planet_resources": all_resources,
        "planet_build_options": all_build_options,
        "planet_projects": all_projects,
        "planet_production_fees": all_production_fees,
        "planet_orbit": all_orbit_data,
        "planet_celestial_bodies": all_celestial_bodies,
        "planet_physical_data": all_physical_data
    }

def convert_planet_population_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    converted_record["infrastructures"] = []
    converted_record["populations"] = []
    infrastructure_data = raw_record["payload"].get("infrastructure")
    population_data_reports = raw_record["payload"].get("reports")
    populationid = raw_record["payload"].get("id")

    for infrastructure in infrastructure_data:
        converted_record["infrastructures"].append(
            {
                "populationid": populationid,
                "type": infrastructure.get("type"),
                "ticker": infrastructure.get("ticker"),
                "projectid": infrastructure.get("projectId"),
                "projectname": infrastructure.get("projectName"),
                "level": infrastructure.get("level"),
                "activelevel": infrastructure.get("activeLevel"),
                "currentlevel": infrastructure.get("currentLevel"),
                "upkeepstatus": infrastructure.get("upkeepStatus"),
                "upgradestatus": infrastructure.get("upgradeStatus"),
            }
        )

    for report in population_data_reports:
        time = report.get("time")
        if time is not None and time.get("timestamp") is not None:
            time = datetime.fromtimestamp(time["timestamp"] / 1000)
        else:
            time = None
        converted_record["populations"].append(
            {
                "populationid": populationid,
                "time": time,
                "simulationperiod": report.get("simulationPeriod"),
                "explorersgraceenabled": report.get("explorersGraceEnabled"),
                "nextpopulationpioneer": report.get("nextPopulation").get("PIONEER"),
                "nextpopulationsettler": report.get("nextPopulation").get("SETTLER"),
                "nextpopulationtechnician": report.get("nextPopulation").get("TECHNICIAN"),
                "nextpopulationengineer": report.get("nextPopulation").get("ENGINEER"),
                "nextpopulationscientist": report.get("nextPopulation").get("SCIENTIST"),
                "populationdifferencepioneer": report.get("populationDifference").get("PIONEER"),
                "populationdifferencesettler": report.get("populationDifference").get("SETTLER"),
                "populationdifferencetechnician": report.get("populationDifference").get("TECHNICIAN"),
                "populationdifferenceengineer": report.get("populationDifference").get("ENGINEER"),
                "populationdifferencescientist": report.get("populationDifference").get("SCIENTIST"),
                "openjobspioneer": report.get("openJobs").get("PIONEER"),
                "openjobssettler": report.get("openJobs").get("SETTLER"),
                "openjobstechnician": report.get("openJobs").get("TECHNICIAN"),
                "openjobsengineer": report.get("openJobs").get("ENGINEER"),
                "openjobsscientist": report.get("openJobs").get("SCIENTIST"),
                "unemploymentratepioneer": report.get("unemploymentRate").get("PIONEER"),
                "unemploymentratesettler": report.get("unemploymentRate").get("SETTLER"),
                "unemploymentratetechnician": report.get("unemploymentRate").get("TECHNICIAN"),
                "unemploymentrateengineer": report.get("unemploymentRate").get("ENGINEER"),
                "unemploymentratescientist": report.get("unemploymentRate").get("SCIENTIST"),
                "averagehappinesspioneer": report.get("averageHappiness").get("PIONEER"),
                "averagehappinesssettler": report.get("averageHappiness").get("SETTLER"),
                "averagehappinesstechnician": report.get("averageHappiness").get("TECHNICIAN"),
                "averagehappinessengineer": report.get("averageHappiness").get("ENGINEER"),
                "averagehappinessscientist": report.get("averageHappiness").get("SCIENTIST"),
                "governmentprogramtype": report.get("governmentProgramType"),
            }
        )

    return converted_record

def convert_planet_infrastructure_project(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    if raw_record.get("payload") is not None:
        upkeeps = raw_record["payload"].get("upkeeps", [])
        upgrade_costs = raw_record["payload"].get("upgradeCosts", [])
        contributions = raw_record["payload"].get("contributions", [])

        converted_record["upgrade_costs"] = []
        converted_record["upkeep"] = []
        converted_record["contributions"] = []
        if upkeeps is not None:
            for upkeep in upkeeps:
                converted_record["upkeep"].append(
                    {
                        "amount": upkeep.get("amount"),
                        "currentamount": upkeep.get("currentAmount"),
                        "duration": upkeep.get("duration"),
                        "materialid": upkeep.get("material").get("id"),
                        "nexttick": upkeep.get("nextTick").get("timestamp"),
                        "storecapacity": upkeep.get("storeCapacity"),
                        "stored": upkeep.get("stored"),
                    }
                )

        if upgrade_costs is not None:
            for upgrade_cost in upgrade_costs:
                converted_record["upgrade_costs"].append(
                    {
                        "materialid": upgrade_cost.get("material").get("id"),
                        "amount": upgrade_cost.get("amount"),
                        "currentamount": upgrade_cost.get("currentAmount"),
                    }
                )

        if contributions is not None:
            for contribution in contributions:
                contributor = contribution.get("contributor")
                materials = contribution.get("materials", [])

                for material in materials:
                    converted_record["contributions"].append(
                        {
                            "contributorid": contributor.get("id"),
                            "contributorname": contributor.get("name"),
                            "contributorcode": contributor.get("code"),
                            "amount": material.get("amount"),
                            "materialid": material.get("material").get("id"),
                            "timestamp": contribution.get("time").get("timestamp"),
                        }
                    )
        converted_record.update(
            {
                "populationid": raw_record["payload"].get("populationid"),
                "projectid": raw_record["payload"].get("id"),
            }
        )
    return converted_record
