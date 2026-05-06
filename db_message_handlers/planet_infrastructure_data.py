import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


async def _prepare_and_execute_upsert(
    con, table_name: str, records: List[Dict[str, Any]], conflict_keys: List[str]
) -> int:
    if not records:
        return 0

    keys = list(records[0].keys())
    values_list: List[Tuple] = [tuple(rec.values()) for rec in records]

    columns = ", ".join(keys)
    placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])

    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in keys if col not in conflict_keys])

    if update_clause:
        query = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({placeholders})
            ON CONFLICT ({", ".join(conflict_keys)}) 
            DO UPDATE SET {update_clause};
        """
    else:
        query = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({placeholders})
            ON CONFLICT ({", ".join(conflict_keys)}) 
            DO NOTHING;
        """

    try:
        await con.executemany(query, values_list)
        return len(values_list)
    except Exception as e:
        logger.error(f"Logged error in {table_name} execute: {e}", exc_info=True)
        raise


async def handle_planet_infrastructure_project(db, data: Dict[str, Any]):
    record = data.get("data", {})
    project_id = record.get("projectid")

    if not project_id:
        logger.warning("Missing projectid in infrastructure record.")
        return {"status": "fail", "message": "Missing projectid"}

    current_time = datetime.now(timezone.utc)

    upkeep_records = []
    for u in record.get("upkeep", []):
        upkeep_records.append(
            {
                "projectid": project_id,
                "materialid": u.get("materialid"),
                "amount": u.get("amount"),
                "currentamount": u.get("currentamount"),
                "duration": u.get("duration"),
                "nexttick": u.get("nexttick"),
                "storecapacity": u.get("storecapacity"),
                "stored": u.get("stored"),
                "updatedat": current_time,
            }
        )

    upgrade_cost_records = []
    for c in record.get("upgrade_costs", []):
        upgrade_cost_records.append(
            {
                "projectid": project_id,
                "materialid": c.get("materialid"),
                "amount": c.get("amount"),
                "currentamount": c.get("currentamount"),
                "updatedat": current_time,
            }
        )

    contribution_records = []
    for c in record.get("contributions", []):
        contribution_records.append(
            {
                "projectid": project_id,
                "contributorid": c.get("contributorid"),
                "contributorname": c.get("contributorname"),
                "contributorcode": c.get("contributorcode"),
                "amount": c.get("amount"),
                "materialid": c.get("materialid"),
                "timestamp": c.get("timestamp"),
                "updatedat": current_time,
            }
        )

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # B. Upsert Upkeep
                if upkeep_records:
                    await _prepare_and_execute_upsert(
                        con,
                        table_name="planet_infrastructure_upkeeps",
                        records=upkeep_records,
                        conflict_keys=["projectid", "materialid"],
                    )

                # C. Upsert Upgrade Costs
                if upgrade_cost_records:
                    await _prepare_and_execute_upsert(
                        con,
                        table_name="planet_infrastructure_upgrade_costs",
                        records=upgrade_cost_records,
                        conflict_keys=["projectid", "materialid"],
                    )

                # D. Upsert Contributions
                # Conflict key assumption: A contributor can't give the same material to the same project at the exact same timestamp twice.
                if contribution_records:
                    await _prepare_and_execute_upsert(
                        con,
                        table_name="planet_infrastructure_contributions",
                        records=contribution_records,
                        conflict_keys=[
                            "projectid",
                            "contributorid",
                            "materialid",
                            "timestamp",
                        ],
                    )

                logger.debug(
                    f"Processed infrastructure project {project_id}: "
                    f"{len(upkeep_records)} upkeeps, "
                    f"{len(upgrade_cost_records)} costs, "
                    f"{len(contribution_records)} contributions."
                )

        return {"status": "success", "message": f"Updated project {project_id}"}

    except Exception as e:
        logger.error(
            f"Error handling planet infrastructure project {project_id}: {e}",
            exc_info=True,
        )
        return {"status": "fail", "message": str(e)}
