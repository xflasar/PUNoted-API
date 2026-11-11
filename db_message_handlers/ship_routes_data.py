# db_message_handlers/ship_routes_data.py
import logging
import time
from typing import Any, Dict, List
import asyncpg
from db import Database
from managers.global_ws_manager import global_ws_manager

logger = logging.getLogger(__name__)


async def _resolve_userid(db: Database, account_id: str) -> str:
    user_response = await db.fetch_one(
        "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
        account_id,
    )
    if user_response and user_response.get("userdataid") is not None:
        return str(user_response.get("userdataid"))
    elif user_response:
        return str(user_response.get("accountid"))
    return account_id


async def handle_ship_routes_data_message(db: Database, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles SHIP_ROUTES_ROUTES and SHIP_ROUTES_ROUTE incoming messages.
    Performs transactional upsert of route headers, waypoints, and steps.
    """
    start_time = time.perf_counter()
    raw_data = payload.get("data", [])
    user_account_id = payload.get("userId")

    routes_to_process = [raw_data] if isinstance(raw_data, dict) else list(raw_data)
    if not routes_to_process:
        return {"success": True, "message": "No route records to process."}

    try:
        userid = await _resolve_userid(db, user_account_id)

        async with db.pool.acquire() as conn:
            async with conn.transaction():
                for route in routes_to_process:
                    route_id = route.get("routeid")
                    if not route_id:
                        continue

                    # 1. Upsert Route Header
                    sql_route = """
                        INSERT INTO ship_routes (routeid, naturalid, name, userid, repeats, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NOW())
                        ON CONFLICT (routeid) DO UPDATE SET
                            naturalid = EXCLUDED.naturalid,
                            name = COALESCE(EXCLUDED.name, ship_routes.name),
                            repeats = EXCLUDED.repeats,
                            created_at = COALESCE(EXCLUDED.created_at, ship_routes.created_at),
                            updated_at = NOW();
                    """
                    await conn.execute(
                        sql_route,
                        route_id,
                        route.get("naturalid"),
                        route.get("name"),
                        userid,
                        route.get("repeats", True),
                        route.get("created_at"),
                    )

                    # 2. Delete existing waypoints (cascades to steps via foreign key ON DELETE CASCADE)
                    await conn.execute("DELETE FROM ship_route_waypoints WHERE routeid = $1;", route_id)

                    # 3. Bulk insert waypoints
                    waypoints = route.get("waypoints", [])
                    if waypoints:
                        wp_records = [
                            (
                                wp["waypointid"],
                                wp["routeid"],
                                wp["waypoint_index"],
                                wp["system_id"],
                                wp["location_id"],
                                wp["location_type"],
                                wp["landing"],
                                wp["fuel_usage_factor"],
                                wp["reactor_usage_factor"],
                                wp["ftl_preference"],
                                wp["use_gateways"],
                            )
                            for wp in waypoints
                        ]
                        sql_insert_wp = """
                            INSERT INTO ship_route_waypoints (
                                waypointid, routeid, waypoint_index, system_id, location_id,
                                location_type, landing, fuel_usage_factor, reactor_usage_factor,
                                ftl_preference, use_gateways, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW());
                        """
                        await conn.executemany(sql_insert_wp, wp_records)

                    # 4. Bulk insert steps
                    steps = route.get("steps", [])
                    if steps:
                        step_records = [
                            (
                                st["stepid"],
                                st["waypointid"],
                                st["routeid"],
                                st["step_index"],
                                st["type"],
                                st["materialid"],
                                st["material_ticker"],
                                st["min_type"],
                                st["min_units"],
                                st["max_type"],
                                st["max_units"],
                                st["retain"],
                                st["fuel_type"],
                                st["fuel_source"],
                                st["wait_amount"],
                                st["wait_unit"],
                            )
                            for st in steps
                        ]
                        sql_insert_steps = """
                            INSERT INTO ship_route_steps (
                                stepid, waypointid, routeid, step_index, type,
                                materialid, material_ticker, min_type, min_units,
                                max_type, max_units, retain, fuel_type, fuel_source,
                                wait_amount, wait_unit, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW());
                        """
                        await conn.executemany(sql_insert_steps, step_records)

        # Broadcast update to user
        try:
            await global_ws_manager.send_personal_message(
                user_account_id,
                {"type": "ROUTE_DATA_UPDATE", "data": routes_to_process},
            )
        except Exception as ws_err:
            logger.debug(f"Failed to broadcast route update: {ws_err}")

        elapsed = time.perf_counter() - start_time
        logger.info(f"Processed {len(routes_to_process)} routes in {elapsed:.3f}s for user {user_account_id}")
        return {"success": True, "message": f"Processed {len(routes_to_process)} routes successfully."}

    except Exception as e:
        logger.error(f"Error handling ship routes data: {e}", exc_info=True)
        raise


async def handle_ship_routes_assign_message(db: Database, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles SHIP_ROUTES_ASSIGN_ROUTE message: assigns a ship to a route.
    """
    data = payload.get("data", {})
    ship_id = data.get("shipId")
    route_id = data.get("routeId")
    user_account_id = payload.get("userId")

    if not ship_id:
        return {"success": False, "message": "Missing shipId."}

    try:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE ships SET routeid = $1 WHERE shipid = $2;",
                route_id,
                ship_id,
            )

        try:
            await global_ws_manager.send_personal_message(
                user_account_id,
                {"type": "SHIP_ROUTE_ASSIGNED", "data": {"shipId": ship_id, "routeId": route_id}},
            )
        except Exception:
            pass

        return {"success": True, "message": f"Assigned ship {ship_id} to route {route_id}."}

    except Exception as e:
        logger.error(f"Error assigning ship to route: {e}", exc_info=True)
        raise


async def handle_ship_routes_delete_message(db: Database, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles SHIP_ROUTES_DELETE_ROUTE message: deletes a route and unassigns ships.
    """
    data = payload.get("data", {})
    route_id = data.get("routeId")
    user_account_id = payload.get("userId")

    if not route_id:
        return {"success": False, "message": "Missing routeId."}

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("UPDATE ships SET routeid = NULL WHERE routeid = $1;", route_id)
                await conn.execute("DELETE FROM ship_routes WHERE routeid = $1;", route_id)

        try:
            await global_ws_manager.send_personal_message(
                user_account_id,
                {"type": "ROUTE_DELETED", "data": {"routeId": route_id}},
            )
        except Exception:
            pass

        return {"success": True, "message": f"Deleted route {route_id}."}

    except Exception as e:
        logger.error(f"Error deleting route: {e}", exc_info=True)
        raise

