# repositories/ship_routes_repo.py
import uuid
import logging
from typing import List, Dict, Any
from asyncpg import Connection

logger = logging.getLogger(__name__)


async def repo_get_user_ship_routes(conn: Connection, user_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves all shipping routes for a user, with nested waypoints,
    steps, resolved location/system names, and currently assigned ships.
    """
    user_uuid = uuid.UUID(user_id) if isinstance(user_id, str) and "-" in user_id else user_id

    query = """
        SELECT 
            r.routeid,
            r.naturalid,
            r.name,
            r.repeats,
            r.created_at,
            r.updated_at,
            
            -- Nested Waypoints with steps and resolved names
            COALESCE(
                (
                    SELECT json_agg(
                        json_build_object(
                            'waypointid', w.waypointid,
                            'waypoint_index', w.waypoint_index,
                            'system_id', w.system_id,
                            'system_name', COALESCE(sys.name, w.system_id),
                            'location_id', w.location_id,
                            'location_type', w.location_type,
                            'location_name', COALESCE(p.name, st.name, w.location_id),
                            'landing', w.landing,
                            'fuel_usage_factor', w.fuel_usage_factor,
                            'reactor_usage_factor', w.reactor_usage_factor,
                            'ftl_preference', w.ftl_preference,
                            'use_gateways', w.use_gateways,
                            'steps', COALESCE(
                                (
                                    SELECT json_agg(
                                        json_build_object(
                                            'stepid', s.stepid,
                                            'step_index', s.step_index,
                                            'type', s.type,
                                            'materialid', s.materialid,
                                            'material_ticker', s.material_ticker,
                                            'min_type', s.min_type,
                                            'min_units', s.min_units,
                                            'max_type', s.max_type,
                                            'max_units', s.max_units,
                                            'retain', s.retain,
                                            'fuel_type', s.fuel_type,
                                            'fuel_source', s.fuel_source,
                                            'wait_amount', s.wait_amount,
                                            'wait_unit', s.wait_unit
                                        ) ORDER BY s.step_index ASC
                                    )
                                    FROM ship_route_steps s
                                    WHERE s.waypointid = w.waypointid
                                ), '[]'::json
                            )
                        ) ORDER BY w.waypoint_index ASC
                    )
                    FROM ship_route_waypoints w
                    LEFT JOIN planets p ON w.location_id = p.planetid
                    LEFT JOIN stations st ON w.location_id = st.stationid
                    LEFT JOIN systems sys ON w.system_id = sys.systemid
                    WHERE w.routeid = r.routeid
                ), '[]'::json
            ) AS waypoints,
            
            -- Assigned ships currently on this route
            COALESCE(
                (
                    SELECT json_agg(
                        json_build_object(
                            'shipid', sh.shipid,
                            'name', COALESCE(sh.name, sh.registration),
                            'registration', sh.registration,
                            'status', sh.status,
                            'flightid', sh.flightid
                        ) ORDER BY sh.name ASC
                    )
                    FROM ships sh
                    WHERE sh.routeid = r.routeid
                ), '[]'::json
            ) AS assigned_ships

        FROM ship_routes r
        JOIN users u ON (r.userid = u.userdataid::text OR r.userid = u.accountid::text)
        WHERE u.accountid = $1::uuid
        ORDER BY r.created_at DESC NULLS LAST, r.naturalid ASC;
    """

# repositories/ship_routes_repo.py
import uuid
import logging
from typing import List, Dict, Any
from asyncpg import Connection

logger = logging.getLogger(__name__)


async def repo_get_user_ship_routes(conn: Connection, user_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves all shipping routes for a user, with nested waypoints,
    steps, resolved location/system names, and currently assigned ships.
    """
    user_uuid = uuid.UUID(user_id) if isinstance(user_id, str) and "-" in user_id else user_id

    query = """
        SELECT 
            r.routeid,
            r.naturalid,
            r.name,
            r.repeats,
            r.created_at,
            r.updated_at,
            
            -- Nested Waypoints with steps and resolved names
            COALESCE(
                (
                    SELECT json_agg(
                        json_build_object(
                            'waypointid', w.waypointid,
                            'waypoint_index', w.waypoint_index,
                            'system_id', w.system_id,
                            'system_name', COALESCE(sys.name, w.system_id),
                            'location_id', w.location_id,
                            'location_type', w.location_type,
                            'location_name', COALESCE(p.name, st.name, w.location_id),
                            'landing', w.landing,
                            'fuel_usage_factor', w.fuel_usage_factor,
                            'reactor_usage_factor', w.reactor_usage_factor,
                            'ftl_preference', w.ftl_preference,
                            'use_gateways', w.use_gateways,
                            'steps', COALESCE(
                                (
                                    SELECT json_agg(
                                        json_build_object(
                                            'stepid', s.stepid,
                                            'step_index', s.step_index,
                                            'type', s.type,
                                            'materialid', s.materialid,
                                            'material_ticker', s.material_ticker,
                                            'min_type', s.min_type,
                                            'min_units', s.min_units,
                                            'max_type', s.max_type,
                                            'max_units', s.max_units,
                                            'retain', s.retain,
                                            'fuel_type', s.fuel_type,
                                            'fuel_source', s.fuel_source,
                                            'wait_amount', s.wait_amount,
                                            'wait_unit', s.wait_unit
                                        ) ORDER BY s.step_index ASC
                                    )
                                    FROM ship_route_steps s
                                    WHERE s.waypointid = w.waypointid
                                ), '[]'::json
                            )
                        ) ORDER BY w.waypoint_index ASC
                    )
                    FROM ship_route_waypoints w
                    LEFT JOIN planets p ON w.location_id = p.planetid
                    LEFT JOIN stations st ON w.location_id = st.stationid
                    LEFT JOIN systems sys ON w.system_id = sys.systemid
                    WHERE w.routeid = r.routeid
                ), '[]'::json
            ) AS waypoints,
            
            -- Assigned ships currently on this route
            COALESCE(
                (
                    SELECT json_agg(
                        json_build_object(
                            'shipid', sh.shipid,
                            'name', COALESCE(sh.name, sh.registration),
                            'registration', sh.registration,
                            'status', sh.status,
                            'flightid', sh.flightid
                        ) ORDER BY sh.name ASC
                    )
                    FROM ships sh
                    WHERE sh.routeid = r.routeid
                ), '[]'::json
            ) AS assigned_ships

        FROM ship_routes r
        JOIN users u ON (r.userid = u.userdataid::text OR r.userid = u.accountid::text)
        WHERE u.accountid = $1::uuid
        ORDER BY r.created_at DESC NULLS LAST, r.naturalid ASC;
    """

    try:
        rows = await conn.fetch(query, user_uuid)
        import json
        results = []
        for row in rows:
            d = dict(row)
            if isinstance(d.get("waypoints"), str):
                d["waypoints"] = json.loads(d["waypoints"])
            if isinstance(d.get("assigned_ships"), str):
                d["assigned_ships"] = json.loads(d["assigned_ships"])
            # Format datetime to isoformat strings if present
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
            if d.get("updated_at"):
                d["updated_at"] = d["updated_at"].isoformat()
            results.append(d)
        return results
    except Exception as e:
        logger.error(f"Error fetching user ship routes: {e}", exc_info=True)
        raise


