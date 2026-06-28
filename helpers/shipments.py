import json
import logging
from typing import Any, Dict

from db import Database

logger = logging.getLogger("Helpers.Shipments")


async def lookup_shipment_channels(conn, flight_id: str) -> Dict[str, Any]:
    """
    Identifies the Current User (Ship Owner) and all Contract Partners involved
    in the shipment on the given flight.

    Returns:
        {
            "contract_id": str,
            "current_user": { "account_id": str, "corp_ids": [str] },
            "partners": [ { "account_id": str, "corp_ids": [str] } ]
        }
    """
    result = {
        "contract_id": None,
        "current_user": {"account_id": None, "corp_ids": []},
        "partners": [],
    }

    query = """
    SELECT 
    -- 1. YOUR INFO (Always returned if ship exists)
    u_owner.accountid as owner_account_id,
    COALESCE(array_agg(DISTINCT cs_owner.corporationid) FILTER (WHERE cs_owner.corporationid IS NOT NULL), '{}') as owner_corp_ids,

    -- 2. CONTRACT INFO (NULL if no shipment found)
    MAX(c.id) as contract_id,

    -- 3. PARTNER INFO (NULL if no shipment found)
    u_partner.accountid as partner_account_id,
    array_agg(DISTINCT cs_partner.corporationid) FILTER (WHERE cs_partner.corporationid IS NOT NULL) as partner_corp_ids

FROM ship_flights sf
JOIN ships s ON sf.shipid = s.shipid

-- A. GET OWNER (Base - Always happens)
LEFT JOIN users u_owner ON u_owner.userdataid = s.userid
LEFT JOIN corporation_shareholders cs_owner ON cs_owner.userid = u_owner.userdataid

-- B. TRAVERSE TO CONTRACT (All LEFT JOINs)
--    If any step here fails (e.g., no item in storage), these columns just become NULL
LEFT JOIN storage_items si ON si.storageid = s.idshipstore AND si.type = 'SHIPMENT' 
LEFT JOIN contract_conditions cc ON cc.shipmentitemid = si.materialid
LEFT JOIN contracts c ON c.id = cc.contractid

-- C. GET PARTNER (Dependent on Contract)
LEFT JOIN company_data cd ON cd.companyid = c.partnerid
LEFT JOIN users u_partner ON u_partner.userdataid = cd.userdataid
LEFT JOIN corporation_shareholders cs_partner ON cs_partner.userid = u_partner.userdataid

WHERE sf.id = $1

GROUP BY 
    u_owner.accountid,
    u_partner.accountid;
    """

    try:
        rows = await conn.fetch(query, flight_id)

        if rows:
            # -- 1. Populate Current User (Ship Owner) --
            # Data is repeated in every row, so just take the first one
            first_row = rows[0]
            result["contract_id"] = first_row["contract_id"]

            if first_row["owner_account_id"]:
                result["current_user"]["account_id"] = str(first_row["owner_account_id"])
                result["current_user"]["corp_ids"] = [str(c) for c in (first_row["owner_corp_ids"] or []) if c]

            # -- 2. Populate Partners --
            seen_partners = set()

            for row in rows:
                p_acc = str(row["partner_account_id"]) if row["partner_account_id"] else None

                # Add to partners list if valid
                if p_acc and p_acc not in seen_partners:
                    # Logic: If you want to exclude the ship owner from the "partners" list
                    # (so they don't get a double notification), uncomment the check below:
                    # if p_acc != result["current_user"]["account_id"]:

                    result["partners"].append(
                        {
                            "account_id": p_acc,
                            # "corp_ids": [str(c) for c in row["partner_corp_ids"] if c]
                        }
                    )
                    seen_partners.add(p_acc)

    except Exception as e:
        logger.error(f"Shipment lookup error: {e}", exc_info=True)

    return result


async def fetch_active_shipments_structured(db: Database, account_id: str):
    """
    Fetches active shipments with FULL contract details.
    """
    query = """
    WITH UserData AS (
        SELECT userdataid FROM users WHERE accountid = $1
    ),
    ActiveContracts AS (
        SELECT 
            c.id as contract_id,
            c.localid as contract_local_id,
            c.partnername,
            c.userid,
            c.partnerid, 
            cd.userdataid as resolved_partner_userid,
            c.date as created_at,
            c.duedate as deadline,
            c.extensiondeadline,
            c.canextend,
            c.status,
			c.party,
            CASE WHEN c.party = 'CUSTOMER' THEN c.userid ELSE cd.userdataid END as carrier_userdataid,
            CASE WHEN c.party = 'CUSTOMER' THEN cd.userdataid ELSE c.userid END as client_userdataid
        FROM contracts c
        LEFT JOIN company_data cd ON c.partnerid = cd.companyid
        WHERE 
          c.userid = (SELECT userdataid FROM UserData)
          AND c.status != 'FULFILLED' AND c.status != 'TERMINATED' AND c.status != 'BREACHED'
          AND c.partnername NOT ILIKE '%Distribution Manager%'
          AND EXISTS (SELECT 1 FROM contract_conditions sub_cc WHERE sub_cc.contractid = c.id AND sub_cc.type = 'DELIVERY_SHIPMENT')
    ),
    ContractConditions AS (
        SELECT 
            ac.contract_id,
            cc.id as condition_id,
            cc.index as condition_index,
            cc.type as condition_type,
            cc.status as condition_status,
            cc.shipmentitemid as item_id,
            cc.amountmoney,
            cc.currencymoney,
            si.storageid,
            CASE 
                WHEN s.shipid IS NOT NULL THEN 'SHIP'
                WHEN stat.stationid IS NOT NULL THEN 'STATION'
                WHEN sites.siteid IS NOT NULL THEN 'SITE'
                ELSE 'UNKNOWN' 
            END as location_type,
            COALESCE(s.name, s.registration, stat.name, site_planet.name, site_planet.naturalid) as location_name,
            COALESCE(stat.systemid, site_sys.systemid) as systemid,
            COALESCE(stat_sys.name, site_sys.name) as system_name,
            COALESCE(site_planet.planetid) as planetid,
            s.shipid,
            s.name as ship_name,
            -- DESTINATION LABEL
            CASE 
                WHEN target_stat.stationid IS NOT NULL THEN target_stat.name || ' (' || COALESCE(target_sys.name, 'Unk. System') || ')'
                WHEN target_planet.planetid IS NOT NULL THEN COALESCE(target_planet.name, target_planet.naturalid) || ' (' || COALESCE(target_sys.name, 'Unk. System') || ')'
                WHEN target_sys.systemid IS NOT NULL THEN target_sys.name
                ELSE NULL 
            END as destination_label,
            -- ORIGIN LABEL
            CASE 
                WHEN origin_stat.stationid IS NOT NULL THEN origin_stat.name || ' (' || COALESCE(origin_sys.name, 'Unk. System') || ')'
                WHEN origin_planet.planetid IS NOT NULL THEN COALESCE(origin_planet.name, origin_planet.naturalid) || ' (' || COALESCE(origin_sys.name, 'Unk. System') || ')'
                WHEN origin_sys.systemid IS NOT NULL THEN origin_sys.name
                ELSE NULL 
            END as origin_label

        FROM ActiveContracts ac
        JOIN contract_conditions cc ON ac.contract_id = cc.contractid AND ac.party = cc.contractparty
        LEFT JOIN storage_items si ON cc.shipmentitemid = si.materialid AND si.type = 'SHIPMENT'
        LEFT JOIN storages st ON si.storageid = st.storageid
        LEFT JOIN ships s ON si.storageid = s.idshipstore
        LEFT JOIN sites ON st.addressableid = sites.siteid
        LEFT JOIN planets site_planet ON sites.addressplanetid = site_planet.planetid
        LEFT JOIN systems site_sys ON sites.addresssystemid = site_sys.systemid
        LEFT JOIN warehouses wh ON st.addressableid = wh.warehouseid
        LEFT JOIN stations stat ON wh.warehouseid = stat.warehouseid
        LEFT JOIN systems stat_sys ON stat.systemid = stat_sys.systemid
        LEFT JOIN systems target_sys ON cc.destinationsystemid = target_sys.systemid
        LEFT JOIN planets target_planet ON cc.destinationplanetid = target_planet.planetid
        LEFT JOIN stations target_stat ON cc.destinationstationid = target_stat.stationid
        LEFT JOIN systems origin_sys ON cc.addresssystemid = origin_sys.systemid
        LEFT JOIN planets origin_planet ON cc.addressplanetid = origin_planet.planetid
        LEFT JOIN stations origin_stat ON cc.addressstationid = origin_stat.stationid
    ),
    UniqueShips AS (
        SELECT DISTINCT shipid FROM ContractConditions WHERE shipid IS NOT NULL
    ),
    FlightSegments AS (
        SELECT 
            sf.id,
            jsonb_agg(
                jsonb_build_object(
                    'segment_index', sfs.segment_index,
                    'origin_system_id', sfs.origin_system_id,
                    'destination_system_id', sfs.destination_system_id,
                    'departure', sfs.departure,
                    'arrival', sfs.arrival,
                    'duration', sfs.duration,
                    'origin_orbit_data', sfs.origin_orbit_data,
                    'destination_orbit_data', sfs.destination_orbit_data,
                    'transferellipse', sfs.transferellipse
                ) ORDER BY sfs.segment_index ASC
            ) as segments_json
        FROM ship_flights sf
        JOIN ship_flight_segments sfs ON sf.id = sfs.flight_id
        WHERE sf.shipid IN (SELECT shipid FROM UniqueShips)
        GROUP BY sf.id
    ),
    ShipData AS (
        SELECT 
            s.shipid,
            jsonb_build_object(
                'id', s.shipid,
                'name', s.name,
                'registration', s.registration,
                'addresssystemid', s.addresssystemid,
                'addressplanetid', s.addressplanetid,
                'addressstationid', s.addressstationid,
                'type', CASE 
                    WHEN st.volumecapacity >= 5000 AND st.weightcapacity >= 5000 THEN 'HCB'
                    WHEN st.volumecapacity = 3000 AND st.weightcapacity = 1000 THEN 'VCB'
                    WHEN st.volumecapacity = 1000 AND st.weightcapacity = 3000 THEN 'WCB'
                    WHEN st.volumecapacity = 2000 AND st.weightcapacity = 2000 THEN 'LCB'
                    WHEN st.volumecapacity = 500  AND st.weightcapacity = 500  THEN 'TINY'
                    ELSE 'UNKNOWN'
                END,
                'docked_label', CASE 
                    WHEN s.flightid IS NULL THEN 
                         CASE 
                            WHEN loc_stat.stationid IS NOT NULL THEN loc_stat.name || ' (' || COALESCE(loc_sys.name, 'Unk. System') || ')'
                            WHEN loc_planet.planetid IS NOT NULL THEN COALESCE(loc_planet.name, loc_planet.naturalid) || ' (' || COALESCE(loc_sys.name, 'Unk. System') || ')'
                            ELSE loc_sys.name
                         END
                    ELSE NULL 
                END,
                'flight', CASE WHEN sf.id IS NOT NULL THEN
                    jsonb_build_object(
                        'id', sf.id,
                        'shipid', s.shipid,
                        'departuretimestamp', sf.departuretimestamp,
                        'arrivaltimestamp', sf.arrivaltimestamp,
                        'destinationsystemid', dest_sys.name,
                        'destinationplanetid', COALESCE(dest_planet.name, dest_planet.naturalid),
                        'originplanetid', COALESCE(origin_planet.name, origin_planet.naturalid),
                        'originsystemid', origin_sys.name,
                        'destinationstationid', dest_stat.name,
                        'originstationid', origin_stat.name,
                        'segments', COALESCE(fs.segments_json, '[]'::jsonb)
                    )
                ELSE NULL END
            ) as ship_json
        FROM UniqueShips us
        JOIN ships s ON us.shipid = s.shipid
        -- Active Flight Logic: If s.flightid exists, join it
        LEFT JOIN ship_flights sf ON s.flightid = sf.id

        JOIN storages st ON s.idshipstore = st.storageid
        
        -- DOCKED Location Joins (Used when flight is NULL)
        LEFT JOIN systems loc_sys ON s.addresssystemid = loc_sys.systemid
        LEFT JOIN planets loc_planet ON s.addressplanetid = loc_planet.planetid
        LEFT JOIN stations loc_stat ON s.addressstationid = loc_stat.stationid

        -- FLIGHT Origin Joins (Used when flight exists)
        LEFT JOIN systems origin_sys ON sf.originsystemid = origin_sys.systemid
        LEFT JOIN planets origin_planet ON sf.originplanetid = origin_planet.planetid
        LEFT JOIN stations origin_stat ON sf.originstationid = origin_stat.stationid

        -- FLIGHT Destination Joins (Used when flight exists)
        LEFT JOIN systems dest_sys ON sf.destinationsystemid = dest_sys.systemid
        LEFT JOIN planets dest_planet ON sf.destinationplanetid = dest_planet.planetid
        LEFT JOIN stations dest_stat ON sf.destinationstationid = dest_stat.stationid
        LEFT JOIN FlightSegments fs ON sf.id = fs.id
    ),
    AggregatedContracts AS (
        SELECT 
            ac.contract_id,
            jsonb_build_object(
                'contract_id', ac.contract_id,
                'local_id', ac.contract_local_id,
                'partner_name', ac.partnername,
                'role', CASE WHEN ac.carrier_userdataid = (SELECT userdataid FROM UserData) THEN 'CARRIER' ELSE 'CLIENT' END,
                'created_at', ac.created_at,
                'deadline', ac.deadline,
                'extension_deadline', ac.extensiondeadline,
                'can_extend', ac.canextend,
                'status', ac.status,
                'items', jsonb_agg(
                    jsonb_build_object(
                        'condition_id', cc.condition_id,
                        'index', cc.condition_index,
                        'type', cc.condition_type,   
                        'status', cc.condition_status, 
                        'price', cc.amountmoney,
                        'currency', cc.currencymoney,
                        'item_id', cc.item_id,
                        'location_type', cc.location_type,
                        'location_name', cc.location_name,
                        'system_name', cc.system_name,
                        'systemid', cc.systemid,
                        'planetid', cc.planetid,
                        'shipid', cc.shipid,
                        'ship_name', cc.ship_name,
                        'destination_label', cc.destination_label,
                        'origin_label', cc.origin_label
                    ) ORDER BY cc.condition_index ASC
                )
            ) as contract_json
        FROM ActiveContracts ac
        JOIN ContractConditions cc ON ac.contract_id = cc.contract_id
        GROUP BY ac.contract_id, ac.contract_local_id, ac.partnername, ac.created_at, ac.deadline, ac.extensiondeadline, ac.canextend, ac.carrier_userdataid, ac.status
    )
    SELECT 
        jsonb_build_object(
            'contracts', (SELECT jsonb_agg(contract_json) FROM AggregatedContracts),
            'ships', (SELECT jsonb_object_agg(shipid, ship_json) FROM ShipData)
        ) as final_payload
    """
    async with db.pool.acquire() as conn:
        parsed_json = await conn.fetchval(query, account_id)
        if not parsed_json:
            return {"contracts": [], "ships": {}}
        return parsed_json
