import json
import logging
from datetime import datetime
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
    Fetches active shipments with full contract details by executing structured queries:
    1. Fetch user & company IDs
    2. Fetch active contracts & conditions (matching both user as client & carrier)
    3. Fetch ship details for items currently in ships
    4. Fetch flight details & flight segments for ships on active flights
    """
    user_info_query = """
        SELECT u.userdataid, cd.companyid 
        FROM users u 
        LEFT JOIN company_data cd ON cd.userdataid = u.userdataid 
        WHERE u.accountid = $1
    """

    contracts_query = """
    SELECT 
        c.id as contract_id,
        c.localid as contract_local_id,
        c.partnername,
        c.userid as creator_userdataid,
        c.partnerid, 
        cd.userdataid as partner_userdataid,
        c.date as created_at,
        c.duedate as deadline,
        c.extensiondeadline,
        c.canextend,
        c.status,
        c.party as contract_party,
        
        -- Condition & Item fields
        cc.id as condition_id,
        cc.index as condition_index,
        cc.type as condition_type,
        cc.status as condition_status,
        cc.shipmentitemid as item_id,
        cc.amountmoney as price,
        cc.currencymoney as currency,
        
        -- Current location of shipment item
        si.storageid,
        s.shipid,
        s.name as ship_name,
        CASE 
            WHEN s.shipid IS NOT NULL THEN 'SHIP'
            WHEN stat.stationid IS NOT NULL THEN 'STATION'
            WHEN sites.siteid IS NOT NULL THEN 'SITE'
            ELSE 'UNKNOWN' 
        END as location_type,
        COALESCE(s.name, s.registration, stat.name, site_planet.name, site_planet.naturalid) as location_name,
        COALESCE(stat.systemid, site_sys.systemid) as systemid,
        COALESCE(stat_sys.name, site_sys.name) as system_name,
        site_planet.planetid,

        -- Destination label
        CASE 
            WHEN target_stat.stationid IS NOT NULL THEN target_stat.name || ' (' || COALESCE(target_sys.name, 'Unk. System') || ')'
            WHEN target_planet.planetid IS NOT NULL THEN COALESCE(target_planet.name, target_planet.naturalid) || ' (' || COALESCE(target_sys.name, 'Unk. System') || ')'
            WHEN target_sys.systemid IS NOT NULL THEN target_sys.name
            ELSE NULL 
        END as destination_label,

        -- Origin label
        CASE 
            WHEN origin_stat.stationid IS NOT NULL THEN origin_stat.name || ' (' || COALESCE(origin_sys.name, 'Unk. System') || ')'
            WHEN origin_planet.planetid IS NOT NULL THEN COALESCE(origin_planet.name, origin_planet.naturalid) || ' (' || COALESCE(origin_sys.name, 'Unk. System') || ')'
            WHEN origin_sys.systemid IS NOT NULL THEN origin_sys.name
            ELSE NULL 
        END as origin_label

    FROM contracts c
    LEFT JOIN company_data cd ON c.partnerid = cd.companyid
    JOIN contract_conditions cc ON c.id = cc.contractid AND c.party = cc.contractparty
    LEFT JOIN storage_items si ON cc.shipmentitemid = si.materialid AND si.type = 'SHIPMENT'
    LEFT JOIN storages st ON si.storageid = st.storageid
    LEFT JOIN ships s ON si.storageid = s.idshipstore
    LEFT JOIN sites ON st.addressableid = sites.siteid
    LEFT JOIN planets site_planet ON sites.addressplanetid = site_planet.planetid
    LEFT JOIN systems site_sys ON sites.addresssystemid = site_sys.systemid
    LEFT JOIN stations stat ON st.addressableid = stat.warehouseid
    LEFT JOIN systems stat_sys ON stat.systemid = stat_sys.systemid
    LEFT JOIN systems target_sys ON cc.destinationsystemid = target_sys.systemid
    LEFT JOIN planets target_planet ON cc.destinationplanetid = target_planet.planetid
    LEFT JOIN stations target_stat ON cc.destinationstationid = target_stat.stationid
    LEFT JOIN systems origin_sys ON cc.addresssystemid = origin_sys.systemid
    LEFT JOIN planets origin_planet ON cc.addressplanetid = origin_planet.planetid
    LEFT JOIN stations origin_stat ON cc.addressstationid = origin_stat.stationid

    WHERE 
      (c.userid = $1 OR (c.partnerid IS NOT NULL AND c.partnerid = $2))
      AND c.status NOT IN ('REJECTED', 'TERMINATED', 'FULFILLED', 'CANCELLED', 'CLOSED')
      AND (c.partnername IS NULL OR c.partnername NOT ILIKE '%Distribution Manager%')
      AND COALESCE(c.extensiondeadline, c.duedate, c.date + interval '7 days') >= (NOW() - interval '3 days')
      AND EXISTS (
          SELECT 1 FROM contract_conditions sub_cc 
          WHERE sub_cc.contractid = c.id AND sub_cc.type = 'DELIVERY_SHIPMENT'
      )
    ORDER BY c.id, cc.index ASC;
    """

    ships_query = """
    SELECT 
        s.shipid,
        s.name,
        s.registration,
        s.addresssystemid,
        s.addressplanetid,
        s.addressstationid,
        s.flightid,
        st.volumecapacity,
        st.weightcapacity,
        sf.id as flight_id,
        sf.departuretimestamp,
        sf.arrivaltimestamp,
        dest_sys.name as destinationsystemid,
        COALESCE(dest_planet.name, dest_planet.naturalid) as destinationplanetid,
        COALESCE(origin_planet.name, origin_planet.naturalid) as originplanetid,
        origin_sys.name as originsystemid,
        dest_stat.name as destinationstationid,
        origin_stat.name as originstationid,
        CASE 
            WHEN s.flightid IS NULL THEN 
                 CASE 
                    WHEN loc_stat.stationid IS NOT NULL THEN loc_stat.name || ' (' || COALESCE(loc_sys.name, 'Unk. System') || ')'
                    WHEN loc_planet.planetid IS NOT NULL THEN COALESCE(loc_planet.name, loc_planet.naturalid) || ' (' || COALESCE(loc_sys.name, 'Unk. System') || ')'
                    ELSE loc_sys.name
                 END
            ELSE NULL 
        END as docked_label
    FROM ships s
    JOIN storages st ON s.idshipstore = st.storageid
    LEFT JOIN ship_flights sf ON s.flightid = sf.id
    LEFT JOIN systems loc_sys ON s.addresssystemid = loc_sys.systemid
    LEFT JOIN planets loc_planet ON s.addressplanetid = loc_planet.planetid
    LEFT JOIN stations loc_stat ON s.addressstationid = loc_stat.stationid
    LEFT JOIN systems origin_sys ON sf.originsystemid = origin_sys.systemid
    LEFT JOIN planets origin_planet ON sf.originplanetid = origin_planet.planetid
    LEFT JOIN stations origin_stat ON sf.originstationid = origin_stat.stationid
    LEFT JOIN systems dest_sys ON sf.destinationsystemid = dest_sys.systemid
    LEFT JOIN planets dest_planet ON sf.destinationplanetid = dest_planet.planetid
    LEFT JOIN stations dest_stat ON sf.destinationstationid = dest_stat.stationid
    WHERE s.shipid = ANY($1::text[])
    """

    segments_query = """
    SELECT 
        sfs.flight_id,
        sfs.segment_index,
        sfs.origin_system_id,
        sfs.destination_system_id,
        sfs.departure,
        sfs.arrival,
        sfs.duration,
        sfs.origin_orbit_data,
        sfs.destination_orbit_data,
        sfs.transferellipse
    FROM ship_flight_segments sfs
    WHERE sfs.flight_id = ANY($1::text[])
    ORDER BY sfs.flight_id, sfs.segment_index ASC
    """

    async with db.pool.acquire() as conn:
        user_row = await conn.fetchrow(user_info_query, account_id)
        if not user_row or not user_row["userdataid"]:
            logger.warning(f"[SHIPMENTS DB] User not found for account {account_id}")
            return {"contracts": [], "ships": {}}

        userdataid = user_row["userdataid"]
        companyid = user_row["companyid"]

        contract_rows = await conn.fetch(contracts_query, userdataid, companyid)

        if not contract_rows:
            logger.info(f"[SHIPMENTS DB] No active contracts found for account {account_id}")
            return {"contracts": [], "ships": {}}

        # 1. Group contract conditions into aggregated contracts dictionary
        contracts_dict = {}
        seen_conditions = set()
        ship_ids = set()

        for r in contract_rows:
            cid = r["contract_id"]
            if r["shipid"]:
                ship_ids.add(r["shipid"])

            # Determine role: If customer is contract creator, creator is client. If provisioner, creator is carrier.
            carrier_userdataid = r["creator_userdataid"] if r["contract_party"] == 'CUSTOMER' else r["partner_userdataid"]
            role = "CARRIER" if carrier_userdataid == userdataid else "CLIENT"

            if cid not in contracts_dict:
                contracts_dict[cid] = {
                    "contract_id": cid,
                    "local_id": r["contract_local_id"],
                    "partner_name": r["partnername"],
                    "role": role,
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "deadline": r["deadline"].isoformat() if r["deadline"] else None,
                    "extension_deadline": r["extensiondeadline"].isoformat() if r["extensiondeadline"] else None,
                    "can_extend": r["canextend"],
                    "status": r["status"],
                    "items": []
                }

            cond_key = (cid, r["condition_id"])
            if cond_key not in seen_conditions:
                seen_conditions.add(cond_key)
                contracts_dict[cid]["items"].append({
                    "condition_id": r["condition_id"],
                    "index": r["condition_index"],
                    "type": r["condition_type"],
                    "status": r["condition_status"],
                    "price": float(r["price"]) if r["price"] is not None else 0,
                    "currency": r["currency"],
                    "item_id": r["item_id"],
                    "location_type": r["location_type"],
                    "location_name": r["location_name"],
                    "system_name": r["system_name"],
                    "systemid": r["systemid"],
                    "planetid": r["planetid"],
                    "shipid": r["shipid"],
                    "ship_name": r["ship_name"],
                    "destination_label": r["destination_label"],
                    "origin_label": r["origin_label"]
                })

        # 2. Fetch ships if any items reside in a ship
        ships_dict = {}
        if ship_ids:
            ship_rows = await conn.fetch(ships_query, list(ship_ids))
            flight_ids = [sr["flight_id"] for sr in ship_rows if sr["flight_id"]]
            
            # Fetch segments for active flights
            segments_by_flight = {}
            if flight_ids:
                seg_rows = await conn.fetch(segments_query, flight_ids)
                for seg in seg_rows:
                    fid = seg["flight_id"]
                    if fid not in segments_by_flight:
                        segments_by_flight[fid] = []
                    dep = seg["departure"]
                    arr = seg["arrival"]
                    dep_val = (
                        dep.isoformat()
                        if isinstance(dep, datetime)
                        else dep
                        if dep is not None
                        else None
                    )
                    arr_val = (
                        arr.isoformat()
                        if isinstance(arr, datetime)
                        else arr
                        if arr is not None
                        else None
                    )
                    segments_by_flight[fid].append({
                        "segment_index": seg["segment_index"],
                        "origin_system_id": seg["origin_system_id"],
                        "destination_system_id": seg["destination_system_id"],
                        "departure": dep_val,
                        "arrival": arr_val,
                        "duration": seg["duration"],
                        "origin_orbit_data": json.loads(seg["origin_orbit_data"]) if isinstance(seg["origin_orbit_data"], str) else seg["origin_orbit_data"],
                        "destination_orbit_data": json.loads(seg["destination_orbit_data"]) if isinstance(seg["destination_orbit_data"], str) else seg["destination_orbit_data"],
                        "transferellipse": json.loads(seg["transferellipse"]) if isinstance(seg["transferellipse"], str) else seg["transferellipse"],
                    })

            for sr in ship_rows:
                sid = sr["shipid"]
                vol = sr["volumecapacity"] or 0
                wt = sr["weightcapacity"] or 0
                
                ship_type = "UNKNOWN"
                if vol >= 5000 and wt >= 5000:
                    ship_type = "HCB"
                elif vol == 3000 and wt == 1000:
                    ship_type = "VCB"
                elif vol == 1000 and wt == 3000:
                    ship_type = "WCB"
                elif vol == 2000 and wt == 2000:
                    ship_type = "LCB"
                elif vol == 500 and wt == 500:
                    ship_type = "TINY"

                flight_obj = None
                if sr["flight_id"]:
                    fid = sr["flight_id"]
                    flight_obj = {
                        "id": fid,
                        "shipid": sid,
                        "departuretimestamp": sr["departuretimestamp"].isoformat() if sr["departuretimestamp"] else None,
                        "arrivaltimestamp": sr["arrivaltimestamp"].isoformat() if sr["arrivaltimestamp"] else None,
                        "destinationsystemid": sr["destinationsystemid"],
                        "destinationplanetid": sr["destinationplanetid"],
                        "originplanetid": sr["originplanetid"],
                        "originsystemid": sr["originsystemid"],
                        "destinationstationid": sr["destinationstationid"],
                        "originstationid": sr["originstationid"],
                        "segments": segments_by_flight.get(fid, [])
                    }

                ships_dict[sid] = {
                    "id": sid,
                    "name": sr["name"],
                    "registration": sr["registration"],
                    "addresssystemid": sr["addresssystemid"],
                    "addressplanetid": sr["addressplanetid"],
                    "addressstationid": sr["addressstationid"],
                    "type": ship_type,
                    "docked_label": sr["docked_label"],
                    "flight": flight_obj
                }

        payload = {
            "contracts": list(contracts_dict.values()),
            "ships": ships_dict
        }
        logger.info(f"[SHIPMENTS DB] Successfully built payload with {len(payload['contracts'])} contracts & {len(ships_dict)} ships.")
        return payload


async def fetch_public_shipment_by_local_id(db: Database, identifier: str, company_code: str | None = None):
    """
    Public lookup for a single shipment contract by contract_id or local_id.
    Requires matching company_code (either creator company or partner company) for security.
    """
    clean_identifier = identifier.strip()
    clean_company = company_code.strip().upper() if company_code else None

    # Support {localid}:{companycode} format directly in identifier
    if not clean_company and ":" in clean_identifier:
        parts = clean_identifier.split(":", 1)
        clean_identifier = parts[0].strip()
        clean_company = parts[1].strip().upper() if parts[1].strip() else None

    # Company code is strictly required for public security
    if not clean_company or not clean_identifier:
        return None

    contract_query = """
    SELECT 
        c.id as contract_id,
        c.localid as contract_local_id,
        c.partnername,
        c.partnercode,
        creator_cd.companycode as creator_company_code,
        c.date as created_at,
        c.duedate as deadline,
        c.extensiondeadline,
        c.canextend,
        c.status,
        
        -- Condition & Item fields
        cc.id as condition_id,
        cc.index as condition_index,
        cc.type as condition_type,
        cc.status as condition_status,
        cc.shipmentitemid as item_id,
        cc.amountmoney as price,
        cc.currencymoney as currency,
        
        -- Current location
        si.storageid,
        s.shipid,
        s.name as ship_name,
        CASE 
            WHEN s.shipid IS NOT NULL THEN 'SHIP'
            WHEN stat.stationid IS NOT NULL THEN 'STATION'
            WHEN sites.siteid IS NOT NULL THEN 'SITE'
            ELSE 'UNKNOWN' 
        END as location_type,
        COALESCE(s.name, s.registration, stat.name, site_planet.name, site_planet.naturalid) as location_name,
        COALESCE(stat.systemid, site_sys.systemid) as systemid,
        COALESCE(stat_sys.name, site_sys.name) as system_name,
        site_planet.planetid,

        -- Destination label
        CASE 
            WHEN target_stat.stationid IS NOT NULL THEN target_stat.name || ' (' || COALESCE(target_sys.name, 'Unk. System') || ')'
            WHEN target_planet.planetid IS NOT NULL THEN COALESCE(target_planet.name, target_planet.naturalid) || ' (' || COALESCE(target_sys.name, 'Unk. System') || ')'
            WHEN target_sys.systemid IS NOT NULL THEN target_sys.name
            ELSE NULL 
        END as destination_label,

        -- Origin label
        CASE 
            WHEN origin_stat.stationid IS NOT NULL THEN origin_stat.name || ' (' || COALESCE(origin_sys.name, 'Unk. System') || ')'
            WHEN origin_planet.planetid IS NOT NULL THEN COALESCE(origin_planet.name, origin_planet.naturalid) || ' (' || COALESCE(origin_sys.name, 'Unk. System') || ')'
            WHEN origin_sys.systemid IS NOT NULL THEN origin_sys.name
            ELSE NULL 
        END as origin_label

    FROM contracts c
    JOIN contract_conditions cc ON c.id = cc.contractid AND c.party = cc.contractparty
    LEFT JOIN company_data creator_cd ON c.userid = creator_cd.userdataid
    LEFT JOIN storage_items si ON cc.shipmentitemid = si.materialid AND si.type = 'SHIPMENT'
    LEFT JOIN storages st ON si.storageid = st.storageid
    LEFT JOIN ships s ON si.storageid = s.idshipstore
    LEFT JOIN sites ON st.addressableid = sites.siteid
    LEFT JOIN planets site_planet ON sites.addressplanetid = site_planet.planetid
    LEFT JOIN systems site_sys ON sites.addresssystemid = site_sys.systemid
    LEFT JOIN stations stat ON st.addressableid = stat.warehouseid
    LEFT JOIN systems stat_sys ON stat.systemid = stat_sys.systemid
    LEFT JOIN systems target_sys ON cc.destinationsystemid = target_sys.systemid
    LEFT JOIN planets target_planet ON cc.destinationplanetid = target_planet.planetid
    LEFT JOIN stations target_stat ON cc.destinationstationid = target_stat.stationid
    LEFT JOIN systems origin_sys ON cc.addresssystemid = origin_sys.systemid
    LEFT JOIN planets origin_planet ON cc.addressplanetid = origin_planet.planetid
    LEFT JOIN stations origin_stat ON cc.addressstationid = origin_stat.stationid

    WHERE (c.id = $1 OR c.localid = $1)
      AND (
          UPPER(c.partnercode) = $2
          OR UPPER(creator_cd.companycode) = $2
      )
      AND EXISTS (
          SELECT 1 FROM contract_conditions sub_cc 
          WHERE sub_cc.contractid = c.id AND sub_cc.type = 'DELIVERY_SHIPMENT'
      )
    ORDER BY cc.index ASC;
    """

    ships_query = """
    SELECT 
        s.shipid,
        s.name,
        s.registration,
        s.addresssystemid,
        s.addressplanetid,
        s.addressstationid,
        s.flightid,
        st.volumecapacity,
        st.weightcapacity,
        sf.id as flight_id,
        sf.departuretimestamp,
        sf.arrivaltimestamp,
        dest_sys.name as destinationsystemid,
        COALESCE(dest_planet.name, dest_planet.naturalid) as destinationplanetid,
        COALESCE(origin_planet.name, origin_planet.naturalid) as originplanetid,
        origin_sys.name as originsystemid,
        dest_stat.name as destinationstationid,
        origin_stat.name as originstationid,
        CASE 
            WHEN s.flightid IS NULL THEN 
                 CASE 
                    WHEN loc_stat.stationid IS NOT NULL THEN loc_stat.name || ' (' || COALESCE(loc_sys.name, 'Unk. System') || ')'
                    WHEN loc_planet.planetid IS NOT NULL THEN COALESCE(loc_planet.name, loc_planet.naturalid) || ' (' || COALESCE(loc_sys.name, 'Unk. System') || ')'
                    ELSE loc_sys.name
                 END
            ELSE NULL 
        END as docked_label
    FROM ships s
    JOIN storages st ON s.idshipstore = st.storageid
    LEFT JOIN ship_flights sf ON s.flightid = sf.id
    LEFT JOIN systems loc_sys ON s.addresssystemid = loc_sys.systemid
    LEFT JOIN planets loc_planet ON s.addressplanetid = loc_planet.planetid
    LEFT JOIN stations loc_stat ON s.addressstationid = loc_stat.stationid
    LEFT JOIN systems origin_sys ON sf.originsystemid = origin_sys.systemid
    LEFT JOIN planets origin_planet ON sf.originplanetid = origin_planet.planetid
    LEFT JOIN stations origin_stat ON sf.originstationid = origin_stat.stationid
    LEFT JOIN systems dest_sys ON sf.destinationsystemid = dest_sys.systemid
    LEFT JOIN planets dest_planet ON sf.destinationplanetid = dest_planet.planetid
    LEFT JOIN stations dest_stat ON sf.destinationstationid = dest_stat.stationid
    WHERE s.shipid = ANY($1::text[])
    """

    segments_query = """
    SELECT 
        sfs.flight_id,
        sfs.segment_index,
        sfs.origin_system_id,
        sfs.destination_system_id,
        sfs.departure,
        sfs.arrival,
        sfs.duration,
        sfs.origin_orbit_data,
        sfs.destination_orbit_data,
        sfs.transferellipse
    FROM ship_flight_segments sfs
    WHERE sfs.flight_id = ANY($1::text[])
    ORDER BY sfs.flight_id, sfs.segment_index ASC
    """

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(contract_query, clean_identifier, clean_company)
        if not rows:
            return None


        first = rows[0]
        contract_data = {
            "contract_id": first["contract_id"],
            "local_id": first["contract_local_id"],
            "partner_name": first["partnername"],
            "role": "TRACKING",
            "created_at": first["created_at"].isoformat() if first["created_at"] else None,
            "deadline": first["deadline"].isoformat() if first["deadline"] else None,
            "extension_deadline": first["extensiondeadline"].isoformat() if first["extensiondeadline"] else None,
            "can_extend": first["canextend"],
            "status": first["status"],
            "items": []
        }

        ship_ids = set()
        seen_conditions = set()
        for r in rows:
            if r["shipid"]:
                ship_ids.add(r["shipid"])

            cond_id = r["condition_id"]
            if cond_id not in seen_conditions:
                seen_conditions.add(cond_id)
                contract_data["items"].append({
                    "condition_id": r["condition_id"],
                    "index": r["condition_index"],
                    "type": r["condition_type"],
                    "status": r["condition_status"],
                    "price": float(r["price"]) if r["price"] is not None else 0,
                    "currency": r["currency"],
                    "item_id": r["item_id"],
                    "location_type": r["location_type"],
                    "location_name": r["location_name"],
                    "system_name": r["system_name"],
                    "systemid": r["systemid"],
                    "planetid": r["planetid"],
                    "shipid": r["shipid"],
                    "ship_name": r["ship_name"],
                    "destination_label": r["destination_label"],
                    "origin_label": r["origin_label"]
                })

        ships_dict = {}
        if ship_ids:
            ship_rows = await conn.fetch(ships_query, list(ship_ids))
            flight_ids = [sr["flight_id"] for sr in ship_rows if sr["flight_id"]]
            
            segments_by_flight = {}
            if flight_ids:
                seg_rows = await conn.fetch(segments_query, flight_ids)
                for seg in seg_rows:
                    fid = seg["flight_id"]
                    if fid not in segments_by_flight:
                        segments_by_flight[fid] = []
                    dep = seg["departure"]
                    arr = seg["arrival"]
                    dep_val = (
                        dep.isoformat()
                        if isinstance(dep, datetime)
                        else dep
                        if dep is not None
                        else None
                    )
                    arr_val = (
                        arr.isoformat()
                        if isinstance(arr, datetime)
                        else arr
                        if arr is not None
                        else None
                    )
                    segments_by_flight[fid].append({
                        "segment_index": seg["segment_index"],
                        "origin_system_id": seg["origin_system_id"],
                        "destination_system_id": seg["destination_system_id"],
                        "departure": dep_val,
                        "arrival": arr_val,
                        "duration": seg["duration"],
                        "origin_orbit_data": json.loads(seg["origin_orbit_data"]) if isinstance(seg["origin_orbit_data"], str) else seg["origin_orbit_data"],
                        "destination_orbit_data": json.loads(seg["destination_orbit_data"]) if isinstance(seg["destination_orbit_data"], str) else seg["destination_orbit_data"],
                        "transferellipse": json.loads(seg["transferellipse"]) if isinstance(seg["transferellipse"], str) else seg["transferellipse"],
                    })

            for sr in ship_rows:
                sid = sr["shipid"]
                vol = sr["volumecapacity"] or 0
                wt = sr["weightcapacity"] or 0

                ship_type = "UNKNOWN"
                if vol >= 5000 and wt >= 5000:
                    ship_type = "HCB"
                elif vol == 3000 and wt == 1000:
                    ship_type = "VCB"
                elif vol == 1000 and wt == 3000:
                    ship_type = "WCB"
                elif vol == 2000 and wt == 2000:
                    ship_type = "LCB"
                elif vol == 500 and wt == 500:
                    ship_type = "TINY"

                flight_obj = None
                if sr["flight_id"]:
                    fid = sr["flight_id"]
                    flight_obj = {
                        "id": fid,
                        "shipid": sid,
                        "departuretimestamp": sr["departuretimestamp"].isoformat() if sr["departuretimestamp"] else None,
                        "arrivaltimestamp": sr["arrivaltimestamp"].isoformat() if sr["arrivaltimestamp"] else None,
                        "destinationsystemid": sr["destinationsystemid"],
                        "destinationplanetid": sr["destinationplanetid"],
                        "originplanetid": sr["originplanetid"],
                        "originsystemid": sr["originsystemid"],
                        "destinationstationid": sr["destinationstationid"],
                        "originstationid": sr["originstationid"],
                        "segments": segments_by_flight.get(fid, [])
                    }

                ships_dict[sid] = {
                    "id": sid,
                    "name": sr["name"],
                    "registration": sr["registration"],
                    "addresssystemid": sr["addresssystemid"],
                    "addressplanetid": sr["addressplanetid"],
                    "addressstationid": sr["addressstationid"],
                    "type": ship_type,
                    "docked_label": sr["docked_label"],
                    "flight": flight_obj
                }

        return {"contract": contract_data, "ships": ships_dict}




