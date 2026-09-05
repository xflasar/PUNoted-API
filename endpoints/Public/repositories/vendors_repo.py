import json
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

async def fetch_public_vendors(
    db,
    search: Optional[str] = None,
    corp: Optional[str] = None,
    operator: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches active vendor stores directory with calculated inventory availability,
    location codes, and pricing data (fixed, corp, cx prices).
    """
    try:
        pool = db.pool

        async with pool.acquire() as con:
            # Step 1: Query active vendors (within 7 days) and their orders
            vendors_orders_query = """
                SELECT
                    UV.VENDORID,
                    UV.COMPANYCODE,
                    UV.COMPANYNAME,
                    UV.CORPNAME,
                    UV.GAMENAME,
                    UV.ISACTIVE,
                    UV.CX,
                    UVO.ORDERID,
                    UVO.MATERIALTICKER,
                    UVO.ORDERTYPE,
                    UVO.FIXEDPRICE,
                    UVO.RESERVED,
                    COALESCE(UVO.LOCATION, '[]'::JSONB) AS LOCATIONS,
                    MP.PRICE AS CORPPRICE,
                    CASE
                        WHEN UVO.ORDERTYPE = 'buy' THEN CXB.ASKPRICE
                        ELSE CXB.BIDPRICE
                    END AS CXPRICE,
                    CASE
                        WHEN EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) < 3600 THEN
                            FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 60)::text || 'm'
                        WHEN EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) < 86400 THEN
                            FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 3600)::text || 'h'
                        ELSE
                            FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 86400)::text || 'd'
                    END AS activity_label
                FROM
                    USER_VENDORS AS UV
                    INNER JOIN USER_VENDOR_ORDERS AS UVO ON UVO.VENDORID = UV.VENDORID
                    LEFT JOIN MATERIAL_PRICES AS MP ON MP.TICKER = UVO.MATERIALTICKER
                    LEFT JOIN CX_BROKERS AS CXB ON CXB.TICKER = (UVO.MATERIALTICKER || '.' || UV.CX)
                    INNER JOIN USERS AS U ON U.ACCOUNTID::text = UV.USERID
                WHERE U.xata_updatedat >= NOW() - INTERVAL '7 days'
                  AND ($1::text IS NULL OR UV.COMPANYCODE ILIKE $1 OR UV.COMPANYNAME ILIKE $1)
                  AND ($2::text IS NULL OR UV.CORPNAME ILIKE $2)
                  AND ($3::text IS NULL OR UV.GAMENAME ILIKE $3);
            """

            p_search = f"%{search}%" if search else None
            p_corp = f"%{corp}%" if corp else None
            p_operator = f"%{operator}%" if operator else None

            vendors_data = await con.fetch(vendors_orders_query, p_search, p_corp, p_operator)

            if not vendors_data:
                return []

            # Step 2: Parse locations & collect all location IDs
            all_location_ids = set()
            parsed_orders = []

            for r in vendors_data:
                r_dict = dict(r)
                raw_loc = r_dict["locations"]
                loc_list = []

                if raw_loc:
                    if isinstance(raw_loc, str):
                        try:
                            loc_list = json.loads(raw_loc)
                        except Exception:
                            loc_list = []
                    else:
                        loc_list = raw_loc

                r_dict["parsed_locations"] = loc_list
                parsed_orders.append(r_dict)

                for loc in loc_list:
                    if "id" in loc:
                        all_location_ids.add(loc["id"])

            # Step 3: Location lookup (Name & Natural Code)
            location_lookup = {}
            if all_location_ids:
                ids_list = list(all_location_ids)
                loc_details_query = """
                    SELECT stationid::text as id, name, naturalid FROM stations WHERE stationid::text = ANY($1)
                    UNION ALL
                    SELECT planetid::text as id, name, naturalid FROM planets WHERE planetid::text = ANY($1)
                """
                loc_rows = await con.fetch(loc_details_query, ids_list)
                for row in loc_rows:
                    location_lookup[row["id"]] = {
                        "name": row["name"],
                        "code": row["naturalid"],
                    }

            # Step 4: Storage inventory lookup
            gamename_ticker_pairs = [(r["gamename"], r["materialticker"]) for r in vendors_data]
            inventory_map = {}

            if gamename_ticker_pairs:
                values_str = ", ".join([f"('{g}', '{t}')" for g, t in set(gamename_ticker_pairs)])
                inventory_query = f"""
                    WITH ItemSums AS (
                        SELECT storageid, materialid, SUM(quantity) as total_qty
                        FROM storage_items
                        GROUP BY storageid, materialid
                    )
                    SELECT 
                        ud.displayname, 
                        mt.ticker, 
                        COALESCE(st.stationid, pl_site.planetid, pl_w.planetid)::text AS location_id,
                        SUM(si.total_qty) as quantity
                    FROM storages s
                    JOIN users_data ud ON ud.userid = s.userid
                    JOIN ItemSums si ON si.storageid = s.storageid
                    JOIN materials mt ON mt.materialid = si.materialid
                    LEFT JOIN warehouses w ON w.storeid::text = s.storageid::text AND s.type = 'WAREHOUSE_STORE'
                    LEFT JOIN stations st ON st.warehouseid = w.warehouseid
                    LEFT JOIN sites site ON site.siteid = s.addressableid AND s.type = 'STORE'
                    LEFT JOIN planets pl_site ON pl_site.planetid = site.addressplanetid
                    LEFT JOIN planets pl_w ON pl_w.planetid = w.addressplanet
                    JOIN (VALUES {values_str}) AS t(displayname, ticker) 
                      ON ud.displayname = t.displayname AND mt.ticker = t.ticker
                    WHERE s.type IN ('STORE', 'WAREHOUSE_STORE')
                    GROUP BY 1, 2, 3
                    HAVING COALESCE(st.stationid, pl_site.planetid, pl_w.planetid) IS NOT NULL;
                """
                inv_rows = await con.fetch(inventory_query)
                inventory_map = {
                    (r["displayname"], r["ticker"], r["location_id"]): float(r["quantity"]) for r in inv_rows
                }

            # Step 5: Construct vendor dictionary
            vendors_dict = {}

            for r in parsed_orders:
                vendor_id = r["vendorid"]
                if vendor_id not in vendors_dict:
                    vendors_dict[vendor_id] = {
                        "vendor": {
                            "vendorid": r["vendorid"],
                            "companycode": r["companycode"],
                            "companyname": r["companyname"],
                            "corpname": r["corpname"],
                            "gamename": r["gamename"],
                            "isactive": r["isactive"],
                            "activity": r["activity_label"],
                            "cx": r["cx"],
                        },
                        "orders": [],
                    }

                final_locations = []
                total_available = 0.0

                for loc in r["parsed_locations"]:
                    loc_id = loc.get("id")
                    details = location_lookup.get(loc_id)

                    storage_qty = inventory_map.get((r["gamename"], r["materialticker"], loc_id), 0.0)
                    target_amount = loc.get("amount", 0)

                    loc_available = 0.0
                    if r["ordertype"] == "buy":
                        loc_available = max(0.0, float(target_amount) - storage_qty)
                    else:
                        loc_available = max(0.0, storage_qty - float(target_amount))

                    total_available += loc_available

                    final_locations.append(
                        {
                            "id": loc_id,
                            "location_name": details["name"] if details else "Unknown",
                            "location_code": details["code"] if details else "???",
                            "available": loc_available,
                        }
                    )

                if total_available <= 0:
                    continue

                order_data = {
                    "orderid": str(r["orderid"]) if r["orderid"] else None,
                    "materialticker": r["materialticker"],
                    "ordertype": r["ordertype"],
                    "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0.0,
                    "location": final_locations,
                    "price": {
                        "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0.0,
                        "corpprice": float(r["corpprice"]) if r["corpprice"] else 0.0,
                        "cxprice": float(r["cxprice"]) if r["cxprice"] else 0.0,
                    },
                    "available": total_available,
                }

                vendors_dict[vendor_id]["orders"].append(order_data)

            # Filter out vendor stores with 0 available orders
            result = [v for v in vendors_dict.values() if len(v["orders"]) > 0]
            return result

    except Exception as e:
        logger.error(f"Error fetching public vendor directory: {e}", exc_info=True)
        raise