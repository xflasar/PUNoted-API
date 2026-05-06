# endpoints/Protected/repositories/vendors_repo.py

SQL_FETCH_VENDORS = """
WITH ActiveVendors AS (
    -- 1. Get only the vendors matching the filters
    SELECT vendorid, companycode, companyname, corpname, gamename, cx, isactive, xata_updatedat
    FROM user_vendors
    WHERE isactive = TRUE
      AND ($1::text IS NULL OR (companycode ILIKE $1 OR companyname ILIKE $1))
      AND ($2::text IS NULL OR corpname ILIKE $2)
      AND ($3::text IS NULL OR gamename ILIKE $3)
),
UnnestedOrders AS (
    -- 2. Crack open the JSON location array to get the true target amounts
    SELECT 
        vo.orderid,
        vo.vendorid,
        vo.ordertype,
        vo.materialticker,
        vo.materialid,
        vo.location,
        vo.pricetype,
        vo.fixedprice,
        vo.minprice,
        vo.maxprice,
        vo.reserved,
        v.gamename,
        loc_elem->>'id' AS loc_id,
        COALESCE((loc_elem->>'amount')::numeric, 0) AS target_amount
    FROM user_vendor_orders vo
    JOIN ActiveVendors v ON vo.vendorid = v.vendorid
    -- Fallback to a dummy row if the location array is empty so we don't lose the order
    LEFT JOIN LATERAL jsonb_array_elements(
        CASE 
            WHEN jsonb_typeof(vo.location) = 'array' AND jsonb_array_length(vo.location) > 0 
            THEN vo.location 
            ELSE '[{"id":"none","amount":0}]'::jsonb 
        END
    ) AS loc_elem ON TRUE
),
RelevantInventory AS (
    -- 3. Fetch real-time physical inventory ONLY for the active vendors to keep it fast
    SELECT 
        ud.displayname AS gamename, 
        mt.ticker, 
        COALESCE(st.stationid, pl.planetid)::text AS loc_id,
        SUM(si.quantity) as storage_qty
    FROM storages s
    JOIN users_data ud ON ud.userid = s.userid
    JOIN storage_items si ON si.storageid = s.storageid
    JOIN materials mt ON mt.materialid = si.materialid
    LEFT JOIN warehouses w ON w.warehouseid = s.addressableid
    LEFT JOIN stations st ON st.warehouseid = w.warehouseid
    LEFT JOIN sites site ON site.siteid = s.addressableid
    LEFT JOIN planets pl ON pl.planetid = site.addressplanetid
    WHERE 
        (st.stationid IS NOT NULL OR pl.planetid IS NOT NULL)
        AND EXISTS (
            SELECT 1 FROM ActiveVendors av WHERE av.gamename = ud.displayname
        )
    GROUP BY 1, 2, 3
),
CalculatedOrders AS (
    -- 4. Apply your Buy/Sell logic to calculate true availability
    SELECT 
        uo.orderid,
        uo.vendorid,
        uo.ordertype,
        uo.materialticker,
        uo.materialid,
        uo.location,
        uo.pricetype,
        uo.fixedprice,
        uo.minprice,
        uo.maxprice,
        uo.reserved,
        SUM(
            CASE 
                WHEN uo.loc_id = 'none' THEN 0
                WHEN uo.ordertype = 'buy' THEN GREATEST(0, uo.target_amount - COALESCE(i.storage_qty, 0))
                ELSE GREATEST(0, COALESCE(i.storage_qty, 0) - uo.target_amount)
            END
        ) AS true_available,
        SUM(CASE WHEN uo.loc_id = 'none' THEN 0 ELSE uo.target_amount END) AS calculated_total
    FROM UnnestedOrders uo
    LEFT JOIN RelevantInventory i 
        ON i.gamename = uo.gamename 
        AND i.ticker = uo.materialticker 
        AND i.loc_id = uo.loc_id
    GROUP BY 
        uo.orderid, uo.vendorid, uo.ordertype, uo.materialticker, uo.materialid, 
        uo.location, uo.pricetype, uo.fixedprice, uo.minprice, uo.maxprice, uo.reserved
),
VendorOrdersAgg AS (
    -- 5. Package the freshly calculated stats back into JSON
    SELECT 
        co.vendorid,
        jsonb_agg(
            jsonb_build_object(
                'OrderId', co.orderid,
                'Type', co.ordertype,
                'MaterialTicker', co.materialticker,
                'MaterialId', co.materialid,
                'Location', co.location, 
                'Stats', jsonb_build_object(
                    'TotalQuantity', COALESCE(co.calculated_total, 0), -- Uses JSON target amounts
                    'Reserved', COALESCE(co.reserved, 0),
                    'Available', COALESCE(co.true_available, 0) -- Accurately checked against storages
                ),
                'Pricing', jsonb_build_object(
                    'Strategy', co.pricetype,
                    'FixedPrice', co.fixedprice,
                    'MinPrice', co.minprice,
                    'MaxPrice', co.maxprice
                )
            ) ORDER BY co.materialticker
        ) as orders_json
    FROM CalculatedOrders co
    GROUP BY co.vendorid
)
-- 6. Final assembly
SELECT 
    COALESCE(jsonb_agg(
        jsonb_build_object(
            'VendorId', v.vendorid,
            'CompanyCode', v.companycode,
            'CompanyName', v.companyname,
            'CorporationName', v.corpname,
            'OperatorName', v.gamename,
            'Exchange', v.cx,
            'IsActive', v.isactive,
            'LastUpdatedEpochMs', EXTRACT(EPOCH FROM v.xata_updatedat) * 1000,
            'Orders', COALESCE(vo.orders_json, '[]'::jsonb)
        ) ORDER BY v.companycode
    )::text, '[]')
FROM ActiveVendors v
LEFT JOIN VendorOrdersAgg vo ON vo.vendorid = v.vendorid;
"""


async def fetch_public_vendors(conn, search: str = None, corp: str = None, operator: str = None):
    """
    Fetches a list of public, active vendors with their orders.
    """
    # Wildcards for partial matching
    p_search = f"%{search}%" if search else None
    p_corp = f"%{corp}%" if corp else None
    p_operator = f"%{operator}%" if operator else None

    result = await conn.fetchval(SQL_FETCH_VENDORS, p_search, p_corp, p_operator)
    return result or "[]"
