SQL_FETCH_VENDORS = """
WITH ActiveVendors AS (
    SELECT 
        UV.*,
        U.xata_updatedat,
        FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 86400)::text || 'd' as activity_label
    FROM USER_VENDORS AS UV
    INNER JOIN USERS AS U ON U.ACCOUNTID::text = UV.USERID
    WHERE U.xata_updatedat >= NOW() - INTERVAL '7 days'
      -- Apply Search Filters here
      AND ($1::text IS NULL OR UV.COMPANYCODE ILIKE $1 OR UV.COMPANYNAME ILIKE $1)
      AND ($2::text IS NULL OR UV.CORPNAME ILIKE $2)
      AND ($3::text IS NULL OR UV.GAMENAME ILIKE $3)
),
OrderLocations AS (
    SELECT 
        UVO.ORDERID,
        UVO.VENDORID,
        AV.GAMENAME,
        UVO.MATERIALTICKER,
        UVO.ORDERTYPE,
        (loc_elem->>'id')::text as loc_id,
        (loc_elem->>'amount')::numeric as target_amount,
        -- Logic: Fixed price -1 ? Corp Price : Fixed Price
        COALESCE(NULLIF(UVO.FIXEDPRICE, -1), MP.PRICE) as final_price,
        MP.PRICE as corpprice,
        CASE WHEN UVO.ORDERTYPE = 'buy' THEN CXB.ASKPRICE ELSE CXB.BIDPRICE END AS cxprice
    FROM USER_VENDOR_ORDERS UVO
    JOIN ActiveVendors AV ON AV.VENDORID = UVO.VENDORID
    LEFT JOIN MATERIAL_PRICES MP ON MP.TICKER = UVO.MATERIALTICKER
    LEFT JOIN CX_BROKERS CXB ON CXB.TICKER = (UVO.MATERIALTICKER || '.' || AV.CX)
    CROSS JOIN LATERAL jsonb_array_elements(UVO.LOCATION) AS loc_elem
),
InventoryCache AS (
    SELECT 
        ud.displayname, mt.ticker, 
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
    WHERE st.stationid IS NOT NULL OR pl.planetid IS NOT NULL
    GROUP BY 1, 2, 3
),
AggregatedOrders AS (
    SELECT 
        ol.VENDORID,
        jsonb_agg(jsonb_build_object(
            'orderid', ol.ORDERID,
            'materialticker', ol.MATERIALTICKER,
            'price', jsonb_build_object('fixedprice', ol.final_price, 'corpprice', ol.corpprice, 'cxprice', ol.cxprice),
            'ordertype', ol.ORDERTYPE,
            'locations', (
                SELECT jsonb_agg(jsonb_build_object(
                    'id', ol2.loc_id,
                    'name', COALESCE(st.name, pl.name, 'Unknown'),
                    'available', CASE 
                        WHEN ol2.ORDERTYPE = 'buy' THEN GREATEST(0, ol2.target_amount - COALESCE(inv.storage_qty, 0))
                        ELSE GREATEST(0, COALESCE(inv.storage_qty, 0) - ol2.target_amount)
                    END
                ))
                FROM OrderLocations ol2
                LEFT JOIN InventoryCache inv ON inv.displayname = ol.GAMENAME AND inv.ticker = ol2.MATERIALTICKER AND inv.loc_id = ol2.loc_id
                LEFT JOIN stations st ON st.stationid::text = ol2.loc_id
                LEFT JOIN planets pl ON pl.planetid::text = ol2.loc_id
                WHERE ol2.ORDERID = ol.ORDERID
            )
        )) as orders
    FROM OrderLocations ol
    GROUP BY ol.VENDORID
)
SELECT 
    jsonb_agg(jsonb_build_object(
        'vendor', jsonb_build_object(
            'vendorid', av.VENDORID,
            'companycode', av.COMPANYCODE,
            'companyname', av.COMPANYNAME,
            'corpname', av.CORPNAME,
            'gamename', av.GAMENAME,
            'isactive', av.ISACTIVE,
            'activity', av.activity_label,
            'cx', av.CX
        ),
        'orders', COALESCE(ao.orders, '[]'::jsonb)
    ))
FROM ActiveVendors av
LEFT JOIN AggregatedOrders ao ON ao.VENDORID = av.VENDORID;
"""

async def fetch_public_vendors(conn, search: str = None, corp: str = None, operator: str = None):
    p_search = f"%{search}%" if search else None
    p_corp = f"%{corp}%" if corp else None
    p_operator = f"%{operator}%" if operator else None
    
    result = await conn.fetchval(SQL_FETCH_VENDORS, p_search, p_corp, p_operator)
    return result or "[]"