from typing import AsyncGenerator, Optional

# --- JSON FETCH (Grouped by User) ---
async def fetch_storages_as_json(db, usernames_list: list, location_filter: Optional[str] = None) -> str:
    async with db.pool.acquire() as conn:
        params = [usernames_list]
        loc_filter_clause = ""
        
        if location_filter:
            params.append(f"%{location_filter}%")
            loc_filter_clause = f"""
                AND (
                    pl_site.naturalid ILIKE ${len(params)} 
                    OR pl_site.name ILIKE ${len(params)} 
    
                    OR pl_w.naturalid ILIKE ${len(params)} 
                    OR pl_w.name ILIKE ${len(params)} 
    
                    OR stn.name ILIKE ${len(params)} 
                    OR w.warehouseid ILIKE ${len(params)} 
    
                    OR sh.name ILIKE ${len(params)} 
                    OR sh.registration ILIKE ${len(params)}
                )
            """

        query = f"""
            WITH valid_storages AS (
                SELECT DISTINCT ON (s.storageid)
                    s.storageid, tu.username, 
                    COALESCE(stn.name, pl_site.name, pl_site.naturalid, pl_w.name, pl_w.naturalid, w.warehouseid, sh.registration, sh.name, 'Unknown') as location,
                    s.type, s.volumecapacity, s.volumeload, s.weightcapacity, s.weightload, s.name, s.fixed, s.addressableid,
                    EXTRACT(EPOCH FROM s.xata_updatedat) * 1000 as timestamp_ms
                FROM storages s
                JOIN users tu ON tu.userdataid = s.userid
                LEFT JOIN sites site ON s.addressableid = site.siteid AND s.type = 'STORE'
                LEFT JOIN planets pl_site ON site.addressplanetid = pl_site.planetid
                LEFT JOIN warehouses w ON s.storageid = w.storeid AND s.type = 'WAREHOUSE_STORE'
                LEFT JOIN stations stn ON stn.warehouseid = w.warehouseid
                LEFT JOIN planets pl_w ON pl_w.planetid = w.addressplanet
                LEFT JOIN ships sh ON (s.storageid = sh.idshipstore OR s.storageid = sh.idstlfuelstore OR s.storageid = sh.idftlfuelstore OR s.addressableid = sh.shipid) AND s.type NOT IN ('STORE', 'WAREHOUSE_STORE')
                WHERE tu.username = ANY($1::text[])
                {loc_filter_clause}
                ORDER BY s.storageid, s.xata_updatedat DESC
            )
            SELECT COALESCE(jsonb_agg(jsonb_build_object('Username', username, 'Storages', storages_json)), '[]'::jsonb)
            FROM (
                SELECT username, jsonb_agg(jsonb_build_object(
                    'StorageId', storageid, 'AddressableId', addressableid, 'Location', location, 'Type', type,
                    'VolumeCapacity', volumecapacity, 'VolumeLoad', volumeload, 'WeightCapacity', weightcapacity, 'WeightLoad', weightload,
                    'Name', name, 'FixedStore', fixed, 'LastUpdatedEpochMs', timestamp_ms,
                    'StorageItems', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'MaterialId', si.materialid, 'MaterialName', m.name, 'MaterialTicker', m.ticker, 
                        'MaterialAmount', si.quantity, 'TotalWeight', si.totalweight, 'TotalVolume', si.totalvolume
                    )), '[]'::jsonb) FROM storage_items si JOIN materials m ON si.materialid = m.materialid WHERE si.storageid = vs.storageid)
                )) as storages_json
                FROM valid_storages vs GROUP BY username
            ) usd;
        """
        return await conn.fetchval(query, *params) or "[]"


# --- CSV STREAM (Flattened & Multi-User) ---
async def stream_storages_csv(db, usernames_list: list, location_filter: Optional[str] = None) -> AsyncGenerator[list, None]:
    async with db.pool.acquire() as conn:

        params = [usernames_list]
        loc_filter_clause = ""
        
        if location_filter:
            params.append(f"%{location_filter}%")
            loc_filter_clause = f"""
                AND (
                    pl_site.naturalid ILIKE ${len(params)} OR 
                    pl_site.name ILIKE ${len(params)} OR 

                    pl_w.naturalid ILIKE ${len(params)} OR 
                    pl_w.name ILIKE ${len(params)} OR 

                    stn.name ILIKE ${len(params)} OR 
                    w.warehouseid ILIKE ${len(params)} OR
                    
                    sh.name ILIKE ${len(params)} OR
                    sh.registration ILIKE ${len(params)}
                )
            """

        query = f"""
            SELECT tu.username, 
                   COALESCE(stn.name, pl_site.naturalid, pl_w.naturalid, w.warehouseid, sh.registration, s.addressableid) as location,
                   s.type, to_char(s.xata_updatedat, 'YYYY-MM-DD"T"HH24:MI:SS') as updated_at,
                   m.ticker, si.quantity::text as amount
            FROM storages s
            JOIN users tu ON tu.userdataid = s.userid
            INNER JOIN storage_items si ON s.storageid = si.storageid
            INNER JOIN materials m ON si.materialid = m.materialid
            WHERE tu.username = ANY($1::text[]) 
            AND s.type IN ('STORE', 'WAREHOUSE_STORE', 'SHIP_STORE', 'SHIP_STL_FUEL_STORE', 'SHIP_FTL_FUEL_STORE')
            {loc_filter_clause}
            ORDER BY tu.username, location, m.ticker
        """
        async for record in conn.cursor(query, *params, prefetch=2000):
            yield list(record.values())