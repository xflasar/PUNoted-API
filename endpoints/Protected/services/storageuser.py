from typing import AsyncGenerator, Optional


# --- JSON FETCH (Grouped by User) ---
async def fetch_storages_as_json(db, usernames_list: list, location_filter: Optional[str] = None) -> str:
    async with db.pool.acquire() as conn:

        # 1. Base Params
        params = [usernames_list]

        # 2. Location Filter Logic
        loc_filter_clause = ""
        if location_filter:
            params.append(f"%{location_filter}%")
            # Checks: Base Planet, Warehouse Planet, Station Name, Warehouse ID, Ship Name, Ship Registration
            loc_filter_clause = f"""
                AND (
                    p.naturalid ILIKE ${len(params)} OR 
                    p.name ILIKE ${len(params)} OR 
                    
                    p_via_w.naturalid ILIKE ${len(params)} OR 
                    p_via_w.name ILIKE ${len(params)} OR 
                    
                    stn.name ILIKE ${len(params)} OR 
                    w.warehouseid ILIKE ${len(params)} OR

                    sh.name ILIKE ${len(params)} OR
                    sh.registration ILIKE ${len(params)}
                )
            """

        query = f"""
            WITH TargetUsers AS (
                SELECT u.username, u.userdataid
                FROM users u
                WHERE u.username = ANY($1::text[])
            ),
            valid_storages AS (
                SELECT 
                    s.storageid,
                    tu.username, 
                    
                    -- 1. LOCATION PRIORITY
                    COALESCE(
                        stn.name, 
                        p.name, 
                        p.naturalid, 
                        p_via_w.naturalid, 
                        w.warehouseid, 
                        sh.registration,
                        sh.name,
                        s.addressableid
                    ) as location,

                    -- 2. TYPE DETERMINATION
                    s.type,
                    s.volumecapacity,
                    s.volumeload,
                    s.weightcapacity,
                    s.weightload,
                    s.name,
                    s.fixed,
                    s.addressableid,
                    
                    EXTRACT(EPOCH FROM s.xata_updatedat) * 1000 as timestamp_ms

                FROM storages s
                JOIN TargetUsers tu ON tu.userdataid = s.userid
                
                -- Path 1: Direct Site (Bases)
                LEFT JOIN sites st ON s.addressableid = st.siteid
                LEFT JOIN planets p ON st.addressplanetid = p.planetid
                
                -- Path 2 & 3: Warehouse (General)
                LEFT JOIN warehouses w ON s.addressableid = w.warehouseid
                LEFT JOIN stations stn ON stn.warehouseid = w.warehouseid
                
                -- Path 3: Warehouse -> Site -> Planet (Warehouses on Planets)
                LEFT JOIN warehouses w_site_p ON w_site_p.warehouseid = s.addressableid
                LEFT JOIN planets p_via_w ON p_via_w.planetid = w_site_p.addressplanet

                -- Path 4: Ships (Cargo, STL Fuel, FTL Fuel)
                LEFT JOIN ships sh ON 
                    s.storageid = sh.idshipstore OR 
                    s.storageid = sh.idstlfuelstore OR 
                    s.storageid = sh.idftlfuelstore OR
                    s.addressableid = sh.shipid

                WHERE (
                      stn.name IS NOT NULL OR 
                      p.naturalid IS NOT NULL OR 
                      p_via_w.naturalid IS NOT NULL OR 
                      w.warehouseid IS NOT NULL OR
                      sh.registration IS NOT NULL
                  )
                  {loc_filter_clause}
            ),
            UserStorageData AS (
                SELECT 
                    vs.username,
                    jsonb_agg(
                        jsonb_build_object(
                            'StorageId', vs.storageid,
                            'AddressableId', vs.addressableid,
                            'Location', vs.location,
                            'Type', vs.type,
                            'VolumeCapacity', vs.volumecapacity,
                            'VolumeLoad', vs.volumeload,
                            'WeightCapacity', vs.weightcapacity,
                            'WeightLoad', vs.weightload,
                            'Name', vs.name,
                            'FixedStore', vs.fixed,
                            'LastUpdatedEpochMs', vs.timestamp_ms,
                            'StorageItems', (
                                SELECT COALESCE(jsonb_agg(
                                    jsonb_build_object(
                                        'MaterialId', si.materialid,
                                        'MaterialName', m.name,
                                        'MaterialCategory', m.category,
                                        'MaterialTicker', m.ticker,
                                        'MaterialVolume', m.volume,
                                        'MaterialWeight', m.weight,
                                        'MaterialAmount', si.quantity,
                                        'MaterialValue', si.currencyamount,
                                        'MaterialCurrency', si.currencytype,
                                        'Type', si.type,
                                        'TotalWeight', si.totalweight,
                                        'TotalVolume', si.totalvolume
                                    ) ORDER BY m.ticker
                                ), '[]'::jsonb)
                                FROM storage_items si
                                JOIN materials m ON si.materialid = m.materialid
                                WHERE si.storageid = vs.storageid
                            )
                        )
                    ) as storages_json
                FROM valid_storages vs
                GROUP BY vs.username
            )
            SELECT 
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'Username', usd.username,
                            'Storages', usd.storages_json
                        )
                    ),
                    '[]'::jsonb
                )
            FROM UserStorageData usd;
        """

        json_str = await conn.fetchval(query, *params)
        return json_str or "[]"


# --- CSV STREAM (Flattened & Multi-User) ---
async def stream_storages_csv(db, usernames_list: list, location_filter: Optional[str] = None) -> AsyncGenerator[list, None]:
    async with db.pool.acquire() as conn:

        params = [usernames_list]

        loc_filter_clause = ""
        if location_filter:
            params.append(f"%{location_filter}%")
            loc_filter_clause = f"""
                AND (
                    p.naturalid ILIKE ${len(params)} OR 
                    p.name ILIKE ${len(params)} OR 

                    p_via_w.naturalid ILIKE ${len(params)} OR 
                    p_via_w.name ILIKE ${len(params)} OR 

                    stn.name ILIKE ${len(params)} OR 
                    w.warehouseid ILIKE ${len(params)} OR
                    
                    sh.name ILIKE ${len(params)} OR
                    sh.registration ILIKE ${len(params)}
                )
            """

        query = f"""
            WITH TargetUsers AS (
                SELECT u.username, u.userdataid
                FROM users u
                WHERE u.username = ANY($1::text[])
            )
            SELECT 
                tu.username,
                COALESCE(stn.name, p.naturalid, p_via_w.naturalid, w.warehouseid, sh.registration, s.addressableid) as location,
                CASE 
                    WHEN stn.name IS NOT NULL THEN 'STATION'
                    WHEN p.naturalid IS NOT NULL THEN 'BASE'
                    WHEN p_via_w.naturalid IS NOT NULL THEN 'WAREHOUSE'
                    WHEN sh.registration IS NOT NULL THEN s.type -- Outputs actual ship store type (e.g. SHIP_STORE, SHIP_STL_FUEL_STORE)
                    ELSE 'UNKNOWN'
                END as type,
                to_char(s.xata_updatedat, 'YYYY-MM-DD"T"HH24:MI:SS') as updated_at,
                m.ticker,
                m.name as material_name,
                m.category as category,
                si.quantity::text as amount,
                (si.quantity * m.weight)::text as total_weight,
                (si.quantity * m.volume)::text as total_volume
            FROM storages s
            JOIN TargetUsers tu ON tu.userdataid = s.userid
            INNER JOIN storage_items si ON s.storageid = si.storageid
            INNER JOIN materials m ON si.materialid = m.materialid
            
            -- Path 1: Direct Site
            LEFT JOIN sites st ON s.addressableid = st.siteid
            LEFT JOIN planets p ON st.addressplanetid = p.planetid
            
            -- Path 2 & 3: Warehouse
            LEFT JOIN warehouses w ON s.addressableid = w.warehouseid
            LEFT JOIN stations stn ON stn.warehouseid = w.warehouseid
            
            -- Path 3: Warehouse -> Site -> Planet
            LEFT JOIN warehouses w_site_p ON w_site_p.warehouseid = s.addressableid
            LEFT JOIN planets p_via_w ON p_via_w.planetid = w_site_p.addressplanet

            -- Path 4: Ships
            LEFT JOIN ships sh ON 
                s.storageid = sh.idshipstore OR 
                s.storageid = sh.idstlfuelstore OR 
                s.storageid = sh.idftlfuelstore OR
                s.addressableid = sh.shipid

            WHERE (
                stn.name IS NOT NULL OR 
                p.naturalid IS NOT NULL OR 
                p_via_w.naturalid IS NOT NULL OR 
                w.warehouseid IS NOT NULL OR
                sh.registration IS NOT NULL
            )
              {loc_filter_clause}
            ORDER BY tu.username, location, m.ticker
        """

        async with conn.transaction():
            async for record in conn.cursor(query, *params, prefetch=2000):
                yield list(record.values())
