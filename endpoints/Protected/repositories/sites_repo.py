# repositories/sites_repo.py

SQL_FETCH_SITES = """
WITH TargetUsers AS (
    SELECT u.username, u.accountid, u.userdataid
    FROM users u
    WHERE u.username = ANY($1::text[]) -- Filter by verified list
),
ReclaimableMat AS (
    SELECT 
        bbm.buildingid,
        jsonb_agg(
            jsonb_build_object(
                'MaterialId', m.materialid,
                'MaterialName', m.name,
                'MaterialTicker', m.ticker,
                'MaterialAmount', bbm.amount
            ) ORDER BY m.ticker
        ) as materials_json
    FROM building_build_materials bbm
    JOIN materials m ON bbm.materialid = m.materialid
    WHERE $4::boolean IS TRUE -- include_reclaimable
    GROUP BY bbm.buildingid
),
RepairMat AS (
    SELECT 
        pm.platformid,
        jsonb_agg(
            jsonb_build_object(
                'MaterialId', m.materialid,
                'MaterialName', m.name,
                'MaterialTicker', m.ticker,
                'MaterialAmount', pm.amount
            ) ORDER BY m.ticker
        ) as materials_json
    FROM platform_materials pm
    JOIN materials m ON pm.materialid = m.materialid
    WHERE $5::boolean IS TRUE -- include_repair
    GROUP BY pm.platformid
),
BuildingData AS (
    SELECT 
        sp.siteid,
        jsonb_agg(
            jsonb_build_object(
                'SiteBuildingId', sp.platformid,
                'BuildingId', sp.buildingid,
                'BuildingCreated', EXTRACT(EPOCH FROM sp.creationtime) * 1000,
                'BuildingName', b.name,
                'BuildingTicker', b.ticker,
                'BuildingLastRepair', EXTRACT(EPOCH FROM sp.lastrepair) * 1000,
                'Condition', sp.condition,
                
                'ReclaimableMaterials', CASE WHEN $4::boolean IS TRUE THEN COALESCE(rcm.materials_json, '[]'::jsonb) ELSE NULL END,
                'RepairMaterials', CASE WHEN $5::boolean IS TRUE THEN COALESCE(rpm.materials_json, '[]'::jsonb) ELSE NULL END
            ) ORDER BY sp.creationtime DESC
        ) as buildings_json
    FROM site_platforms sp
    JOIN buildings b ON sp.buildingid = b.buildingid
    LEFT JOIN ReclaimableMat rcm ON rcm.buildingid = sp.buildingid AND $4::boolean IS TRUE
    LEFT JOIN RepairMat rpm ON rpm.platformid = sp.platformid AND $5::boolean IS TRUE
    GROUP BY sp.siteid
),
FilteredSites AS (
    SELECT 
        s.siteid, s.foundedtimestamp, s.investedpermits, s.maximumpermits, s.xata_updatedat,
        tu.username,
        p.planetid, p.naturalid, p.name as planet_name,
        CASE WHEN $3::boolean IS TRUE THEN COALESCE(bd.buildings_json, '[]'::jsonb) ELSE NULL END as buildings_json
    FROM sites s
    JOIN TargetUsers tu ON tu.userdataid = s.userid
    LEFT JOIN planets p ON p.planetid = s.addressplanetid
    LEFT JOIN BuildingData bd ON bd.siteid = s.siteid AND $3::boolean IS TRUE
    WHERE 
        -- Location Filter
        ($2::text IS NULL OR (p.naturalid ILIKE $2 OR p.name ILIKE $2))
)
SELECT 
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'Username', sub.username,
                'Sites', sub.sites
            )
        ),
        '[]'::jsonb
    )
FROM (
    SELECT 
        username,
        jsonb_agg(
            jsonb_build_object(
                'SiteId', siteid,
                'PlanetId', planetid,
                'PlanetIdentifier', naturalid,
                'PlanetName', planet_name,
                'PlanetFoundedEpochMs', EXTRACT(EPOCH FROM foundedtimestamp) * 1000,
                'InvestedPermits', investedpermits,
                'MaximumPermits', maximumpermits,
                'UserNameSubmitted', username,
                'Timestamp', xata_updatedat,
                
                'Buildings', buildings_json
            )
        ) as sites
    FROM FilteredSites
    GROUP BY username
) sub;
"""

async def fetch_sites(
    conn,
    usernames_list: list,
    location: str = None,
    include_buildings: bool = False,
    include_reclaimable: bool = False,
    include_repair: bool = False,
):
    """
    Fetches sites for the provided list of users.
    Returns: [{ "Username": "x", "Sites": [...] }]
    """
    if not include_buildings:
        include_reclaimable = False
        include_repair = False

    params = [
        usernames_list,
        f"%{location}%" if location else None,
        include_buildings,
        include_reclaimable,
        include_repair,
    ]

    result = await conn.fetchval(SQL_FETCH_SITES, *params)
    return result or "[]"
