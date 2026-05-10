import logging

logger = logging.getLogger(__name__)

SQL_GET_MINIMAL = """
    SELECT naturalid AS "PlanetNaturalId", name AS "PlanetName"
    FROM planets
    ORDER BY naturalid ASC;
"""

# -----------------------------------------------------------------------------
# BASE CTEs (Shared Logic)
# -----------------------------------------------------------------------------
BASE_CTES = """
WITH latest_planets AS (
    SELECT DISTINCT ON (planetid) * FROM planets ORDER BY planetid, xata_updatedat DESC
),
latest_phys AS (
    SELECT DISTINCT ON (planetid) * FROM planet_physical_data ORDER BY planetid, xata_updatedat DESC
),
latest_orb AS (
    SELECT DISTINCT ON (planetid) * FROM planet_orbit ORDER BY planetid, xata_updatedat DESC
),
latest_res AS (
    SELECT DISTINCT ON (planetid, materialid) * FROM planet_resources ORDER BY planetid, materialid, xata_updatedat DESC
),
res_agg AS (
    SELECT planetid, jsonb_agg(jsonb_build_object('MaterialId', materialid, 'ResourceType', type, 'Factor', factor)) AS resources
    FROM latest_res GROUP BY planetid
),
latest_fees AS (
    SELECT DISTINCT ON (planetid, category, workforcelevel) * FROM planet_production_fees ORDER BY planetid, category, workforcelevel, xata_updatedat DESC
),
fees_agg AS (
    SELECT planetid, jsonb_agg(jsonb_build_object('Category', category, 'WorkforceLevel', workforcelevel, 'FeeAmount', feeamount, 'FeeCurrency', feecurrency)) AS production_fees
    FROM latest_fees GROUP BY planetid
),
latest_build_opts AS (
    SELECT DISTINCT ON (planetid, sitetype) * FROM planet_build_options ORDER BY planetid, sitetype, xata_updatedat DESC
),
build_opts_agg AS (
    SELECT planetid, jsonb_agg(jsonb_build_object('SiteType', sitetype, 'BillOfMaterial', CASE WHEN billofmaterial IS NOT NULL THEN billofmaterial::jsonb ELSE '[]'::jsonb END)) AS build_requirements
    FROM latest_build_opts GROUP BY planetid
),
planet_objects AS (
    SELECT p.naturalid, jsonb_build_object(
        'Resources', COALESCE(res.resources, '[]'::jsonb),
        'BuildRequirements', COALESCE(build_opts.build_requirements, '[]'::jsonb),
        'ProductionFees', COALESCE(fees.production_fees, '[]'::jsonb),
        'COGCPrograms', '[]'::jsonb,
        'COGCVotes', '[]'::jsonb,
        'COGCUpkeep', '[]'::jsonb,
        'PlanetId', p.planetid,
        'PlanetNaturalId', p.naturalid,
        'PlanetName', p.name,
        'Namer', p.namer,
        'NamingDataEpochMs', EXTRACT(EPOCH FROM p.namingdate) * 1000,
        'Nameable', p.nameable,
        'SystemId', p.systemid,
        'Gravity', phys.gravity,
        'MagneticField', phys.magneticfield,
        'Mass', COALESCE(phys.mass, p.mass),
        'MassEarth', phys.massearth,
        'Pressure', phys.pressure,
        'Radiation', phys.radiation,
        'Radius', phys.radius,
        'Sunlight', COALESCE(phys.sunlight, p.sunlight),
        'Surface', COALESCE(phys.surface, p.surface),
        'Temperature', COALESCE(phys.temperature, p.temperature),
        'Fertility', COALESCE(phys.fertility, p.fertility),
        'OrbitSemiMajorAxis', orb.semimajoraxis,
        'OrbitEccentricity', orb.eccentricity,
        'OrbitInclination', orb.inclination,
        'OrbitRightAscension', orb.rightascension,
        'OrbitPeriapsis', orb.periapsis,
        'OrbitIndex', orb.orbitindex,
        'HasLocalMarket', false, 
        'HasChamberOfCommerce', false,
        'HasWarehouse', false,
        'HasAdministrationCenter', CASE WHEN p.admincenterid IS NOT NULL THEN true ELSE false END,
        'HasShipyard', false,
        'FactionCode', p.countrycode,
        'FactionName', p.countryname,
        'GoverningEntity', p.admincenterid,
        'CurrencyName', NULL,
        'CurrencyCode', NULL,
        'BaseLocalMarketFee', 0,
        'LocalMarketFeeFactor', 0,
        'WarehouseFee', 0,
        'EstablishmentFee', 0,
        'PopulationId', p.populationid,
        'COGCProgramStatus', p.cogc,
        'PlanetTier', 0,
        'Plots', p.plots,
        'Timestamp', p.xata_updatedat
    ) AS p_json
    FROM latest_planets p
    LEFT JOIN latest_phys phys ON p.planetid = phys.planetid
    LEFT JOIN latest_orb orb ON p.planetid = orb.planetid
    LEFT JOIN res_agg res ON p.planetid = res.planetid
    LEFT JOIN fees_agg fees ON p.planetid = fees.planetid
    LEFT JOIN build_opts_agg build_opts ON p.planetid = build_opts.planetid
)
"""

# Returns ONE massive string containing the entire array `[{...}, {...}]`
SQL_GET_FULL_ALL = BASE_CTES + """
SELECT json_agg(p_json ORDER BY naturalid ASC)::text AS final_payload 
FROM planet_objects;
"""

# Returns ONE string containing a single object `{...}`
SQL_GET_FULL_SINGLE = BASE_CTES + """
SELECT p_json::text AS final_payload 
FROM planet_objects 
WHERE naturalid = $1::text;
"""

async def fetch_minimal_planets(db) -> list:
    async with db.pool.acquire() as con:
        return await con.fetch(SQL_GET_MINIMAL)

async def fetch_full_planets_all(db) -> str:
    async with db.pool.acquire() as con:
        # Increase timeout just for the database to crunch the data
        await con.execute("SET statement_timeout = '60s';")
        record = await con.fetchrow(SQL_GET_FULL_ALL)
        return record["final_payload"] if record and record["final_payload"] else "[]"

async def fetch_full_planet_single(db, ticker: str) -> str:
    async with db.pool.acquire() as con:
        record = await con.fetchrow(SQL_GET_FULL_SINGLE, ticker)
        return record["final_payload"] if record and record["final_payload"] else "{}"
