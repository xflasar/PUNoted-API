CREATE TEMP TABLE users (
    accountid uuid PRIMARY KEY,
    username text NOT NULL,
    userdataid text NOT NULL,
    xata_id text,
    is_synchronized boolean DEFAULT true,
    xata_createdat timestamptz DEFAULT now(),
    xata_updatedat timestamptz DEFAULT now()
) ON COMMIT DROP;

CREATE TEMP TABLE users_data (
    userid text PRIMARY KEY,
    displayname text,
    subscriptionlevel text,
    subscriptionexpiry timestamptz,
    corporationid text,
    xata_createdat timestamptz DEFAULT now(),
    xata_updatedat timestamptz DEFAULT now()
) ON COMMIT DROP;

CREATE TEMP TABLE user_api_tokens (
    user_id text NOT NULL,
    group_id text,
    token_hash text NOT NULL
) ON COMMIT DROP;

CREATE TABLE IF NOT EXISTS user_global_settings (
    userid TEXT NOT NULL,
    internal_leased_sites JSONB DEFAULT '[]'::jsonb,
    internal_excluded_sites JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (userid)
);

CREATE TEMP TABLE data_group_members (
    group_id text NOT NULL,
    user_id text NOT NULL,
    status text,
    personal_suffix text,
    can_read_data boolean,
    granted_permissions jsonb
) ON COMMIT DROP;

CREATE TEMP TABLE company_data (
    userdataid text NOT NULL,
    companyid text NOT NULL,
    companycode text NOT NULL,
    companyname text NOT NULL,
    xata_updatedat timestamptz NOT NULL
) ON COMMIT DROP;

CREATE TABLE IF NOT EXISTS public_users_data (
    id TEXT PRIMARY KEY,
    username TEXT,
    company_id TEXT,
    company_name TEXT,
    company_code TEXT,
    subscription_level TEXT,
    highest_tier TEXT,
    pioneer BOOLEAN DEFAULT false,
    moderator BOOLEAN DEFAULT false,
    team BOOLEAN DEFAULT false,
    translator BOOLEAN DEFAULT false,
    active_days_per_week INTEGER DEFAULT 0,
    created_timestamp BIGINT DEFAULT 0,
    gifts JSONB DEFAULT '{}'::jsonb
);

CREATE TEMP TABLE corporation_shareholders (
    companyid text NOT NULL,
    corporationid text NOT NULL,
    userid text,
    companycode text,
    companyname text
) ON COMMIT DROP;

CREATE TEMP TABLE corporations (
    id text PRIMARY KEY,
    name text NOT NULL,
    code text NOT NULL,
    founder text,
    officers text[]
) ON COMMIT DROP;

CREATE TEMP TABLE corporation_subsidiaries (
    id uuid DEFAULT gen_random_uuid(),
    corporationmainid text NOT NULL,
    corporationsubid text NOT NULL,
    linkedat timestamptz DEFAULT now()
) ON COMMIT DROP;

CREATE TEMP TABLE material_prices (
    ticker text PRIMARY KEY,
    price double precision NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE planets (
    planetid text PRIMARY KEY,
    naturalid text,
    name text,
    admincenterid text,
    xata_updatedat timestamptz DEFAULT now()
) ON COMMIT DROP;

CREATE TEMP TABLE planet_government_terms (
    termid text PRIMARY KEY,
    admincenterid text NOT NULL,
    planet_natural_id integer,
    election_start timestamptz,
    election_end timestamptz,
    term_start timestamptz,
    term_end timestamptz,
    parliament_size integer,
    election_ongoing boolean DEFAULT false
) ON COMMIT DROP;

CREATE TEMP TABLE planet_government_candidates (
    id text PRIMARY KEY,
    termid text NOT NULL,
    userid text,
    username text,
    corporation_name text,
    corporation_code text,
    country_code text,
    votes integer,
    votes_percentage double precision,
    is_winner boolean DEFAULT false,
    start_of_run timestamptz
) ON COMMIT DROP;

CREATE TEMP TABLE planet_motions (
    motionid text PRIMARY KEY,
    admincenterid text,
    naturalid text,
    name text,
    creator_id text,
    creator_username text,
    created_at timestamptz,
    status text,
    voting_start timestamptz,
    voting_end timestamptz
) ON COMMIT DROP;

CREATE TEMP TABLE planet_motion_votes (
    id text PRIMARY KEY,
    motionid text NOT NULL,
    voter_id text,
    voter_username text,
    role text,
    status text,
    voted_at timestamptz
) ON COMMIT DROP;

CREATE TEMP TABLE planet_motion_components (
    componentid text PRIMARY KEY,
    motionid text NOT NULL,
    type text,
    contributor_id text,
    contributor_username text,
    recipient_id text,
    recipient_username text,
    amount double precision,
    currency text,
    program text,
    category text
) ON COMMIT DROP;

CREATE TABLE IF NOT EXISTS material_categories (
    id text PRIMARY KEY,
    name text
);

CREATE TABLE IF NOT EXISTS materials (
    category TEXT,
    materialid TEXT NOT NULL,
    name TEXT,
    resource BOOLEAN,
    ticker TEXT,
    volume DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    PRIMARY KEY (materialid)
);

CREATE TABLE IF NOT EXISTS stations (
    comexid TEXT,
    name TEXT,
    naturalid TEXT,
    stationid TEXT NOT NULL,
    warehouseid TEXT,
    systemid TEXT,
    PRIMARY KEY (stationid)
);

CREATE TABLE IF NOT EXISTS sites (
    addressplanetid TEXT,
    addresssystemid TEXT,
    area INTEGER DEFAULT 100,
    investedpermits INTEGER DEFAULT 1,
    maximumpermits INTEGER DEFAULT 5,
    foundedtimestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    siteid TEXT NOT NULL,
    userid TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (siteid)
);

CREATE TABLE IF NOT EXISTS site_platforms (
    area integer,
    buildingid text,
    condition double precision,
    platformid text PRIMARY KEY,
    siteid text,
    creationtime timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    lastrepair timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS buildings (
    area integer DEFAULT 10,
    buildingid text PRIMARY KEY,
    ticker text,
    type text,
    name text,
    expertisecategory text DEFAULT 'CHEMISTRY',
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_id text DEFAULT 'b1'
);

INSERT INTO buildings (buildingid, ticker, type, name, expertisecategory, area)
VALUES ('b1', 'RIG', 'PRODUCTION', 'Extraction Rig', 'CHEMISTRY', 10);

INSERT INTO site_platforms (platformid, siteid, buildingid, condition, area, creationtime, lastrepair)
VALUES ('plat1', 'site1', 'b1', 1.0, 10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS building_build_materials (
    amount integer,
    buildingid text,
    materialid text
);

INSERT INTO building_build_materials (amount, buildingid, materialid)
VALUES (10, 'b1', 'm1');

CREATE TABLE IF NOT EXISTS platform_materials (
    amount integer,
    materialid text,
    materialtype text,
    platformid text
);

INSERT INTO platform_materials (amount, materialid, materialtype, platformid)
VALUES (5, 'm1', 'repair', 'plat1');

CREATE TABLE IF NOT EXISTS building_workforce_capacities (
    buildingid text,
    capacity integer,
    workforcelevel text,
    ishabitation boolean DEFAULT false
);

INSERT INTO building_workforce_capacities (buildingid, capacity, workforcelevel, ishabitation)
VALUES ('b1', 50, 'PIONEER', false);

CREATE TABLE IF NOT EXISTS warehouses (
    addressplanet TEXT,
    storeid TEXT NOT NULL,
    units INTEGER,
    userid TEXT,
    warehouseid TEXT NOT NULL,
    PRIMARY KEY (warehouseid, storeid)
);

CREATE TABLE IF NOT EXISTS storages (
    addressableid TEXT,
    fixed BOOLEAN,
    name TEXT,
    storageid TEXT NOT NULL,
    type TEXT,
    userid TEXT NOT NULL,
    volumecapacity INTEGER,
    volumeload DOUBLE PRECISION,
    weightcapacity INTEGER,
    weightload DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (storageid)
);

CREATE TABLE IF NOT EXISTS storage_items (
    compositekey TEXT NOT NULL,
    currencyamount DOUBLE PRECISION,
    currencytype TEXT,
    materialid TEXT,
    quantity INTEGER,
    storageid TEXT,
    totalvolume DOUBLE PRECISION,
    totalweight DOUBLE PRECISION,
    type TEXT,
    PRIMARY KEY (compositekey)
);

CREATE TABLE IF NOT EXISTS ships (
    shipid TEXT PRIMARY KEY,
    idshipstore TEXT,
    idstlfuelstore TEXT,
    idftlfuelstore TEXT,
    name TEXT,
    registration TEXT,
    flightid TEXT,
    commissioningtime TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    condition DOUBLE PRECISION DEFAULT 1.0,
    stlfuelflowrate DOUBLE PRECISION DEFAULT 1.0,
    addresssystemid TEXT,
    addressplanetid TEXT,
    addressstationid TEXT,
    userid TEXT
);

INSERT INTO ships (shipid, name, registration, userid, addresssystemid, addressplanetid)
VALUES ('s1', 'Ship 1', 'REG-1', 'userid1', 'sys1', 'p1');

INSERT INTO users (accountid, username, userdataid, xata_id)
VALUES ('aaaaaaaa-aa04-aaaa-aaaa-aaaaaaaaaaaa', 'testuser', 'userid1', 'fakexataid');

INSERT INTO users_data (userid, displayname, corporationid)
VALUES ('userid1', 'testuser', 'corp1');

INSERT INTO user_api_tokens (user_id, group_id, token_hash)
VALUES ('aaaaaaaa-aa04-aaaa-aaaa-aaaaaaaaaaaa', NULL, 'ptk_fake');

INSERT INTO company_data (userdataid, companyid, companycode, companyname, xata_updatedat)
VALUES ('userid1', 'company1', 'FAKE', 'fake co', TIMESTAMPTZ '2000-01-01 00:00:00+00');

INSERT INTO public_users_data (id, username, company_id, company_name, company_code)
VALUES ('userid1', 'testuser', 'company1', 'fake co', 'FAKE');

INSERT INTO corporations (id, name, code, founder, officers)
VALUES ('corp1', 'fake corp', 'FC', 'aaaaaaaa-aa04-aaaa-aaaa-aaaaaaaaaaaa', ARRAY[]::text[]), ('COSM', 'cosm corp', 'COSM', 'aaaaaaaa-aa04-aaaa-aaaa-aaaaaaaaaaaa', ARRAY[]::text[]);

INSERT INTO corporation_shareholders (companyid, corporationid, userid, companycode, companyname)
VALUES ('company1', 'corp1', 'userid1', 'FAKE', 'fake co');

INSERT INTO material_prices (ticker, price)
VALUES ('SF', 123.45);

INSERT INTO planets (planetid, naturalid, name)
VALUES ('p1', 'HRT', 'Hortus');

INSERT INTO sites (siteid, addressplanetid, userid)
VALUES ('site1', 'p1', 'userid1');

INSERT INTO material_categories (id, name)
VALUES ('cat_water', 'Water');

INSERT INTO materials (materialid, name, ticker, weight, volume, category)
VALUES ('m1', 'Rations', 'RAT', 1.0, 1.0, 'cat_water'), ('m_h2o', 'Water', 'H2O', 1.0, 1.0, 'cat_water');

INSERT INTO storages (
    storageid, addressableid, userid, type, name, fixed,
    volumecapacity, volumeload, weightcapacity, weightload, xata_updatedat
) VALUES (
    'st1', 'site1', 'userid1', 'STORE', 'Hortus Storage', true,
    1000, 10.0, 1000, 10.0, TIMESTAMPTZ '2000-01-01 00:00:00+00'
);

INSERT INTO storage_items (
    compositekey, storageid, materialid, quantity, totalweight, totalvolume
) VALUES ('st1-m1', 'st1', 'm1', 10, 10.0, 10.0);

CREATE TABLE IF NOT EXISTS user_currency_accounts (
    balanceamount double precision,
    balancecurrencycode text,
    bookbalanceamount double precision,
    bookbalancecurrencycode text,
    category text,
    number integer,
    type integer,
    userid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0
);

INSERT INTO user_currency_accounts (balanceamount, balancecurrencycode, bookbalanceamount, bookbalancecurrencycode, category, number, type, userid, xata_updatedat)
VALUES (100.0, 'ICA', 100.0, 'ICA', 'ICA', 123, 1, 'userid1', TIMESTAMPTZ '2000-01-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS contracts (
    id text PRIMARY KEY,
    localid text,
    date timestamp with time zone,
    party text,
    partnerid text,
    partnername text,
    partnercode text,
    status text,
    duedate timestamp with time zone,
    name text,
    preamble text,
    extensiondeadline timestamp with time zone,
    relatedcontracts text[],
    contracttype text,
    userid text,
    terminationreceived boolean DEFAULT false,
    terminationsent boolean DEFAULT false,
    agentcontract boolean DEFAULT false,
    canextend boolean DEFAULT false,
    canrequesttermination boolean DEFAULT false,
    updated_at timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contract_conditions (
    id text PRIMARY KEY,
    contractid text,
    deadline timestamp with time zone,
    deadlineduration_millis bigint,
    amountmoney numeric,
    currencymoney text,
    dependencies text[],
    addresssystemid text,
    addressplanetid text,
    addressstationid text,
    destinationsystemid text,
    destinationplanetid text,
    destinationstationid text,
    index integer,
    type text,
    party text,
    status text,
    autoprovisionstoreid text,
    reputationchange numeric,
    blockid text,
    shipmentitemid text,
    contractparty text
);

CREATE TABLE IF NOT EXISTS systems (
    systemid text PRIMARY KEY,
    name text,
    naturalid text
);

INSERT INTO systems (systemid, name, naturalid)
VALUES ('sys1', 'System 1', 'S1');

CREATE TABLE IF NOT EXISTS contract_materials (
    contractconditionid text,
    materialid text,
    amount numeric,
    pickedupamount numeric DEFAULT 0,
    contractparty text
);

CREATE TABLE IF NOT EXISTS contract_loan_installments (
    conditionid text,
    interestamount numeric,
    repaymentamount numeric,
    totalamount numeric,
    currency text,
    contractparty text
);

INSERT INTO contracts (id, localid, date, party, partnerid, partnername, partnercode, status, duedate, name, preamble, userid, canextend, canrequesttermination, terminationsent, terminationreceived)
VALUES ('c1', 'l1', TIMESTAMPTZ '2000-01-01 00:00:00+00', 'CUSTOMER', 'p1', 'Partner', 'PTN', 'FULFILLED', NULL, 'Contract 1', '', 'userid1', false, false, false, false);

CREATE TABLE IF NOT EXISTS comex_trade_orders (
    amount integer,
    brokerid text,
    created timestamp without time zone,
    exchangeid text,
    initialamount integer,
    limitamount numeric,
    limitcurrency text,
    materialid text,
    orderid text,
    status text,
    type text,
    userid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0
);

INSERT INTO comex_trade_orders (amount, brokerid, created, exchangeid, initialamount, limitamount, limitcurrency, materialid, orderid, status, type, userid)
VALUES (10, 'broker1', CURRENT_TIMESTAMP - interval '1 day', 'ex1', 10, 100.0, 'ICA', 'm1', 'o1', 'FILLED', 'BUY', 'userid1');

CREATE TABLE IF NOT EXISTS comex_trade_orders_trades (
    amount integer,
    orderid text,
    partnercode text,
    partnerid text,
    partnername text,
    priceamount numeric,
    pricecurrency text,
    tradeid text,
    tradetime timestamp without time zone,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0
);

INSERT INTO comex_trade_orders_trades (amount, orderid, tradeid, tradetime, priceamount, pricecurrency)
VALUES (10, 'o1', 't1', CURRENT_TIMESTAMP - interval '1 day', 100.0, 'ICA');

CREATE TABLE IF NOT EXISTS ship_flights (
    aborted boolean DEFAULT false,
    arrivaltimestamp timestamp without time zone,
    departuretimestamp timestamp without time zone,
    destinationplanetid text,
    destinationstationid text,
    destinationsystemid text,
    ftldistance double precision DEFAULT 0,
    ftltotalconsumption integer DEFAULT 0,
    id text PRIMARY KEY,
    originplanetid text,
    originstationid text,
    originsystemid text,
    shipid text,
    stldistance double precision DEFAULT 10,
    stltotalconsumption integer DEFAULT 1,
    damage double precision DEFAULT 0,
    currentsegmentindex integer DEFAULT 0,
    userid text
);

CREATE TABLE IF NOT EXISTS ship_flight_segments (
    segment_id bigint PRIMARY KEY,
    flight_id text,
    segment_index integer,
    segment_type text,
    departure bigint,
    arrival bigint,
    duration bigint,
    origin_system_id text,
    origin_location_id text,
    origin_location_type text,
    origin_orbit_data jsonb,
    destination_system_id text,
    destination_location_id text,
    destination_location_type text,
    destination_orbit_data jsonb,
    stl_distance double precision,
    stl_fuel integer,
    ftl_distance double precision,
    ftl_fuel integer,
    damage double precision,
    transferellipse jsonb
);

INSERT INTO ship_flights (id, shipid, departuretimestamp, arrivaltimestamp, currentsegmentindex, aborted, stldistance, ftldistance, userid, originsystemid, destinationsystemid)
VALUES ('f1', 's1', TIMESTAMPTZ '2000-01-01 00:00:00', TIMESTAMPTZ '2000-01-01 01:00:00', 0, false, 10.0, 0.0, 'userid1', 'sys1', 'sys1');

INSERT INTO ship_flight_segments (segment_id, flight_id, segment_index, segment_type, departure, arrival, duration, origin_system_id, origin_location_id, origin_location_type, origin_orbit_data, destination_system_id, destination_location_id, destination_location_type, destination_orbit_data, stl_distance, stl_fuel, ftl_distance, ftl_fuel, damage, transferellipse)
VALUES (1, 'f1', 0, 'STL', 946684800000, 946688400000, 3600, 'sys1', 'p1', 'PLANET', '{}'::jsonb, 'sys1', 'p1', 'PLANET', '{}'::jsonb, 10.0, 1, 0.0, 0, 0.0, '{}'::jsonb);

CREATE TABLE IF NOT EXISTS workforces (
    capacity integer,
    population integer,
    required integer,
    reserve integer,
    satisfaction double precision,
    siteid text,
    level text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0,
    userid text,
    workforceid text
);

INSERT INTO workforces (capacity, population, required, reserve, satisfaction, siteid, level, userid, workforceid)
VALUES (150, 100, 50, 10, 1.0, 'site1', 'PIONEER', 'userid1', 'site1-PIONEER');

CREATE TABLE IF NOT EXISTS workforce_needs (
    category text,
    essential boolean,
    materialid text,
    satisfaction double precision,
    unitsper100 double precision,
    unitsperinterval double precision,
    workforceid text,
    workforceneedid text
);

INSERT INTO workforce_needs (category, essential, materialid, satisfaction, unitsper100, unitsperinterval, workforceid, workforceneedid)
VALUES ('Food', true, 'm1', 1.0, 1.0, 1.0, 'site1-PIONEER', 'site1-PIONEER-Food');

CREATE TABLE IF NOT EXISTS site_production_lines (
    capacity integer,
    condition double precision,
    efficiency double precision,
    productionlineid text PRIMARY KEY,
    siteid text,
    slots integer,
    type text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0
);

CREATE TABLE IF NOT EXISTS production_recipes (
    duration bigint,
    efficiency double precision,
    effortfactor double precision,
    experience integer,
    name text,
    productionfee double precision,
    productionfeecurrency text,
    productiontemplateid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0,
    materialid text,
    productionlineid text
);

CREATE TABLE IF NOT EXISTS production_recipe_input_factors (
    factor double precision,
    id integer,
    materialid text,
    productiontemplateid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0,
    productionlineid text
);

CREATE TABLE IF NOT EXISTS production_recipe_output_factors (
    factor double precision,
    id integer,
    materialid text,
    productiontemplateid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0,
    productionlineid text
);

INSERT INTO site_production_lines (capacity, condition, efficiency, productionlineid, siteid, slots, type)
VALUES (1, 1.0, 1.0, 'line1', 'site1', 1, 'RIG');

INSERT INTO production_recipes (duration, efficiency, effortfactor, experience, name, productionfee, productionfeecurrency, productiontemplateid, productionlineid)
VALUES (3600000, 1.0, 1.0, 1, 'Recipe 1', 10.0, 'ICA', 'r1', 'line1');

INSERT INTO production_recipe_input_factors (factor, id, materialid, productiontemplateid, productionlineid)
VALUES (1.0, 1, 'm1', 'r1', 'line1');

INSERT INTO production_recipe_output_factors (factor, id, materialid, productiontemplateid, productionlineid)
VALUES (1.0, 1, 'm1', 'r1', 'line1');

CREATE TABLE IF NOT EXISTS site_production_line_orders (
    completed boolean,
    completion timestamp without time zone,
    created timestamp without time zone,
    duration bigint,
    halted boolean,
    lastupdated timestamp without time zone,
    orderid text,
    productionfeeamount double precision,
    productionfeecurrency text,
    productionlineid text,
    recipeid text,
    recurring boolean,
    started timestamp without time zone,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0
);

INSERT INTO site_production_line_orders (
    orderid, productionlineid, recipeid, created, completion, duration, halted, recurring, completed, started
) VALUES (
    'o1', 'line1', 'r1', CURRENT_TIMESTAMP - interval '1 hour', CURRENT_TIMESTAMP + interval '1 day', 3600000, false, true, false, CURRENT_TIMESTAMP - interval '1 hour'
);

CREATE TABLE IF NOT EXISTS user_vendors (
    companycode text,
    companyname text,
    corpname text,
    cx text,
    gamename text,
    isactive boolean DEFAULT true,
    userid text,
    vendorid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_vendor_orders (
    fixedprice double precision,
    materialid text,
    materialticker text,
    maxprice double precision,
    minprice double precision,
    orderid text,
    ordertype text,
    pricetype text,
    quantity integer,
    reserved integer DEFAULT 0,
    vendorid text,
    xata_createdat timestamp with time zone DEFAULT now(),
    xata_id text,
    xata_updatedat timestamp with time zone DEFAULT now(),
    xata_version integer DEFAULT 0,
    location jsonb
);

INSERT INTO user_vendors (companycode, companyname, corpname, cx, gamename, isactive, userid, vendorid)
VALUES ('FAKE', 'fake co', 'fake corp', 'IC1', 'testuser', true, 'userid1', 'v1');

CREATE TABLE IF NOT EXISTS ship_build_presets (
    id serial PRIMARY KEY,
    corporation_id text,
    name text,
    price numeric,
    price_corp numeric,
    parts jsonb DEFAULT '[]'::jsonb,
    is_admin_preset boolean DEFAULT false,
    created_by text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);

INSERT INTO ship_build_presets (id, corporation_id, name, price, price_corp, parts, is_admin_preset, created_by)
VALUES (1, 'COSM', 'Preset1', 100.0, 90.0, '[]'::jsonb, true, 'user1');

CREATE TABLE IF NOT EXISTS corp_ship_orders (
    id serial PRIMARY KEY,
    corporation_id text,
    customer_username text,
    customer_company_code text,
    owner_type text,
    owner_id text,
    guest_pin text,
    ship_config jsonb DEFAULT '{}'::jsonb,
    price numeric,
    wait_time_days integer,
    status text,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    completed_at timestamp without time zone
);

INSERT INTO corp_ship_orders (
    id, corporation_id, customer_username, customer_company_code,
    guest_pin, ship_config, price, wait_time_days, status, notes
) VALUES (
    1, 'COSM', 'testuser', 'FAKE',
    '1234', '{}'::jsonb, 100.0, 1, 'QUEUED', 'Sample ship order'
);

CREATE TABLE IF NOT EXISTS material_recipes (
    id character varying PRIMARY KEY,
    reactor_id character varying,
    duration_ms bigint,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS material_recipe_ingredients (
    recipe_id character varying,
    material_id character varying,
    material_ticker character varying,
    amount numeric,
    type character varying
);

INSERT INTO material_recipes (id, reactor_id, duration_ms)
VALUES ('rec_water', 'b1', 60000);

INSERT INTO material_recipe_ingredients (recipe_id, material_id, material_ticker, amount, type)
VALUES 
    ('rec_water', 'm_in', 'H', 2, 'INPUT'),
    ('rec_water', 'm_h2o', 'H2O', 1, 'OUTPUT');

CREATE TABLE IF NOT EXISTS cx_brokers (
    brokermaterialid text PRIMARY KEY,
    ticker text,
    price numeric DEFAULT 100.0,
    priceaverage numeric DEFAULT 100.0,
    askamount integer DEFAULT 10,
    askprice numeric DEFAULT 105.0,
    bidamount integer DEFAULT 10,
    bidprice numeric DEFAULT 95.0,
    xata_updatedat timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cx_brokers_history (
    brokermaterialid text,
    ticker text,
    price numeric DEFAULT 100.0,
    priceaverage numeric DEFAULT 100.0,
    askprice numeric DEFAULT 105.0,
    bidprice numeric DEFAULT 95.0,
    snapshot_at timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cx_brokers_buy_orders (
    brokermaterialid text,
    priceamount numeric DEFAULT 100.0,
    tradername text DEFAULT 'Insitor Cooperative Market Maker'
);

CREATE TABLE IF NOT EXISTS cx_brokers_sell_orders (
    brokermaterialid text,
    priceamount numeric DEFAULT 100.0,
    tradername text DEFAULT 'Insitor Cooperative Market Maker'
);

INSERT INTO cx_brokers (brokermaterialid, ticker, priceaverage, askamount, askprice, bidamount, bidprice)
VALUES ('bm_h2o_ai1', 'H2O.AI1', 100.0, 10, 105.0, 10, 95.0);

INSERT INTO cx_brokers_history (brokermaterialid, ticker, priceaverage, snapshot_at)
VALUES ('bm_h2o_ai1', 'H2O.AI1', 100.0, now());

INSERT INTO cx_brokers_buy_orders (brokermaterialid, priceamount, tradername)
VALUES ('bm_h2o_ai1', 500.0, 'Insitor Cooperative Market Maker');

INSERT INTO cx_brokers_sell_orders (brokermaterialid, priceamount, tradername)
VALUES ('bm_h2o_ai1', 500.0, 'Insitor Cooperative Market Maker');

CREATE TABLE IF NOT EXISTS planet_physical_data (
    planetid text PRIMARY KEY,
    gravity double precision DEFAULT 1.0,
    temperature double precision DEFAULT 20.0,
    xata_updatedat timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS planet_orbit (
    planetid text PRIMARY KEY,
    semimajoraxis double precision DEFAULT 1.0,
    xata_updatedat timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS planet_resources (
    planetid text,
    materialid text,
    type text DEFAULT 'GASEOUS',
    factor double precision DEFAULT 1.0,
    xata_updatedat timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS planet_production_fees (
    planetid text,
    category text,
    workforcelevel text,
    feeamount double precision DEFAULT 10.0,
    feecurrency text DEFAULT 'CIS',
    xata_updatedat timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS planet_build_options (
    planetid text,
    sitetype text,
    billofmaterial text DEFAULT '[]',
    xata_updatedat timestamp with time zone DEFAULT now()
);

INSERT INTO planet_physical_data (planetid) VALUES ('p1');
INSERT INTO planet_orbit (planetid) VALUES ('p1');
INSERT INTO planet_resources (planetid, materialid) VALUES ('p1', 'm1');

-- Banking Tables
CREATE SEQUENCE IF NOT EXISTS player_banks_id_seq;
CREATE TABLE IF NOT EXISTS player_banks (
    id INTEGER DEFAULT nextval('player_banks_id_seq'::regclass) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    owner_username VARCHAR(100) NOT NULL,
    liquidity NUMERIC DEFAULT 0,
    default_interest_rate NUMERIC DEFAULT 5.0,
    max_interest_rate NUMERIC DEFAULT 10.0,
    weekly_yield_rate NUMERIC DEFAULT 1.5,
    max_weekly_yield_rate NUMERIC DEFAULT 3.0,
    deposit_status VARCHAR(50) DEFAULT 'OPEN',
    deposit_strategy VARCHAR(50) DEFAULT 'BALANCED',
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    currency VARCHAR(20) DEFAULT 'ICA',
    default_repay_days INTEGER DEFAULT 14,
    default_interval VARCHAR(50) DEFAULT 'At Maturity',
    min_loan_amount NUMERIC DEFAULT 0,
    max_loan_amount NUMERIC DEFAULT 0,
    headline VARCHAR(255),
    last_yield_distribution TIMESTAMP WITH TIME ZONE DEFAULT now(),
    public_show_loans BOOLEAN DEFAULT TRUE,
    public_show_deposits BOOLEAN DEFAULT TRUE,
    public_show_withdrawals BOOLEAN DEFAULT TRUE,
    public_show_names BOOLEAN DEFAULT FALSE,
    is_public BOOLEAN DEFAULT TRUE
);

CREATE SEQUENCE IF NOT EXISTS bank_loan_requests_id_seq;
CREATE TABLE IF NOT EXISTS bank_loan_requests (
    id INTEGER DEFAULT nextval('bank_loan_requests_id_seq'::regclass) PRIMARY KEY,
    bank_id INTEGER REFERENCES player_banks(id) ON DELETE CASCADE,
    requester_username VARCHAR(100) NOT NULL,
    amount NUMERIC NOT NULL,
    interest_rate NUMERIC NOT NULL,
    term_days INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    contract_id VARCHAR(100),
    transaction_type VARCHAR(30) DEFAULT 'LOAN',
    is_verified BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bank_weekly_snapshots (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES player_banks(id) ON DELETE CASCADE,
    week_date DATE NOT NULL,
    total_pool NUMERIC(20, 2) NOT NULL DEFAULT 0,
    owner_equity NUMERIC(20, 2) NOT NULL DEFAULT 0,
    client_deposits NUMERIC(20, 2) NOT NULL DEFAULT 0,
    total_yield_distributed NUMERIC(20, 2) NOT NULL DEFAULT 0,
    depositors_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT uq_bank_weekly UNIQUE (bank_id, week_date)
);

CREATE TABLE IF NOT EXISTS bank_depositor_weekly_snapshots (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES player_banks(id) ON DELETE CASCADE,
    username VARCHAR(100) NOT NULL,
    week_date DATE NOT NULL,
    starting_balance NUMERIC(20, 2) NOT NULL DEFAULT 0,
    net_deposits NUMERIC(20, 2) NOT NULL DEFAULT 0,
    effective_capital NUMERIC(20, 2) NOT NULL DEFAULT 0,
    yield_earned NUMERIC(20, 2) NOT NULL DEFAULT 0,
    closing_balance NUMERIC(20, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT uq_bank_depositor_weekly UNIQUE (bank_id, username, week_date)
);

INSERT INTO player_banks (id, name, owner_username, liquidity, default_interest_rate, currency, is_public)
VALUES (101, 'Test Reserve Bank', 'testuser', 1000000.0, 4.5, 'ICA', true);

