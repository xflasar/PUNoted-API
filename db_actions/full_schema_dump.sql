-- ============================================================================
-- Complete PUNoted Database Full Schema & Indexes Dump
-- Generated automatically for production database environment deployment
-- Safe to run on existing database (All statements use IF NOT EXISTS)
-- ============================================================================

SET statement_timeout = 0;
SET client_encoding = 'UTF8';

-- ----------------------------------------------------------------------------
-- Table: bank_loan_requests
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bank_loan_requests (
    id INTEGER DEFAULT nextval('bank_loan_requests_id_seq'::regclass) NOT NULL,
    bank_id INTEGER,
    requester_username VARCHAR(100) NOT NULL,
    amount NUMERIC NOT NULL,
    interest_rate NUMERIC NOT NULL,
    term_days INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING'::character varying,
    contract_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_bank_loans_bank ON public.bank_loan_requests USING btree (bank_id);
CREATE INDEX IF NOT EXISTS idx_bank_loans_requester ON public.bank_loan_requests USING btree (requester_username);

-- ----------------------------------------------------------------------------
-- Table: blueprint_components_modifiers
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS blueprint_components_modifiers (
    componentid TEXT,
    id INTEGER DEFAULT nextval('blueprint_components_modifiers_id_seq'::regclass) NOT NULL,
    type TEXT,
    value DOUBLE PRECISION,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: blueprint_performance
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS blueprint_performance (
    acceleration DOUBLE PRECISION,
    accelerationmax DOUBLE PRECISION,
    blueprintid TEXT,
    emitterchargetime DOUBLE PRECISION,
    fltfuelcapacity DOUBLE PRECISION,
    fltmaxspeed DOUBLE PRECISION,
    id TEXT NOT NULL,
    maxgfactor INTEGER,
    maxoverchargetime DOUBLE PRECISION,
    minreactorusage DOUBLE PRECISION,
    operatingemptymass DOUBLE PRECISION,
    stlfuelcapacity DOUBLE PRECISION,
    storecapacitymass DOUBLE PRECISION,
    storecapacityvolume DOUBLE PRECISION,
    totalvolume DOUBLE PRECISION,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: building_build_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS building_build_materials (
    amount INTEGER,
    buildingid TEXT,
    materialid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS building_build_materials_id ON public.building_build_materials USING btree (buildingid, materialid);
CREATE INDEX IF NOT EXISTS idx_bbm_buildingid ON public.building_build_materials USING btree (buildingid);

-- ----------------------------------------------------------------------------
-- Table: building_workforce_capacities
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS building_workforce_capacities (
    buildingid TEXT NOT NULL,
    capacity INTEGER,
    workforcelevel TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    ishabitation BOOLEAN DEFAULT false
);

CREATE UNIQUE INDEX IF NOT EXISTS building_workforce_capacities_id ON public.building_workforce_capacities USING btree (buildingid, workforcelevel);

-- ----------------------------------------------------------------------------
-- Table: buildings
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buildings (
    area INTEGER,
    buildingid TEXT NOT NULL,
    expertisecategory TEXT,
    name TEXT,
    needsfertilesoil BOOLEAN,
    ticker TEXT,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (buildingid)
);

-- ----------------------------------------------------------------------------
-- Table: cocg_programs
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cocg_programs (
    category TEXT,
    id INTEGER NOT NULL,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: comex_trade_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS comex_trade_orders (
    amount INTEGER,
    brokerid TEXT,
    created TIMESTAMP WITHOUT TIME ZONE,
    exchangeid TEXT,
    initialamount INTEGER,
    limitamount NUMERIC,
    limitcurrency TEXT,
    materialid TEXT,
    orderid TEXT NOT NULL,
    status TEXT,
    type TEXT,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);

CREATE INDEX IF NOT EXISTS idx_cx_orders_user_created ON public.comex_trade_orders USING btree (userid, created DESC);
CREATE INDEX IF NOT EXISTS idx_cx_orders_user_status ON public.comex_trade_orders USING btree (userid, status);

-- ----------------------------------------------------------------------------
-- Table: comex_trade_orders_trades
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS comex_trade_orders_trades (
    amount INTEGER,
    orderid TEXT,
    partnercode TEXT,
    partnerid TEXT,
    partnername TEXT,
    priceamount NUMERIC,
    pricecurrency TEXT,
    tradeid TEXT NOT NULL,
    tradetime TIMESTAMP WITHOUT TIME ZONE,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (tradeid)
);

CREATE INDEX IF NOT EXISTS idx_cx_trades_orderid_amount ON public.comex_trade_orders_trades USING btree (orderid) INCLUDE (amount);

-- ----------------------------------------------------------------------------
-- Table: commodity_exchanges
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commodity_exchanges (
    currencycode TEXT,
    currencydecimals INTEGER,
    currencyname TEXT,
    currencynumericcode INTEGER,
    id TEXT NOT NULL,
    name TEXT,
    operatorid TEXT,
    stationid TEXT,
    systemid TEXT,
    code TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: company_data
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_data (
    companycode TEXT,
    companyid TEXT NOT NULL,
    companyname TEXT,
    countryid TEXT,
    headquartersid TEXT,
    ratingreportid TEXT,
    representationid TEXT,
    startinglocationplanetid TEXT,
    startinglocationsystemid TEXT,
    startingprofile TEXT,
    userdataid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (companyid)
);

CREATE INDEX IF NOT EXISTS idx_company_companycode_trgm ON public.company_data USING gin (companycode gin_trgm_ops);

-- ----------------------------------------------------------------------------
-- Table: contract_conditions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_conditions (
    id TEXT NOT NULL,
    contractid TEXT NOT NULL,
    deadline TIMESTAMP WITH TIME ZONE,
    deadlineduration_millis BIGINT,
    amountmoney NUMERIC,
    currencymoney TEXT,
    dependencies TEXT[],
    addresssystemid TEXT,
    addressplanetid TEXT,
    addressstationid TEXT,
    destinationsystemid TEXT,
    destinationplanetid TEXT,
    destinationstationid TEXT,
    index INTEGER,
    type TEXT NOT NULL,
    party TEXT,
    status TEXT NOT NULL,
    autoprovisionstoreid TEXT,
    reputationchange NUMERIC,
    blockid TEXT,
    shipmentitemid TEXT,
    contractparty TEXT NOT NULL,
    PRIMARY KEY (id, contractparty)
);

CREATE INDEX IF NOT EXISTS idx_conditions_address_planet ON public.contract_conditions USING btree (addressplanetid);
CREATE INDEX IF NOT EXISTS idx_conditions_address_station ON public.contract_conditions USING btree (addressstationid);
CREATE INDEX IF NOT EXISTS idx_conditions_contractid_index ON public.contract_conditions USING btree (contractid, index);
CREATE INDEX IF NOT EXISTS idx_conditions_contractid_type ON public.contract_conditions USING btree (contractid, type);
CREATE INDEX IF NOT EXISTS idx_conditions_dest_planet ON public.contract_conditions USING btree (destinationplanetid);
CREATE INDEX IF NOT EXISTS idx_conditions_dest_station ON public.contract_conditions USING btree (destinationstationid);

-- ----------------------------------------------------------------------------
-- Table: contract_loan_installments
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_loan_installments (
    conditionid TEXT NOT NULL,
    interestamount NUMERIC,
    repaymentamount NUMERIC,
    totalamount NUMERIC NOT NULL,
    currency TEXT NOT NULL,
    contractparty TEXT NOT NULL,
    PRIMARY KEY (conditionid, contractparty)
);

CREATE INDEX IF NOT EXISTS idx_loans_conditionid ON public.contract_loan_installments USING btree (conditionid);

-- ----------------------------------------------------------------------------
-- Table: contract_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_materials (
    contractconditionid TEXT NOT NULL,
    materialid TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    pickedupamount NUMERIC DEFAULT 0,
    contractparty TEXT NOT NULL,
    PRIMARY KEY (contractconditionid, materialid, contractparty)
);

CREATE INDEX IF NOT EXISTS idx_materials_conditionid ON public.contract_materials USING btree (contractconditionid);

-- ----------------------------------------------------------------------------
-- Table: contracts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contracts (
    id TEXT NOT NULL,
    localid TEXT,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    party TEXT NOT NULL,
    partnerid TEXT,
    partnername TEXT,
    partnercode TEXT,
    status TEXT NOT NULL,
    duedate TIMESTAMP WITH TIME ZONE,
    name TEXT,
    preamble TEXT,
    extensiondeadline TIMESTAMP WITH TIME ZONE,
    relatedcontracts TEXT[],
    contracttype TEXT,
    userid TEXT,
    terminationreceived BOOLEAN,
    terminationsent BOOLEAN,
    agentcontract BOOLEAN,
    canextend BOOLEAN,
    canrequesttermination BOOLEAN,
    PRIMARY KEY (id, party)
);

CREATE INDEX IF NOT EXISTS idx_contracts_filtering ON public.contracts USING btree (userid, status, party, partnercode);
CREATE INDEX IF NOT EXISTS idx_contracts_userid_date_desc ON public.contracts USING btree (userid, date DESC);

-- ----------------------------------------------------------------------------
-- Table: corp_ship_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corp_ship_orders (
    id INTEGER DEFAULT nextval('corp_ship_orders_id_seq'::regclass) NOT NULL,
    corporation_id VARCHAR(100),
    customer_username VARCHAR(255),
    customer_company_code VARCHAR(100),
    owner_type VARCHAR(50),
    owner_id VARCHAR(255),
    guest_pin VARCHAR(255),
    ship_config JSONB,
    price NUMERIC,
    wait_time_days INTEGER,
    status VARCHAR(50) DEFAULT 'QUEUED'::character varying,
    notes TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: corporation_project_bill_contributions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_project_bill_contributions (
    amount INTEGER,
    id INTEGER DEFAULT nextval('corporation_project_bill_contributions_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    projectid TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    userid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: corporation_project_bill_of_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_project_bill_of_materials (
    amount INTEGER,
    currentamount INTEGER,
    id INTEGER DEFAULT nextval('corporation_project_bill_of_materials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    projectid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: corporation_projects
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_projects (
    completiondate TIMESTAMP WITHOUT TIME ZONE,
    corporationid TEXT,
    id TEXT NOT NULL,
    naturalid TEXT,
    planetid TEXT,
    systemid TEXT,
    type TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: corporation_shareholder_holdings
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_shareholder_holdings (
    amount INTEGER,
    code TEXT,
    corporationid TEXT,
    currency TEXT,
    id INTEGER DEFAULT nextval('corporation_shareholder_holdings_id_seq'::regclass) NOT NULL,
    name TEXT,
    userid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: corporation_shareholders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_shareholders (
    corporationid TEXT,
    companyid TEXT NOT NULL,
    relativeshare INTEGER,
    shares INTEGER,
    userid TEXT,
    companycode TEXT,
    companyname TEXT,
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS corporation_shareholders_id ON public.corporation_shareholders USING btree (corporationid, companyid);
CREATE INDEX IF NOT EXISTS idx_corp_shareholders_corporationid_userid ON public.corporation_shareholders USING btree (corporationid, userid);
CREATE INDEX IF NOT EXISTS idx_corp_shareholders_userid_corporationid ON public.corporation_shareholders USING btree (userid, corporationid);
CREATE INDEX IF NOT EXISTS idx_cs_corp_user ON public.corporation_shareholders USING btree (corporationid, userid);

-- ----------------------------------------------------------------------------
-- Table: corporation_shareholders_history
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_shareholders_history (
    corporationid TEXT,
    companyid TEXT NOT NULL,
    relativeshare INTEGER,
    shares INTEGER,
    userid TEXT,
    companycode TEXT,
    companyname TEXT,
    id UUID NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_corp_history_company ON public.corporation_shareholders_history USING btree (companycode);
CREATE INDEX IF NOT EXISTS idx_corp_history_date ON public.corporation_shareholders_history USING btree (snapshot_at);

-- ----------------------------------------------------------------------------
-- Table: corporation_subsidiaries
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporation_subsidiaries (
    corporationmainid TEXT NOT NULL,
    corporationsubid TEXT NOT NULL,
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    linkedat TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: corporations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporations (
    code TEXT,
    countryid TEXT,
    currencycode TEXT,
    foundedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    id TEXT NOT NULL,
    name TEXT,
    totalshares INTEGER,
    founder TEXT,
    officers TEXT[],
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: countries
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS countries (
    code TEXT,
    currencycode TEXT,
    currencydecimals INTEGER,
    currencyname TEXT,
    currencynumericcode INTEGER,
    id TEXT NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: currencies
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS currencies (
    code TEXT,
    decimals INTEGER,
    id TEXT NOT NULL,
    name TEXT,
    numericcode INTEGER,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: cx_brokers
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cx_brokers (
    addressstationid TEXT,
    addresssystemid TEXT,
    alltimehigh NUMERIC,
    alltimelow NUMERIC,
    askamount INTEGER,
    askprice NUMERIC,
    bidamount INTEGER,
    bidprice NUMERIC,
    brokermaterialid TEXT NOT NULL,
    currencyid TEXT,
    demand INTEGER,
    exchangeid TEXT,
    high NUMERIC,
    low NUMERIC,
    materialid TEXT,
    narrowpricebandhigh NUMERIC,
    narrowpricebandlow NUMERIC,
    price NUMERIC,
    priceaverage NUMERIC,
    pricetime TIMESTAMP WITHOUT TIME ZONE,
    supply INTEGER,
    ticker TEXT,
    traded INTEGER,
    volume INTEGER,
    widepricebandhigh NUMERIC,
    widepricebandlow NUMERIC,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (brokermaterialid)
);

-- ----------------------------------------------------------------------------
-- Table: cx_brokers_buy_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cx_brokers_buy_orders (
    amount INTEGER,
    brokermaterialid TEXT,
    orderid TEXT NOT NULL,
    priceamount NUMERIC,
    pricecurrency TEXT,
    tradercode TEXT,
    traderid TEXT,
    tradername TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);

-- ----------------------------------------------------------------------------
-- Table: cx_brokers_history
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cx_brokers_history (
    addressstationid TEXT,
    addresssystemid TEXT,
    alltimehigh NUMERIC,
    alltimelow NUMERIC,
    askamount INTEGER,
    askprice NUMERIC,
    bidamount INTEGER,
    bidprice NUMERIC,
    brokermaterialid TEXT NOT NULL,
    currencyid TEXT,
    demand INTEGER,
    exchangeid TEXT,
    high NUMERIC,
    low NUMERIC,
    materialid TEXT,
    narrowpricebandhigh NUMERIC,
    narrowpricebandlow NUMERIC,
    price NUMERIC,
    priceaverage NUMERIC,
    pricetime TIMESTAMP WITHOUT TIME ZONE,
    supply INTEGER,
    ticker TEXT,
    traded INTEGER,
    volume INTEGER,
    widepricebandhigh NUMERIC,
    widepricebandlow NUMERIC,
    xata_createdat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_version INTEGER NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_date ON public.cx_brokers_history USING btree (snapshot_at);

-- ----------------------------------------------------------------------------
-- Table: cx_brokers_sell_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cx_brokers_sell_orders (
    amount INTEGER,
    brokermaterialid TEXT,
    orderid TEXT NOT NULL,
    priceamount NUMERIC,
    pricecurrency TEXT,
    tradercode TEXT,
    traderid TEXT,
    tradername TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);

-- ----------------------------------------------------------------------------
-- Table: data_group_members
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_group_members (
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    status TEXT DEFAULT 'INVITED'::text,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    personal_suffix TEXT,
    can_read_data BOOLEAN DEFAULT false,
    granted_permissions JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (group_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_suffix ON public.data_group_members USING btree (group_id, personal_suffix);

-- ----------------------------------------------------------------------------
-- Table: data_sharing_groups
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_sharing_groups (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    owner_id UUID NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    access_key TEXT,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS data_sharing_groups_access_key_key ON public.data_sharing_groups USING btree (access_key);
CREATE INDEX IF NOT EXISTS idx_group_access_key ON public.data_sharing_groups USING btree (access_key);

-- ----------------------------------------------------------------------------
-- Table: efficiency_gains
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS efficiency_gains (
    category TEXT,
    gain DOUBLE PRECISION,
    headquartersid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- ----------------------------------------------------------------------------
-- Table: efficiency_gains_next_level
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS efficiency_gains_next_level (
    category TEXT,
    gain DOUBLE PRECISION,
    headquartersid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- ----------------------------------------------------------------------------
-- Table: gateway_fuel_contractors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateway_fuel_contractors (
    gateway_id TEXT NOT NULL,
    phase_index INTEGER NOT NULL,
    contractor_id TEXT,
    contractor_code TEXT,
    contractor_name TEXT,
    contract_id TEXT NOT NULL,
    PRIMARY KEY (gateway_id, phase_index, contract_id)
);

-- ----------------------------------------------------------------------------
-- Table: gateway_traffic
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateway_traffic (
    gateway_id TEXT NOT NULL,
    total_jumps INTEGER,
    current_phase_jumps INTEGER,
    current_phase_inbound INTEGER,
    current_phase_start TIMESTAMP WITHOUT TIME ZONE,
    current_phase_end TIMESTAMP WITHOUT TIME ZONE,
    avg_jumps DOUBLE PRECISION,
    avg_inbound DOUBLE PRECISION,
    raw_current_phase JSONB,
    raw_last_phase JSONB,
    raw_averages JSONB,
    PRIMARY KEY (gateway_id)
);

-- ----------------------------------------------------------------------------
-- Table: gateway_upkeep
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateway_upkeep (
    gateway_id TEXT NOT NULL,
    average_uptime DOUBLE PRECISION,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (gateway_id)
);

-- ----------------------------------------------------------------------------
-- Table: gateway_upkeep_contractors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateway_upkeep_contractors (
    gateway_id TEXT NOT NULL,
    phase_index INTEGER NOT NULL,
    contractor_id TEXT,
    contractor_code TEXT,
    contractor_name TEXT,
    contract_id TEXT NOT NULL,
    PRIMARY KEY (gateway_id, phase_index, contract_id)
);

-- ----------------------------------------------------------------------------
-- Table: gateway_upkeep_phases
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateway_upkeep_phases (
    id TEXT NOT NULL,
    gateway_id TEXT,
    natural_id INTEGER,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    service_level DOUBLE PRECISION,
    materials_json JSONB,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: gateway_upkeep_requirements
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateway_upkeep_requirements (
    gateway_id TEXT NOT NULL,
    material_id TEXT NOT NULL,
    material_ticker TEXT,
    material_name TEXT,
    amount_current DOUBLE PRECISION,
    amount_required DOUBLE PRECISION,
    PRIMARY KEY (gateway_id, material_id)
);

-- ----------------------------------------------------------------------------
-- Table: gateways
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gateways (
    id TEXT NOT NULL,
    natural_id TEXT,
    name TEXT,
    type TEXT,
    system_id TEXT,
    planet_id TEXT,
    owner_admin_center_id TEXT,
    currency_code TEXT,
    established TIMESTAMP WITHOUT TIME ZONE,
    operational_state TEXT,
    link_status TEXT,
    outgoing_link_id TEXT,
    incoming_links TEXT[],
    is_linked BOOLEAN,
    max_ship_volume DOUBLE PRECISION,
    linking_radius DOUBLE PRECISION,
    jumps_per_day DOUBLE PRECISION,
    fuel_available DOUBLE PRECISION,
    fuel_max DOUBLE PRECISION,
    fuel_per_jump DOUBLE PRECISION,
    fuel_usage_fee DOUBLE PRECISION,
    fuel_usage_currency TEXT,
    avg_fuel_availability DOUBLE PRECISION,
    capacity_upgrades INTEGER,
    volume_upgrades INTEGER,
    distance_upgrades INTEGER,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    satellite_id TEXT,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_gateways_system ON public.gateways USING btree (system_id);

-- ----------------------------------------------------------------------------
-- Table: group_members
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS group_members (
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    role VARCHAR(20) NOT NULL,
    joined_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_user ON public.group_members USING btree (user_id);
CREATE INDEX IF NOT EXISTS idx_members_user_id ON public.group_members USING btree (user_id);

-- ----------------------------------------------------------------------------
-- Table: headquarters
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS headquarters (
    additionalbasepermits INTEGER,
    additionalproductionqueueslots INTEGER,
    addressplanetid TEXT,
    addresssystemid TEXT,
    basepermits INTEGER,
    headquarterslevel INTEGER,
    headquartersnextupgradeid TEXT,
    nextrelocationtime TIMESTAMP WITHOUT TIME ZONE,
    relocationlocked BOOLEAN,
    usedbasepermits INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    headquartersid UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (headquartersid)
);

-- ----------------------------------------------------------------------------
-- Table: headquarters_upgrade_items
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS headquarters_upgrade_items (
    amount INTEGER,
    amountlimit INTEGER,
    headquartersid TEXT,
    materialid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- ----------------------------------------------------------------------------
-- Table: leaderboard_history
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS leaderboard_history (
    record_date DATE DEFAULT CURRENT_DATE NOT NULL,
    category TEXT NOT NULL,
    time_range TEXT NOT NULL,
    material_ticker TEXT DEFAULT 'NONE'::text NOT NULL,
    company_id TEXT NOT NULL,
    rank INTEGER,
    score NUMERIC
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_leaderboard_daily ON public.leaderboard_history USING btree (record_date, category, time_range, material_ticker, company_id);
CREATE INDEX IF NOT EXISTS idx_leaderboard_history_company ON public.leaderboard_history USING btree (company_id, category, record_date);
CREATE INDEX IF NOT EXISTS idx_leaderboard_history_daily ON public.leaderboard_history USING btree (record_date DESC, category, time_range, material_ticker);

-- ----------------------------------------------------------------------------
-- Table: material_categories
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS material_categories (
    id TEXT NOT NULL,
    name TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: material_prices
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS material_prices (
    price DOUBLE PRECISION,
    ticker TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (ticker)
);

-- ----------------------------------------------------------------------------
-- Table: material_processes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS material_processes (
    processid UUID DEFAULT gen_random_uuid() NOT NULL,
    reactorid TEXT NOT NULL,
    durationmillis BIGINT,
    processtype TEXT,
    PRIMARY KEY (processid)
);

-- ----------------------------------------------------------------------------
-- Table: material_recipe_ingredients
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS material_recipe_ingredients (
    recipe_id VARCHAR(32) NOT NULL,
    material_id VARCHAR(64) NOT NULL,
    material_ticker VARCHAR(10),
    amount NUMERIC NOT NULL,
    type VARCHAR(6) NOT NULL,
    PRIMARY KEY (recipe_id, material_id, type)
);

CREATE INDEX IF NOT EXISTS idx_mat_recipe_ing_reverse_lookup ON public.material_recipe_ingredients USING btree (material_ticker, type);

-- ----------------------------------------------------------------------------
-- Table: material_recipes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS material_recipes (
    id VARCHAR(32) NOT NULL,
    reactor_id VARCHAR(64) NOT NULL,
    duration_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_mat_recipes_reactor ON public.material_recipes USING btree (reactor_id);

-- ----------------------------------------------------------------------------
-- Table: materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS materials (
    category TEXT,
    materialid TEXT NOT NULL,
    name TEXT,
    resource BOOLEAN,
    ticker TEXT,
    volume DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (materialid)
);

CREATE INDEX IF NOT EXISTS idx_materials_materialid ON public.materials USING btree (materialid);
CREATE INDEX IF NOT EXISTS idx_materials_matid ON public.materials USING btree (materialid);
CREATE INDEX IF NOT EXISTS idx_materials_ticker ON public.materials USING btree (ticker);

-- ----------------------------------------------------------------------------
-- Table: planet_build_options
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_build_options (
    billofmaterial TEXT,
    id INTEGER DEFAULT nextval('planet_build_options_id_seq'::regclass) NOT NULL,
    planetid TEXT NOT NULL,
    sitetype TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (planetid, sitetype)
);

CREATE INDEX IF NOT EXISTS idx_planet_build_options_planetid ON public.planet_build_options USING btree (planetid);

-- ----------------------------------------------------------------------------
-- Table: planet_celestial_bodies
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_celestial_bodies (
    id TEXT NOT NULL,
    planetid TEXT,
    systemid TEXT,
    satelliteid TEXT,
    name TEXT,
    naturalid TEXT,
    semimajoraxis DOUBLE PRECISION,
    eccentricity DOUBLE PRECISION,
    inclination DOUBLE PRECISION,
    rightascension DOUBLE PRECISION,
    periapsis DOUBLE PRECISION,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_planet_celestial_bodies_id_planetid ON public.planet_celestial_bodies USING btree (id, planetid);

-- ----------------------------------------------------------------------------
-- Table: planet_infrastructure_contributions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_infrastructure_contributions (
    contributorid TEXT NOT NULL,
    contributorname TEXT NOT NULL,
    contributorcode TEXT NOT NULL,
    amount INTEGER NOT NULL,
    materialid TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    projectid TEXT NOT NULL,
    id TEXT DEFAULT gen_random_uuid() NOT NULL,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS planet_infrastructure_contributions_unq ON public.planet_infrastructure_contributions USING btree (projectid, contributorid, materialid, "timestamp");

-- ----------------------------------------------------------------------------
-- Table: planet_infrastructure_upgrade_costs
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_infrastructure_upgrade_costs (
    projectid TEXT NOT NULL,
    id TEXT DEFAULT gen_random_uuid() NOT NULL,
    amount INTEGER NOT NULL,
    currentamount INTEGER NOT NULL,
    materialid TEXT NOT NULL,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS planet_infrastructure_upgrade_costs_unq ON public.planet_infrastructure_upgrade_costs USING btree (projectid, materialid);

-- ----------------------------------------------------------------------------
-- Table: planet_infrastructure_upkeeps
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_infrastructure_upkeeps (
    projectid TEXT NOT NULL,
    id TEXT DEFAULT gen_random_uuid() NOT NULL,
    amount INTEGER,
    currentamount INTEGER,
    duration BIGINT,
    materialid TEXT,
    storecapacity INTEGER,
    stored INTEGER,
    nexttick BIGINT,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS planet_infrastructure_upkeeps_unq ON public.planet_infrastructure_upkeeps USING btree (projectid, materialid);

-- ----------------------------------------------------------------------------
-- Table: planet_infrastructures
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_infrastructures (
    activelevel INTEGER,
    currentlevel INTEGER,
    level INTEGER,
    populationid TEXT NOT NULL,
    projectid TEXT NOT NULL,
    projectname TEXT,
    ticker TEXT,
    type TEXT NOT NULL,
    upgradestatus INTEGER,
    upkeepstatus INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (populationid, type, projectid)
);

-- ----------------------------------------------------------------------------
-- Table: planet_orbit
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_orbit (
    eccentricity DOUBLE PRECISION,
    id INTEGER DEFAULT nextval('planet_orbit_id_seq'::regclass) NOT NULL,
    inclination DOUBLE PRECISION,
    orbitindex INTEGER,
    periapsis DOUBLE PRECISION,
    planetid TEXT NOT NULL,
    rightascension DOUBLE PRECISION,
    semimajoraxis DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (planetid)
);

CREATE INDEX IF NOT EXISTS idx_planet_orbit_planetid ON public.planet_orbit USING btree (planetid);

-- ----------------------------------------------------------------------------
-- Table: planet_physical_data
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_physical_data (
    gravity DOUBLE PRECISION,
    id INTEGER DEFAULT nextval('planet_physical_data_id_seq'::regclass) NOT NULL,
    magneticfield DOUBLE PRECISION,
    mass DOUBLE PRECISION,
    massearth DOUBLE PRECISION,
    planetid TEXT,
    pressure DOUBLE PRECISION,
    radiation DOUBLE PRECISION,
    radius DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    fertility DOUBLE PRECISION,
    surface BOOLEAN,
    sunlight DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_planet_physical_data_planetid ON public.planet_physical_data USING btree (planetid);

-- ----------------------------------------------------------------------------
-- Table: planet_populations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_populations (
    averagehappinessengineer DOUBLE PRECISION,
    averagehappinesspioneer DOUBLE PRECISION,
    averagehappinessscientist DOUBLE PRECISION,
    averagehappinesssettler DOUBLE PRECISION,
    averagehappinesstechnician DOUBLE PRECISION,
    explorersgraceenabled BOOLEAN,
    governmentprogramtype TEXT,
    nextpopulationengineer INTEGER,
    nextpopulationpioneer INTEGER,
    nextpopulationscientist INTEGER,
    nextpopulationsettler INTEGER,
    nextpopulationtechnician INTEGER,
    openjobsengineer INTEGER,
    openjobspioneer INTEGER,
    openjobsscientist INTEGER,
    openjobssettler INTEGER,
    openjobstechnician INTEGER,
    populationdifferenceengineer INTEGER,
    populationdifferencepioneer INTEGER,
    populationdifferencescientist INTEGER,
    populationdifferencesettler INTEGER,
    populationdifferencetechnician INTEGER,
    populationid TEXT NOT NULL,
    simulationperiod INTEGER,
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    unemploymentrateengineer DOUBLE PRECISION,
    unemploymentratepioneer DOUBLE PRECISION,
    unemploymentratescientist DOUBLE PRECISION,
    unemploymentratesettler DOUBLE PRECISION,
    unemploymentratetechnician DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (populationid, time)
);

CREATE INDEX IF NOT EXISTS idx_planet_populations_latest ON public.planet_populations USING btree (populationid, "time" DESC);

-- ----------------------------------------------------------------------------
-- Table: planet_production_fees
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_production_fees (
    category TEXT,
    feeamount DOUBLE PRECISION,
    feecurrency TEXT,
    id INTEGER DEFAULT nextval('planet_production_fees_id_seq'::regclass) NOT NULL,
    planetid TEXT,
    workforcelevel TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_ppf ON public.planet_production_fees USING btree (category, planetid, workforcelevel);
CREATE INDEX IF NOT EXISTS idx_planet_production_fees_planetid ON public.planet_production_fees USING btree (planetid);

-- ----------------------------------------------------------------------------
-- Table: planet_projects
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_projects (
    entityid TEXT,
    planetid TEXT,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- ----------------------------------------------------------------------------
-- Table: planet_resources
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planet_resources (
    factor DOUBLE PRECISION,
    id INTEGER DEFAULT nextval('planet_resources_id_seq'::regclass) NOT NULL,
    materialid TEXT NOT NULL,
    planetid TEXT NOT NULL,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (planetid, materialid)
);

CREATE INDEX IF NOT EXISTS idx_planet_resources_planetid ON public.planet_resources USING btree (planetid);

-- ----------------------------------------------------------------------------
-- Table: planetbuildoptionmaterials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planetbuildoptionmaterials (
    amount INTEGER,
    id INTEGER DEFAULT nextval('planetbuildoptionmaterials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    planetid TEXT,
    sitetype TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: planetmarketfees
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planetmarketfees (
    id INTEGER DEFAULT nextval('planetmarketfees_id_seq'::regclass) NOT NULL,
    localmarketfeebase INTEGER,
    localmarketfeetimefactor INTEGER,
    planetid TEXT,
    productionfeelimitfactors TEXT,
    siteestablishmentfee INTEGER,
    warehousefee INTEGER,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: planets
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS planets (
    admincenterid TEXT,
    countrycode TEXT,
    countryname TEXT,
    fertility DOUBLE PRECISION,
    mass DOUBLE PRECISION DEFAULT '0'::double precision,
    name TEXT,
    nameable BOOLEAN,
    namer TEXT,
    namingdate TIMESTAMP WITHOUT TIME ZONE,
    naturalid TEXT,
    planetid TEXT NOT NULL,
    plots INTEGER,
    populationid TEXT,
    sunlight DOUBLE PRECISION,
    surface BOOLEAN,
    systemid TEXT,
    temperature DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    cogc TEXT,
    PRIMARY KEY (planetid)
);

CREATE INDEX IF NOT EXISTS idx_planets_name_trgm ON public.planets USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_planets_naturalid_trgm ON public.planets USING gin (naturalid gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_planets_planetid ON public.planets USING btree (planetid);
CREATE INDEX IF NOT EXISTS idx_planets_populationid ON public.planets USING btree (populationid);

-- ----------------------------------------------------------------------------
-- Table: platform_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform_materials (
    amount INTEGER,
    materialid TEXT,
    materialtype TEXT,
    platformid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS platform_materials_id ON public.platform_materials USING btree (materialid, platformid, materialtype);
CREATE INDEX IF NOT EXISTS idx_pm_platformid ON public.platform_materials USING btree (platformid);

-- ----------------------------------------------------------------------------
-- Table: player_banks
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_banks (
    id INTEGER DEFAULT nextval('player_banks_id_seq'::regclass) NOT NULL,
    name VARCHAR(100) NOT NULL,
    owner_username VARCHAR(100) NOT NULL,
    liquidity NUMERIC DEFAULT 0,
    default_interest_rate NUMERIC DEFAULT 5.0,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS player_banks_name_key ON public.player_banks USING btree (name);
CREATE INDEX IF NOT EXISTS idx_player_banks_owner ON public.player_banks USING btree (owner_username);

-- ----------------------------------------------------------------------------
-- Table: population_available_reserve_workforce
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS population_available_reserve_workforce (
    siteid TEXT NOT NULL,
    workforceamountengineer INTEGER,
    workforceamountpioneer INTEGER,
    workforceamountscientist INTEGER,
    workforceamountsettler INTEGER,
    workforceamounttechnician INTEGER,
    PRIMARY KEY (siteid)
);

-- ----------------------------------------------------------------------------
-- Table: process_material_io
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS process_material_io (
    processid UUID NOT NULL,
    materialid TEXT NOT NULL,
    iotype TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    PRIMARY KEY (processid, materialid, iotype)
);

-- ----------------------------------------------------------------------------
-- Table: production_groups
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_groups (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    name VARCHAR(255) NOT NULL,
    owner_id UUID NOT NULL,
    chain_data JSONB DEFAULT '{}'::jsonb NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_groups_owner_id ON public.production_groups USING btree (owner_id);
CREATE INDEX IF NOT EXISTS idx_groups_updated_at ON public.production_groups USING btree (updated_at);

-- ----------------------------------------------------------------------------
-- Table: production_line_order_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_line_order_materials (
    materialid TEXT,
    poroductionlineorderid TEXT,
    quantity INTEGER,
    type TEXT,
    valueamount DOUBLE PRECISION,
    valuecurrency TEXT
);

-- ----------------------------------------------------------------------------
-- Table: production_line_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_line_orders (
    completed DOUBLE PRECISION,
    completiontimestamp TIMESTAMP WITHOUT TIME ZONE,
    createdtimestamp TIMESTAMP WITHOUT TIME ZONE,
    durationmillis INTEGER,
    halted BOOLEAN,
    id TEXT NOT NULL,
    lastupdatedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    productionfeeamount DOUBLE PRECISION,
    productionfeecurrency TEXT,
    productionlineid TEXT,
    recipeid TEXT,
    recurring BOOLEAN,
    startedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: production_lines
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_lines (
    addressplanetid TEXT,
    addresssystemid TEXT,
    capacity INTEGER,
    condition DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    id TEXT NOT NULL,
    siteid TEXT,
    slots INTEGER,
    type TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: production_recipe_input_factors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_recipe_input_factors (
    factor DOUBLE PRECISION NOT NULL,
    id INTEGER DEFAULT nextval('production_recipe_input_factors_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    productiontemplateid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    productionlineid TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS production_recipe_input_factors_id ON public.production_recipe_input_factors USING btree (materialid, productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_input_factors_template_text ON public.production_recipe_input_factors USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_inputs_composite ON public.production_recipe_input_factors USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_inputs_templateid ON public.production_recipe_input_factors USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_prif_group ON public.production_recipe_input_factors USING btree (productiontemplateid, productionlineid, materialid);
CREATE INDEX IF NOT EXISTS idx_prif_templateid_lineid_materialid ON public.production_recipe_input_factors USING btree (productiontemplateid, productionlineid, materialid);
CREATE INDEX IF NOT EXISTS idx_recipe_inputs_template ON public.production_recipe_input_factors USING btree (productiontemplateid);

-- ----------------------------------------------------------------------------
-- Table: production_recipe_output_factors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_recipe_output_factors (
    factor DOUBLE PRECISION NOT NULL,
    id INTEGER DEFAULT nextval('production_recipe_output_factors_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    productiontemplateid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    productionlineid TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS pg ON public.production_recipe_output_factors USING btree (materialid, productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_output_factors_template_text ON public.production_recipe_output_factors USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_outputs_composite ON public.production_recipe_output_factors USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_outputs_templateid ON public.production_recipe_output_factors USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_prof_group ON public.production_recipe_output_factors USING btree (productiontemplateid, productionlineid, materialid);
CREATE INDEX IF NOT EXISTS idx_prof_templateid_lineid_materialid ON public.production_recipe_output_factors USING btree (productiontemplateid, productionlineid, materialid);
CREATE INDEX IF NOT EXISTS idx_recipe_outputs_template ON public.production_recipe_output_factors USING btree (productiontemplateid);

-- ----------------------------------------------------------------------------
-- Table: production_recipes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_recipes (
    duration BIGINT,
    efficiency DOUBLE PRECISION,
    effortfactor DOUBLE PRECISION,
    experience INTEGER,
    name TEXT NOT NULL,
    productionfee DOUBLE PRECISION,
    productionfeecurrency TEXT,
    productiontemplateid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    materialid TEXT,
    productionlineid TEXT NOT NULL,
    PRIMARY KEY (productionlineid, productiontemplateid)
);

CREATE INDEX IF NOT EXISTS idx_pr_valid ON public.production_recipes USING btree (productiontemplateid, productionlineid) WHERE (duration > 0);
CREATE INDEX IF NOT EXISTS idx_production_recipes_templateid_lineid ON public.production_recipes USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_recipes_composite ON public.production_recipes USING btree (productiontemplateid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_recipes_template_id_text ON public.production_recipes USING btree (productiontemplateid);
CREATE INDEX IF NOT EXISTS idx_recipes_templateid ON public.production_recipes USING btree (productiontemplateid);

-- ----------------------------------------------------------------------------
-- Table: production_workforces
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_workforces (
    efficiency DOUBLE PRECISION,
    productionlineid TEXT,
    workforcelevel TEXT
);

-- ----------------------------------------------------------------------------
-- Table: public_announcements
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public_announcements (
    id INTEGER DEFAULT nextval('public_announcements_id_seq'::regclass) NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT DEFAULT 'info'::text,
    link TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_public_announcements_active ON public.public_announcements USING btree (is_active, created_at DESC);

-- ----------------------------------------------------------------------------
-- Table: public_users_data
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public_users_data (
    id TEXT NOT NULL,
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
    gifts JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_public_companycode_trgm ON public.public_users_data USING gin (company_code gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_public_username_trgm ON public.public_users_data USING gin (username gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_public_users_data_company_id ON public.public_users_data USING btree (company_id);
CREATE INDEX IF NOT EXISTS idx_public_users_data_subscription_level ON public.public_users_data USING btree (subscription_level);
CREATE INDEX IF NOT EXISTS idx_public_users_data_username ON public.public_users_data USING btree (username);

-- ----------------------------------------------------------------------------
-- Table: rating_reports
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rating_reports (
    contractcount INTEGER,
    earliestcontract TIMESTAMP WITHOUT TIME ZONE,
    overallrating TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    ratingreportid UUID DEFAULT gen_random_uuid() NOT NULL,
    PRIMARY KEY (ratingreportid)
);

-- ----------------------------------------------------------------------------
-- Table: recipes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recipes (
    materialid TEXT NOT NULL,
    input_recipe_ids JSONB DEFAULT '[]'::jsonb,
    output_recipe_ids JSONB DEFAULT '[]'::jsonb,
    PRIMARY KEY (materialid)
);

-- ----------------------------------------------------------------------------
-- Table: representation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS representation (
    contributednextlevelamount INTEGER,
    contributednextlevelcurrency TEXT,
    contributedtotalamount INTEGER,
    contributedtotalcurrency TEXT,
    costnextlevelamount INTEGER,
    costnextlevelcurrency TEXT,
    currentlevel INTEGER,
    leftnextlevelamount INTEGER,
    leftnextlevelcurrency TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    representationid TEXT NOT NULL,
    PRIMARY KEY (representationid)
);

-- ----------------------------------------------------------------------------
-- Table: representation_contributors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS representation_contributors (
    amountcontributed INTEGER,
    representationid TEXT,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- ----------------------------------------------------------------------------
-- Table: scheduled_tasks
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scheduled_tasks (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    accountid VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    reference_id VARCHAR(255),
    trigger_time TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB DEFAULT '{}'::jsonb,
    is_processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_pending ON public.scheduled_tasks USING btree (trigger_time) WHERE (is_processed = false);
CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_reference ON public.scheduled_tasks USING btree (reference_id, event_type) WHERE (is_processed = false);

-- ----------------------------------------------------------------------------
-- Table: sectors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sectors (
    externalsectorid TEXT NOT NULL,
    hexq INTEGER,
    hexr INTEGER,
    hexs INTEGER,
    name TEXT,
    size INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (externalsectorid)
);

-- ----------------------------------------------------------------------------
-- Table: ship_blueprint_bill_of_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_blueprint_bill_of_materials (
    amount INTEGER,
    blueprintid TEXT,
    id INTEGER DEFAULT nextval('ship_blueprint_bill_of_materials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    user_id TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: ship_blueprint_components
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_blueprint_components (
    amount INTEGER,
    blueprintid TEXT,
    cardinality TEXT,
    id TEXT NOT NULL,
    option TEXT,
    optionmaterialid TEXT,
    type TEXT,
    user_id TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: ship_blueprints
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_blueprints (
    buildtime INTEGER,
    createdtimestamp TIMESTAMP WITHOUT TIME ZONE,
    id TEXT NOT NULL,
    name TEXT,
    naturalid TEXT,
    status TEXT,
    natural_id TEXT,
    user_id TEXT,
    bill_of_material JSONB,
    selections JSONB,
    performance JSONB,
    build_time INTEGER,
    xata_updatedat TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    type TEXT,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_ship_blueprints_user_id ON public.ship_blueprints USING btree (user_id);
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_natural_id ON public.ship_blueprints USING btree (natural_id);
CREATE INDEX IF NOT EXISTS idx_ship_blueprints_status ON public.ship_blueprints USING btree (status);

-- ----------------------------------------------------------------------------
-- Table: ship_blueprints_component_options
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_blueprints_component_options (
    id TEXT NOT NULL,
    materialname TEXT,
    option TEXT,
    type TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: ship_blueprints_component_types
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_blueprints_component_types (
    cardinality TEXT,
    id TEXT NOT NULL,
    selectable BOOLEAN,
    type TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: ship_build_presets
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_build_presets (
    id INTEGER DEFAULT nextval('ship_build_presets_id_seq'::regclass) NOT NULL,
    corporation_id VARCHAR(100),
    name VARCHAR(255) NOT NULL,
    price NUMERIC NOT NULL,
    price_corp NUMERIC NOT NULL,
    parts JSONB NOT NULL,
    is_admin_preset BOOLEAN DEFAULT false,
    created_by VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: ship_flight_segments
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_flight_segments (
    segment_id BIGINT DEFAULT nextval('ship_flight_segments_segment_id_seq'::regclass) NOT NULL,
    flight_id TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    segment_type TEXT NOT NULL,
    departure BIGINT,
    arrival BIGINT,
    duration BIGINT,
    origin_system_id TEXT,
    origin_location_id TEXT,
    origin_location_type TEXT,
    origin_orbit_data JSONB,
    destination_system_id TEXT,
    destination_location_id TEXT,
    destination_location_type TEXT,
    destination_orbit_data JSONB,
    stl_distance DOUBLE PRECISION,
    stl_fuel INTEGER,
    ftl_distance DOUBLE PRECISION,
    ftl_fuel INTEGER,
    damage DOUBLE PRECISION,
    transferellipse JSONB,
    PRIMARY KEY (segment_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ship_flight_segments_flight_id_segment_index_key ON public.ship_flight_segments USING btree (flight_id, segment_index);

-- ----------------------------------------------------------------------------
-- Table: ship_flights
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_flights (
    aborted BOOLEAN,
    arrivaltimestamp TIMESTAMP WITHOUT TIME ZONE,
    departuretimestamp TIMESTAMP WITHOUT TIME ZONE,
    destinationplanetid TEXT,
    destinationstationid TEXT,
    destinationsystemid TEXT,
    ftldistance DOUBLE PRECISION,
    ftltotalconsumption INTEGER,
    id TEXT NOT NULL,
    originplanetid TEXT,
    originstationid TEXT,
    originsystemid TEXT,
    shipid TEXT NOT NULL,
    stldistance DOUBLE PRECISION,
    stltotalconsumption INTEGER,
    damage DOUBLE PRECISION,
    currentsegmentindex INTEGER,
    userid TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ship_flights_unique ON public.ship_flights USING btree (id, shipid, userid);

-- ----------------------------------------------------------------------------
-- Table: ship_production
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_production (
    completed BOOLEAN,
    corpmember BOOLEAN,
    notes TEXT,
    ordercompleted TIMESTAMP WITHOUT TIME ZONE,
    orderdate TIMESTAMP WITHOUT TIME ZONE,
    orderid INTEGER DEFAULT nextval('ship_production_orderid_seq'::regclass) NOT NULL,
    orderwaittime INTEGER,
    price INTEGER,
    shiptype TEXT,
    username TEXT,
    position INTEGER,
    PRIMARY KEY (orderid)
);

-- ----------------------------------------------------------------------------
-- Table: ship_repair_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ship_repair_materials (
    amount INTEGER,
    materialid TEXT,
    shipid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ships_repair_materials_id ON public.ship_repair_materials USING btree (shipid, materialid);
CREATE INDEX IF NOT EXISTS idx_ship_repair_materials_shipid ON public.ship_repair_materials USING btree (shipid);

-- ----------------------------------------------------------------------------
-- Table: ships
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ships (
    acceleration DOUBLE PRECISION,
    addressplanetid TEXT,
    addressstationid TEXT,
    addresssystemid TEXT,
    blueprintnaturalid TEXT,
    commissioningtime TIMESTAMP WITHOUT TIME ZONE,
    condition DOUBLE PRECISION,
    emitterpower INTEGER,
    flightid TEXT,
    idftlfuelstore TEXT,
    idshipstore TEXT,
    idstlfuelstore TEXT,
    lastrepair TIMESTAMP WITHOUT TIME ZONE,
    mass DOUBLE PRECISION,
    name TEXT,
    operatingemptymass DOUBLE PRECISION,
    operatingtimeftl BIGINT,
    operatingtimestl BIGINT,
    reactorpower INTEGER,
    registration TEXT,
    shipid TEXT NOT NULL,
    status TEXT,
    stlfuelflowrate DOUBLE PRECISION,
    thrust INTEGER,
    userid TEXT,
    volume INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    type TEXT,
    PRIMARY KEY (shipid)
);

CREATE UNIQUE INDEX IF NOT EXISTS ships_id ON public.ships USING btree (shipid);
CREATE INDEX IF NOT EXISTS idx_ships_addressplanetid ON public.ships USING btree (addressplanetid);
CREATE INDEX IF NOT EXISTS idx_ships_addressstationid ON public.ships USING btree (addressstationid);
CREATE INDEX IF NOT EXISTS idx_ships_addresssystemid ON public.ships USING btree (addresssystemid);
CREATE INDEX IF NOT EXISTS idx_ships_shipid_userid ON public.ships USING btree (shipid, userid);

-- ----------------------------------------------------------------------------
-- Table: shipyard_project_materials
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shipyard_project_materials (
    amount INTEGER,
    amountlimit INTEGER,
    id INTEGER DEFAULT nextval('shipyard_project_materials_id_seq'::regclass) NOT NULL,
    materialid TEXT,
    projectid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: shipyard_projects
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shipyard_projects (
    blueprintnaturalid TEXT,
    canstart BOOLEAN,
    creationtimestamp TIMESTAMP WITHOUT TIME ZONE,
    endtimestamp TIMESTAMP WITHOUT TIME ZONE,
    id TEXT NOT NULL,
    originblueprintnaturalid TEXT,
    shipid TEXT,
    shipyardid TEXT,
    starttimestamp TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: shipyards
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shipyards (
    activeprojectstotal INTEGER,
    createdprojectstotal INTEGER,
    currencyid TEXT,
    finishedprojectsmonth INTEGER,
    finishedprojectssemiannually INTEGER,
    finishedprojectstotal INTEGER,
    finishedprojectsweek INTEGER,
    id TEXT NOT NULL,
    operatortype TEXT,
    planetid TEXT,
    systemid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: site_available_reserve_populations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS site_available_reserve_populations (
    engineer INTEGER DEFAULT 0 NOT NULL,
    pioneer INTEGER DEFAULT 0 NOT NULL,
    planetid TEXT NOT NULL,
    scientist INTEGER DEFAULT 0 NOT NULL,
    settler INTEGER DEFAULT 0 NOT NULL,
    siteid TEXT NOT NULL,
    technician INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS site_available_reserve_populations_id ON public.site_available_reserve_populations USING btree (planetid, siteid);

-- ----------------------------------------------------------------------------
-- Table: site_experts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS site_experts (
    available INTEGER,
    category TEXT,
    current INTEGER,
    efficiencygain DOUBLE PRECISION,
    elimit INTEGER,
    id TEXT NOT NULL,
    progress DOUBLE PRECISION,
    siteid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: site_platforms
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS site_platforms (
    area INTEGER,
    bookvalueamount DOUBLE PRECISION,
    bookvaluecurrency TEXT,
    buildingid TEXT,
    condition DOUBLE PRECISION,
    creationtime TIMESTAMP WITHOUT TIME ZONE,
    lastrepair TIMESTAMP WITHOUT TIME ZONE,
    platformid TEXT NOT NULL,
    siteid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (platformid)
);

CREATE INDEX IF NOT EXISTS idx_site_platforms_buildingid ON public.site_platforms USING btree (buildingid);
CREATE INDEX IF NOT EXISTS idx_site_platforms_site_created ON public.site_platforms USING btree (siteid, creationtime DESC);

-- ----------------------------------------------------------------------------
-- Table: site_production_line_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS site_production_line_orders (
    completed BOOLEAN,
    completion TIMESTAMP WITHOUT TIME ZONE,
    created TIMESTAMP WITHOUT TIME ZONE,
    duration BIGINT,
    halted BOOLEAN,
    lastupdated TIMESTAMP WITHOUT TIME ZONE,
    orderid TEXT NOT NULL,
    productionfeeamount DOUBLE PRECISION,
    productionfeecurrency TEXT,
    productionlineid TEXT,
    recipeid TEXT,
    recurring BOOLEAN,
    started TIMESTAMP WITHOUT TIME ZONE,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);

CREATE INDEX IF NOT EXISTS idx_orders_line_completion ON public.site_production_line_orders USING btree (productionlineid, completion);
CREATE INDEX IF NOT EXISTS idx_site_prod_line_orders_productionlineid_started_orderid ON public.site_production_line_orders USING btree (productionlineid, started, orderid);
CREATE INDEX IF NOT EXISTS idx_splo_active ON public.site_production_line_orders USING btree (productionlineid, orderid) WHERE (started IS NULL);
CREATE INDEX IF NOT EXISTS idx_splo_lineid_started ON public.site_production_line_orders USING btree (productionlineid) WHERE (started IS NULL);

-- ----------------------------------------------------------------------------
-- Table: site_production_lines
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS site_production_lines (
    capacity INTEGER,
    condition DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    productionlineid TEXT NOT NULL,
    siteid TEXT,
    slots INTEGER,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (productionlineid)
);

CREATE INDEX IF NOT EXISTS idx_prod_lines_siteid ON public.site_production_lines USING btree (siteid);
CREATE INDEX IF NOT EXISTS idx_site_prod_lines_siteid_productionlineid ON public.site_production_lines USING btree (siteid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_spl_site ON public.site_production_lines USING btree (siteid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_spl_site_line ON public.site_production_lines USING btree (siteid, productionlineid);
CREATE INDEX IF NOT EXISTS idx_spl_siteid ON public.site_production_lines USING btree (siteid);

-- ----------------------------------------------------------------------------
-- Table: sites
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sites (
    addressplanetid TEXT,
    addresssystemid TEXT,
    area INTEGER,
    buildingoptions TEXT[],
    foundedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    investedpermits INTEGER,
    maximumpermits INTEGER,
    siteid TEXT NOT NULL,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (siteid)
);

CREATE INDEX IF NOT EXISTS idx_sites_addressplanetid ON public.sites USING btree (addressplanetid);
CREATE INDEX IF NOT EXISTS idx_sites_user ON public.sites USING btree (userid, siteid);
CREATE INDEX IF NOT EXISTS idx_sites_userid ON public.sites USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_sites_userid_siteid ON public.sites USING btree (userid, siteid);

-- ----------------------------------------------------------------------------
-- Table: stations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stations (
    comexid TEXT,
    commissioningtime TIMESTAMP WITHOUT TIME ZONE,
    countryid TEXT,
    governingentityid TEXT,
    localmarketid TEXT,
    name TEXT,
    naturalid TEXT,
    stationid TEXT NOT NULL,
    systemid TEXT,
    warehouseid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    orbit JSONB,
    PRIMARY KEY (stationid)
);

-- ----------------------------------------------------------------------------
-- Table: storage_items
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS storage_items (
    compositekey TEXT NOT NULL,
    currencyamount DOUBLE PRECISION,
    currencytype TEXT,
    materialid TEXT,
    quantity INTEGER,
    storageid TEXT,
    totalvolume DOUBLE PRECISION,
    totalweight DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    type TEXT,
    PRIMARY KEY (compositekey)
);

CREATE INDEX IF NOT EXISTS idx_storage_items_storageid ON public.storage_items USING btree (storageid) INCLUDE (quantity);

-- ----------------------------------------------------------------------------
-- Table: storages
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS storages (
    addressableid TEXT,
    fixed BOOLEAN,
    locked BOOLEAN,
    name TEXT,
    rank INTEGER,
    storageid TEXT NOT NULL,
    tradestore BOOLEAN,
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

CREATE INDEX IF NOT EXISTS idx_storage_user ON public.storages USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_storages_capacities ON public.storages USING btree (weightcapacity, volumecapacity);

-- ----------------------------------------------------------------------------
-- Table: subsector_vertices
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subsector_vertices (
    externalsubsectorid TEXT NOT NULL,
    index INTEGER,
    x DOUBLE PRECISION,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    y DOUBLE PRECISION,
    z DOUBLE PRECISION
);

CREATE UNIQUE INDEX IF NOT EXISTS subsector_vertices_id ON public.subsector_vertices USING btree (index, externalsubsectorid);

-- ----------------------------------------------------------------------------
-- Table: subsectors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subsectors (
    externalsectorid TEXT NOT NULL,
    externalsubsectorid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (externalsubsectorid)
);

-- ----------------------------------------------------------------------------
-- Table: system_connections
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_connections (
    systemiddestination TEXT,
    systemidorigin TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS system_connections_id ON public.system_connections USING btree (systemidorigin, systemiddestination);

-- ----------------------------------------------------------------------------
-- Table: systems
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS systems (
    mass DOUBLE PRECISION DEFAULT '0'::double precision,
    masssol DOUBLE PRECISION DEFAULT '0'::double precision,
    microasteroidcount DOUBLE PRECISION,
    name TEXT,
    naturalid TEXT,
    positionx DOUBLE PRECISION,
    positiony DOUBLE PRECISION,
    positionz DOUBLE PRECISION,
    sectorid TEXT,
    subsectorid TEXT,
    systemid TEXT NOT NULL,
    type TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (systemid)
);

CREATE INDEX IF NOT EXISTS idx_systems_name_trgm ON public.systems USING gin (name gin_trgm_ops);

-- ----------------------------------------------------------------------------
-- Table: user_api_tokens
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_api_tokens (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    user_id UUID NOT NULL,
    token_hash TEXT NOT NULL,
    token_prefix TEXT NOT NULL,
    label TEXT NOT NULL,
    description TEXT,
    permissions JSONB DEFAULT '[]'::jsonb NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    last_used_at TIMESTAMP WITH TIME ZONE,
    group_id UUID,
    allow_group_access BOOLEAN DEFAULT false,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS user_api_tokens_token_hash_key ON public.user_api_tokens USING btree (token_hash);
CREATE INDEX IF NOT EXISTS idx_api_tokens_hash ON public.user_api_tokens USING btree (token_hash);
CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON public.user_api_tokens USING btree (user_id);

-- ----------------------------------------------------------------------------
-- Table: user_currency_accounts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_currency_accounts (
    balanceamount DOUBLE PRECISION,
    balancecurrencycode TEXT,
    bookbalanceamount DOUBLE PRECISION,
    bookbalancecurrencycode TEXT,
    category TEXT,
    number INTEGER,
    type INTEGER,
    userid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS user_currency_accounts_unique_key ON public.user_currency_accounts USING btree (userid, category, type, number, balancecurrencycode);

-- ----------------------------------------------------------------------------
-- Table: user_currency_accounts_history
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_currency_accounts_history (
    balanceamount DOUBLE PRECISION,
    balancecurrencycode TEXT,
    bookbalanceamount DOUBLE PRECISION,
    bookbalancecurrencycode TEXT,
    category TEXT,
    number INTEGER,
    type INTEGER,
    userid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_version INTEGER NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_currency_history_date ON public.user_currency_accounts_history USING btree (snapshot_at);
CREATE INDEX IF NOT EXISTS idx_currency_history_user ON public.user_currency_accounts_history USING btree (userid);

-- ----------------------------------------------------------------------------
-- Table: user_custom_prices
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_custom_prices (
    accountid TEXT NOT NULL,
    ticker VARCHAR(32) NOT NULL,
    price NUMERIC DEFAULT 0.00 NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid, ticker)
);

CREATE INDEX IF NOT EXISTS idx_user_custom_prices_acc ON public.user_custom_prices USING btree (accountid);

-- ----------------------------------------------------------------------------
-- Table: user_entity_settings
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_entity_settings (
    accountid TEXT NOT NULL,
    domain TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    settings JSONB DEFAULT '{}'::jsonb NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid, domain, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_user_entity_settings_acc_domain ON public.user_entity_settings USING btree (accountid, domain);

-- ----------------------------------------------------------------------------
-- Table: user_gifts_received
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_gifts_received (
    giftid TEXT,
    id INTEGER DEFAULT nextval('user_gifts_received_id_seq'::regclass) NOT NULL,
    userid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: user_gifts_sent
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_gifts_sent (
    giftid TEXT,
    id INTEGER DEFAULT nextval('user_gifts_sent_id_seq'::regclass) NOT NULL,
    userid TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: user_global_settings
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_global_settings (
    userid TEXT NOT NULL,
    default_cx_code TEXT DEFAULT 'IC1'::text,
    default_currency TEXT DEFAULT 'ICA'::text,
    internal_excluded_sites JSONB DEFAULT '[]'::jsonb,
    internal_leased_sites JSONB DEFAULT '[]'::jsonb,
    privacy_settings JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (userid)
);

-- ----------------------------------------------------------------------------
-- Table: user_notification_rules
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_notification_rules (
    accountid TEXT NOT NULL,
    fleet_enabled BOOLEAN DEFAULT true,
    health_threshold INTEGER DEFAULT 70,
    storage_enabled BOOLEAN DEFAULT true,
    storage_threshold INTEGER DEFAULT 90,
    production_enabled BOOLEAN DEFAULT true,
    supply_days_threshold DOUBLE PRECISION DEFAULT 1.0,
    contracts_enabled BOOLEAN DEFAULT true,
    cx_enabled BOOLEAN DEFAULT true,
    cx_market_watchers JSONB DEFAULT '[]'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid)
);

-- ----------------------------------------------------------------------------
-- Table: user_notifications
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_notifications (
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    accountid VARCHAR(255) NOT NULL,
    type VARCHAR(50) DEFAULT 'info'::character varying,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    is_read BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    is_deleted BOOLEAN DEFAULT false,
    category TEXT,
    dedup_key TEXT,
    data JSONB,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_user_notifications_userid ON public.user_notifications USING btree (accountid) WHERE (is_read = false);
CREATE INDEX IF NOT EXISTS idx_user_notif_acc_active ON public.user_notifications USING btree (accountid, is_deleted, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS user_notifications_dedup_key_key ON public.user_notifications USING btree (dedup_key);
CREATE INDEX IF NOT EXISTS idx_user_notif_acc_created ON public.user_notifications USING btree (accountid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_notif_unread ON public.user_notifications USING btree (accountid, is_read);

-- ----------------------------------------------------------------------------
-- Table: user_starting_profiles
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_starting_profiles (
    basematerials TEXT,
    buildingtickers TEXT,
    commodities TEXT,
    name TEXT,
    ships INTEGER,
    workforce TEXT
);

-- ----------------------------------------------------------------------------
-- Table: user_tokens
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_tokens (
    expiresat TIMESTAMP WITHOUT TIME ZONE,
    id INTEGER DEFAULT nextval('user_tokens_id_seq'::regclass) NOT NULL,
    refreshtoken TEXT,
    token TEXT,
    userid TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    type TEXT,
    user_agent TEXT,
    last_ip TEXT,
    iat_id TEXT,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: user_vendor_orders
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_vendor_orders (
    fixedprice DOUBLE PRECISION,
    materialid TEXT,
    materialticker TEXT NOT NULL,
    maxprice DOUBLE PRECISION,
    minprice DOUBLE PRECISION,
    orderid TEXT NOT NULL,
    ordertype TEXT,
    pricetype TEXT,
    quantity INTEGER,
    reserved INTEGER DEFAULT 0,
    vendorid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    location JSONB,
    PRIMARY KEY (orderid)
);

-- ----------------------------------------------------------------------------
-- Table: user_vendors
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_vendors (
    companycode TEXT,
    companyname TEXT NOT NULL,
    corpname TEXT,
    cx TEXT,
    gamename TEXT,
    isactive BOOLEAN DEFAULT true,
    userid TEXT NOT NULL,
    vendorid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (vendorid)
);

-- ----------------------------------------------------------------------------
-- Table: user_verification_codes
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_verification_codes (
    code TEXT,
    email TEXT,
    expiresat TIMESTAMP WITHOUT TIME ZONE,
    id INTEGER DEFAULT nextval('user_verification_codes_id_seq'::regclass) NOT NULL,
    servercode TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (id)
);

-- ----------------------------------------------------------------------------
-- Table: user_web_settings
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_web_settings (
    user_id UUID NOT NULL,
    page_context TEXT NOT NULL,
    preferences JSONB DEFAULT '{}'::jsonb NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (user_id, page_context)
);

-- ----------------------------------------------------------------------------
-- Table: users
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    email TEXT,
    fioapikey TEXT,
    isverified BOOLEAN DEFAULT false,
    password_hash TEXT,
    type TEXT DEFAULT 'user'::character varying,
    userdataid TEXT,
    username TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    accountid UUID DEFAULT gen_random_uuid() NOT NULL,
    is_synchronized BOOLEAN DEFAULT false NOT NULL,
    dataapikeys TEXT[],
    displayname TEXT,
    PRIMARY KEY (accountid)
);

CREATE INDEX IF NOT EXISTS idx_users_accountid ON public.users USING btree (accountid);
CREATE INDEX IF NOT EXISTS idx_users_userdataid ON public.users USING btree (userdataid);
CREATE INDEX IF NOT EXISTS idx_users_xata ON public.users USING btree (xata_updatedat);
CREATE INDEX IF NOT EXISTS idx_users_xata_updatedat ON public.users USING btree (xata_updatedat);

-- ----------------------------------------------------------------------------
-- Table: users_data
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users_data (
    activedaysperweek INTEGER,
    companyid TEXT,
    created TIMESTAMP WITHOUT TIME ZONE,
    displayname TEXT,
    highesttier TEXT,
    ismuted BOOLEAN,
    ispayinguser BOOLEAN,
    owncurrencyid TEXT,
    preferredlocale TEXT,
    subscriptionexpiry TIMESTAMP WITHOUT TIME ZONE,
    subscriptionlevel TEXT,
    userid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    corporationid TEXT,
    PRIMARY KEY (userid)
);

CREATE INDEX IF NOT EXISTS idx_users_data_userid ON public.users_data USING btree (userid);
CREATE INDEX IF NOT EXISTS idx_users_displayname_trgm ON public.users_data USING gin (displayname gin_trgm_ops);

-- ----------------------------------------------------------------------------
-- Table: warehouses
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS warehouses (
    addressplanet TEXT,
    addresssystem TEXT,
    feeamount INTEGER,
    feecurrency TEXT,
    nextpayment TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    storeid TEXT NOT NULL,
    units INTEGER,
    userid TEXT,
    volumecapacity INTEGER,
    warehouseid TEXT NOT NULL,
    weightcapacity INTEGER,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (warehouseid, storeid)
);

CREATE UNIQUE INDEX IF NOT EXISTS warehouses_warehouseid_storeid_key ON public.warehouses USING btree (warehouseid, storeid);

-- ----------------------------------------------------------------------------
-- Table: workforce_needs
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workforce_needs (
    category TEXT,
    essential BOOLEAN,
    materialid TEXT,
    satisfaction DOUBLE PRECISION,
    unitsper100 DOUBLE PRECISION,
    unitsperinterval DOUBLE PRECISION,
    workforceid TEXT NOT NULL,
    workforceneedid TEXT NOT NULL,
    PRIMARY KEY (workforceneedid)
);

-- ----------------------------------------------------------------------------
-- Table: workforces
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workforces (
    capacity INTEGER,
    population INTEGER,
    required INTEGER,
    reserve INTEGER,
    satisfaction DOUBLE PRECISION,
    siteid TEXT,
    level TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    userid TEXT,
    workforceid TEXT NOT NULL,
    PRIMARY KEY (workforceid)
);

CREATE UNIQUE INDEX IF NOT EXISTS workforces_unique_key ON public.workforces USING btree (siteid, level);
