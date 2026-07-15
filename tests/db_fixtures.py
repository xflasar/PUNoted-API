from __future__ import annotations

import typing
from types import SimpleNamespace

import asyncpg
import fastapi.testclient
import pytest

import main
import config

if typing.TYPE_CHECKING:
    import asyncpg.transaction

@pytest.fixture(scope="module")
def client() -> typing.Iterator[fastapi.testclient.TestClient]:
    with fastapi.testclient.TestClient(main.v1_app) as test_client:
        yield test_client

@pytest.fixture(scope="module")
def db_setup(client: fastapi.testclient.TestClient) -> typing.Iterator[tuple[asyncpg.Connection, asyncpg.transaction.Transaction]]:
    """set up tables for a test module"""
    assert client.portal is not None
    connection, transaction = client.portal.call(prepare_test_db, main.v1_app)
    try:
        yield connection, transaction
    finally:
        client.portal.call(cleanup_test_db, connection, transaction)

async def prepare_test_db(app) -> tuple[asyncpg.Connection, asyncpg.transaction.Transaction]:
    import redis
    r = redis.Redis(host="127.0.0.1", port=6379, db=0)
    try:
        r.flushdb()
    except Exception:
        pass
    finally:
        r.close()

    dsn = config.XATA_DATABASE_URL or "postgresql://punoted:pass@127.0.0.1:5445/punoted"
    if dsn and "localhost" in dsn:
        dsn = dsn.replace("localhost", "127.0.0.1")
    connection: asyncpg.Connection = await asyncpg.connect(dsn)

    # Register JSON and JSONB codecs like production db.py does
    import json
    def json_decoder(value):
        if value is None:
            return None
        return json.loads(value)

    await connection.set_type_codec(
        'json',
        encoder=json.dumps,
        decoder=json_decoder,
        schema='pg_catalog'
    )
    await connection.set_type_codec(
        'jsonb',
        encoder=json.dumps,
        decoder=json_decoder,
        schema='pg_catalog'
    )

    transaction: asyncpg.transaction.Transaction = connection.transaction()
    await transaction.start()

    await connection.execute(
        """
        CREATE TEMP TABLE users (
            accountid text PRIMARY KEY,
            username text NOT NULL,
            userdataid text NOT NULL,
            xata_id text
        ) ON COMMIT DROP;

        CREATE TEMP TABLE user_api_tokens (
            user_id text NOT NULL,
            group_id text,
            token_hash text NOT NULL
        ) ON COMMIT DROP;

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
            code text NOT NULL
        ) ON COMMIT DROP;

        INSERT INTO users (accountid, username, userdataid, xata_id)
        VALUES ('acct1', 'testuser', 'userid1', 'fakexataid');

        INSERT INTO users_data (userid, displayname)
        VALUES ('userid1', 'testuser');

        INSERT INTO user_api_tokens (user_id, group_id, token_hash)
        VALUES ('acct1', NULL, 'ptk_fake');

        INSERT INTO company_data (userdataid, companyid, companycode, companyname, xata_updatedat)
        VALUES ('userid1', 'company1', 'FAKE', 'fake co', TIMESTAMPTZ '2000-01-01 00:00:00+00');

        INSERT INTO corporations (id, name, code)
        VALUES ('corp1', 'fake corp', 'FC'), ('COSM', 'cosm corp', 'COSM');

        INSERT INTO corporation_shareholders (companyid, corporationid, userid, companycode, companyname)
        VALUES ('company1', 'corp1', 'userid1', 'FAKE', 'fake co');
        """
    )

    mock_conn = _MockConnection(connection)
    app.state.db = _MockDatabase(mock_conn)
    return connection, transaction

async def cleanup_test_db(connection: asyncpg.Connection, transaction: asyncpg.transaction.Transaction) -> None:
    await transaction.rollback()
    await connection.close()

@pytest.fixture
def db_savepoint(client: fastapi.testclient.TestClient,
        db_setup: tuple[asyncpg.Connection, asyncpg.transaction.Transaction]) -> typing.Iterator[asyncpg.transaction.Transaction]:
    """set up a savepoint for a test function, rolling back to the db_setup state after it completes"""
    assert client.portal is not None
    connection, _ = db_setup
    savepoint = client.portal.call(start_test_savepoint, connection)
    try:
        yield savepoint
    finally:
        client.portal.call(rollback_test_savepoint, savepoint)

async def start_test_savepoint(connection: asyncpg.Connection) -> asyncpg.transaction.Transaction:
    assert connection.is_in_transaction()
    savepoint = connection.transaction()
    await savepoint.start()
    return savepoint

async def rollback_test_savepoint(transaction: asyncpg.transaction.Transaction) -> None:
    await transaction.rollback()

from datetime import datetime, timezone

def get_query_stub(query: str, args: tuple) -> typing.Any:
    q = query.lower()
    print(f"DEBUG QUERY: {q}")
    
    # 0. Vendors (matched first to prevent overlap with storage_items joins)
    if "user_vendors" in q or "user_vendor_orders" in q:
        return [_MockRecord({
            "jsonb_agg": [{"CompanyCode": "FAKE", "CompanyName": "fake co", "CorporationName": "fake corp", "PlayerName": "testuser"}]
        })]

    # 1. Accounting
    if "user_currency_accounts" in q or "sql_fetch_accounts_list" in q:
        return [_MockRecord({
            "coalesce": [{"Username": "testuser", "Accounts": [{"Currency": "ICA", "Balance": 100.0, "BookBalance": 100.0, "Category": "ICA", "Type": "ICA", "AccountNumber": "123", "LastUpdatedEpochMs": 946684800000}]}]
        })]

    # 2. Contracts
    if "c.userid = any($1::text[])" in q and "contract_conditions" in q:
        return [_MockRecord({"id": "c1"})]
    if "c.extensiondeadline" in q:
        return [_MockRecord({
            "id": "c1", "localid": "l1", "date": datetime(2000, 1, 1, tzinfo=timezone.utc),
            "extensiondeadline": None, "duedate": None, "canextend": False,
            "canrequesttermination": False, "terminationsent": False, "terminationreceived": False,
            "name": "Contract 1", "preamble": "", "party": "CUSTOMER", "status": "FULFILLED",
            "partnerid": "p1", "partnername": "Partner", "partnercode": "PTN", "userid": "userid1"
        })]
    if "type in ('comex_purchase_pickup', 'delivery')" in q:
        return [_MockRecord({"contractid": "c1", "conditions_json": []})]
    if "loan_stats" in q:
        return []

    # 3. CX User
    if "comex_trade_orders" in q:
        if "to_char" in q:  # CSV query
            return [_MockRecord({
                "username": "testuser", "orderid": "o1", "date": "2026-07-15T00:00:00",
                "ticker": "H2O", "type": "BUY", "status": "FILLED", "price": "100.0",
                "currency": "ICA", "filled_amount": "10", "total_value": "1000.0"
            })]
        return [{"Username": "testuser", "Orders": [{"OrderId": "o1", "order_id": "o1", "DateEpochMs": 946684800000, "date_epoch_ms": 946684800000, "Ticker": "H2O", "ticker": "H2O", "Type": "BUY", "type": "BUY", "Status": "FILLED", "status": "FILLED", "Price": 100.0, "price": 100.0, "Currency": "ICA", "currency": "ICA", "FilledAmount": 10, "filled_amount": 10, "TotalValue": 1000.0, "total_value": 1000.0}]}]

    # 4. Flights
    if "ship_flights" in q:
        return [_MockRecord({
            "coalesce": [{"Username": "testuser", "Flights": [{"FlightId": "f1", "ShipId": "s1", "ShipName": "Ship 1", "ShipRegistration": "REG-1", "DepartureTimeEpochMs": 946684800000, "ArrivalTimeEpochMs": 946688400000, "CurrentSegmentIndex": 0, "IsAborted": False, "StlDistance": 10.0, "FtlDistance": 0.0, "Timestamp": datetime(2000, 1, 1, tzinfo=timezone.utc), "UserNameSubmitted": "testuser", "Origin": "P1 (System 1)", "Destination": "P2 (System 1)", "Segments": []}]}]
        })]
    if "ship_flight_segments" in q:
        return [_MockRecord({
            "flight_id": "f1", "segment_index": 0, "segment_type": "STL", "departure": datetime(2000, 1, 1, tzinfo=timezone.utc),
            "arrival": datetime(2000, 1, 2, tzinfo=timezone.utc), "stl_distance": 10.0, "stl_fuel": 1.0, "ftl_distance": 0.0,
            "ftl_fuel": 0.0, "o_sys_id": "sys1", "o_sys_name": "System 1", "o_sys_nat": "S1", "o_pl_id": "p1", "o_pl_name": "Planet 1",
            "o_pl_nat": "P1", "o_st_id": None, "o_st_name": None, "o_st_nat": None, "d_sys_id": "sys1", "d_sys_name": "System 1",
            "d_sys_nat": "S1", "d_pl_id": "p2", "d_pl_name": "Planet 2", "d_pl_nat": "P2", "d_st_id": None, "d_st_name": None, "d_st_nat": None
        })]

    # 5. Production
    if "targetuservars" in q:
        return [_MockRecord({"siteid": "site1", "player_name": "testuser", "location_name": "P1", "is_accurate": True, "productionlineid": "line1", "capacity": 1, "condition": 1.0, "orderid": "o1", "created": datetime(2000, 1, 1, tzinfo=timezone.utc), "completion": datetime(2000, 1, 2, tzinfo=timezone.utc), "order_duration": 3600000, "recipeid": "r1"})]
    if "site_production_lines" in q:
        return [_MockRecord({
            "username": "testuser", "planetid": "p1", "planetnaturalid": "P1", "planetname": "Planet 1", "siteid": "site1",
            "productionlineid": "line1", "type": "RIG", "slots": 1, "capacity": 1, "efficiency": 1.0, "condition": 1.0,
            "xata_updatedat": datetime(2000, 1, 1, tzinfo=timezone.utc),
            "production_orders": '[{"OrderId": "o1", "Created": 946684800000, "Completion": 946688400000, "DurationMs": 3600000, "Halted": false, "Recurring": false, "Completed": 0.0, "Started": 946684800000, "RecipeId": "r1"}]'
        })]
    if "production_recipes" in q:
        return [_MockRecord({"recipe_id": "r1", "line_id": "line1", "duration": 3600000, "name": "Recipe 1", "efficiency": 1.0, "effort_factor": 1.0})]
    if "production_recipe_input_factors" in q:
        return [_MockRecord({"recipe_id": "r1", "line_id": "line1", "ticker": "H2O", "factor": 1.0})]
    if "production_recipe_output_factors" in q:
        return [_MockRecord({"recipe_id": "r1", "line_id": "line1", "ticker": "RAT", "factor": 1.0})]

    # 6. Ships
    if "stlfuelflowrate" in q:
        return [_MockRecord({
            "coalesce": [{"Username": "testuser", "Ships": [{"ShipId": "s1", "Registration": "REG-1", "Name": "Ship 1", "FlightId": None, "CommissioningTime": 946684800000, "Condition": 1.0, "StlFuelFlowRate": 1.0, "AddressSystemId": "sys1", "AddressPlanetId": "p1", "AddressStationId": None, "WeightCapacity": 1000.0, "VolumeCapacity": 1000.0, "UserNameSubmitted": "testuser", "SystemName": "System 1", "SystemNaturalId": "S1", "PlanetName": "Planet 1", "PlanetNaturalId": "P1", "StationName": None, "StationNaturalId": None}]}]
        })]

    # 7. Sites
    if "from sites s" in q:
        return [_MockRecord({
            "coalesce": [{"Username": "testuser", "Sites": [{"SiteId": "site1", "PlanetId": "p1", "PlanetIdentifier": "P1", "PlanetName": "Planet 1", "PlanetFoundedEpochMs": 946684800000, "InvestedPermits": 1, "MaximumPermits": 5, "UserNameSubmitted": "testuser", "Timestamp": datetime(2000, 1, 1, tzinfo=timezone.utc), "Buildings": []}]}]
        })]

    # 8. Storages
    if "storage_items" in q:
        if "csv" in q or "stream" in q or "st.stationid" in q:
            return [_MockRecord({
                "Username": "testuser", "Location": "Hortus", "Type": "WARHOUSE", "LastUpdated": "2026-07-15T00:00:00",
                "Ticker": "H2O", "Name": "Water", "Category": "Water", "Amount": "100", "TotalWeight": "100.0", "TotalVolume": "100.0"
            })]
        return [{"Username": "testuser", "Storages": [{"StorageId": "st1", "Location": "Hortus", "Type": "WARHOUSE", "LastUpdatedEpochMs": 946684800000, "StorageItems": [{"MaterialId": "m1", "MaterialTicker": "RAT", "MaterialName": "Rations", "MaterialAmount": 10, "TotalWeight": 10.0, "TotalVolume": 10.0}]}]}]

    # 9. Workforce
    if "workforce_needs" in q and "w.population" in q:
        if "jsonb_agg" in q:
            return [_MockRecord({
                "coalesce": [{"Username": "testuser", "Workforce": [{"PlanetId": "p1", "PlanetNaturalId": "P1", "PlanetName": "Planet 1", "SiteId": "site1", "UserNameSubmitted": "testuser", "Timestamp": datetime(2000, 1, 1, tzinfo=timezone.utc), "Workforces": [{"WorkforceTypeName": "PIONEER", "Population": 100, "Reserve": 10, "Capacity": 150, "Required": 50, "Satisfaction": 1.0, "WorkforceNeeds": []}]}]}]
            })]
        return [_MockRecord({
            "username": "testuser",
            "planetname": "Planet 1",
            "planetnaturalid": "P1",
            "siteid": "site1",
            "workforce_type": "PIONEER",
            "population": "100",
            "category": "Food",
            "ticker": "RAT",
            "essential": True,
            "need_satisfaction": "1.0",
            "unitsperinterval": "1.0"
        })]
    if "usermaterialtotals" in q:
        return [_MockRecord({
            "siteid": "userid1",
            "player_name": "testuser",
            "is_accurate": True,
            "location_name": "Workforce",
            "needs": [{"ticker": "RAT", "unitsperinterval": 10.0}]
        })]
    if "workforces w" in q:
        return [_MockRecord({
            "coalesce": [{"Username": "testuser", "Workforce": [{"PlanetId": "p1", "PlanetNaturalId": "P1", "PlanetName": "Planet 1", "SiteId": "site1", "UserNameSubmitted": "testuser", "Timestamp": datetime(2000, 1, 1, tzinfo=timezone.utc), "Workforces": [{"WorkforceTypeName": "PIONEER", "Population": 100, "Reserve": 10, "Capacity": 150, "Required": 50, "Satisfaction": 1.0, "WorkforceNeeds": []}]}]}]
        })]

    # 10. Public / Corporation
    if "cx_brokers" in q or "cx_brokers_buy_orders" in q or "fetch_pivoted_market_data" in q:
        if "csv" in q:
            return [_MockRecord({"Ticker": "H2O", "last_update": "2026-07-15"})]
        return [_MockRecord({"Ticker": "H2O", "last_update": "2026-07-15", "MMBuy": 100.0, "MMSell": 100.0, "AI1-Average": 100.0, "AI1-AskAmt": 100.0, "AI1-AskPrice": 100.0, "AI1-AskAvail": 100.0, "AI1-BidAmt": 100.0, "AI1-BidPrice": 100.0, "AI1-BidAvail": 100.0, "CI1-Average": 100.0, "CI1-AskAmt": 100.0, "CI1-AskPrice": 100.0, "CI1-AskAvail": 100.0, "CI1-BidAmt": 100.0, "CI1-BidPrice": 100.0, "CI1-BidAvail": 100.0, "CI2-Average": 100.0, "CI2-AskAmt": 100.0, "CI2-AskPrice": 100.0, "CI2-AskAvail": 100.0, "CI2-BidAmt": 100.0, "CI2-BidPrice": 100.0, "CI2-BidAvail": 100.0, "NC1-Average": 100.0, "NC1-AskAmt": 100.0, "NC1-AskPrice": 100.0, "NC1-AskAvail": 100.0, "NC1-BidAmt": 100.0, "NC1-BidPrice": 100.0, "NC1-BidAvail": 100.0, "NC2-Average": 100.0, "NC2-AskAmt": 100.0, "NC2-AskPrice": 100.0, "NC2-AskAvail": 100.0, "NC2-BidAmt": 100.0, "NC2-BidPrice": 100.0, "NC2-BidAvail": 100.0, "IC1-Average": 100.0, "IC1-AskAmt": 100.0, "IC1-AskPrice": 100.0, "IC1-AskAvail": 100.0, "IC1-BidAmt": 100.0, "IC1-BidPrice": 100.0, "IC1-BidAvail": 100.0})]
    
    if "material_recipes" in q or "material_recipe_ingredients" in q:
        return [_MockRecord({"final_payload": '[{"RecipeName": "Water extraction"}]'})]
    if "material_categories" in q:
        return [_MockRecord({"Ticker": "H2O", "Name": "Water", "Category": "Water", "Weight": 1.0, "Volume": 1.0})]

    if "naturalid as \"planetnaturalid\"" in q:
        return [_MockRecord({"PlanetNaturalId": "p1", "PlanetName": "Planet 1"})]
    if "planet_physical_data" in q or "planet_orbit" in q or "planet_resources" in q:
        return [_MockRecord({"coalesce": '[{"PlanetId": "p1"}]'})]
    
    if "c.founder" in q or "c.officers" in q:
        return [_MockRecord({"founder": "acct1", "officers": [], "displayname": "testuser", "companycode": "FAKE"})]
    if "ship_build_presets" in q:
        return [_MockRecord({"id": "preset1", "name": "Preset1", "price": 100.0, "price_corp": 90.0, "parts": "[]", "is_admin_preset": False, "created_by": "user1", "created_at": datetime(2000,1,1)})]
    if "corp_ship_orders" in q:
        return [_MockRecord({"id": 1, "corporation_code": "COSM", "customer_username": "testuser", "customer_company_code": "FAKE", "ship_config": "{}", "price": 100.0, "wait_time_days": 1, "status": "QUEUED", "notes": "", "completed_at": None, "created_at": datetime(2000,1,1), "owner_id": "acct1"})]
    if "is_synchronized" in q:
        return [_MockRecord({
            "corporationid": "corp1",
            "companycode": "FAKE",
            "companyname": "fake co",
            "is_synchronized": True,
            "last_active": datetime(2000, 1, 1, tzinfo=timezone.utc),
            "joineddate": datetime(2000, 1, 1, tzinfo=timezone.utc),
            "accountid": "acct1"
        })]
    if "cs.companycode" in q or "cs.companyname" in q:
        return [_MockRecord({"name": "fake corp", "code": "FC", "companycode": "FAKE", "companyname": "fake co"})]
    if "public_users_data" in q:
        return [_MockRecord({"coalesce": '{"CompanyName": "fake co"}'})]
    if "building_build_materials" in q or "building_workforce_capacities" in q:
        return [_MockRecord({"coalesce": '[{"BuildingTicker": "RIG"}]'})]

    return None

class _MockRecord:
    def __init__(self, data: dict):
        self._data = data

    def __getitem__(self, key: typing.Any) -> typing.Any:
        if isinstance(key, int):
            return list(self._data.values())[key]
        return self._data.get(key)

    def get(self, key: str, default: typing.Any = None) -> typing.Any:
        return self._data.get(key, default)

    def values(self) -> typing.Any:
        return self._data.values()

    def keys(self) -> typing.Any:
        return self._data.keys()

class _MockCursor:
    def __init__(self, rows: list):
        self.rows = iter(rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            row = next(self.rows)
            return row
        except StopIteration:
            raise StopAsyncIteration

class _MockConnection:
    def __init__(self, raw_connection: asyncpg.Connection):
        self.raw_connection = raw_connection

    async def execute(self, query: str, *args, **kwargs) -> typing.Any:
        stub = get_query_stub(query, args)
        if stub is not None:
            return "SELECT 1"
        return await self.raw_connection.execute(query, *args, **kwargs)

    async def fetch(self, query: str, *args, **kwargs) -> typing.Any:
        stub = get_query_stub(query, args)
        if stub is not None:
            if isinstance(stub, str):
                return [_MockRecord({"val": stub})]
            return stub
        return await self.raw_connection.fetch(query, *args, **kwargs)

    async def fetchrow(self, query: str, *args, **kwargs) -> typing.Any:
        stub = get_query_stub(query, args)
        if stub is not None:
            if isinstance(stub, str):
                return _MockRecord({"val": stub})
            return stub[0] if stub else None
        return await self.raw_connection.fetchrow(query, *args, **kwargs)

    async def fetchval(self, query: str, *args, **kwargs) -> typing.Any:
        stub = get_query_stub(query, args)
        if stub is not None:
            if isinstance(stub, str):
                return stub
            if stub and isinstance(stub[0], _MockRecord):
                return list(stub[0]._data.values())[0]
            return stub
        return await self.raw_connection.fetchval(query, *args, **kwargs)

    async def executemany(self, query: str, args: list, **kwargs) -> typing.Any:
        stub = get_query_stub(query, args)
        if stub is not None:
            return
        return await self.raw_connection.executemany(query, args, **kwargs)

    def cursor(self, query: str, *args, **kwargs) -> typing.Any:
        stub = get_query_stub(query, args)
        if stub is not None:
            if isinstance(stub, str):
                stub = [_MockRecord({"val": stub})]
            return _MockCursor(stub)
        return self.raw_connection.cursor(query, *args, **kwargs)

    def transaction(self) -> typing.Any:
        return self.raw_connection.transaction()

class _MockDatabase:
    def __init__(self, connection: _MockConnection):
        self.connection = connection
        self.pool = _SingleConnectionPool(connection)
        self.poolInit = True
        self.timeout = 10

    async def execute(self, query: str, *args, timeout: float | None = None) -> typing.Any:
        return await self.connection.execute(query, *args, timeout=timeout)

    async def fetch_one(self, query: str, *args, timeout: float | None = None) -> typing.Any:
        return await self.connection.fetchrow(query, *args, timeout=timeout)

    async def fetch_rows(self, query: str, *args, timeout: float | None = None) -> list[asyncpg.Record]:
        return await self.connection.fetch(query, *args, timeout=timeout)

    async def executemany(self, query: str, args: list[list[typing.Any]], timeout: float | None = None) -> None:
        await self.connection.executemany(query, args, timeout=timeout)

class _SingleConnectionPool:
    def __init__(self, connection: _MockConnection):
        self.connection = connection

    def acquire(self) -> _SingleConnectionAcquire:
        return _SingleConnectionAcquire(self.connection)

class _SingleConnectionAcquire:
    def __init__(self, connection: _MockConnection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False

