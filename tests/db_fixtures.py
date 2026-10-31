from __future__ import annotations

import typing
import unittest.mock
from types import SimpleNamespace


class MockRedis:
    def __init__(self, *args, **kwargs):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        self.store[key] = str(value)
        return True

    async def delete(self, *keys):
        count = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                count += 1
        return count

    async def flushdb(self):
        self.store.clear()
        return True

    async def exists(self, *keys):
        count = 0
        for key in keys:
            if key in self.store:
                count += 1
        return count

    @classmethod
    def from_url(cls, *args, **kwargs):
        return cls()

mock_redis_async = MockRedis()
unittest.mock.patch("redis.asyncio.from_url", return_value=mock_redis_async).start()
unittest.mock.patch("app.core.redis_client.redis_client", mock_redis_async).start()
unittest.mock.patch("db.Database.create_pool").start()
unittest.mock.patch("db.Database.close_pool").start()

from datetime import datetime, timezone
from pathlib import Path

import asyncpg

import fastapi.testclient
import pytest

import config
import main

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
    import os
    await mock_redis_async.flushdb()

    # Mount internal and public data routes to v1_app for testing purposes to avoid cross-loop issues
    from routers.internal.production import production_router
    from routers.internal.storage import storage_router
    from routers.internal.banking import banking_router
    from endpoints.Public.routers.shipments import shipments_router as shipments_public_external_router
    from endpoints.Public.routers.banking import public_banking_router as public_banking_external_router
    from endpoints.Protected.routers.banking import protected_banking_router as api_banking_external_router
    if not any(hasattr(r, "path") and r.path.startswith("/internal/production") for r in app.routes):
        app.include_router(production_router, prefix="/internal/production")
        app.include_router(storage_router, prefix="/internal/storage")
    if not any(hasattr(r, "path") and r.path.startswith("/internal/banking") for r in app.routes):
        app.include_router(banking_router, prefix="/internal/banking")
    if not any(hasattr(r, "path") and r.path.startswith("/public/shipments") for r in app.routes):
        app.include_router(shipments_public_external_router, prefix="/public/shipments")
    if not any(hasattr(r, "path") and r.path.startswith("/banking") for r in app.routes):
        app.include_router(public_banking_external_router, prefix="/banking")
        app.include_router(api_banking_external_router, prefix="/api/banking")

    dsn = os.getenv("TEST_DATABASE_URL") or "postgresql://punoted:pass@127.0.0.1:5446/punoted_test"
    if dsn and "localhost" in dsn:
        dsn = dsn.replace("localhost", "127.0.0.1")
    connection: asyncpg.Connection = await asyncpg.connect(dsn)

    # Recreate the public schema to ensure each test run starts with an empty DB
    if "test" in dsn or "5446" in dsn:
        await connection.execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")

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

    schema_path = Path(__file__).parent / "test_db_schema.sql"
    await connection.execute(schema_path.read_text(encoding="utf-8"))

    mock_conn = _MockConnection(connection)
    mock_db = _MockDatabase(mock_conn)
    app.state.db = mock_db
    main.app.state.db = mock_db
    main.v1_app.state.db = mock_db
    main.db.pool = mock_db.pool
    main.db.poolInit = True
    main.db.fetch_one = mock_db.fetch_one
    main.db.fetch_rows = mock_db.fetch_rows
    main.db.execute = mock_db.execute
    main.db.executemany = mock_db.executemany
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

class _MockConnection:
    def __init__(self, raw_connection: asyncpg.Connection):
        self.raw_connection = raw_connection

    async def execute(self, query: str, *args, **kwargs) -> typing.Any:
        return await self.raw_connection.execute(query, *args, **kwargs)

    async def fetch(self, query: str, *args, **kwargs) -> typing.Any:
        return await self.raw_connection.fetch(query, *args, **kwargs)

    async def fetchrow(self, query: str, *args, **kwargs) -> typing.Any:
        return await self.raw_connection.fetchrow(query, *args, **kwargs)

    async def fetchval(self, query: str, *args, **kwargs) -> typing.Any:
        return await self.raw_connection.fetchval(query, *args, **kwargs)

    async def executemany(self, query: str, args: list, **kwargs) -> typing.Any:
        return await self.raw_connection.executemany(query, args, **kwargs)

    def cursor(self, query: str, *args, **kwargs) -> typing.Any:
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

