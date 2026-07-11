from __future__ import annotations

import typing
from types import SimpleNamespace

import asyncpg
import fastapi.testclient
import pytest

import main

if typing.TYPE_CHECKING:
    import asyncpg.transaction

@pytest.fixture(scope="module")
def client() -> typing.Iterator[fastapi.testclient.TestClient]:
    with fastapi.testclient.TestClient(main.v1_app) as test_client:
        yield test_client

@pytest.fixture(scope="module")
def db_setup(client: fastapi.testclient.TestClient) -> typing.Iterator[None]:
    assert client.portal is not None
    connection, transaction = client.portal.call(prepare_test_db, main.v1_app)
    try:
        yield
    finally:
        client.portal.call(cleanup_test_db, connection, transaction)

async def prepare_test_db(app) -> tuple[asyncpg.Connection, asyncpg.transaction.Transaction]:
    connection: asyncpg.Connection = await asyncpg.connect("postgresql://punoted:pass@127.0.0.1:5445/punoted")
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
            corporationid text NOT NULL
        ) ON COMMIT DROP;

        CREATE TEMP TABLE corporations (
            id text PRIMARY KEY,
            name text NOT NULL,
            code text NOT NULL
        ) ON COMMIT DROP;

        INSERT INTO users (accountid, username, userdataid, xata_id)
        VALUES ('acct1', 'testuser', 'userid1', 'fakexataid');

        INSERT INTO user_api_tokens (user_id, group_id, token_hash)
        VALUES ('acct1', NULL, 'ptk_fake');

        INSERT INTO company_data (userdataid, companyid, companycode, companyname, xata_updatedat)
        VALUES ('userid1', 'company1', 'FAKE', 'fake co', TIMESTAMPTZ '2000-01-01 00:00:00+00');

        INSERT INTO corporations (id, name, code)
        VALUES ('corp1', 'fake corp', 'FC');

        INSERT INTO corporation_shareholders (companyid, corporationid)
        VALUES ('company1', 'corp1');
        """
    )

    app.state.db = SimpleNamespace(pool=_SingleConnectionPool(connection))
    return connection, transaction

async def cleanup_test_db(connection: asyncpg.Connection, transaction: asyncpg.transaction.Transaction) -> None:
    await transaction.rollback()
    await connection.close()

class _SingleConnectionPool:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    def acquire(self) -> _SingleConnectionAcquire:
        return _SingleConnectionAcquire(self.connection)

class _SingleConnectionAcquire:
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False
