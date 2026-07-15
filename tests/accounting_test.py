from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_accounting(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/accounting", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert response.json() == [{"Username": "testuser", "Accounts": [{"Currency": "ICA", "Balance": 100.0, "BookBalance": 100.0, "Category": "ICA", "Type": "ICA", "AccountNumber": "123", "LastUpdatedEpochMs": 946684800000}]}]

def test_get_accounting_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/accounting/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    assert response.json() == [{"Currency": "ICA", "Balance": 100.0, "BookBalance": 100.0, "Category": "ICA", "Type": "ICA", "AccountNumber": "123", "LastUpdatedEpochMs": 946684800000}]
