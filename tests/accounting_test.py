from __future__ import annotations

import typing

import pytest

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_accounting(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/accounting", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["Username"] == "testuser"
    assert len(data[0]["Accounts"]) == 1
    acc = data[0]["Accounts"][0]
    assert acc["Currency"] == "ICA"
    assert acc["Balance"] == 100.0
    assert str(acc["AccountNumber"]) == "123"

def test_get_accounting_user(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/accounting/user", headers={"X-Data-Token": "ptk_fake"})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["Currency"] == "ICA"
    assert data[0]["Balance"] == 100.0
    assert str(data[0]["AccountNumber"]) == "123"

