from __future__ import annotations

import typing
import pytest
from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_unauthorized_protected_v1(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    # Missing auth token header (X-Data-Token)
    response = client.get("/production/user")
    assert response.status_code == 401

def test_unauthorized_internal(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    # Missing session auth headers
    response = client.get("/internal/production/user_production")
    assert response.status_code in [401, 403]

def test_not_found(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/v1/nonexistent_route_nuked_check")
    assert response.status_code == 404
