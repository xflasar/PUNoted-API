from __future__ import annotations

import typing

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_user_storage_internal(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/internal/storage/user_storage", headers={"Authorization": "Bearer ptk_fake", "Origin": "http://localhost:5174"})
    assert response.status_code == 200
    assert response.json()["success"] is True
