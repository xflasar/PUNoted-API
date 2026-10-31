from __future__ import annotations

import typing

from tests.db_fixtures import client, db_savepoint, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_user_storage_internal(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:  # noqa: F811
    response = client.get("/internal/storage/user_storage", headers={"Authorization": "Bearer ptk_fake", "Origin": "http://localhost:5174"})
    assert response.status_code == 200
    assert response.json()["success"] is True


def test_storage_non_company_context_ignored(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    from db_message_handlers.storage_data import handle_storage_data_message
    import main

    payload_gov = {
        "userId": "aaaaaaaa-aa04-aaaa-aaaa-aaaaaaaaaaaa",
        "context": "GOVERNMENT",
        "data": {
            "full_refresh": True,
            "storages": [
                {"storageid": "str_gov_1", "addressableid": "site_gov", "name": "Gov Storage", "type": "WAREHOUSE", "storage_items": []}
            ]
        }
    }

    assert client.portal is not None
    result = client.portal.call(handle_storage_data_message, main.db, payload_gov)
    assert result["success"] is True
    assert "Ignored non-company" in result["message"]


def test_storage_upsert_updates_userid(client: fastapi.testclient.TestClient, db_savepoint: None) -> None:
    from db_message_handlers.storage_data import handle_storage_data_message
    import main

    payload_initial = {
        "userId": "aaaaaaaa-aa04-aaaa-aaaa-aaaaaaaaaaaa",
        "context": "COMPANY",
        "data": {
            "full_refresh": False,
            "storages": [
                {"storageid": "str_test_upsert", "addressableid": "site_test", "name": "Base Storage", "type": "STORE", "storage_items": []}
            ]
        }
    }

    assert client.portal is not None
    res1 = client.portal.call(handle_storage_data_message, main.db, payload_initial)
    assert res1["success"] is True

    # Verify storage exists and points to test_user's resolved userid
    async def check_storage():
        async with main.db.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT userid FROM storages WHERE storageid = 'str_test_upsert'")
            return row["userid"] if row else None

    uid = client.portal.call(check_storage)
    assert uid == "userid1"


