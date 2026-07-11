from __future__ import annotations

import typing

from tests.db_fixtures import client, db_setup  # noqa: F401

if typing.TYPE_CHECKING:
    import fastapi.testclient

def test_get_company_data(client: fastapi.testclient.TestClient, db_setup: None) -> None:  # noqa: F811
    response = client.get("/user/companydata", headers={"X-Data-Token": "ptk_fake"})

    assert response.status_code == 200
    assert response.json() == [
        {
            "Username": "testuser",
            "Company": {
                "CompanyId": "company1",
                "CompanyCode": "FAKE",
                "CompanyName": "fake co",
                "CorporationId": "corp1",
                "CorporationCode": "FC",
                "CorporationName": "fake corp",
                "Timestamp": 946684800000,
            },
        }
    ]
