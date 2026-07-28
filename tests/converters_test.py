from __future__ import annotations

import pytest
import datetime
from typing import Any, Dict

import data_converter
import converters

# ==============================================================================
# SAMPLE TEST PAYLOADS
# ==============================================================================

USER_PAYLOAD = {
    "payload": {
        "id": "user123",
        "username": "martin_flasar",
        "companyId": "COSM",
        "subscriptionLevel": "PRO",
        "subscriptionExpiry": {"timestamp": 1785123456789},
        "created": {"timestamp": 1600000000000},
        "preferredLocale": "en-US",
        "highestTier": "tier3",
        "isPayingUser": True,
        "isMuted": False
    }
}

COMPANY_PAYLOAD = {
    "payload": {
        "id": "COSM",
        "name": "Cosmic Corp",
        "code": "CSM",
        "startingLocation": {
            "lines": [
                {"entity": {"id": "sys1"}},
                {"entity": {"id": "planet1"}}
            ]
        },
        "startingProfile": "Victualler",
        "countryId": "country1",
        "representation": {
            "contributedNextLevel": {"amount": 100.0, "currency": "ICA"},
            "contributedTotal": {"amount": 500.0, "currency": "ICA"},
            "currentLevel": 2,
            "costNextLevel": {"amount": 1000.0, "currency": "ICA"},
            "leftNextLevel": {"amount": 900.0, "currency": "ICA"},
            "contributors": []
        },
        "ratingReport": {
            "contractCount": 42,
            "earliestContract": {"timestamp": 1610000000000},
            "overallRating": "A"
        },
        "headquarters": {
            "address": {
                "lines": [
                    {"entity": {"id": "sys1"}},
                    {"entity": {"id": "planet1"}}
                ]
            },
            "level": 3,
            "nextRelocationTime": {"timestamp": 1720000000000},
            "relocationLocked": False,
            "basePermits": 5,
            "usedBasePermits": 2,
            "additionalBasePermits": 0,
            "additionalProductionQueueSlots": 1,
            "inventory": {
                "items": [
                    {"material": {"id": "RAT"}, "amount": 10, "limit": 100}
                ]
            },
            "efficiencyGains": [
                {"category": "metallurgy", "gain": 0.05}
            ],
            "efficiencyGainsNextLevel": [
                {"category": "metallurgy", "gain": 0.07}
            ]
        }
    }
}

STORAGE_PAYLOAD = {
    "payload": {
        "stores": [
            {
                "id": "store123",
                "addressableId": "site123",
                "name": "Main Depot",
                "weightLoad": 500.0,
                "weightCapacity": 1000.0,
                "volumeLoad": 300.0,
                "volumeCapacity": 800.0,
                "fixed": True,
                "tradeStore": False,
                "rank": 1,
                "locked": False,
                "type": "WAREHOUSE",
                "items": [
                    {
                        "id": "RAT",
                        "weight": 1.0,
                        "volume": 1.0,
                        "type": "REGULAR",
                        "quantity": {
                            "amount": 100,
                            "value": {"amount": 50.0, "currency": "ICA"}
                        }
                    },
                    {
                        "id": "BLOCKED_ITEM",
                        "weight": 0.0,
                        "volume": 0.0,
                        "type": "BLOCKED",
                        "quantity": None
                    }
                ]
            }
        ]
    }
}

PLANET_PAYLOAD = {
    "payload": {
        "planetId": "planet123",
        "planetName": "Arrakis",
        "planetNaturalId": "ARK",
        "namingDate": {"timestamp": 1600000000000},
        "namingLocked": True,
        "nameable": True,
        "namer": {"username": "Paul"},
        "governorId": "gov123",
        "governorName": "Paul",
        "currency": "ICA",
        "country": {"name": "Imperium", "code": "IMP"},
        "address": {
            "lines": [
                {"entity": {"id": "sys123"}, "type": "SYSTEM"}
            ]
        },
        "data": {
            "mass": 1.2,
            "radius": 6000.0,
            "surfaceGravity": 9.8,
            "escapeVelocity": 11.2,
            "pressure": 1.0,
            "temperature": 300.0,
            "lowTemperature": 250.0,
            "highTemperature": 350.0,
            "sunId": "sun123",
            "orbitIndex": 2,
            "sunlight": 1.0,
            "surface": "DESERT",
            "plots": 50,
            "fertility": 0.1,
            "orbit": {
                "semiMajorAxis": 1.5,
                "eccentricity": 0.05,
                "inclination": 1.0,
                "rightAscension": 45.0,
                "periapsis": 90.0
            },
            "resources": [
                {"materialId": "SPICE", "type": "MINERAL", "factor": 1.5}
            ]
        },
        "buildOptions": {
            "options": [
                {"siteType": "FARM", "billOfMaterial": {}}
            ]
        },
        "projects": [
            {"type": "INFRASTRUCTURE", "entityId": "proj1"}
        ],
        "localRules": {
            "productionFees": {
                "fees": [
                    {"category": "metallurgy", "workforceLevel": "PIONEER", "fee": {"amount": 10.0, "currency": "ICA"}}
                ]
            }
        },
        "celestialBodies": [
            {
                "id": "cb1",
                "name": "Moon A",
                "naturalId": "MOON_A",
                "address": {
                    "lines": [
                        {"entity": {"id": "sys123"}, "type": "SYSTEM"},
                        {"entity": {"id": "planet123"}, "type": "PLANET"}
                    ]
                },
                "orbit": {
                    "semiMajorAxis": 1000.0,
                    "eccentricity": 0.01,
                    "inclination": 0.0,
                    "rightAscension": 0.0,
                    "periapsis": 0.0
                }
            }
        ]
    }
}

SHIPS_PAYLOAD = {
    "payload": {
        "ships": [
            {
                "id": "ship123",
                "idShipStore": "store1",
                "idStlFuelStore": "store2",
                "idFtlFuelStore": "store3",
                "registration": "NCC-1701",
                "name": "Enterprise",
                "commissioningTime": {"timestamp": 1600000000000},
                "blueprintNaturalId": "bp_nat_1",
                "blueprint": {"id": "bp123"},
                "flightId": None,
                "address": {
                    "lines": [
                        {"entity": {"type": "System", "id": "sys1"}, "type": "SYSTEM"},
                        {"entity": {"type": "Planet", "id": "planet1"}, "type": "PLANET"}
                    ]
                },
                "lastRepair": {"timestamp": 1610000000000},
                "acceleration": 10.0,
                "thrust": 100.0,
                "mass": 1000.0,
                "operatingEmptyMass": 800.0,
                "volume": 200.0,
                "reactorPower": 50.0,
                "emitterPower": 10.0,
                "stlFuelFlowRate": 1.0,
                "operatingTimeStl": {"millis": 500000},
                "operatingTimeFtl": {"millis": 100000},
                "condition": 0.95,
                "status": "OPERATIONAL",
                "type": "CORVETTE",
                "repairMaterials": [
                    {"material": {"id": "RAT"}, "amount": 5}
                ]
            }
        ]
    }
}

PRODUCTION_PAYLOAD = {
    "payload": {
        "siteId": "site123",
        "productionLines": [
            {
                "id": "line123",
                "siteId": "site123",
                "type": "FARM",
                "capacity": 100.0,
                "slots": 2,
                "efficiency": 1.0,
                "condition": 1.0,
                "orders": [],
                "productionTemplates": [],
                "efficiencyFactors": [],
                "workforces": []
            }
        ]
    }
}

WORKFORCE_PAYLOAD = {
    "payload": {
        "siteId": "site123",
        "workforces": [
            {
                "id": "wf123",
                "level": "PIONEER",
                "population": 100,
                "reserve": 10,
                "capacity": 150,
                "required": 50,
                "satisfaction": 1.0,
                "needs": []
            }
        ]
    }
}

CONTRACTS_PAYLOAD = {
    "payload": {
        "contracts": [
            {
                "id": "contract123",
                "name": "CON-001",
                "party": "CSM",
                "partner": {"id": "part1", "code": "PT1", "name": "Partner One"},
                "status": "ACCEPTED",
                "date": {"timestamp": 1600000000000},
                "dueDate": {"timestamp": 1610000000000},
                "canPull": True,
                "canPropose": False,
                "canReject": False,
                "canAccept": False,
                "canTerminate": True,
                "preamble": "Contract Preamble",
                "preambleSol": "Sol Preamble",
                "totalValue": {"amount": 1000.0, "currency": "ICA"},
                "mtime": {"timestamp": 1605000000000},
                "conditions": []
            }
        ]
    }
}

# ==============================================================================
# TEST CASES (1:1 PARITY ASSERTIONS)
# ==============================================================================

def test_convert_users_data_table() -> None:
    res_orig = data_converter.convert_users_data_table(USER_PAYLOAD)
    res_ref = converters.convert_users_data_table(USER_PAYLOAD)
    assert res_orig == res_ref
    assert res_ref[0]["userid"] == "user123"

def test_convert_company_data() -> None:
    res_orig = data_converter.convert_company_data(COMPANY_PAYLOAD)
    res_ref = converters.convert_company_data(COMPANY_PAYLOAD)
    
    if "representation" in res_orig:
        res_orig["representation"].pop("representationid", None)
    if "representation" in res_ref:
        res_ref["representation"].pop("representationid", None)
        
    assert res_orig == res_ref
    assert res_ref["company_data"]["companyid"] == "COSM"

def test_convert_storages_data() -> None:
    res_orig = data_converter.convert_storages_data(STORAGE_PAYLOAD)
    res_ref = converters.convert_storages_data(STORAGE_PAYLOAD)
    
    for s in res_orig["storages"]:
        s.pop("xata_updatedat", None)
    for s in res_ref["storages"]:
        s.pop("xata_updatedat", None)
        
    assert res_orig == res_ref

def test_convert_planets_data() -> None:
    res_orig = data_converter.convert_planets_data(PLANET_PAYLOAD)
    res_ref = converters.convert_planets_data(PLANET_PAYLOAD)
    
    # Strip dates and dynamic update timestamps
    for p in res_orig["planets"]:
        p.pop("xata_updatedat", None)
    for p in res_ref["planets"]:
        p.pop("xata_updatedat", None)
        
    assert res_orig == res_ref

def test_convert_ships_data() -> None:
    res_orig = data_converter.convert_ships_data(SHIPS_PAYLOAD)
    res_ref = converters.convert_ships_data(SHIPS_PAYLOAD)
    assert res_orig == res_ref

def test_convert_production_lines_data() -> None:
    res_orig = data_converter.convert_production_lines_data(PRODUCTION_PAYLOAD)
    res_ref = converters.convert_production_lines_data(PRODUCTION_PAYLOAD)
    assert res_orig == res_ref

def test_convert_workforces_data() -> None:
    res_orig = data_converter.convert_workforces_data(WORKFORCE_PAYLOAD)
    res_ref = converters.convert_workforces_data(WORKFORCE_PAYLOAD)
    assert res_orig == res_ref

def test_convert_contracts_payload() -> None:
    res_orig = data_converter.convert_contracts_payload(CONTRACTS_PAYLOAD)
    res_ref = converters.convert_contracts_payload(CONTRACTS_PAYLOAD)
    assert res_orig == res_ref
