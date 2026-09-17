import json
from typing import Optional
from endpoints.Public.repositories.governance_repo import fetch_planet_government_terms, fetch_planet_motions

async def get_governance_terms_data(db, planet: Optional[str] = None, termid: Optional[str] = None) -> str:
    data = await fetch_planet_government_terms(db, planet=planet, termid=termid)
    return json.dumps(data, default=str)

async def get_governance_motions_data(
    db, 
    planet: Optional[str] = None, 
    motionid: Optional[str] = None, 
    status: Optional[str] = None,
    full: bool = False
) -> str:
    data = await fetch_planet_motions(db, planet=planet, motionid=motionid, status=status, full=full)
    return json.dumps(data, default=str)
