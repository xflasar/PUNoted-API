# endpoints/Protected/services/workforce.py
import csv
import io
from typing import List, Optional

from endpoints.Protected.repositories.workforce_repo import fetch_workforce_flat


async def generate_workforce_csv(db, usernames_list: List[str], location: Optional[str] = None) -> io.StringIO:
    rows = await fetch_workforce_flat(db, usernames_list, location)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Username",
        "PlanetName", "PlanetNaturalId",
        "SiteId", "WorkforceType", "Population",
        "NeedCategory", "MaterialTicker", "Essential",
        "NeedSatisfaction", "UnitsPerInterval"
    ])

    for row in rows:
        writer.writerow([
            row["username"],
            row["planetname"],
            row["planetnaturalid"],
            row["siteid"],
            row["workforce_type"],
            row["population"],
            row["category"],
            row["ticker"],
            "True" if row["essential"] else "False",
            row["need_satisfaction"],
            row["unitsperinterval"]
        ])

    output.seek(0)
    return output

async def get_workforce_data(db, usernames_list: List[str], location: Optional[str] = None) -> list:
    from endpoints.Protected.repositories.workforce_repo import fetch_workforce_json
    return await fetch_workforce_json(db, usernames_list, location)
