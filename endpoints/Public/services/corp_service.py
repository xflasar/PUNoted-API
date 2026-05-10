import logging
from decimal import Decimal

from endpoints.Public.repositories.corp_repo import fetch_corp_prices

logger = logging.getLogger(__name__)

CSV_HEADERS = [
    "ticker", "price"
]

async def generate_json_data(db) -> list:
    try:
        records = await fetch_corp_prices(db)
        json_data = []

        for record in records:
            # Using the walrus operator (:=) to fetch, assign, and type-check in one optimized step.
            json_data.append({
                header: float(val) if isinstance(val := record.get(header, 0), Decimal) else val
                for header in CSV_HEADERS
            })

        return json_data
    except Exception as e:
        logger.error(f"Failed to generate JSON data for market data: {e}", exc_info=True)
        raise
