import csv
import json
import logging
from io import StringIO

from app.core.redis_client import redis_client
from endpoints.Public.repositories.materials_repo import fetch_materials_data

logger = logging.getLogger(__name__)

CSV_HEADERS = [
    "Ticker", "Name", "Category", "Weight", "Volume"
]

async def generate_materials_data_csv(db) -> str:
    cache_key = "materials_csv_data"

    try:
        # 1. Check Redis Cache
        cached_csv = await redis_client.get(cache_key)
        if cached_csv:
            return cached_csv

        # 2. Cache Miss - Generate CSV from Database
        records = await fetch_materials_data(db)

        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(CSV_HEADERS)

        # Write data rows
        for record in records:
            row_data = []
            for header in CSV_HEADERS:
                value = record.get(header)

                if value is None or value == "":
                    row_data.append("0")
                else:
                    row_data.append(str(value))

            writer.writerow(row_data)

        csv_string = output.getvalue()
        output.close()

        # 3. Store in Redis Cache
        await redis_client.set(cache_key, csv_string, ex=86400)

        return csv_string

    except Exception as e:
        logger.error(f"Failed to generate CSV for materials data: {e}", exc_info=True)
        raise

async def generate_materials_data_json(db) -> str:
    cache_key = "materials_json_data"

    try:
        # 1. Check Redis Cache
        cached_json = await redis_client.get(cache_key)
        if cached_json:
            return cached_json

        # 2. Cache Miss - Generate JSON from Database
        records = await fetch_materials_data(db)

        processed_data = []
        for record in records:
            # Safely cast to float, preventing crashes if DB returns None or ""
            try:
                weight = float(record.get("Weight") or 0.0)
            except ValueError:
                weight = 0.0

            try:
                volume = float(record.get("Volume") or 0.0)
            except ValueError:
                volume = 0.0

            # Map to lowercase keys for standard frontend JSON consumption
            processed_data.append({
                "ticker": record.get("Ticker", ""),
                "name": record.get("Name", ""),
                "category": record.get("Category", ""),
                "weight": weight,
                "volume": volume
            })

        # Convert the list of dictionaries to a JSON string
        json_string = json.dumps(processed_data)

        # 3. Store in Redis Cache
        await redis_client.set(cache_key, json_string, ex=86400)

        return json_string

    except Exception as e:
        logger.error(f"Failed to generate JSON for materials data: {e}", exc_info=True)
        raise