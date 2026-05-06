import csv
import logging
from io import StringIO
from endpoints.Public.repositories.materials_repo import fetch_materials_data
from app.core.redis_client import redis_client

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