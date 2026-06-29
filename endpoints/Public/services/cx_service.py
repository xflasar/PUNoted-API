import csv
import json
import logging
from decimal import Decimal
from io import StringIO

from app.core.redis_client import redis_client
from endpoints.Public.repositories.cx_repo import fetch_pivoted_market_data

logger = logging.getLogger(__name__)

CSV_HEADERS = [
    "Ticker", "last_update", "MMBuy", "MMSell",
    "AI1-Average", "AI1-AskAmt", "AI1-AskPrice", "AI1-AskAvail", "AI1-BidAmt", "AI1-BidPrice", "AI1-BidAvail",
    "CI1-Average", "CI1-AskAmt", "CI1-AskPrice", "CI1-AskAvail", "CI1-BidAmt", "CI1-BidPrice", "CI1-BidAvail",
    "CI2-Average", "CI2-AskAmt", "CI2-AskPrice", "CI2-AskAvail", "CI2-BidAmt", "CI2-BidPrice", "CI2-BidAvail",
    "NC1-Average", "NC1-AskAmt", "NC1-AskPrice", "NC1-AskAvail", "NC1-BidAmt", "NC1-BidPrice", "NC1-BidAvail",
    "NC2-Average", "NC2-AskAmt", "NC2-AskPrice", "NC2-AskAvail", "NC2-BidAmt", "NC2-BidPrice", "NC2-BidAvail",
    "IC1-Average", "IC1-AskAmt", "IC1-AskPrice", "IC1-AskAvail", "IC1-BidAmt", "IC1-BidPrice", "IC1-BidAvail",
]

async def generate_market_data_csv(db) -> str:
    try:
        cache_key = "cx_prices_csv_data"
        
        # 1. Check Redis Cache
        cached_csv = await redis_client.get(cache_key)
        if cached_csv:
            return cached_csv

        records = await fetch_pivoted_market_data(db)

        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(CSV_HEADERS)

        # Write data rows
        for record in records:
            row_data = []
            for header in CSV_HEADERS:
                value = record.get(header)
                # Strict fallback for NULL/Empty DB values
                if value is None or value == "":
                    row_data.append("0")
                else:
                    row_data.append(str(value))

            writer.writerow(row_data)

        csv_string = output.getvalue()
        output.close()

        # Cache in Redis for 30 minutes
        await redis_client.set(cache_key, csv_string, ex=1800)

        return csv_string

    except Exception as e:
        logger.error(f"Failed to generate CSV for market data: {e}", exc_info=True)
        raise


async def generate_json_data(db) -> list:
    try:
        cache_key = "cx_prices_json_data"
        
        # 1. Check Redis Cache (Crucial for frontend performance)
        cached_json = await redis_client.get(cache_key)
        if cached_json:
            return cached_json

        records = await fetch_pivoted_market_data(db)
        json_data = []

        for record in records:
            row_dict = {}
            for header in CSV_HEADERS:
                if header == "last_update":
                    continue
                
                val = record.get(header)
                
                # Strict fallback to prevent React NaN errors from DB NULLs
                if val is None or val == "":
                    row_dict[header] = 0
                elif isinstance(val, Decimal):
                    row_dict[header] = float(val)
                else:
                    row_dict[header] = val
                    
            json_data.append(row_dict)

        # Cache in Redis for 30 minutes (Store as JSON string)
        await redis_client.set(cache_key, json.dumps(json_data), ex=1800)

        return json_data

    except Exception as e:
        logger.error(f"Failed to generate JSON data for market data: {e}", exc_info=True)
        raise