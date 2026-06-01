# data_handlers.py

import asyncio
import csv
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from io import StringIO
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Path,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.requests import ClientDisconnect

from auth import get_current_user_id
from db import Database
from helpers.production_lines import get_production_data_nested

data_router = APIRouter()
logger = logging.getLogger(__name__)


# The message object with a flexible payload
class Message(BaseModel):
    messageType: str
    payload: Dict[str, Any]


# The items inside the nested list
class DataItem(BaseModel):
    id: str
    message: Message


# The top-level payload model
class DataBatchPayload(BaseModel):
    # This expects a list of lists of DataItem objects
    data: List[DataItem]

MAX_PAYLOAD_SIZE = 200 * 1024 * 1024

@data_router.post("/data_batch")
async def data_batch(
    request: Request,
    user_id: str = Depends(get_current_user_id),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    # 1. IMMEDIATE HEADER CHECK (Pre-download)
    # This prevents the server from even trying to download a 1GB file
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_SIZE:
        logger.warning(f"SECURITY: Blocked oversized header ({content_length} bytes) from User: {user_id}")
        raise HTTPException(status_code=413, detail="Payload too large.")

    # 2. SAFE BODY RETRIEVAL
    # Wrapping in try/except handles the 'starlette.requests.ClientDisconnect'
    try:
        body_bytes = await request.body()
    except ClientDisconnect:
        logger.warning(f"Network: Client {user_id} disconnected during upload.")
        return JSONResponse(status_code=408, content={"detail": "Connection closed by client"})
    except Exception as e:
        logger.error(f"Unexpected error during body read for {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during data transfer")

    # 3. SECONDARY SIZE CHECK (Post-Decompression)
    original_size = request.scope.get("original_gzip_size", len(body_bytes))
    if original_size > MAX_PAYLOAD_SIZE:
        logger.warning(f"SECURITY: Blocked oversized payload ({original_size} bytes) from User: {user_id}")
        raise HTTPException(status_code=413, detail="Payload too large.")

    # 4. PARSE JSON
    try:
        payload = json.loads(body_bytes)
    except Exception as e:
        # Get REAL IP for security logging
        x_forwarded_for = request.headers.get("X-Forwarded-For")
        ip_address = x_forwarded_for.split(",")[0].strip() if x_forwarded_for else (request.client.host if request.client else "N/A")

        logger.warning(f"SECURITY: {ip_address} | User: {user_id} | JSON Parse Error: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # 5. VALIDATE STRUCTURE
    items_to_process = payload.get("data")
    if not isinstance(items_to_process, list) or not items_to_process:
        # If the data is bad, we still return a 400 so the extension knows to clear the batch
        raise HTTPException(status_code=400, detail="Payload 'data' must be a non-empty list.")

    # 6. UPDATE USER TIMESTAMP
    try:
        async with request.app.state.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET xata_updatedat = $2 WHERE accountid = $1;",
                user_id,
                datetime.now()
            )
    except Exception as e:
        # We log the error but don't stop processing; data is more important than the timestamp
        logger.error(f"DB Update failed for user {user_id}: {e}")

    # 7. HAND OFF TO BACKGROUND TASK
    from tasks import process_data_batch_task
    background_tasks.add_task(
        process_data_batch_task,
        items_to_process=items_to_process,
        user_id=user_id,
        db=request.app.state.db,
    )

    return {"success": True, "count": len(items_to_process)}

# FIXME: FROM THIS POINT EVERYTHING UNDER THIS IS MIX OF ENDPOINTS THAT NEED TO BE REFACTORED TO THE CORRECT PLACES

@data_router.get("/market_price_all")
async def get_market(request: Request):
    try:
        data = await get_market_data(request.app.state.db)

        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        logger.error(f"Failed to fetch market data: {e}", exc_info=True)
        return {"success": False, "message": "Failed to retrieve market data."}


@data_router.get("/market_price_csv")
async def get_market_csv(request: Request):
    try:
        csv_content = await get_market_data_for_csv(request.app.state.db)

        return Response(
            content=csv_content,
            media_type="text/csv",
            # headers={"Content-Disposition": "attachment; filename=market_data.csv"}
        )
    except Exception as e:
        logger.error(f"Failed to fetch market data: {e}", exc_info=True)
        return {"success": False, "message": "Failed to retrieve market data."}


async def get_market_data(db: Database) -> List[Dict[str, Any]]:
    """
    Fetches market data from the database and handles Decimal serialization.
    """
    sql_query = """
    SELECT
    SPLIT_PART(cb.ticker, '.', 1) AS Ticker,
    COALESCE(SUM(cbb_mm.priceamount), 0) AS MMBuy,
    COALESCE(SUM(cbs_mm.priceamount), 0) AS MMSell,
    
    -- AI1 Data
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.priceaverage END) AS "AI1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.askamount END) AS "AI1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.askprice END) AS "AI1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.bidamount END) AS "AI1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.bidprice END) AS "AI1-BidPrice",
    TO_CHAR(MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb."xata_updatedat" END), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "AI1-UpdatedAt",

    -- CI1 Data
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.priceaverage END) AS "CI1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.askamount END) AS "CI1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.askprice END) AS "CI1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.bidamount END) AS "CI1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.bidprice END) AS "CI1-BidPrice",
    TO_CHAR(MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb."xata_updatedat" END), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CI1-UpdatedAt",

    -- CI2 Data
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.priceaverage END) AS "CI2-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.askamount END) AS "CI2-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.askprice END) AS "CI2-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.bidamount END) AS "CI2-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.bidprice END) AS "CI2-BidPrice",
    TO_CHAR(MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb."xata_updatedat" END), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CI2-UpdatedAt",

    -- NC1 Data
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.priceaverage END) AS "NC1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.askamount END) AS "NC1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.askprice END) AS "NC1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.bidamount END) AS "NC1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.bidprice END) AS "NC1-BidPrice",
    TO_CHAR(MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb."xata_updatedat" END), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "NC1-UpdatedAt",

    -- NC2 Data
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.priceaverage END) AS "NC2-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.askamount END) AS "NC2-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.askprice END) AS "NC2-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.bidamount END) AS "NC2-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.bidprice END) AS "NC2-BidPrice",
    TO_CHAR(MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb."xata_updatedat" END), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "NC2-UpdatedAt",

    -- IC1 Data
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.priceaverage END) AS "IC1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.askamount END) AS "IC1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.askprice END) AS "IC1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.bidamount END) AS "IC1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.bidprice END) AS "IC1-BidPrice",
    TO_CHAR(MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb."xata_updatedat" END), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "IC1-UpdatedAt",

    -- Global Fallback
    TO_CHAR(MAX(cb."xata_updatedat"), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "last_update"

    FROM
    cx_brokers AS cb
    LEFT JOIN
    cx_brokers_buy_orders AS cbb_mm
    ON cb.brokermaterialid = cbb_mm.brokermaterialid AND cbb_mm.tradername = 'Insitor Cooperative Market Maker'
    LEFT JOIN
    cx_brokers_sell_orders AS cbs_mm
    ON cb.brokermaterialid = cbs_mm.brokermaterialid AND cbs_mm.tradername = 'Insitor Cooperative Market Maker'
    WHERE
    cb.ticker IS NOT NULL AND POSITION('.' IN cb.ticker) > 0
    GROUP BY
    SPLIT_PART(cb.ticker, '.', 1)
    ORDER BY
    SPLIT_PART(cb.ticker, '.', 1);
    """

    try:
        async with db.pool.acquire() as con:
            data = await con.fetch(sql_query)

            data_formatted = []
            for record in data:
                row = dict(record)
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)
                data_formatted.append(row)

            return data_formatted

    except Exception as e:
        logger.error(f"Failed to fetch market data: {e}", exc_info=True)
        return []


async def get_market_data_for_csv(db: Database) -> str:
    """
    Fetches market data from the database, pivots it for CSV output,
    and returns it as a CSV string.
    """
    sql_query = """
    SELECT
    SPLIT_PART(cb.ticker, '.', 1) AS Ticker,
    COALESCE(SUM(cbb_mm.priceamount), 0) AS MMBuy,
    COALESCE(SUM(cbs_mm.priceamount), 0) AS MMSell,
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.priceaverage END) AS "AI1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.askamount END) AS "AI1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.askprice END) AS "AI1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.askamount END) AS "AI1-AskAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.bidamount END) AS "AI1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.bidprice END) AS "AI1-BidPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'AI1' THEN cb.bidamount END) AS "AI1-BidAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.priceaverage END) AS "CI1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.askamount END) AS "CI1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.askprice END) AS "CI1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.askamount END) AS "CI1-AskAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.bidamount END) AS "CI1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.bidprice END) AS "CI1-BidPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI1' THEN cb.bidamount END) AS "CI1-BidAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.priceaverage END) AS "CI2-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.askamount END) AS "CI2-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.askprice END) AS "CI2-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.askamount END) AS "CI2-AskAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.bidamount END) AS "CI2-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.bidprice END) AS "CI2-BidPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'CI2' THEN cb.bidamount END) AS "CI2-BidAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.priceaverage END) AS "NC1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.askamount END) AS "NC1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.askprice END) AS "NC1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.askamount END) AS "NC1-AskAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.bidamount END) AS "NC1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.bidprice END) AS "NC1-BidPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC1' THEN cb.bidamount END) AS "NC1-BidAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.priceaverage END) AS "NC2-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.askamount END) AS "NC2-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.askprice END) AS "NC2-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.askamount END) AS "NC2-AskAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.bidamount END) AS "NC2-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.bidprice END) AS "NC2-BidPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'NC2' THEN cb.bidamount END) AS "NC2-BidAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.priceaverage END) AS "IC1-Average",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.askamount END) AS "IC1-AskAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.askprice END) AS "IC1-AskPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.askamount END) AS "IC1-AskAvail",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.bidamount END) AS "IC1-BidAmt",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.bidprice END) AS "IC1-BidPrice",
    MAX(CASE WHEN SPLIT_PART(cb.ticker, '.', 2) = 'IC1' THEN cb.bidamount END) AS "IC1-BidAvail",
    MAX(cb."xata_updatedat") AS "last_update"
FROM
    cx_brokers AS cb
LEFT JOIN
    cx_brokers_buy_orders AS cbb_mm
    ON cb.brokermaterialid = cbb_mm.brokermaterialid AND cbb_mm.tradername = 'Insitor Cooperative Market Maker'
LEFT JOIN
    cx_brokers_sell_orders AS cbs_mm
    ON cb.brokermaterialid = cbs_mm.brokermaterialid AND cbs_mm.tradername = 'Insitor Cooperative Market Maker'
WHERE
    cb.ticker IS NOT NULL AND POSITION('.' IN cb.ticker) > 0
GROUP BY
    SPLIT_PART(cb.ticker, '.', 1)
ORDER BY
    SPLIT_PART(cb.ticker, '.', 1);
    """

    # Define the exact CSV headers in order
    csv_headers = [
        "Ticker",
        "last_update",
        "MMBuy",
        "MMSell",
        "AI1-Average",
        "AI1-AskAmt",
        "AI1-AskPrice",
        "AI1-AskAvail",
        "AI1-BidAmt",
        "AI1-BidPrice",
        "AI1-BidAvail",
        "CI1-Average",
        "CI1-AskAmt",
        "CI1-AskPrice",
        "CI1-AskAvail",
        "CI1-BidAmt",
        "CI1-BidPrice",
        "CI1-BidAvail",
        "CI2-Average",
        "CI2-AskAmt",
        "CI2-AskPrice",
        "CI2-AskAvail",
        "CI2-BidAmt",
        "CI2-BidPrice",
        "CI2-BidAvail",
        "NC1-Average",
        "NC1-AskAmt",
        "NC1-AskPrice",
        "NC1-AskAvail",
        "NC1-BidAmt",
        "NC1-BidPrice",
        "NC1-BidAvail",
        "NC2-Average",
        "NC2-AskAmt",
        "NC2-AskPrice",
        "NC2-AskAvail",
        "NC2-BidAmt",
        "NC2-BidPrice",
        "NC2-BidAvail",
        "IC1-Average",
        "IC1-AskAmt",
        "IC1-AskPrice",
        "IC1-AskAvail",
        "IC1-BidAmt",
        "IC1-BidPrice",
        "IC1-BidAvail",
    ]

    try:
        records = []
        async with db.pool.acquire() as con:
            # Set a lock_timeout in case another process is holding a lock
            await con.execute("SET lock_timeout = '10s';")
            records = await con.fetch(sql_query)

        # Use StringIO to build the CSV string in memory
        output = StringIO()
        writer = csv.writer(output)

        # Write the header row
        writer.writerow(csv_headers)

        # Write data rows
        for record in records:
            row_data = []
            for header in csv_headers:
                # Get value by header name, default to empty string for None
                value = record.get(header)
                # Handle potential float values that might appear like integers
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                row_data.append(str(value) if value is not None else "")
            writer.writerow(row_data)

        csv_string = output.getvalue()
        output.close()
        return csv_string

    except Exception as e:
        logger.error(f"Failed to generate CSV for market data: {e}", exc_info=True)
        raise


@data_router.get("/corp_prices_all")
async def get_corp_prices(request: Request):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as con:
            data = await con.fetch("""
                SELECT ticker, price FROM material_prices;
            """)

            material_data = [{"ticker": record["ticker"], "price": float(record["price"])} for record in data]

    except Exception as e:
        print(f"An error occurred: {e}")
        return JSONResponse(
            content={"success": False, "message": "Failed to retrieve data."},
            status_code=500,
        )

    return JSONResponse(content={"success": True, "data": material_data})


fio_url = "https://rest.fnar.net/storage/Filefolders/0543343118b30be210a472db8c4a13d6"
fio_auth_token = os.environ.get("ffApi")


@data_router.post("/get_ship_production")
async def get_ship_production(request: Request, payload: Optional[Dict[str, bool]] = None):
    """
    Fetches ship production and storage data. Can fetch storage data from either the local database
    or an external FIO API based on the 'fio' flag in the request payload.
    """
    try:
        pool = request.app.state.db.pool
        fio = payload.get("fio", False) if payload else False

        # Acquire a connection from the pool for the database calls
        async with pool.acquire() as conn:
            # Always fetch ship_production data from the database
            records = await conn.fetch(
                "SELECT orderid, orderwaittime, price, shiptype, username, position FROM ship_production ORDER BY orderid ASC"
            )
            ship_orders = [dict(record) for record in records]

            storage_items = []

            """ if fio:
                # Use httpx to make an asynchronous GET request to the FIO API
                headers = {
                    "accept": "application/json",
                    "Authorization": fio_auth_token,
                }

                async with httpx.AsyncClient() as client:
                    try:
                        response = await client.get(fio_url, headers=headers, timeout=10.0)
                        response.raise_for_status()  # Raises an exception for 4xx/5xx responses
                        fio_data = response.json()

                        tickers_of_interest = {
                            "MSL",
                            "FFC",
                            "LHP",
                            "CQL",
                            "QCR",
                            "WCB",
                            "LFL",
                            "HCB",
                            "BR1",
                            "SFE",
                            "MFE",
                            "SSC",
                            "LFE",
                            "FSE",
                            "CQM",
                            "LCB",
                            "VCB",
                            "CQS",
                            "BRS",
                            "SSL",
                        }

                        # Process the StorageItems from the FIO API response
                        if "StorageItems" in fio_data and isinstance(fio_data["StorageItems"], list):
                            for item in fio_data["StorageItems"]:
                                material_ticker = item.get("MaterialTicker")
                                if material_ticker in tickers_of_interest:
                                    storage_items.append(
                                        {
                                            "ticker": material_ticker,
                                            "quantity": item.get("MaterialAmount"),
                                        }
                                    )
                        else:
                            raise ValueError("FIO API response is missing 'StorageItems' or it's not a list.")

                    except httpx.HTTPStatusError as e:
                        logger.error(f"HTTP error fetching FIO data: {e.response.status_code} - {e.response.text}")
                        raise HTTPException(
                            status_code=e.response.status_code,
                            detail=f"FIO API error: {e.response.status_code} - {e.response.text}",
                        )
                    except httpx.RequestError as e:
                        logger.error(f"Network error fetching FIO data: {e}")
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Could not connect to FIO API.",
                        )
                    except (ValueError, KeyError) as e:
                        logger.error(f"Error parsing FIO API response: {e}")
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to parse FIO API response data.",
                        )
            else:
                 """
            # Fallback to fetching storage data from the local database
            storage_records = await conn.fetch("""SELECT
                                                    mt.ticker,
                                                    si.quantity
                                                FROM
                                                    storages AS s
                                                INNER JOIN
                                                    warehouses AS w ON w.warehouseid = s.addressableid
                                                INNER JOIN
                                                    storage_items AS si ON si.storageid = s.storageid
                                                INNER JOIN
                                                    materials AS mt ON mt.materialid = si.materialid
                                                INNER JOIN
                                                    systems AS sys ON w.addresssystem = sys.systemid
                                                INNER JOIN
                                                    users_data AS ud ON ud.userid = s.userid
                                                INNER JOIN
                                                    stations AS st ON st.warehouseid = w.warehouseid
                                                WHERE
                                                    sys.name = 'Hortus'
                                                    AND ud.displayname = 'Filefolders'
                                                    AND st.name != 'Hortus'
                                                    AND mt.ticker IN ('MSL', 'FFC', 'LHP', 'CQL', 'QCR', 'WCB', 'LFL', 'HCB', 'BR1', 'SFE', 'MFE', 'SSC', 'LFE', 'FSE', 'CQM', 'LCB', 'VCB', 'CQS', 'BRS', 'SSL');
                                                """)
            storage_items = [dict(record) for record in storage_records]
        data = {"shiporders": ship_orders, "storageitems": storage_items}

        return JSONResponse(content={"success": True, "data": data})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch ship production data: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": f"Failed to retrieve ship production data: {e}",
            },
        )


@data_router.get("/planet_shipments")
async def get_shipments(request: Request, planetId: str):
    """
    Fetches shipments from the database based on a planet ID.

    Args:
        request: The FastAPI request object.
        planetId: The ID of the planet to filter by.
    """
    try:
        # Get the database pool from the request state (assuming middleware setup)
        pool = request.app.state.db.pool
        if not pool:
            return JSONResponse(
                status_code=500,
                content={"success": False, "message": "Database pool not configured."},
            )

        # Acquire a connection from the pool
        async with pool.acquire() as conn:
            # Query the database for shipments on the specified planetId
            records = await conn.fetch(
                "SELECT * FROM planet_shipments WHERE planetid = $1 ORDER BY id",
                planetId,
            )

            # Convert the asyncpg.Record objects to a list of dictionaries
            shipments = [dict(record) for record in records]

            return JSONResponse(content={"success": True, "data": shipments})

    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"PostgreSQL error when fetching shipments: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "A database error occurred."},
        )
    except Exception as e:
        logger.error(f"Failed to fetch shipments: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected error occurred."},
        )


@data_router.post("/create_vendor_store")
async def create_vendor_store(
    payload: Dict[str, Any],
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """
    Creates a new vendor store and its associated orders in a single transaction.
    Returns the created vendor store object.
    """
    try:
        pool = request.app.state.db.pool

        vendor_data = payload.get("vendor_data", {})
        orders_data = payload.get("materials", [])

        vendor_id = str(uuid.uuid4())

        # Extract vendor details
        company_name = vendor_data.get("companyname")
        game_name = vendor_data.get("gamename")
        company_code = vendor_data.get("companycode")
        corp_name = vendor_data.get("corpname")
        is_active = vendor_data.get("isactive", True)
        cx = vendor_data.get("cx")

        required_fields = {
            "Company Name": company_name,
            "Game Name": game_name,
            "Company Code": company_code,
            "CX": cx,
        }

        for name, value in required_fields.items():
            if isinstance(value, str):
                cleaned_value = value.strip()
            else:
                cleaned_value = value

            if not cleaned_value:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "message": f"Vendor {name} is required.",
                    },
                )

        company_name = company_name.strip() if company_name else None
        game_name = game_name.strip() if game_name else None
        company_code = company_code.strip() if company_code else None
        corp_name = corp_name.strip() if corp_name else None
        cx = cx.strip() if cx else None

        async with pool.acquire() as conn:
            async with conn.transaction():
                vendor_query = """
                INSERT INTO user_vendors (
                    vendorid, userid, companyname, gamename, companycode, isactive, corpname, cx
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8
                );
                """
                try:
                    await conn.execute(
                        vendor_query,
                        vendor_id,
                        user_id,
                        company_name,
                        game_name,
                        company_code,
                        is_active,
                        corp_name,
                        cx,
                    )
                except Exception as e:
                    logger.error(f"Failed on inserting vendorData: {e}.")
                    raise

                order_records = []
                created_orders = []

                for order in orders_data:
                    order_id = str(uuid.uuid4())

                    fixed_price = order.get("fixedprice")
                    reserved_quantity = order.get("reserved")
                    material_id = order.get("materialid")
                    ticker = order.get("ticker")
                    order_type = order.get("orderType")

                    # Tuple for batch insertion
                    order_records.append(
                        (
                            order_id,
                            vendor_id,
                            material_id,
                            ticker,
                            order_type,
                            fixed_price,
                            reserved_quantity,
                        )
                    )

                    created_orders.append(
                        {
                            "orderid": order_id,
                            "vendorid": vendor_id,
                            "materialid": material_id,
                            "materialticker": ticker,
                            "ordertype": order_type,
                            "fixedprice": fixed_price,
                            "reserved": reserved_quantity,
                        }
                    )

                orders_query = """
                INSERT INTO user_vendor_orders (
                    orderid, vendorid, materialid, materialticker, ordertype, fixedprice, reserved
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7
                );
                """
                try:
                    if order_records:
                        await conn.executemany(orders_query, order_records)
                except Exception as e:
                    logger.error(f"Failed to insert orders: {e}")
                    raise

        response_vendor = {
            "vendorid": vendor_id,
            "userid": user_id,
            "companyname": company_name,
            "gamename": game_name,
            "companycode": company_code,
            "isactive": is_active,
            "corpname": corp_name,
            "cx": cx,
        }

        return JSONResponse(
            content={
                "success": True,
                "message": "Vendor store and orders created.",
                "vendor_store": {
                    "vendor": response_vendor,
                    "orders": created_orders,
                },
            }
        )

    except asyncpg.UniqueViolationError:
        logger.warning(f"Attempt to create a second vendor store for user '{user_id}'.")
        return JSONResponse(
            status_code=409,
            content={
                "success": False,
                "message": "A vendor store already exists for this user.",
            },
        )
    except Exception as e:
        logger.error(
            f"Failed to create vendor store and orders for user '{user_id}': {e}",
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": "An unexpected server error occurred.",
            },
        )


def serialize_record(record):
    # This helper function converts non-JSON serializable objects
    return {
        key: value.isoformat() if isinstance(value, datetime) else float(value) if isinstance(value, Decimal) else value
        for key, value in record.items()
    }


@data_router.get("/user_vendor_store")
async def get_user_vendor_stores(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            # Step 1: Fetch vendor
            vendor_record = await conn.fetchrow(
                "SELECT vendorid, companycode, companyname, corpname, cx, gamename, isactive FROM user_vendors WHERE userid = $1;",
                user_id,
            )

            if not vendor_record:
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "message": "No vendor store found."},
                )

            # Step 2: Fetch Inventory (Optimized)
            inventory_records = await conn.fetch(
                """
                SELECT m.ticker, COALESCE(st.stationid, pl.planetid, pl_w.planetid)::text AS location_id, SUM(si.quantity) AS quantity
                FROM storages s
                JOIN storage_items si ON si.storageid = s.storageid
                JOIN materials m ON m.materialid = si.materialid
                JOIN warehouses w ON w.warehouseid = s.addressableid
                LEFT JOIN stations st ON st.warehouseid = w.warehouseid
                LEFT JOIN sites site ON site.siteid = s.addressableid
                LEFT JOIN planets pl ON pl.planetid = site.addressplanetid
                LEFT JOIN planets pl_w ON pl_w.planetid = w.addressplanet
                INNER JOIN users u ON u.userdataid = s.userid
                WHERE u.accountid = $1
                GROUP BY m.ticker, location_id
                """,
                user_id,
            )
            inventory_map = {
                (r["ticker"], r["location_id"]): float(r["quantity"]) for r in inventory_records if r["location_id"]
            }

            # Step 3: Fetch Orders
            orders_records = await conn.fetch(
                """
                SELECT 
                    uvo.orderid, uvo.materialticker, uvo.ordertype, uvo.fixedprice, uvo.reserved, uvo.materialid,
                    COALESCE(uvo.location, '[]'::jsonb) AS locations,
                    mp.price AS corpprice,
                    CASE WHEN uvo.ordertype = 'buy' THEN cxb.askprice ELSE cxb.bidprice END AS cxprice
                FROM user_vendor_orders AS uvo
                LEFT JOIN material_prices AS mp ON mp.ticker = uvo.materialticker
                LEFT JOIN cx_brokers AS cxb ON cxb.ticker = (uvo.materialticker || '.' || $2)
                WHERE uvo.vendorid = $1;
                """,
                vendor_record["vendorid"],
                vendor_record["cx"],
            )

            # --- LOCATION LOOKUP START ---
            import json

            all_location_ids = set()
            parsed_orders = []

            # 3a. Extract IDs
            for r in orders_records:
                r_dict = dict(r)
                raw = r_dict["locations"]
                locs = []
                if raw:
                    locs = json.loads(raw) if isinstance(raw, str) else raw
                r_dict["parsed_locs"] = locs
                parsed_orders.append(r_dict)
                for l in locs:
                    if "id" in l:
                        all_location_ids.add(l["id"])

            # 3b. Fetch Details
            loc_lookup = {}
            if all_location_ids:
                loc_rows = await conn.fetch(
                    """
                    SELECT stationid::text as id, name, naturalid FROM stations WHERE stationid::text = ANY($1)
                    UNION ALL
                    SELECT planetid::text as id, name, naturalid FROM planets WHERE planetid::text = ANY($1)
                """,
                    list(all_location_ids),
                )
                for row in loc_rows:
                    loc_lookup[row["id"]] = {
                        "name": row["name"],
                        "code": row["naturalid"],
                    }
            # --- LOCATION LOOKUP END ---

            # Step 4: Construct Response
            orders_list = []
            for r in parsed_orders:
                final_locations = []
                total_in_store = 0

                for loc in r["parsed_locs"]:
                    lid = loc.get("id")
                    details = loc_lookup.get(lid)

                    # Storage injection
                    storage_qty = inventory_map.get((r["materialticker"], lid), 0.0)
                    total_in_store += storage_qty

                    final_locations.append(
                        {
                            "id": lid,
                            "amount": loc.get("amount", 0),
                            "location_name": details["name"] if details else "Unknown",  # Added
                            "location_code": details["code"] if details else "???",  # Added
                            "storage_amount": storage_qty,
                        }
                    )

                orders_list.append(
                    {
                        "orderid": str(r["orderid"]),
                        "materialid": r["materialid"],
                        "materialticker": r["materialticker"],
                        "ordertype": r["ordertype"],
                        "reserved": int(r["reserved"]) if r["reserved"] else 0,
                        "location": final_locations,
                        "quantity": total_in_store,
                        "price": {
                            "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0,
                            "corpprice": float(r["corpprice"]) if r["corpprice"] else 0,
                            "cxprice": float(r["cxprice"]) if r["cxprice"] else 0,
                        },
                    }
                )

            return JSONResponse(
                content={
                    "success": True,
                    "data": {"vendor": dict(vendor_record), "orders": orders_list},
                }
            )

    except Exception as e:
        logger.error(f"Failed user vendor fetch: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "Server error."})


@data_router.get("/vendor_stores")
async def get_vendor_stores(request: Request):
    try:
        pool = request.app.state.db.pool

        async with pool.acquire() as con:
            # Step 1: Fetch vendors, orders, raw JSON locations, and prices
            vendors_orders_query = """
                SELECT
                	UV.VENDORID,
                	UV.COMPANYCODE,
                	UV.COMPANYNAME,
                	UV.CORPNAME,
                	UV.GAMENAME,
                	UV.ISACTIVE,
                	UV.CX,
                	UVO.ORDERID,
                	UVO.MATERIALTICKER,
                	UVO.ORDERTYPE,
                	UVO.FIXEDPRICE,
                	UVO.RESERVED,
                	COALESCE(UVO.LOCATION, '[]'::JSONB) AS LOCATIONS,
                	MP.PRICE AS CORPPRICE,
                	CASE
                		WHEN UVO.ORDERTYPE = 'buy' THEN CXB.ASKPRICE
                		ELSE CXB.BIDPRICE
                	END AS CXPRICE,

                	-- Calculate activity label (e.g., '15m', '3h', '5d')
                	CASE
                		WHEN EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) < 3600 THEN
                			FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 60)::text || 'm'
                		WHEN EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) < 86400 THEN
                			FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 3600)::text || 'h'
                		ELSE
                			FLOOR(EXTRACT(EPOCH FROM NOW() - U.xata_updatedat) / 86400)::text || 'd'
                	END AS activity_label

                FROM
                	USER_VENDORS AS UV
                	INNER JOIN USER_VENDOR_ORDERS AS UVO ON UVO.VENDORID = UV.VENDORID
                	LEFT JOIN MATERIAL_PRICES AS MP ON MP.TICKER = UVO.MATERIALTICKER
                	LEFT JOIN CX_BROKERS AS CXB ON CXB.TICKER = (UVO.MATERIALTICKER || '.' || UV.CX)
                	INNER JOIN USERS AS U ON U.ACCOUNTID::text = UV.USERID

                -- Only select users who have been active within the last 7 days
                WHERE U.xata_updatedat >= NOW() - INTERVAL '7 days';
            """
            vendors_data = await con.fetch(vendors_orders_query)

            if not vendors_data:
                return JSONResponse(status_code=200, content={"success": True, "vendors": []})

            # --- OPTIMIZATION: Bulk fetch location details ---

            # 1a. Collect all unique Location IDs & Pre-parse JSON
            import json

            all_location_ids = set()
            parsed_orders = []

            for r in vendors_data:
                r_dict = dict(r)
                raw_loc = r_dict["locations"]
                loc_list = []

                if raw_loc:
                    if isinstance(raw_loc, str):
                        try:
                            loc_list = json.loads(raw_loc)
                        except:
                            loc_list = []
                    else:
                        loc_list = raw_loc

                r_dict["parsed_locations"] = loc_list
                parsed_orders.append(r_dict)

                for loc in loc_list:
                    if "id" in loc:
                        all_location_ids.add(loc["id"])

            # 1b. Bulk Lookup Names & Codes
            location_lookup = {}
            if all_location_ids:
                ids_list = list(all_location_ids)
                loc_details_query = """
                    SELECT stationid::text as id, name, naturalid FROM stations WHERE stationid::text = ANY($1)
                    UNION ALL
                    SELECT planetid::text as id, name, naturalid FROM planets WHERE planetid::text = ANY($1)
                """
                loc_rows = await con.fetch(loc_details_query, ids_list)
                for row in loc_rows:
                    location_lookup[row["id"]] = {
                        "name": row["name"],
                        "code": row["naturalid"],
                    }

            # Step 2: Inventory Lookup (Granular per Location)
            # We map (Gamename, Ticker, LocationID) -> Quantity
            gamename_ticker_pairs = [(r["gamename"], r["materialticker"]) for r in vendors_data]
            inventory_map = {}

            if gamename_ticker_pairs:
                # Create a VALUES list for efficient filtering
                values_str = ", ".join([f"('{g}', '{t}')" for g, t in set(gamename_ticker_pairs)])

                # Fetches stock for ALL relevant locations
                inventory_query = f"""
                    SELECT 
                        ud.displayname, 
                        mt.ticker, 
                        COALESCE(st.stationid, pl.planetid)::text AS location_id,
                        SUM(si.quantity) as quantity
                    FROM storages s
                    JOIN users_data ud ON ud.userid = s.userid
                    JOIN storage_items si ON si.storageid = s.storageid
                    JOIN materials mt ON mt.materialid = si.materialid
                    -- Resolve Address (Station OR Planet)
                    LEFT JOIN warehouses w ON w.warehouseid = s.addressableid
                    LEFT JOIN stations st ON st.warehouseid = w.warehouseid
                    LEFT JOIN sites site ON site.siteid = s.addressableid
                    LEFT JOIN planets pl ON pl.planetid = site.addressplanetid
                    -- Filter by Vendor Owners and Tickers
                    JOIN (VALUES {values_str}) AS t(displayname, ticker) 
                      ON ud.displayname = t.displayname AND mt.ticker = t.ticker
                    WHERE 
                        (st.stationid IS NOT NULL OR pl.planetid IS NOT NULL)
                    GROUP BY 1, 2, 3;
                """
                inv_rows = await con.fetch(inventory_query)

                # Key: (Gamename, Ticker, LocationID) -> Amount
                inventory_map = {
                    (r["displayname"], r["ticker"], r["location_id"]): float(r["quantity"]) for r in inv_rows
                }

            # Step 3: Construct Response
            vendors_dict = {}

            for r in parsed_orders:
                vendor_id = r["vendorid"]
                if vendor_id not in vendors_dict:
                    vendors_dict[vendor_id] = {
                        "vendor": {
                            "vendorid": r["vendorid"],
                            "companycode": r["companycode"],
                            "companyname": r["companyname"],
                            "corpname": r["corpname"],
                            "gamename": r["gamename"],
                            "isactive": r["isactive"],
                            "activity": r["activity_label"],
                            "cx": r["cx"],
                        },
                        "orders": [],
                    }

                # Enrich locations & Calculate Total Available (Server-Side Logic)
                final_locations = []
                total_available = 0

                for loc in r["parsed_locations"]:
                    loc_id = loc.get("id")
                    details = location_lookup.get(loc_id)

                    # 1. Get current stock
                    storage_qty = inventory_map.get((r["gamename"], r["materialticker"], loc_id), 0.0)

                    # 2. Get target/reserve amount
                    target_amount = loc.get("amount", 0)

                    # 3. Calculate Availability (Demand or Supply)
                    loc_available = 0
                    if r["ordertype"] == "buy":
                        # Buy Order (Demand): Max(0, Target - Stock)
                        # Example: Wants 100, has 20 -> Demand is 80
                        loc_available = max(0, target_amount - storage_qty)
                    else:
                        # Sell Order (Supply): Max(0, Stock - Reserve)
                        # Example: Has 120, Reserves 100 -> Supply is 20
                        loc_available = max(0, storage_qty - target_amount)

                    total_available += loc_available

                    final_locations.append(
                        {
                            "id": loc_id,
                            "location_name": details["name"] if details else "Unknown",
                            "location_code": details["code"] if details else "???",
                            "available": loc_available,
                        }
                    )

                if total_available <= 0:
                    continue

                order_data = {
                    "orderid": str(r["orderid"]) if r["orderid"] else None,
                    "materialticker": r["materialticker"],
                    "ordertype": r["ordertype"],
                    "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0,
                    "location": final_locations,
                    "price": {
                        "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0,
                        "corpprice": float(r["corpprice"]) if r["corpprice"] else 0,
                        "cxprice": float(r["cxprice"]) if r["cxprice"] else 0,
                    },
                    "available": total_available,  # <--- SEND PRE-CALCULATED TOTAL
                }

                vendors_dict[vendor_id]["orders"].append(order_data)

            return JSONResponse(
                status_code=200,
                content={"success": True, "vendors": list(vendors_dict.values())},
            )

    except Exception as e:
        logger.error(f"Failed to get vendor stores: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "Server error."})


@data_router.post("/materials_price_list")
async def get_materials_price_list(
    payload: Dict[str, Any],
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    try:
        pool = request.app.state.db.pool

        cx = payload.get("cx", None)

        if not cx:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": "The 'cx' field is required in the request payload.",
                },
            )

        current_user_id_str = str(user_id)

        query = """
            -- 1. Find the single storage ID for the authenticated user (if they have one).
            WITH user_single_storage AS (
                SELECT
                    s.storageid
                FROM
                    storages AS s
                INNER JOIN
                    warehouses AS w ON w.warehouseid = s.addressableid
                INNER JOIN
                    systems AS sys ON w.addresssystem = sys.systemid
                INNER JOIN
                    users_data AS ud ON ud.userid = s.userid
                INNER JOIN
                    users AS u ON u.userdataid = ud.userid
                INNER JOIN
                    stations AS st ON st.warehouseid = w.warehouseid
                WHERE
                    u.accountid = $2 
                    AND sys.name = 'Hortus'
                    AND st.name != 'Hortus'
                LIMIT 1
            )
            -- 2. Select all materials for the given CX, and optionally join the user's storage quantity.
            SELECT
                mt.ticker,
                mt.materialid,
                COALESCE(si.quantity, 0) AS quantity,
                cxb.askprice,
                mp.price AS corpprice
            FROM
                cx_brokers AS cxb
            INNER JOIN
                materials AS mt ON mt.materialid = cxb.materialid
            INNER JOIN
                material_prices AS mp ON mp.ticker = mt.ticker
            LEFT JOIN
                storage_items AS si 
                ON 
                    si.materialid = mt.materialid 
                    AND si.storageid = (SELECT storageid FROM user_single_storage) 
            WHERE
                cxb.ticker LIKE $1
            ORDER BY
                cxb.ticker;
        """

        search_pattern = f"%.{cx}"

        async with pool.acquire() as con:
            materials_data = await con.fetch(query, search_pattern, current_user_id_str)

            if not materials_data:
                return JSONResponse(
                    status_code=404,
                    content={
                        "success": False,
                        "message": "No materials found for the given CX code.",
                    },
                )

            # --- SANITIZATION: Convert Decimal to float ---
            data = []
            for record in materials_data:
                row = dict(record)
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)
                data.append(row)
            # ---------------------------------------------

            return JSONResponse(status_code=200, content={"success": True, "materials": data})

    except Exception as e:
        logger.error(f"Failed to get materials price list: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": "An unexpected server error occurred.",
            },
        )


# --- 1. Model for updating the Vendor's metadata ---
class VendorUpdateData(BaseModel):
    """Data model for the vendor_to_update object."""

    companyName: str
    companyCode: str
    corpName: str
    gameName: str
    # Assuming 'cx' is an optional contact field for the vendor
    cx: Optional[str] = None


class Location(BaseModel):
    id: str
    amount: int


# --- 2. Model for a single Order item being updated or created ---
class OrderUpdateItem(BaseModel):
    """Data model for a single item in orders_to_update."""

    # orderid is Optional because new orders created on the client won't have one yet.
    orderid: Optional[str] = None
    materialticker: str
    materialid: str
    # The ordertype should match the values used in the database (e.g., "buy" or "sell")
    ordertype: str
    # Use float for price, but consider Decimal for financial precision if needed
    fixedprice: float
    # Reserved quantity is an integer
    location: List[Location] = Field(default_factory=list)


# --- 3. The main request payload model ---
class EditOrdersRequest(BaseModel):
    """The complete request body for /vendor_stores/edit_orders."""

    # The unique ID of the vendor store being edited
    vendorid: str

    # Nested object for the vendor's basic metadata
    vendor_to_update: VendorUpdateData

    # List of all current orders (both existing and new ones without an orderid)
    orders_to_update: List[OrderUpdateItem]

    # List of order IDs that should be deleted from the database.
    # Defaults to an empty list to avoid requiring the field if no orders are deleted.
    order_ids_to_delete: List[str] = Field(default_factory=list)


@data_router.post("/vendor_stores/edit_orders")
async def edit_vendor_orders(
    request: Request,
    payload: EditOrdersRequest,
    user_id: str = Depends(get_current_user_id),
):
    try:
        pool = request.app.state.db.pool

        async with pool.acquire() as con:
            async with con.transaction():
                # Step 1: Verify Authorization
                vendor_record = await con.fetchrow(
                    "SELECT userid, gamename, cx FROM user_vendors WHERE vendorid = $1",
                    payload.vendorid,
                )

                if not vendor_record or vendor_record["userid"] != user_id:
                    raise HTTPException(
                        status_code=403,
                        detail="Not authorized to edit this vendor store.",
                    )

                # Step 2: Update Vendor Details
                vendor_data = payload.vendor_to_update
                await con.execute(
                    """
                    UPDATE user_vendors
                    SET companyname = $1, companycode = $2, corpname = $3, gamename = $4, cx = $5
                    WHERE vendorid = $6;
                """,
                    vendor_data.companyName,
                    vendor_data.companyCode,
                    vendor_data.corpName,
                    vendor_data.gameName,
                    vendor_data.cx,
                    payload.vendorid,
                )

                # Step 3: Delete Orders
                if payload.order_ids_to_delete:
                    await con.executemany(
                        "DELETE FROM user_vendor_orders WHERE orderid = $1 AND vendorid = $2",
                        [(order_id, payload.vendorid) for order_id in payload.order_ids_to_delete],
                    )

                # Step 4: Add or Update Orders
                for order in payload.orders_to_update:
                    # Generate ID if new
                    order_id = order.orderid if order.orderid else str(uuid.uuid4())

                    if order.location:
                        # Convert [Location(id=...), Location(id=...)] -> [{"id":...}, {"id":...}]
                        locations_data = [loc.dict() for loc in order.location]
                        locations_json = json.dumps(locations_data)
                    else:
                        locations_json = "[]"
                    # ---------------------------------------------------------------

                    await con.execute(
                        """
                        INSERT INTO user_vendor_orders (
                            orderid, vendorid, materialticker, materialid, ordertype, fixedprice, location
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                        ON CONFLICT (orderid) DO UPDATE SET
                            materialticker = EXCLUDED.materialticker,
                            materialid = EXCLUDED.materialid,
                            ordertype = EXCLUDED.ordertype,
                            fixedprice = EXCLUDED.fixedprice,
                            location = EXCLUDED.location
                        """,
                        order_id,
                        payload.vendorid,
                        order.materialticker,
                        order.materialid,
                        order.ordertype,
                        order.fixedprice,
                        locations_json,
                    )

            # --- STEP 5: FETCH FINAL RESPONSE (Outside Transaction) ---

            # 5a. Fetch basic vendor info again (to get updated fields)
            updated_vendor = await con.fetchrow(
                "SELECT vendorid, companycode, companyname, corpname, cx, gamename, isactive FROM user_vendors WHERE vendorid = $1",
                payload.vendorid,
            )

            # 5b. Bulk Fetch User Inventory for 'storage_amount'
            inventory_records = await con.fetch(
                """
                SELECT m.ticker, COALESCE(st.stationid, pl.planetid)::text AS location_id, SUM(si.quantity) AS quantity
                FROM storages s
                JOIN storage_items si ON si.storageid = s.storageid
                JOIN materials m ON m.materialid = si.materialid
                JOIN warehouses w ON w.warehouseid = s.addressableid
                LEFT JOIN stations st ON st.warehouseid = w.warehouseid
                LEFT JOIN sites site ON site.siteid = s.addressableid
                LEFT JOIN planets pl ON pl.planetid = site.addressplanetid
                INNER JOIN users u ON u.userdataid = s.userid
                WHERE u.accountid = $1
                GROUP BY m.ticker, location_id
                """,
                user_id,
            )
            inventory_map = {
                (r["ticker"], r["location_id"]): float(r["quantity"]) for r in inventory_records if r["location_id"]
            }

            # 5c. Fetch Orders with Prices and JSON Locations
            orders_records = await con.fetch(
                """
                SELECT 
                    uvo.orderid, uvo.materialticker, uvo.ordertype, uvo.fixedprice, uvo.reserved, uvo.materialid,
                    COALESCE(uvo.location, '[]'::jsonb) AS locations,
                    mp.price AS corpprice,
                    CASE WHEN uvo.ordertype = 'buy' THEN cxb.askprice ELSE cxb.bidprice END AS cxprice
                FROM user_vendor_orders AS uvo
                LEFT JOIN material_prices AS mp ON mp.ticker = uvo.materialticker
                LEFT JOIN cx_brokers AS cxb ON cxb.ticker = (uvo.materialticker || '.' || $2)
                WHERE uvo.vendorid = $1;
                """,
                payload.vendorid,
                updated_vendor["cx"],
            )

            # 5d. Prepare Location ID Lookup
            all_location_ids = set()
            parsed_orders = []

            for r in orders_records:
                r_dict = dict(r)
                raw = r_dict["locations"]
                locs = []
                if raw:
                    locs = json.loads(raw) if isinstance(raw, str) else raw

                r_dict["parsed_locs"] = locs
                parsed_orders.append(r_dict)
                for l in locs:
                    if "id" in l:
                        all_location_ids.add(l["id"])

            # 5e. Bulk Fetch Location Details (Names/Codes)
            loc_lookup = {}
            if all_location_ids:
                loc_rows = await con.fetch(
                    """
                    SELECT stationid::text as id, name, naturalid FROM stations WHERE stationid::text = ANY($1)
                    UNION ALL
                    SELECT planetid::text as id, name, naturalid FROM planets WHERE planetid::text = ANY($1)
                """,
                    list(all_location_ids),
                )
                for row in loc_rows:
                    loc_lookup[row["id"]] = {
                        "name": row["name"],
                        "code": row["naturalid"],
                    }

            # 5f. Construct Final Order List
            final_orders_list = []
            for r in parsed_orders:
                enriched_locations = []
                total_in_store_qty = 0

                for loc in r["parsed_locs"]:
                    lid = loc.get("id")
                    details = loc_lookup.get(lid)

                    # Storage injection
                    storage_qty = inventory_map.get((r["materialticker"], lid), 0.0)
                    total_in_store_qty += storage_qty

                    enriched_locations.append(
                        {
                            "id": lid,
                            "amount": loc.get("amount", 0),
                            "location_name": details["name"] if details else "Unknown",
                            "location_code": details["code"] if details else "???",
                            "storage_amount": storage_qty,
                        }
                    )

                final_orders_list.append(
                    {
                        "orderid": str(r["orderid"]),
                        "materialid": r["materialid"],
                        "materialticker": r["materialticker"],
                        "ordertype": r["ordertype"],
                        "reserved": int(r["reserved"]),  # Total reserved across all locations
                        "location": enriched_locations,  # Full list with names and codes
                        "quantity": total_in_store_qty,  # Total found in user storage
                        "price": {
                            "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0,
                            "corpprice": float(r["corpprice"]) if r["corpprice"] else 0,
                            "cxprice": float(r["cxprice"]) if r["cxprice"] else 0,
                        },
                    }
                )

            # 5g. Return Response
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": "Vendor store updated successfully.",
                    "vendor_store": {
                        "vendor": dict(updated_vendor),
                        "orders": final_orders_list,
                    },
                },
            )

    except HTTPException as e:
        logger.error(f"Authorization error: {e.detail}")
        return JSONResponse(status_code=e.status_code, content={"success": False, "message": e.detail})

    except Exception as e:
        logger.error(f"Failed to edit vendor store: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": "An unexpected server error occurred.",
            },
        )


@data_router.delete("/vendor_stores/{vendor_id}")
async def delete_vendor(vendor_id: str, request: Request):
    """
    Performs a HARD DELETE of a vendor store and all its associated orders.
    Orders are deleted first due to foreign key constraints, then the vendor.
    """
    pool = request.app.state.db.pool
    try:
        async with pool.acquire() as con:
            async with con.transaction():
                # 1. HARD DELETE all related orders
                await con.execute(
                    """
                    DELETE FROM user_vendor_orders
                    WHERE vendorid = $1;
                """,
                    vendor_id,
                )

                # 2. HARD DELETE the vendor store itself
                result = await con.execute(
                    """
                    DELETE FROM user_vendors
                    WHERE vendorid = $1;
                """,
                    vendor_id,
                )

                # Check if a row was actually deleted
                if result == "DELETE 0":
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Vendor with ID {vendor_id} not found.",
                    )

            return {
                "success": True,
                "message": f"Vendor store (ID: {vendor_id}) and associated orders permanently deleted.",
            }

    except HTTPException:
        # Re-raise the 404
        raise
    except Exception as e:
        print(f"Error during vendor store hard deletion: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to permanently delete the vendor store.",
        )


@data_router.get("/production_lines")
async def get_user_production_lines(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        production_lines = await get_production_data_nested(request.app.state.db.pool, user_id)

        return JSONResponse(content={"success": True, "data": production_lines})
    except Exception as e:
        logger.error(f"Failed to fetch user production lines: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occured: {e}",
        )


def optimize_json_fallback(obj):
    """Fast fallback for standard json.dumps to handle Postgres numeric/date types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

# --- Cache Configuration ---
CACHE_TTL_SECONDS = 60
_map_cache = {
    "data": None,
    "timestamp": 0
}

@data_router.get("/dashboard_map")
async def get_dashboard_map(request: Request):
    """
    Fetches and structures all static star map data.
    """
    global _map_cache

    # 1. Check Cache
    current_time = time.time()
    if _map_cache["data"] and (current_time - _map_cache["timestamp"] < CACHE_TTL_SECONDS):
        return Response(content=_map_cache["data"], media_type="application/json")

    try:
        pool = request.app.state.db.pool

        query_systems = """
            SELECT COALESCE(json_agg(t), '[]')::text FROM (
                SELECT 
                    sys.systemid, sys.name, sys.type as systemType,
                    sys.positionx, sys.positiony, sys.positionz,
                    sys.sectorid, sys.microasteroidcount, sys.mass
                FROM systems AS sys
            ) t;
        """

        query_planets = """
            SELECT COALESCE(json_agg(t), '[]')::text FROM (
                WITH latest_pop AS (
                    SELECT DISTINCT ON (populationid)
                        populationid, time, simulationperiod, explorersgraceenabled, governmentprogramtype,
                        nextpopulationpioneer, nextpopulationsettler, nextpopulationtechnician, nextpopulationengineer, nextpopulationscientist,
                        populationdifferencepioneer, populationdifferencesettler, populationdifferencetechnician, populationdifferenceengineer, populationdifferencescientist,
                        openjobspioneer, openjobssettler, openjobstechnician, openjobsengineer, openjobsscientist,
                        unemploymentratepioneer, unemploymentratesettler, unemploymentratetechnician, unemploymentrateengineer, unemploymentratescientist,
                        averagehappinesspioneer, averagehappinesssettler, averagehappinesstechnician, averagehappinessengineer, averagehappinessscientist,
                        (COALESCE(nextpopulationpioneer, 0) + 
                         COALESCE(nextpopulationsettler, 0) + 
                         COALESCE(nextpopulationtechnician, 0) + 
                         COALESCE(nextpopulationengineer, 0) + 
                         COALESCE(nextpopulationscientist, 0))::BIGINT AS total_pop
                    FROM planet_populations
                    ORDER BY populationid, time DESC
                ),
                res_agg AS (
                    SELECT 
                        pr.planetid,
                        json_agg(
                            json_build_object('material', m.ticker, 'factor', pr.factor, 'type', pr.type)
                        ) AS resources
                    FROM planet_resources pr
                    JOIN materials m ON m.materialid = pr.materialid
                    GROUP BY pr.planetid
                )
                SELECT 
                    p.name, p.systemid, p.planetid, p.mass, p.countryname, p.countrycode,
                    pd.orbitindex, pd.semimajoraxis, pd.eccentricity, pd.inclination, pd.rightascension,
                    CASE 
                        WHEN p.surface IS TRUE AND p.temperature > 0 AND p.temperature < 40 AND p.fertility > 0 THEN 'EARTH_LIKE'
                        WHEN p.surface IS TRUE AND p.temperature > 100 THEN 'ROCKY_LIKE_LAVA'
                        WHEN p.surface IS TRUE AND p.temperature < 100 AND p.temperature > 0 AND p.fertility <= 0 THEN 'ROCKY_LIKE_ROCK'
                        WHEN p.surface IS TRUE AND p.temperature < 0 AND p.fertility <= 0  THEN 'ROCKY_LIKE_ICE'
                        WHEN p.surface IS FALSE AND p.temperature > 0 AND p.fertility <= 0 THEN 'GAS_LIKE_HOT'
                        WHEN p.surface IS FALSE AND p.temperature < 0 AND p.fertility <= 0 THEN 'GAS_LIKE_COLD'
                        ELSE 'UNKNOWN'
                    END AS planet_type,
                    pd.periapsis,
                    COALESCE(lp.total_pop, 0) AS population,
                    TO_CHAR(lp.time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS time, 
                    lp.simulationperiod, lp.explorersgraceenabled, lp.governmentprogramtype,
                    COALESCE(ra.resources, '[]'::json) AS resources,
                    
                    jsonb_build_object(
                        'PIONEER', COALESCE(lp.nextpopulationpioneer, 0),
                        'SETTLER', COALESCE(lp.nextpopulationsettler, 0),
                        'TECHNICIAN', COALESCE(lp.nextpopulationtechnician, 0),
                        'ENGINEER', COALESCE(lp.nextpopulationengineer, 0),
                        'SCIENTIST', COALESCE(lp.nextpopulationscientist, 0)
                    ) AS "nextPopulation",

                    jsonb_build_object(
                        'PIONEER', COALESCE(lp.populationdifferencepioneer, 0),
                        'SETTLER', COALESCE(lp.populationdifferencesettler, 0),
                        'TECHNICIAN', COALESCE(lp.populationdifferencetechnician, 0),
                        'ENGINEER', COALESCE(lp.populationdifferenceengineer, 0),
                        'SCIENTIST', COALESCE(lp.populationdifferencescientist, 0)
                    ) AS "populationDifference",

                    jsonb_build_object(
                        'PIONEER', COALESCE(lp.openjobspioneer, 0),
                        'SETTLER', COALESCE(lp.openjobssettler, 0),
                        'TECHNICIAN', COALESCE(lp.openjobstechnician, 0),
                        'ENGINEER', COALESCE(lp.openjobsengineer, 0),
                        'SCIENTIST', COALESCE(lp.openjobsscientist, 0)
                    ) AS "openJobs",

                    jsonb_build_object(
                        'PIONEER', COALESCE(lp.unemploymentratepioneer, 0)::DOUBLE PRECISION,
                        'SETTLER', COALESCE(lp.unemploymentratesettler, 0)::DOUBLE PRECISION,
                        'TECHNICIAN', COALESCE(lp.unemploymentratetechnician, 0)::DOUBLE PRECISION,
                        'ENGINEER', COALESCE(lp.unemploymentrateengineer, 0)::DOUBLE PRECISION,
                        'SCIENTIST', COALESCE(lp.unemploymentratescientist, 0)::DOUBLE PRECISION
                    ) AS "unemploymentRate",

                    jsonb_build_object(
                        'PIONEER', COALESCE(lp.averagehappinesspioneer, 0)::DOUBLE PRECISION,
                        'SETTLER', COALESCE(lp.averagehappinesssettler, 0)::DOUBLE PRECISION,
                        'TECHNICIAN', COALESCE(lp.averagehappinesstechnician, 0)::DOUBLE PRECISION,
                        'ENGINEER', COALESCE(lp.averagehappinessengineer, 0)::DOUBLE PRECISION,
                        'SCIENTIST', COALESCE(lp.averagehappinessscientist, 0)::DOUBLE PRECISION
                    ) AS "averageHappiness",

                    SUM(COALESCE(lp.total_pop, 0)) OVER(PARTITION BY p.systemid) AS "totalSystemPopulation"
                FROM planets AS p
                LEFT JOIN planet_orbit as pd ON pd.planetid = p.planetid
                LEFT JOIN res_agg ra ON ra.planetid = p.planetid
                LEFT JOIN latest_pop lp ON lp.populationid = p.populationid
            ) t;
        """

        query_sectors = "SELECT COALESCE(json_agg(t), '[]')::text FROM (SELECT externalsectorid, name, hexq, hexr, hexs, size FROM sectors) t;"
        query_vertices = "SELECT COALESCE(json_agg(t), '[]')::text FROM (SELECT externalsubsectorid, index, x, y, z FROM subsector_vertices) t;"
        query_subsectors = "SELECT COALESCE(json_agg(t), '[]')::text FROM (SELECT externalsectorid, externalsubsectorid FROM subsectors) t;"
        query_connections = "SELECT COALESCE(json_agg(t), '[]')::text FROM (SELECT systemidorigin, systemiddestination FROM system_connections) t;"

        query_stations = """
            SELECT COALESCE(json_agg(t), '[]')::text FROM (
                SELECT 
                    stationid, name, naturalid, systemid, comexid, 
                    orbit, 
                    warehouseid 
                FROM stations
            ) t;
        """

        query_gateways = """
            SELECT COALESCE(json_agg(t), '[]')::text FROM (
                SELECT 
                    pcb.name, pcb.naturalid, pcb.semimajoraxis, pcb.eccentricity, pcb.inclination,
                    g.operational_state, g.link_status, g.is_linked, g.fuel_available, g.fuel_max,
                    g.jumps_per_day, g.fuel_usage_fee, g.max_ship_volume, g.system_id, g.planet_id,
                    g.outgoing_link_id, g.incoming_links, g.capacity_upgrades, g.volume_upgrades,
                    g.distance_upgrades, g.linking_radius, g.established, g.currency_code, g.id
                FROM planet_celestial_bodies pcb
                INNER JOIN gateways g ON pcb.id = g.id
                WHERE pcb.naturalid LIKE 'GTW%'
            ) t;
        """

        (
            sys_str, plan_str, sec_str, subv_str,
            subs_str, sysc_str, stat_str, gw_str
        ) = await asyncio.gather(
            pool.fetchval(query_systems),
            pool.fetchval(query_planets),
            pool.fetchval(query_sectors),
            pool.fetchval(query_vertices),
            pool.fetchval(query_subsectors),
            pool.fetchval(query_connections),
            pool.fetchval(query_stations),
            pool.fetchval(query_gateways)
        )

        final_payload = (
            f'{{"success":true,"data":{{'
            f'"systems":{sys_str},'
            f'"planets":{plan_str},'
            f'"sectors":{sec_str},'
            f'"subsectors":{subs_str},'
            f'"subsector_vertices":{subv_str},'
            f'"system_connections":{sysc_str},'
            f'"gateways":{gw_str},'
            f'"stations":{stat_str}'
            f'}}}}'
        )

        # 2. Store in Cache
        _map_cache["data"] = final_payload
        _map_cache["timestamp"] = time.time()

        return Response(content=final_payload, media_type="application/json")

    except Exception as e:
        logger.error(f"Failed to fetch dashboard map data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}",
        )

# Test Not used??
@data_router.get("/dashboard_presence", response_model=Dict[str, Any])
async def get_dashboard_presence(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Fetches dynamic user and fleet presence data (locations and colors) for the map.
    This combines the current user's ships/sites with tracked fleet positions.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            # 1. Fetch CURRENT USER's Presence (e.g., ships/sites)
            # This query finds all locations where the current user has assets
            user_presence_records = await conn.fetch(
                """
                -- Example: Get location of all user-owned sites
                SELECT 
                    st.siteid AS location_id,
                    u.accountid AS user_id,
                    '#10b981' AS color_code, -- Current User's Color (Emerald Green)
                    'Your Site' AS name,
                    p.x, 
                    p.y,
                    'planet' AS location_type
                FROM sites AS st
                INNER JOIN planets AS p ON st.addressplanetid = p.planetid
                INNER JOIN users AS u ON st.userid = u.userdataid
                WHERE u.accountid = $1
                
                UNION ALL 
                
                -- Example: Get location of all user-owned ships
                SELECT
                    ship.shipid AS location_id,
                    u.accountid AS user_id,
                    '#10b981' AS color_code,
                    ship.name AS name,
                    sys.x,
                    sys.y,
                    'system' AS location_type
                FROM ships AS ship
                INNER JOIN systems AS sys ON ship.currentsystemid = sys.systemid
                INNER JOIN users AS u ON ship.userid = u.userdataid
                WHERE u.accountid = $1;
            """,
                user_id,
            )

            # 2. Fetch TRACKED USERS/FLEETS Presence (Dynamic tracking logic here)

            tracked_presence_records = await conn.fetch("""
                SELECT 
                    'fleet-123' AS location_id,
                    'other-user-01' AS user_id,
                    '#facc15' AS color_code, -- Tracked Fleet Color (Yellow)
                    'Tracked Fleet Alpha' AS name,
                    sys.x, 
                    sys.y,
                    'system' AS location_type
                FROM systems AS sys
                WHERE sys.name = 'Alpha Centauri'; -- Mock location for demo
            """)

        # Combine all records
        all_presence_data = [dict(r) for r in user_presence_records] + [dict(r) for r in tracked_presence_records]

        return JSONResponse(content={"success": True, "data": all_presence_data})

    except Exception as e:
        logger.error(f"Failed to fetch dashboard presence data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}",
        )


@data_router.get("/materials", response_model=Dict[str, Any])
async def get_materials(request: Request):
    try:
        # --- 1. Fetch Base Material Data ---
        base_query = """
            SELECT materialid, category, ticker, volume::FLOAT8, weight::FLOAT8, resource
            FROM materials;
        """

        # --- 2. Fetch Production Recipe Details (Inputs/Outputs) ---
        recipes_query = """
            SELECT
                r.materialid AS base_materialid,
                mp.processid::TEXT,
                mp.reactorid,
                mp.durationmillis,
                pmio.iotype,
                pmio.amount::FLOAT8,
                pmio.materialid AS io_materialid,
                m_io.ticker AS io_material_ticker
            FROM 
                recipes r
            JOIN
                material_processes mp ON r.output_recipe_ids @> jsonb_build_array(mp.processid)
            JOIN
                process_material_io pmio ON pmio.processid = mp.processid
            JOIN
                materials m_io ON m_io.materialid = pmio.materialid
            ORDER BY 
                r.materialid, mp.processid, pmio.iotype DESC;
        """

        # --- 3. Fetch Consumption Relationships (RequiredFor) ---
        required_for_query = """
            SELECT DISTINCT
                t1.materialid AS input_materialid, 
                t2.materialid AS consumer_materialid, 
                m_consumer.ticker AS consumer_ticker
            FROM
                process_material_io t1 
            JOIN
                recipes t2 ON t2.output_recipe_ids @> jsonb_build_array(t1.processid)
            JOIN
                materials m_consumer ON m_consumer.materialid = t2.materialid
            WHERE
                t1.iotype = 'INPUT';
        """

        async with request.app.state.db.pool.acquire() as conn:
            materials = await conn.fetch(base_query)
            recipe_rows = await conn.fetch(recipes_query)
            required_for_rows = await conn.fetch(required_for_query)

        # --- 4. Process and Merge Data in Python ---

        # Initialize final data structure with the key: 'inputRecipes'
        final_data = {r["materialid"]: dict(r, requiredFor=[], inputRecipes=[]) for r in materials}

        # Helper structure for organizing recipes by process
        recipes_by_material = {}

        # Process Production Recipes (Inputs/Outputs)
        for row in recipe_rows:
            base_materialid = row["base_materialid"]
            processid = row["processid"]
            iotype = row["iotype"]

            # Use processid as the key for grouping recipe details
            if base_materialid not in recipes_by_material:
                recipes_by_material[base_materialid] = {}
            if processid not in recipes_by_material[base_materialid]:
                recipes_by_material[base_materialid][processid] = {
                    "processid": processid,
                    "reactorid": row["reactorid"],
                    "durationmillis": row["durationmillis"],
                    "inputs": [],
                    "outputs": [],
                }

            recipe = recipes_by_material[base_materialid][processid]
            io_item = {
                "materialid": row["io_materialid"],
                "ticker": row["io_material_ticker"],
                "amount": row["amount"],
            }

            if iotype == "INPUT":
                recipe["inputs"].append(io_item)
            elif iotype == "OUTPUT":
                recipe["outputs"].append(io_item)

        # Merge Production Recipes into final_data
        for materialid, processes in recipes_by_material.items():
            if materialid in final_data:
                # Assigning the production recipes to 'inputRecipes'
                final_data[materialid]["inputRecipes"] = list(processes.values())

        # Process Consumption Relationships (RequiredFor)
        for row in required_for_rows:
            input_materialid = row["input_materialid"]

            # The material that requires the input (the consumer)
            consumer_item = {
                "materialid": row["consumer_materialid"],
                "ticker": row["consumer_ticker"],
            }

            if input_materialid in final_data:
                # Append the consumer to the current material's 'requiredFor' list
                final_data[input_materialid]["requiredFor"].append(consumer_item)

        # Final output is the list of values from the dictionary
        return JSONResponse(content={"success": True, "data": list(final_data.values())})

    except Exception as e:
        logger.error(f"Failed to fetch material flow data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occured: {e}",
        )


# What the hell is this???
# @data_router.post('/user_production', response_model=Dict[str, Any])
async def get_materials(request: Request, payload: Dict[str, List[str]]):
    try:
        query = """
            WITH RecipeInputs AS (
    -- 1. Aggregate all INPUT FACTORS for each recipe
    SELECT
        pri.productiontemplateid,
        json_agg(
            json_build_object(
                'id', pri.id,
                'ticker', mti.ticker,
                'factor', pri.factor
            )
        ) AS inputs
    FROM 
        production_recipe_input_factors AS pri
    INNER JOIN 
        materials AS mti ON mti.materialid = pri.materialid
    GROUP BY pri.productiontemplateid
),
RecipeOutputs AS (
    -- 2. Aggregate all OUTPUT FACTORS for each recipe
    SELECT
        pro.productiontemplateid,
        json_agg(
            json_build_object(
                'id', pro.id,
                'ticker', mto.ticker,
                'factor', pro.factor
            )
        ) AS outputs
    FROM 
        production_recipe_output_factors AS pro
    INNER JOIN 
        materials AS mto ON mto.materialid = pro.materialid
    GROUP BY pro.productiontemplateid
),
ProductionRecipesAgg AS (
    -- 3. Combine RECIPE details with aggregated inputs/outputs
    SELECT
        pr.productiontemplateid,
        json_build_object(
            'name', pr.name,
            'efficiency', pr.efficiency,
            'effort_factor', pr.effortfactor,
            'inputs', COALESCE(ri.inputs, '[]'::json),
            'outputs', COALESCE(ro.outputs, '[]'::json)
        ) AS production_recipe
    FROM
        production_recipes AS pr
    LEFT JOIN 
        RecipeInputs AS ri ON ri.productiontemplateid = pr.productiontemplateid
    LEFT JOIN 
        RecipeOutputs AS ro ON ro.productiontemplateid = pr.productiontemplateid
),
ProductionOrdersAgg AS (
    -- 4. Aggregate ORDER details and attach the aggregated recipe
    SELECT
        splo.productionlineid,
        json_agg(
            json_build_object(
                'order_id', splo.orderid,
                'duration', splo.duration,
                'production_recipe', pra.production_recipe
            )
        ) AS production_orders
    FROM
        site_production_line_orders AS splo
    INNER JOIN
        ProductionRecipesAgg AS pra ON pra.productiontemplateid = splo.recipeid
    WHERE splo.started IS NULL
    GROUP BY splo.productionlineid
),
ProductionLinesAgg AS (
    -- 5. Aggregate PRODUCTION LINES and attach the aggregated orders
    SELECT
        spl.siteid,
        json_agg(
            json_build_object(
                'line_id', spl.productionlineid,
                'type', spl.type,
                'slots', spl.slots,
                'capacity', spl.capacity,
                'efficiency', spl.efficiency,
                'condition', spl.condition,
                'production_orders', COALESCE(poa.production_orders, '[]'::json)
            )
        ) AS production_lines
    FROM
        site_production_lines AS spl
    LEFT JOIN
        ProductionOrdersAgg AS poa ON poa.productionlineid = spl.productionlineid
    WHERE spl.productionlineid = ANY($2)
    GROUP BY spl.siteid
)
-- 6. Final Query: Combine SITE and PLANET data with aggregated lines
SELECT
    u.accountid::TEXT,
    json_agg(
        json_build_object(
            'site_id', site.siteid,
            -- Preserve the site_details object structure you created previously
            'site_details', json_build_object( 
                'planet_name', p.name,
                'production_lines', COALESCE(pla.production_lines, '[]'::json)
            )
        )
    ) AS user_production_data
FROM
    sites AS site
INNER JOIN
    planets AS p ON p.planetid = site.addressplanetid
INNER JOIN
    users_data AS ud ON ud.userid = site.userid
INNER JOIN
    users AS u ON u.userdataid = ud.userid
INNER JOIN 
    ProductionLinesAgg AS pla ON pla.siteid = site.siteid
WHERE 
    u.accountid = ANY($1)
GROUP BY 
    u.accountid;
        """

        async with request.app.state.db.pool.acquire() as conn:
            production_data = await conn.fetch(query, payload.get("userids"), payload.get("productionlineids"))
            processed_data = [dict(r) for r in production_data]
            return JSONResponse(content={"success": True, "data": processed_data})
    except Exception as e:
        logger.error(f"Failed to fetch dashboard presence data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occured: {e}",
        )


@data_router.get("/buildings", response_model=Dict[str, Any])
async def get_buildings(request: Request):
    try:
        query = """
            SELECT
                b.buildingid,
                b.name AS building_name,
                b.ticker AS building_ticker,
                b.type,
                b.area,
                b.expertisecategory,
                b.needsfertilesoil,
                bp.amount,
                bp.materialid,
                bp.id,
                bwc.workforcelevel,
                bwc.capacity,
                mt.ticker
            FROM
                buildings b
            LEFT JOIN
                building_build_materials bp ON b.buildingid = bp.buildingid
            LEFT JOIN
                building_workforce_capacities bwc ON b.buildingid = bwc.buildingid
            LEFT JOIN
                materials mt ON mt.materialid = bp.materialid;
        """

        async with request.app.state.db.pool.acquire() as conn:
            building_data = await conn.fetch(query)

            # Group rows by building to aggregate build_materials and workforce
            buildings_map = {}
            for row in building_data:
                row_dict = dict(row)
                building_id = row_dict["buildingid"]

                if building_id not in buildings_map:
                    buildings_map[building_id] = {
                        "buildingid": row_dict["buildingid"],
                        "ticker": row_dict["building_ticker"],
                        "name": row_dict["building_name"],
                        "type": row_dict["type"],
                        "area": row_dict["area"],
                        "expertisecategory": row_dict["expertisecategory"],
                        "needsfertilesoil": row_dict["needsfertilesoil"],
                        "build_materials": [],
                        "workforce": [],
                        "_material_ids_added": set(),
                        "_workforce_levels_added": set(),
                    }

                # 1. Aggregate unique build materials
                material_id = row_dict["materialid"]

                # Check if materialid is present AND if it hasn't been added yet
                if material_id and material_id not in buildings_map[building_id]["_material_ids_added"]:
                    buildings_map[building_id]["build_materials"].append(
                        {
                            "materialid": material_id,
                            "ticker": row_dict["ticker"],
                            "amount": row_dict["amount"],
                        }
                    )
                    buildings_map[building_id]["_material_ids_added"].add(material_id)

                # 2. Aggregate unique workforce capacities
                workforce_level = row_dict["workforcelevel"]

                # Check if workforcelevel is present AND if it hasn't been added yet
                if workforce_level and workforce_level not in buildings_map[building_id]["_workforce_levels_added"]:
                    buildings_map[building_id]["workforce"].append(
                        {
                            "workforcelevel": workforce_level,
                            "capacity": row_dict["capacity"],
                        }
                    )
                    buildings_map[building_id]["_workforce_levels_added"].add(workforce_level)

            # 3. Final processing: Remove helper sets and convert to list
            processed_data = []
            for building_data in buildings_map.values():
                # Clean up the temporary sets before returning
                del building_data["_material_ids_added"]
                del building_data["_workforce_levels_added"]
                processed_data.append(building_data)

            return JSONResponse(content={"success": True, "data": processed_data})
    except Exception as e:
        logger.error(f"Failed to fetch building data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occured: {e}",
        )


# --- Pydantic Models (Updated) ---
class ProducerConsumerItem(BaseModel):
    loc: str = Field(..., description="Site location ID (e.g., LEO-1, MAR-2)")
    player: str = Field(..., description="Player username or name")
    amount: float = Field(..., description="Daily amount produced or consumed at this site.")  # ADDED


class ProductionSummaryItem(BaseModel):
    ticker: str = Field(..., description="Material Ticker (e.g., WTR, MCG)")
    production: float = Field(..., description="Total corp-wide Production (capacity-limited)")
    consumption: float = Field(..., description="Total corp-wide Consumption (capacity-limited)")
    net: float = Field(..., description="Net flow (Production - Consumption)")
    producers: List[ProducerConsumerItem] = Field(
        ...,
        description="List of unique sites/users that contribute to this material's production flow.",
    )
    consumers: List[ProducerConsumerItem] = Field(
        ..., description="List of unique sites/users that consume this material's flow."
    )


class CorpMember(BaseModel):
    companyCode: Optional[str] = Field(None, description="Company code from corporation_shareholders.")
    companyName: Optional[str] = Field(None, description="Company name from corporation_shareholders.")
    isSynchronized: bool = Field(..., description="True if user has linked users_data entry (extension user).")
    lastActive: Optional[str] = Field(None, description="Date when member was last active, if available.")
    joinedDate: Optional[str] = Field(None, description="Date when member joined the corporation, if available.")

class CorpOverviewResponse(BaseModel):
    name: str = Field(..., description="Corporation's name.")
    code: str = Field(..., description="Corporation's code.")
    memberCount: int = Field(..., description="Total number of members in the corporation.")
    productionSummary: List[ProductionSummaryItem] = Field(
        ..., description="List of all materials and their aggregated flows."
    )
    members: List[CorpMember] = Field(..., description="List of all members of the corporation.")


class ProducerConsumerItem(BaseModel):
    loc: str
    player: str
    amount: float


class ProductionSummaryItem(BaseModel):
    ticker: str
    production: float
    consumption: float
    net: float
    producers: List[ProducerConsumerItem]
    consumers: List[ProducerConsumerItem]


class UserProductionResponse(BaseModel):
    productionSummary: List[ProductionSummaryItem]


MS_PER_DAY = 1000 * 60 * 60 * 24  # 86,400,000

SQL_FETCH_NESTED_PRODUCTION_DATA = """
WITH 
-- 1. ANCHOR: Get only this user's sites first. 
-- Everything else joins to this to limit scan size immediately.
UserSites AS (
    SELECT 
        s.siteid, 
        s.area, 
        s.investedpermits, 
        s.maximumpermits, 
        s.foundedtimestamp,
        p.naturalid AS planet_name
    FROM sites s
    INNER JOIN users u ON u.userdataid = s.userid
    INNER JOIN planets p ON p.planetid = s.addressplanetid
    WHERE u.accountid = $1
),

-- 2. Platforms (Filtered by UserSites)
PlatformData AS (
    SELECT
        sp.siteid,
        COALESCE(AVG(CASE WHEN b.type IN ('PRODUCTION', 'RESOURCES') THEN sp.condition END), 0.0) AS overall_platform_condition,
        jsonb_agg(DISTINCT b.ticker ORDER BY b.ticker) AS site_building_tickers,
        jsonb_agg(
            jsonb_build_object(
                'building_ticker', b.ticker, 
                'platform_condition', sp.condition
            )
        ) AS site_platform_conditions
    FROM site_platforms sp
    INNER JOIN UserSites us ON us.siteid = sp.siteid -- <--- FILTER
    INNER JOIN buildings b ON b.buildingid = sp.buildingid
    GROUP BY sp.siteid
),

-- 3. Storage (Filtered by UserSites)
StorageData AS (
    SELECT
        s.siteid,
        jsonb_agg(
            jsonb_build_object(
                'material_id', ssi.materialid,
                'ticker', m.ticker,
                'amount', ssi.quantity
            )
        ) AS storage_items
    FROM storage_items ssi
    INNER JOIN storages st ON st.storageid = ssi.storageid
    INNER JOIN sites s ON s.siteid = st.addressableid
    INNER JOIN UserSites us ON us.siteid = s.siteid -- <--- FILTER
    INNER JOIN materials m ON m.materialid = ssi.materialid
    GROUP BY s.siteid
),

-- 4. User's Production Lines (Filtered)
UserLines AS (
    SELECT pl.* FROM site_production_lines pl
    INNER JOIN UserSites us ON us.siteid = pl.siteid
),

-- 5. User's Active Orders (Filtered)
UserOrders AS (
    SELECT po.* FROM site_production_line_orders po
    INNER JOIN UserLines ul ON ul.productionlineid = po.productionlineid
    WHERE po.started IS NULL -- Only get queued items
),

-- 6. Relevant Recipes (Only fetch inputs/outputs for recipes currently in UserOrders)
RelevantRecipes AS (
    SELECT DISTINCT recipeid FROM UserOrders
),

-- 7. Aggregated Recipe Inputs (Filtered by RelevantRecipes)
RecipeInputs AS (
    SELECT
        i.productiontemplateid,
        jsonb_agg(jsonb_build_object('ticker', m.ticker, 'factor', i.factor)) AS inputs
    FROM production_recipe_input_factors i
    INNER JOIN RelevantRecipes rr ON rr.recipeid = i.productiontemplateid -- <--- FILTER
    INNER JOIN materials m ON m.materialid = i.materialid
    GROUP BY i.productiontemplateid
),

-- 8. Aggregated Recipe Outputs (Filtered by RelevantRecipes)
RecipeOutputs AS (
    SELECT
        o.productiontemplateid,
        jsonb_agg(jsonb_build_object('ticker', m.ticker, 'factor', o.factor)) AS outputs
    FROM production_recipe_output_factors o
    INNER JOIN RelevantRecipes rr ON rr.recipeid = o.productiontemplateid -- <--- FILTER
    INNER JOIN materials m ON m.materialid = o.materialid
    GROUP BY o.productiontemplateid
),

-- 9. Complete Order Objects
EnrichedOrders AS (
    SELECT
        uo.productionlineid,
        jsonb_agg(
            jsonb_build_object(
                'order_id', uo.orderid,
                'created', uo.created,
                'duration', uo.duration,
                'production_recipe', jsonb_build_object(
                    'name', r.name,
                    'inputs', COALESCE(ri.inputs, '[]'::jsonb),
                    'outputs', COALESCE(ro.outputs, '[]'::jsonb)
                )
            ) ORDER BY uo.created ASC
        ) AS production_orders
    FROM UserOrders uo
    INNER JOIN production_recipes r ON r.productiontemplateid = uo.recipeid
    LEFT JOIN RecipeInputs ri ON ri.productiontemplateid = uo.recipeid
    LEFT JOIN RecipeOutputs ro ON ro.productiontemplateid = uo.recipeid
    GROUP BY uo.productionlineid
),

-- 10. Aggregated Production Lines
LinesAgg AS (
    SELECT
        ul.siteid,
        jsonb_agg(
            jsonb_build_object(
                'line_id', ul.productionlineid,
                'type', ul.type,
                'capacity', ul.capacity,
                'efficiency', ul.efficiency,
                'condition', ul.condition,
                'queue', COALESCE(eo.production_orders, '[]'::jsonb)
            )
        ) AS production_lines
    FROM UserLines ul
    LEFT JOIN EnrichedOrders eo ON eo.productionlineid = ul.productionlineid
    GROUP BY ul.siteid
)

-- FINAL SELECT
SELECT 
    us.siteid AS site_id,
    jsonb_build_object(
        'planet_name', us.planet_name,
        'area', us.area,
        'invested_permits', us.investedpermits,
        'maximum_permits', us.maximumpermits,
        
        -- Platform Data
        'overall_platform_condition', COALESCE(pd.overall_platform_condition, 0.0),
        'site_building_tickers', COALESCE(pd.site_building_tickers, '[]'::jsonb), 
        'site_platform_conditions', COALESCE(pd.site_platform_conditions, '[]'::jsonb), 
        'platform_repair_list', '[]'::jsonb, 
        
        -- Production Lines
        'production_lines', COALESCE(la.production_lines, '[]'::jsonb),
        
        -- Storage Items
        'storage_items', COALESCE(sd.storage_items, '[]'::jsonb) 
    ) AS site_details
FROM UserSites us
LEFT JOIN PlatformData pd ON pd.siteid = us.siteid 
LEFT JOIN LinesAgg la ON la.siteid = us.siteid
LEFT JOIN StorageData sd ON sd.siteid = us.siteid;
"""


@data_router.get("/user_production")
async def get_user_production(request: Request, user_id: str = Depends(get_current_user_id)):
    pool = request.app.state.db.pool

    async with pool.acquire() as conn:
        records = await conn.fetch(SQL_FETCH_NESTED_PRODUCTION_DATA, user_id)

    async def generate():
        yield '{"success": true, "data": {'
        first_site = True

        for record in records:
            if not first_site:
                yield ","
            site_id = record["site_id"]
            # Ensure proper handling of JSON string vs object from driver
            site_details = (
                json.loads(record["site_details"])
                if isinstance(record["site_details"], str)
                else record["site_details"]
            )
            site_details["site_daily_flow"] = {}

            # --- 1. Line Flow Calculation (Existing Logic) ---
            for line in site_details.get("production_lines", []) or []:
                orders = line.get("production_orders") or []
                if not orders or line.get("capacity", 0) <= 0:
                    line["queue"] = []
                    line["daily_flow"] = {}
                    continue

                active_orders = [o for o in orders if o.get("completion")]
                template_orders = [o for o in orders if not o.get("completion")]
                active_orders.sort(
                    key=lambda o: datetime.fromisoformat(o["completion"]) if o.get("completion") else datetime.max
                )
                template_orders.sort(key=lambda o: datetime.fromisoformat(o["created"]))
                queue = active_orders + template_orders
                queue = queue[: int(line["capacity"])]
                line["queue"] = queue
                line["daily_flow"] = {}
                line_unscaled_flow = defaultdict(float)

                total_ms = sum(o["duration"] for o in template_orders)
                if total_ms <= 0:
                    continue
                daily_cycles = (line["capacity"] * MS_PER_DAY) / total_ms  # MS_PER_DAY constant used here

                for order in template_orders:
                    recipe = order.get("production_recipe", {})
                    if not recipe:
                        continue
                    inputs = recipe.get("inputs") or []
                    outputs = recipe.get("outputs") or []

                    duration_multiplier = order.get("duration") / recipe.get("duration")

                    for factor in inputs:
                        ticker = factor["ticker"]
                        flow = -factor["factor"] * duration_multiplier
                        line_unscaled_flow[ticker] += flow

                    for factor in outputs:
                        ticker = factor["ticker"]
                        flow = factor["factor"] * duration_multiplier
                        line_unscaled_flow[ticker] += flow

                for ticker, unscaled_flow in line_unscaled_flow.items():
                    line["daily_flow"][ticker] = unscaled_flow * daily_cycles

                for ticker, flow in line["daily_flow"].items():
                    # Aggregating flow into the site-level flow
                    site_details["site_daily_flow"][ticker] = site_details["site_daily_flow"].get(ticker, 0) + flow

            # --- 2. Post-Process Site Flow with Storage Data (NEW LOGIC) ---
            # Create a map for fast lookup of storage amounts by ticker
            storage_map = {item["ticker"]: item["amount"] for item in site_details.get("storage_items", []) or []}

            # Since site_daily_flow is fully calculated, update its structure
            site_flow_copy = site_details["site_daily_flow"].copy()
            for ticker, flow in site_flow_copy.items():
                # Add 'currentAmount' from storage_map, defaulting to 0 if material is not in storage
                current_amount = storage_map.get(ticker, 0)
                site_details["site_daily_flow"][ticker] = {
                    "flow": flow,
                    "currentAmount": current_amount,
                }

            # --- 3. Stream Output (Existing Logic) ---
            yield f'"{site_id}":'
            yield json.dumps(site_details, separators=(",", ":"))
            first_site = False

        yield "}}"

    return StreamingResponse(generate(), media_type="application/json")


SQL_FETCH_USER_WORKFORCE = """
    SELECT
        wf.siteid::text AS siteid, -- Explicitly cast to text
        wf.level,
        wf.population,
        wf.reserve,
        wf.capacity,
        wf.required,
        wf.satisfaction,
        needs_data.needs -- The aggregated JSONB array
    FROM
        workforces wf

    -- ----------------------------------------------------------------------
    -- 1. LATERAL JOIN for JSON Aggregation (Groups needs for each workforce row)
    -- ----------------------------------------------------------------------
    INNER JOIN LATERAL (
        SELECT
            jsonb_agg(
                jsonb_build_object(
                    'ticker', m.ticker,
                    'category', wfn.category,
                    'essential', wfn.essential,
                    'satisfaction', wfn.satisfaction,
                    'unitsperinterval', wfn.unitsperinterval,
                    'unitsper100', wfn.unitsper100,
                    'currentamount', COALESCE(si.quantity, 0) 
                )
            ) AS needs
        FROM
            workforce_needs wfn
        INNER JOIN
            materials m ON m.materialid = wfn.materialid
        -- LEFT JOIN to get the storage item amount for this material at this site
        LEFT JOIN
            storages st ON st.addressableid = wf.siteid
        LEFT JOIN
            storage_items si ON 
                si.materialid = wfn.materialid AND -- Match the material
                si.storageid = st.storageid           -- Match the site where the workforce is located
        WHERE
            wfn.workforceid = wf.workforceid
        GROUP BY
            wf.workforceid
    ) AS needs_data ON TRUE
    WHERE
        wf.siteid::text = ANY($1::text[]) -- Match the array of allowed site IDs
    ORDER BY
        wf.siteid, wf.level;
"""


# --- SECURITY QUERY: RESOLVE ALLOWED SITES ---
SQL_GET_ALLOWED_SITES = """
WITH Me AS (
    SELECT ud.displayname as username, cd.companycode
    FROM users u
    LEFT JOIN users_data ud ON u.userdataid = ud.userid
    LEFT JOIN company_data cd ON u.userdataid = cd.userdataid
    WHERE u.accountid = $1::uuid
),
MyOwnedSites AS (
    SELECT s.siteid::text as siteid
    FROM sites s
    JOIN users u ON u.userdataid = s.userid
    WHERE u.accountid = $1::uuid
),
MyOutboundLeases AS (
    SELECT l->>'siteId' as siteid, l->>'tenant' as tenant
    FROM user_global_settings ugs
    CROSS JOIN jsonb_array_elements(COALESCE(ugs.internal_leased_sites, '[]'::jsonb)) l
    WHERE ugs.userid::text = $1::text
),
MyInboundLeases AS (
    SELECT l->>'siteId' as siteid, 
           (SELECT COALESCE(ud2.displayname, cd2.companyname, 'Unknown') 
            FROM users u2 
            LEFT JOIN users_data ud2 ON ud2.userid = u2.userdataid 
            LEFT JOIN company_data cd2 ON cd2.userdataid = u2.userdataid 
            WHERE u2.accountid::text = ugs.userid::text) as landlord
    FROM user_global_settings ugs
    CROSS JOIN jsonb_array_elements(COALESCE(ugs.internal_leased_sites, '[]'::jsonb)) l
    CROSS JOIN Me
    WHERE ugs.userid::text != $1::text
      AND (
          l->>'tenant' = Me.username 
          OR l->>'tenant' = Me.companycode 
          OR l->>'tenant' = Me.username || ' (' || Me.companycode || ')'
      )
)
SELECT 
    o.siteid
FROM MyOwnedSites o
LEFT JOIN MyOutboundLeases outbound ON outbound.siteid = o.siteid

UNION ALL

SELECT 
    inbound.siteid
FROM MyInboundLeases inbound;
"""

@data_router.get("/user_workforce_with_needs")
async def get_user_workforce_with_needs(request: Request, user_id: str = Depends(get_current_user_id)):
    pool = request.app.state.db.pool

    async with pool.acquire() as conn:
        # 1. Figure out which sites this user is allowed to see (Owned + Leased)
        allowed_sites_records = await conn.fetch(SQL_GET_ALLOWED_SITES, user_id)
        
        if not allowed_sites_records:
            return JSONResponse(content={"success": True, "data": {}})
            
        target_site_ids = list(set([r["siteid"] for r in allowed_sites_records]))

        # 2. Pass the ARRAY of site IDs to your workforce query
        records = await conn.fetch(SQL_FETCH_USER_WORKFORCE, target_site_ids)

    # 3. Initialize the dictionary for grouping
    workforce_by_site: Dict[str, List[Dict[str, Any]]] = {}

    # 4. Process and Group the Records
    for record in records:
        mutable_record = dict(record)

        # Safely extract and cast site_id to string so it acts as a proper JSON key
        site_id = str(mutable_record.pop("siteid"))

        needs_data = mutable_record.get("needs")

        if isinstance(needs_data, str):
            mutable_record["needs"] = json.loads(needs_data)

        if site_id not in workforce_by_site:
            workforce_by_site[site_id] = []

        workforce_by_site[site_id].append(mutable_record)

    return JSONResponse(content={"success": True, "data": workforce_by_site})
