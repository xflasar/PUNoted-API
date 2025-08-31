# data_handlers.py

import asyncio
import csv
from io import StringIO
import asyncpg
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response, status, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Dict, List
import logging
from auth import get_current_user_id
from db import Database

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

@data_router.post('/data_batch')
async def data_batch(payload: Dict[str, Any], user_id: str = Depends(get_current_user_id), request: Request = None, background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Receives a batch of data and queues a background task for processing.
    """
    # The `get_current_user_id` dependency handles all token validation and
    # returns the user ID, or raises an HTTPException if it fails.

    items_to_process = payload['data']
    
    # Check for malformed payload
    if not isinstance(items_to_process, List) or not items_to_process:
        logger.warning(f"User ID '{user_id}' submitted a malformed payload.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload 'data' must resolve to a non-empty list of items."
        )

    # Get the client IP address
    ip_address = request.client.host if request else 'N/A'
    
    # Process the received items to get IDs
    arrived_ids = [item.get("id") for item in items_to_process if isinstance(item, dict) and item.get("id")]
    
    logger.info(f"IP: {ip_address} - Received batch of {len(items_to_process)} items from user ID '{user_id}'.")
    
    from tasks import process_data_batch_task
    background_tasks.add_task(process_data_batch_task,
        items_to_process=items_to_process, 
        user_id=user_id, 
        db=request.state.db
    )

    return {
        "success": True,
        "message": f"Successfully queued {len(items_to_process)} items for processing.",
        "message_ids": arrived_ids,
        "task_queued": True
    }

@data_router.get('/market_price_all')
async def get_market(request: Request):
    try:
        csv_content = await get_market_data_for_csv(request.state.db)
        
        return Response(
            content=csv_content,
            media_type="text/csv",
            #headers={"Content-Disposition": "attachment; filename=market_data.csv"}
        )
    except Exception as e:
        logger.error(f"Failed to fetch market data: {e}", exc_info=True)
        return {"success": False, "message": "Failed to retrieve market data."}

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
        "ticker", "last_update", "mmbuy", "mmsell",
        "AI1-Average", "AI1-AskAmt", "AI1-AskPrice", "AI1-AskAvail", "AI1-BidAmt", "AI1-BidPrice", "AI1-BidAvail",
        "CI1-Average", "CI1-AskAmt", "CI1-AskPrice", "CI1-AskAvail", "CI1-BidAmt", "CI1-BidPrice", "CI1-BidAvail",
        "CI2-Average", "CI2-AskAmt", "CI2-AskPrice", "CI2-AskAvail", "CI2-BidAmt", "CI2-BidPrice", "CI2-BidAvail",
        "NC1-Average", "NC1-AskAmt", "NC1-AskPrice", "NC1-AskAvail", "NC1-BidAmt", "NC1-BidPrice", "NC1-BidAvail",
        "NC2-Average", "NC2-AskAmt", "NC2-AskPrice", "NC2-AskAvail", "NC2-BidAmt", "NC2-BidPrice", "NC2-BidAvail",
        "IC1-Average", "IC1-AskAmt", "IC1-AskPrice", "IC1-AskAvail", "IC1-BidAmt", "IC1-BidPrice", "IC1-BidAvail"
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
                row_data.append(str(value) if value is not None else '')
            writer.writerow(row_data)

        csv_string = output.getvalue()
        output.close()
        return csv_string

    except Exception as e:
        logger.error(f"Failed to generate CSV for market data: {e}", exc_info=True)
        # Re-raise the exception or handle as appropriate for your application flow
        raise

@data_router.get("/ship_production")
async def get_ship_production(request: Request):
    try:
        pool = request.state.db.pool

        # Acquire a connection from the pool
        async with pool.acquire() as conn:
            # Fetch all rows from the ship_production table
            records = await conn.fetch("SELECT * FROM ship_production ORDER BY orderid ASC")
            
            # Convert the list of asyncpg.Record objects to a list of dictionaries
            data = [dict(record) for record in records]
            
            return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        logger.error(f"Failed to fetch ship production data: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Failed to retrieve ship production data: {e}"}
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
        pool = request.state.db.pool
        if not pool:
            return JSONResponse(status_code=500, content={"success": False, "message": "Database pool not configured."})

        # Acquire a connection from the pool
        async with pool.acquire() as conn:
            # Query the database for shipments on the specified planetId
            records = await conn.fetch(
                "SELECT * FROM planet_shipments WHERE planetid = $1 ORDER BY id", 
                planetId
            )
            
            # Convert the asyncpg.Record objects to a list of dictionaries
            shipments = [dict(record) for record in records]
            
            return JSONResponse(content={"success": True, "data": shipments})

    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"PostgreSQL error when fetching shipments: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "A database error occurred."}
        )
    except Exception as e:
        logger.error(f"Failed to fetch shipments: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected error occurred."}
        )