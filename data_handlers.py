# data_handlers.py

import asyncio
import csv
from datetime import datetime
from decimal import Decimal
from io import StringIO
import uuid
import asyncpg
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response, status, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
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

@data_router.get("/corp_prices_all")
async def get_corp_prices(request: Request):
    try:
        pool = request.state.db.pool
        async with pool.acquire() as con:
            data = await con.fetch("""
                SELECT ticker, price FROM material_prices;
            """)

            # Convert the list of records to a list of dictionaries.
            # Convert the Decimal 'price' to a float to make it JSON serializable.
            material_data = [
                {
                    "ticker": record["ticker"], 
                    "price": float(record["price"]) 
                } 
                for record in data
            ]

    except Exception as e:
        print(f"An error occurred: {e}") 
        return JSONResponse(content={"success": False, "message": "Failed to retrieve data."}, status_code=500)
        
    return JSONResponse(content={"success": True, "data": material_data})

@data_router.get("/ship_production")
async def get_ship_production(request: Request):
    try:
        pool = request.state.db.pool

        # Acquire a connection from the pool
        async with pool.acquire() as conn:
            # Fetch all rows from the ship_production table
            records = await conn.fetch("SELECT * FROM ship_production ORDER BY orderid ASC")

            try:
                storage = await conn.fetch("""SELECT
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
            except Exception as e:
                raise
            # Convert the list of asyncpg.Record objects to a list of dictionaries
            data = {
                'shiporders': [dict(record) for record in records],
                'storageitems': [dict(record) for record in storage]
            }
            
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

@data_router.post("/create_vendor_store")
async def create_vendor_store(
    payload: Dict[str, Any], 
    request: Request, 
    user_id: str = Depends(get_current_user_id)
):
    """
    Creates a new vendor store and its associated orders in a single transaction.
    """
    try:
        pool = request.state.db.pool

        # Access vendor and orders data from the payload
        vendor_data = payload.get("vendorData", {})
        orders_data = payload.get("materials", [])

        # Generate a unique vendor ID
        vendor_id = str(uuid.uuid4())

        # Extract vendor details
        company_name = vendor_data.get("company_name")
        game_name = vendor_data.get("game_name")
        company_code = vendor_data.get("company_code")
        corp_name = vendor_data.get("corp_name")
        is_active = vendor_data.get("is_active", True)
        cx = vendor_data.get("cx")

        if not company_name:
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "Vendor company name is required."}
            )
        
        

        # Use a database transaction to ensure atomicity
        async with pool.acquire() as conn:
            async with conn.transaction():
                # SQL to insert into user_vendors
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
                        cx
                    )
                except Exception as e:
                    logger.error(f"Failed on inserting vendorData: {e}.")
                    raise

                # Prepare the batch insert for user_vendor_orders
                order_records = []
                for order in orders_data:
                    order_id = str(uuid.uuid4())
                    #price_type = order.get("price_type", "fixed").lower()
                    
                    fixed_price = order.get("price")
                    #min_price = None
                    #max_price = None
                    #if price_type == "fixed":
                    #    fixed_price = order.get("price")
                    #elif price_type == "range":
                    #    min_price = order.get("min_price")
                    #    max_price = order.get("max_price")

                    # Each tuple represents one row to be inserted
                    order_records.append((
                        order_id,
                        vendor_id,
                        order.get("materialid"),
                        order.get("ticker"),
                        order.get("orderType"),
                        #order.get("quantity"),
                        #price_type,
                        fixed_price,
                        #min_price,
                        #max_price
                    ))
                
                # SQL to insert into user_vendor_orders
                orders_query = """
                INSERT INTO user_vendor_orders (
                    orderid, vendorid, materialid, materialticker, ordertype, fixedprice
                ) VALUES (
                    $1, $2, $3, $4, $5, $6
                );
                """
                try:
                    if order_records:
                        await conn.executemany(orders_query, order_records)
                except Exception as e:
                    logger.error(f"Failed to insert orders: {e}")
                    raise

        logger.info(f"Vendor store and {len(orders_data)} orders created for user '{user_id}'.")
        return JSONResponse(
            content={"success": True, "message": "Vendor store and orders created."}
        )

    except asyncpg.UniqueViolationError:
        logger.warning(f"Attempt to create a second vendor store for user '{user_id}'.")
        return JSONResponse(
            status_code=409,
            content={"success": False, "message": "A vendor store already exists for this user."}
        )
    except Exception as e:
        logger.error(f"Failed to create vendor store and orders for user '{user_id}': {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected server error occurred."}
        )

def serialize_record(record):
    # This helper function converts non-JSON serializable objects
    return {
        key: value.isoformat() if isinstance(value, datetime) 
            else float(value) if isinstance(value, Decimal)
            else value
        for key, value in record.items()
    }

@data_router.get("/user_vendor_store")
async def get_user_vendor_stores(
    request: Request, 
    user_id: str = Depends(get_current_user_id)
):
    try:
        async with request.state.db.pool.acquire() as conn:
            # Step 1: Fetch the single vendor record for the user
            vendor_record = await conn.fetch(
                "SELECT companycode, companyname, corpname, cx, gamename, isactive, vendorid FROM user_vendors WHERE userid = $1;", user_id
            )

            if not vendor_record:
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "message": "No vendor store found for this user."}
                )
            vendor_record = vendor_record[0]
            
            # Step 2: Fetch all orders associated with this vendor
            orders_records = await conn.fetch(
                "SELECT orderid, materialid, materialticker, ordertype, fixedprice FROM user_vendor_orders WHERE vendorid = $1;", vendor_record['vendorid']
            )

            # Step 3: Combine the data into a single, structured response
            vendor_store = {
                "vendor": serialize_record(vendor_record),
                "orders": [serialize_record(order) for order in orders_records]
            }

            return JSONResponse(
                content={"success": True, "data": vendor_store}
            )

    except Exception as e:
        logger.error(f"Failed to retrieve vendor store and orders for user '{user_id}': {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected server error occurred."}
        )

@data_router.get("/vendor_stores")
async def get_vendor_stores(request: Request):
    try:
        pool = request.state.db.pool

        async with pool.acquire() as con:
            # Step 1: Fetch all vendors and their orders in a single query
            vendors_orders_query = """
                SELECT uv.vendorid, uv.companycode, uv.companyname, uv.corpname, uv.gamename, uv.isactive,
                       uvo.orderid, uvo.materialticker, uvo.ordertype, uvo.fixedprice
                FROM user_vendors AS uv
                INNER JOIN user_vendor_orders AS uvo ON uvo.vendorid = uv.vendorid;
            """
            vendors_data = await con.fetch(vendors_orders_query)

            # Check if there are any vendors with orders
            if not vendors_data:
                return JSONResponse(status_code=200, content={"success": True, "vendors": []})

            # Step 2: Extract a unique list of (gamename, materialticker) pairs
            gamename_ticker_pairs = [
                (record['gamename'], record['materialticker'])
                for record in vendors_data
            ]
            
            # Step 3: Dynamically construct and run the material data query
            # We use a VALUES clause to handle the bulk fetch in a single query.
            values_str = ", ".join([f"('{g}', '{t}')" for g, t in gamename_ticker_pairs])
            
            material_data_query = f"""
                SELECT
                    ud.displayname,
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
                INNER JOIN
                    (VALUES {values_str}) AS t(displayname, ticker)
                    ON ud.displayname = t.displayname AND mt.ticker = t.ticker
                WHERE
                    sys.name = 'Hortus'
                    AND st.name != 'Hortus';
            """
            
            materials_records = await con.fetch(material_data_query)
            
            # Create a dictionary for quick lookups by a (gamename, ticker) tuple
            materials_dict = {(record['displayname'], record['ticker']): record['quantity'] for record in materials_records}

            # Step 4: Process the joined results and attach the material data
            vendors_dict = {}
            for record in vendors_data:
                vendor_id = record['vendorid']
                if vendor_id not in vendors_dict:
                    vendors_dict[vendor_id] = {
                        "vendor": {
                            "vendorid": record['vendorid'],
                            "companycode": record['companycode'],
                            "companyname": record['companyname'],
                            "corpname": record['corpname'],
                            "gamename": record['gamename'],
                            "isactive": record['isactive']
                        },
                        "orders": []
                    }
                
                order_data = serialize_record({
                    "orderid": record['orderid'],
                    "materialticker": record['materialticker'],
                    "ordertype": record['ordertype'],
                    "fixedprice": record['fixedprice']
                })
                
                # Attach the quantity to the correct order
                gamename_ticker_key = (record['gamename'], record['materialticker'])
                order_data['quantity'] = materials_dict.get(gamename_ticker_key, 0)
                
                vendors_dict[vendor_id]["orders"].append(order_data)

            vendors_list = list(vendors_dict.values())
            
            return JSONResponse(status_code=200, content={"success": True, "vendors": vendors_list})

    except Exception as e:
        logger.error(f"Failed to get vendor stores: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected server error occurred."}
        )

@data_router.post("/materials_price_list")
async def get_materials_price_list(payload: Dict[str, Any], request: Request):
    try:
        pool = request.state.db.pool

        cx = payload.get('cx', None)

        if not cx:
            raise HTTPException(
                status_code=400,
                detail={"success": False, "message": "The 'cx' field is required in the request payload."}
            )

        query = """
            WITH user_storage AS (
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
                stations AS st ON st.warehouseid = w.warehouseid
            WHERE
                ud.displayname = 'xSupeFly'
                AND sys.name = 'Hortus'
                AND st.name != 'Hortus'
        )
        SELECT
            mt.ticker,
            mt.materialid,
            COALESCE(si.quantity, 0) AS quantity,
            cxb.askprice
        FROM
            cx_brokers AS cxb
        INNER JOIN
            materials AS mt ON mt.materialid = cxb.materialid
        LEFT JOIN
            user_storage AS us ON true
        LEFT JOIN
            storage_items AS si ON si.storageid = us.storageid AND si.materialid = mt.materialid
        WHERE
            cxb.ticker LIKE $1
        ORDER BY
            cxb.ticker;
        """
        
        search_pattern = f'%.{cx}'

        async with pool.acquire() as con:
            materials_data = await con.fetch(query, search_pattern)

            # Check if any materials were found and return the data
            if not materials_data:
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "message": "No materials found for the given CX code."}
                )
            
            data = [dict(record) for record in materials_data]

            return JSONResponse(status_code=200, content={"success": True, "materials": data})

    except Exception as e:
        logger.error(f"Failed to get materials price list: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected server error occurred."}
        )
    
# Pydantic models for request body validation
class OrderItem(BaseModel):
    orderid: Optional[str] = None
    materialticker: str
    ordertype: str
    fixedprice: float
    materialid: str

class EditOrdersRequest(BaseModel):
    vendorid: str
    orders_to_update: List[OrderItem] = Field(default_factory=list)
    order_ids_to_delete: List[str] = Field(default_factory=list)

@data_router.post("/vendor_stores/edit_orders")
async def edit_vendor_orders(
    request: Request,
    payload: EditOrdersRequest,
    user_id: str = Depends(get_current_user_id)
):
    try:
        pool = request.state.db.pool

        async with pool.acquire() as con:
            async with con.transaction():
                # Step 1: Verify the user owns the vendor store
                vendor_record = await con.fetch(
                    "SELECT userid, gamename FROM user_vendors WHERE vendorid = $1", payload.vendorid
                )

                vendor_record = vendor_record[0]

                if not vendor_record or vendor_record['userid'] != user_id:
                    raise HTTPException(status_code=403, detail="Not authorized to edit this vendor store.")

                # Step 2: Delete orders
                if payload.order_ids_to_delete:
                    await con.executemany(
                        "DELETE FROM user_vendor_orders WHERE orderid = $1 AND vendorid = $2",
                        [(order_id, payload.vendorid) for order_id in payload.order_ids_to_delete]
                    )

                # Step 3: Add or update orders
                for order in payload.orders_to_update:
                    if order.orderid:
                        # Update existing order
                        await con.execute(
                            """
                            INSERT INTO user_vendor_orders (orderid, vendorid, materialticker, materialid, ordertype, fixedprice)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (orderid) DO UPDATE SET
                                materialticker = EXCLUDED.materialticker,
                                ordertype = EXCLUDED.ordertype,
                                fixedprice = EXCLUDED.fixedprice;
                            """,
                            order.orderid,
                            payload.vendorid,
                            order.materialticker,
                            order.materialid,
                            order.ordertype,
                            order.fixedprice
                        )

                from discord_bot.webhook import send_discord_message_sync
                send_discord_message_sync(f"Vendor store of {vendor_record['gamename']} was updated!")

                return JSONResponse(status_code=200, content={"success": True, "message": "Vendor store orders updated successfully."})
    
    except HTTPException as e:
        logger.error(f"Authorization error for user '{user_id}': {e.detail}")
        return JSONResponse(status_code=e.status_code, content={"success": False, "message": e.detail})
    
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Database error while editing orders for user '{user_id}': {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "A database error occurred."})

    except Exception as e:
        logger.error(f"Failed to edit vendor store orders for user '{user_id}': {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "An unexpected server error occurred."})