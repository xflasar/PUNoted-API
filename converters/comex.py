# converters/comex.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


def convert_comex_trade_orders_data(
    raw_records: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    for record in raw_records["payload"]["orders"]:
        trades = []

        for trade in record.get("trades"):
            # Handle earliest contract timestamp
            trade_time = trade.get("time")
            if trade_time is not None and trade_time.get("timestamp") is not None:
                trade_time = datetime.fromtimestamp(trade_time["timestamp"] / 1000)
            else:
                trade_time = None  # Or None, 0, etc. based on your needs

            trades.append(
                {
                    "tradeid": trade.get("id"),
                    "amount": trade.get("amount"),
                    "priceamount": trade.get("price").get("amount"),
                    "pricecurrency": trade.get("price").get("currency"),
                    "tradetime": trade_time,
                    "partnerid": trade.get("partner").get("id"),
                    "partnername": trade.get("partner").get("name"),
                    "partnercode": trade.get("partner").get("code"),
                }
            )

        # Handle earliest contract timestamp
        created = record.get("created")
        if created is not None and created.get("timestamp") is not None:
            created = datetime.fromtimestamp(created["timestamp"] / 1000)
        else:
            created = None  # Or None, 0, etc. based on your needs

        converted_records.append(
            {
                "orderid": record.get("id"),
                "exchangeid": record.get("exchange").get("id"),
                "brokerid": record.get("brokerId"),
                "type": record.get("type"),
                "materialid": record.get("material").get("id"),
                "amount": record.get("amount"),
                "initialamount": record.get("initialAmount"),
                "limitamount": record.get("limit").get("amount"),
                "limitcurrency": record.get("limit").get("currency"),
                "status": record.get("status"),
                "created": created,
                "trades": trades,
            }
        )
    return converted_records

def handle_comex_trade_order_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    trades = []

    record = raw_record["payload"]

    for trade in record.get("trades"):
        # Handle earliest contract timestamp
        trade_time = trade.get("time")
        if trade_time is not None and trade_time.get("timestamp") is not None:
            trade_time = datetime.fromtimestamp(trade_time["timestamp"] / 1000)
        else:
            trade_time = None  # Or None, 0, etc. based on your needs

        trades.append(
            {
                "tradeid": trade.get("id"),
                "amount": trade.get("amount"),
                "priceamount": trade.get("price").get("amount"),
                "pricecurrency": trade.get("price").get("currency"),
                "tradetime": trade_time,
                "partnerid": trade.get("partner").get("id"),
                "partnername": trade.get("partner").get("name"),
                "partnercode": trade.get("partner").get("code"),
            }
        )

    # Handle earliest contract timestamp
    created = record.get("created")
    if created is not None and created.get("timestamp") is not None:
        created = datetime.fromtimestamp(created["timestamp"] / 1000)
    else:
        created = None  # Or None, 0, etc. based on your needs

    converted_record = {
        "orderid": record.get("id"),
        "exchangeid": record.get("exchange").get("id"),
        "brokerid": record.get("brokerId"),
        "type": record.get("type"),
        "materialid": record.get("material").get("id"),
        "amount": record.get("amount"),
        "initialamount": record.get("initialAmount"),
        "limitamount": record.get("limit").get("amount"),
        "limitcurrency": record.get("limit").get("currency"),
        "status": record.get("status"),
        "created": created,
        "trades": trades,
    }
    return converted_record


def convert_comex_trade_order_added_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = handle_comex_trade_order_data(raw_record)
    return converted_record

def convert_comex_trade_order_update_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = handle_comex_trade_order_data(raw_record)
    return converted_record

def convert_comex_trade_order_remove(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """Converts raw data to match the 'comex_trade_order_remove' table schema."""
    record = raw_record["payload"]
    converted_record = {"orderid": record.get("orderId")}
    return converted_record

def convert_comex_broker_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    record = raw_records["payload"]
    buyOrders = []
    sellOrders = []
    for buy in record.get("buyingOrders"):
        buyOrders.append(
            {
                "orderid": buy.get("id"),
                "amount": buy.get("amount"),
                "priceamount": buy.get("limit").get("amount"),
                "pricecurrency": buy.get("limit").get("currency"),
                "traderid": buy.get("trader").get("id"),
                "tradername": buy.get("trader").get("name"),
                "tradercode": buy.get("trader").get("code"),
            }
        )

    for sell in record.get("sellingOrders"):
        sellOrders.append(
            {
                "orderid": sell.get("id"),
                "amount": sell.get("amount"),
                "priceamount": sell.get("limit").get("amount"),
                "pricecurrency": sell.get("limit").get("currency"),
                "traderid": sell.get("trader").get("id"),
                "tradername": sell.get("trader").get("name"),
                "tradercode": sell.get("trader").get("code"),
            }
        )
    # Handle earliest contract timestamp
    price_time = record.get("priceTime")
    if price_time is not None and price_time.get("timestamp") is not None:
        price_time = datetime.fromtimestamp(price_time["timestamp"] / 1000)
    else:
        price_time = None  # Or None, 0, etc. based on your needs

    converted_records.append(
        {
            "brokermaterialid": record.get("id"),
            "addresssystemid": record.get("address", {}).get("lines", [{}, {}])[0].get("entity", {}).get("id"),
            "addressstationid": record.get("address", {}).get("lines", [{}, {}])[1].get("entity", {}).get("id"),
            "exchangeid": record.get("exchange" or {}).get("id"),
            "currencyid": record.get("currency" or {}).get("code"),
            "demand": record.get("demand"),
            "supply": record.get("supply"),
            "traded": record.get("traded"),
            "ticker": record.get("ticker"),
            "askamount": (record.get("ask") or {}).get("amount"),
            "askprice": (record.get("ask") or {}).get("price", {}).get("amount"),
            "bidamount": (record.get("bid") or {}).get("amount"),
            "bidprice": (record.get("bid") or {}).get("price", {}).get("amount"),
            "high": (record.get("high") or {}).get("amount"),
            "low": (record.get("low") or {}).get("amount"),
            "materialid": record.get("material", {}).get("id"),
            "narrowpricebandhigh": (record.get("narrowPriceBand") or {}).get("high"),
            "narrowpricebandlow": (record.get("narrowPriceBand") or {}).get("low"),
            "price": (record.get("price") or {}).get("amount"),
            "priceaverage": (record.get("price") or {}).get("amount"),
            "pricetime": price_time,
            "volume": (record.get("volume") or {}).get("amount"),
            "widepricebandhigh": (record.get("widePriceBand") or {}).get("high"),
            "widepricebandlow": (record.get("widePriceBand") or {}).get("low"),
            "alltimehigh": (record.get("allTimeHigh") or {}).get("amount"),
            "alltimelow": (record.get("allTimeLow") or {}).get("amount"),
            "buy": buyOrders,
            "sell": sellOrders,
        }
    )
    return converted_records

def convert_commodity_exchanges_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'commodity_exchanges' table schema."""
    converted_records = []
    for record in raw_records['payload']:
        # 1. Safely extract nested objects
        operator = record.get("operator", {})
        currency = record.get("currency", {})
        address_lines = record.get("address", {}).get("lines", [])

        # 2. Iterate through address lines to find System and Station IDs
        system_id = None
        station_id = None

        for line in address_lines:
            line_type = line.get("type")
            entity = line.get("entity", {})

            if line_type == "SYSTEM":
                system_id = entity.get("id")
            elif line_type == "STATION":
                station_id = entity.get("id")

        # 3. Build the flat record
        converted_records.append(
            {
                "id": record.get("id"),
                "name": record.get("name"),
                "code": record.get("code"),  # e.g. "AI1", "IC1"
                "operatorid": operator.get("id"),
                "currencyname": currency.get("name"),
                "currencycode": currency.get("code"), # e.g. "AIC", "ICA"
                "currencynumericcode": currency.get("numericCode"),
                "currencydecimals": currency.get("decimals"),
                "systemid": system_id,
                "stationid": station_id,
            }
        )

    return converted_records
