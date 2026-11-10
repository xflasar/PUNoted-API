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
    payload = raw_records.get("payload") if isinstance(raw_records, dict) else {}
    if not isinstance(payload, dict):
        payload = {}
    orders = payload.get("orders") or []
    for record in orders:
        if not isinstance(record, dict):
            continue
        trades = []

        for trade in (record.get("trades") or []):
            if not isinstance(trade, dict):
                continue
            trade_time = trade.get("time")
            if trade_time is not None and isinstance(trade_time, dict) and trade_time.get("timestamp") is not None:
                trade_time = datetime.fromtimestamp(trade_time["timestamp"] / 1000)
            else:
                trade_time = None

            price_obj = trade.get("price") or {}
            partner_obj = trade.get("partner") or {}

            trades.append(
                {
                    "tradeid": trade.get("id"),
                    "amount": trade.get("amount"),
                    "priceamount": price_obj.get("amount"),
                    "pricecurrency": price_obj.get("currency"),
                    "tradetime": trade_time,
                    "partnerid": partner_obj.get("id"),
                    "partnername": partner_obj.get("name"),
                    "partnercode": partner_obj.get("code"),
                }
            )

        created = record.get("created")
        if created is not None and isinstance(created, dict) and created.get("timestamp") is not None:
            created = datetime.fromtimestamp(created["timestamp"] / 1000)
        else:
            created = None

        exch = record.get("exchange") or {}
        mat = record.get("material") or {}
        lim = record.get("limit") or {}

        converted_records.append(
            {
                "orderid": record.get("id"),
                "exchangeid": exch.get("id"),
                "brokerid": record.get("brokerId"),
                "type": record.get("type"),
                "materialid": mat.get("id"),
                "amount": record.get("amount"),
                "initialamount": record.get("initialAmount"),
                "limitamount": lim.get("amount"),
                "limitcurrency": lim.get("currency"),
                "status": record.get("status"),
                "created": created,
                "trades": trades,
            }
        )
    return converted_records

def handle_comex_trade_order_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    trades = []

    record = raw_record.get("payload") if isinstance(raw_record, dict) else {}
    if not isinstance(record, dict):
        record = {}

    for trade in (record.get("trades") or []):
        if not isinstance(trade, dict):
            continue
        trade_time = trade.get("time")
        if trade_time is not None and isinstance(trade_time, dict) and trade_time.get("timestamp") is not None:
            trade_time = datetime.fromtimestamp(trade_time["timestamp"] / 1000)
        else:
            trade_time = None

        price_obj = trade.get("price") or {}
        partner_obj = trade.get("partner") or {}

        trades.append(
            {
                "tradeid": trade.get("id"),
                "amount": trade.get("amount"),
                "priceamount": price_obj.get("amount"),
                "pricecurrency": price_obj.get("currency"),
                "tradetime": trade_time,
                "partnerid": partner_obj.get("id"),
                "partnername": partner_obj.get("name"),
                "partnercode": partner_obj.get("code"),
            }
        )

    created = record.get("created")
    if created is not None and isinstance(created, dict) and created.get("timestamp") is not None:
        created = datetime.fromtimestamp(created["timestamp"] / 1000)
    else:
        created = None

    exch = record.get("exchange") or {}
    mat = record.get("material") or {}
    lim = record.get("limit") or {}

    converted_record = {
        "orderid": record.get("id"),
        "exchangeid": exch.get("id"),
        "brokerid": record.get("brokerId"),
        "type": record.get("type"),
        "materialid": mat.get("id"),
        "amount": record.get("amount"),
        "initialamount": record.get("initialAmount"),
        "limitamount": lim.get("amount"),
        "limitcurrency": lim.get("currency"),
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
    record = raw_record.get("payload") if isinstance(raw_record, dict) else {}
    if not isinstance(record, dict):
        record = {}
    converted_record = {"orderid": record.get("orderId")}
    return converted_record

def convert_comex_broker_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    record = raw_records.get("payload") if isinstance(raw_records, dict) else {}
    if not isinstance(record, dict):
        record = {}
    buyOrders = []
    sellOrders = []
    for buy in (record.get("buyingOrders") or []):
        if not isinstance(buy, dict):
            continue
        lim = buy.get("limit") or {}
        trader = buy.get("trader") or {}
        buyOrders.append(
            {
                "orderid": buy.get("id"),
                "amount": buy.get("amount"),
                "priceamount": lim.get("amount"),
                "pricecurrency": lim.get("currency"),
                "traderid": trader.get("id"),
                "tradername": trader.get("name"),
                "tradercode": trader.get("code"),
            }
        )

    for sell in (record.get("sellingOrders") or []):
        if not isinstance(sell, dict):
            continue
        lim = sell.get("limit") or {}
        trader = sell.get("trader") or {}
        sellOrders.append(
            {
                "orderid": sell.get("id"),
                "amount": sell.get("amount"),
                "priceamount": lim.get("amount"),
                "pricecurrency": lim.get("currency"),
                "traderid": trader.get("id"),
                "tradername": trader.get("name"),
                "tradercode": trader.get("code"),
            }
        )
    price_time = record.get("priceTime")
    if price_time is not None and isinstance(price_time, dict) and price_time.get("timestamp") is not None:
        price_time = datetime.fromtimestamp(price_time["timestamp"] / 1000)
    else:
        price_time = None

    addr = record.get("address") or {}
    lines = addr.get("lines") or []
    sys_id = (lines[0].get("entity") or {}).get("id") if len(lines) > 0 and isinstance(lines[0], dict) else None
    sta_id = (lines[1].get("entity") or {}).get("id") if len(lines) > 1 and isinstance(lines[1], dict) else None

    exch = record.get("exchange") or {}
    curr = record.get("currency") or {}
    mat = record.get("material") or {}

    converted_records.append(
        {
            "brokermaterialid": record.get("id"),
            "addresssystemid": sys_id,
            "addressstationid": sta_id,
            "exchangeid": exch.get("id"),
            "currencyid": curr.get("code"),
            "demand": record.get("demand"),
            "supply": record.get("supply"),
            "traded": record.get("traded"),
            "ticker": record.get("ticker"),
            "askamount": (record.get("ask") or {}).get("amount"),
            "askprice": ((record.get("ask") or {}).get("price") or {}).get("amount"),
            "bidamount": (record.get("bid") or {}).get("amount"),
            "bidprice": ((record.get("bid") or {}).get("price") or {}).get("amount"),
            "high": (record.get("high") or {}).get("amount"),
            "low": (record.get("low") or {}).get("amount"),
            "materialid": mat.get("id"),
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
