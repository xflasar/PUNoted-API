import csv
import logging
from io import StringIO
from typing import Optional, List, Dict, Any
from endpoints.Public.repositories.vendors_repo import fetch_public_vendors

logger = logging.getLogger(__name__)

async def get_vendors_data(
    db, search: Optional[str] = None, corp: Optional[str] = None, operator: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Service layer for public vendors data directory.
    """
    try:
        return await fetch_public_vendors(db, search=search, corp=corp, operator=operator)
    except Exception as e:
        logger.error(f"Failed to fetch vendors data in service: {e}", exc_info=True)
        raise

async def generate_vendors_csv(
    db, search: Optional[str] = None, corp: Optional[str] = None, operator: Optional[str] = None
) -> str:
    """
    Generates a CSV string representation of the public vendor directory.
    """
    try:
        vendors = await get_vendors_data(db, search=search, corp=corp, operator=operator)
        output = StringIO()
        writer = csv.writer(output)

        # Write CSV Header
        writer.writerow([
            "vendor_id", "company_code", "company_name", "corp_name", "operator_name",
            "active", "last_active", "exchange", "order_id", "ticker", "order_type",
            "fixed_price", "corp_price", "cx_price", "total_available",
            "location_id", "location_name", "location_code", "location_available"
        ])

        for v in vendors:
            v_info = v.get("vendor", {})
            orders = v.get("orders", [])
            for o in orders:
                price = o.get("price", {})
                locations = o.get("location", [])
                for loc in locations:
                    writer.writerow([
                        v_info.get("vendorid", ""),
                        v_info.get("companycode", ""),
                        v_info.get("companyname", ""),
                        v_info.get("corpname", ""),
                        v_info.get("gamename", ""),
                        v_info.get("isactive", False),
                        v_info.get("activity", ""),
                        v_info.get("cx", ""),
                        o.get("orderid", ""),
                        o.get("materialticker", ""),
                        o.get("ordertype", ""),
                        o.get("fixedprice", 0.0),
                        price.get("corpprice", 0.0),
                        price.get("cxprice", 0.0),
                        o.get("available", 0.0),
                        loc.get("id", ""),
                        loc.get("location_name", ""),
                        loc.get("location_code", ""),
                        loc.get("available", 0.0),
                    ])

        return output.getvalue()
    except Exception as e:
        logger.error(f"Failed to generate CSV for vendors data: {e}", exc_info=True)
        raise
