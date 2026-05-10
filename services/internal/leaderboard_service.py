import logging

from repositories import leaderboard_repo

logger = logging.getLogger(__name__)

async def get_formatted_production_leaderboard(db):
    """
    Orchestrates the fetching and formatting of the leaderboard and its history.
    """
    try:
        # 1. Fetch raw data from DB
        current_rows = await leaderboard_repo.fetch_current_top_25(db)
        history_rows = await leaderboard_repo.fetch_top_25_history(db)

        # 2. Format the history into Recharts pivot structure
        # Expected shape: { "AAR": { "2026-04-01": {"date": "2026-04-01", "COMP1": 100, "COMP2": 95} } }
        history_pivot = {}
        for row in history_rows:
            ticker = row["material_ticker"]
            date_str = row["history_date"].isoformat()
            comp_code = row["company_code"] or "UNKNOWN"
            score = float(row["score"])

            if ticker not in history_pivot:
                history_pivot[ticker] = {}

            if date_str not in history_pivot[ticker]:
                history_pivot[ticker][date_str] = {"date": date_str}

            history_pivot[ticker][date_str][comp_code] = score

        # 3. Format the current top 25
        leaderboard_data = {}
        for row in current_rows:
            ticker = row["material_ticker"]

            if ticker not in leaderboard_data:
                # Retrieve and sort the history for this specific ticker
                material_history = []
                if ticker in history_pivot:
                    material_history = sorted(list(history_pivot[ticker].values()), key=lambda x: x["date"])

                leaderboard_data[ticker] = {
                    "ticker": ticker,
                    "avg_price_7d": float(row["avg_price_7d"]),
                    "record_date": row["record_date"].isoformat() if row["record_date"] else None,
                    "top_25": [],
                    "history30d": material_history
                }

            leaderboard_data[ticker]["top_25"].append({
                "company_code": row["company_code"] or "UNKNOWN",
                "company_name": row["company_name"] or "Unknown Company",
                "score": float(row["score"]),
                "estimated_value": float(row["estimated_value_7d"])
            })

        # Return as a flat list
        return list(leaderboard_data.values())

    except Exception as e:
        logger.error(f"Service Error processing production leaderboard: {e}", exc_info=True)
        return []
