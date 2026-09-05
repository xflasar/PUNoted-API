import logging
from typing import List, Optional, Dict, Any
import asyncpg

logger = logging.getLogger(__name__)

SQL_GET_PIVOTED_MARKET_DATA = """
WITH parsed_brokers AS (
    SELECT 
        brokermaterialid,
        SPLIT_PART(ticker, '.', 1) AS material_ticker,
        SPLIT_PART(ticker, '.', 2) AS exchange_code,
        priceaverage, askamount, askprice, bidamount, bidprice,
        xata_updatedat
    FROM cx_brokers
    WHERE ticker IS NOT NULL AND POSITION('.' IN ticker) > 0
      AND ($2::text IS NULL OR $2::text = '' OR SPLIT_PART(ticker, '.', 1) = SPLIT_PART($2::text, '.', 1) OR brokermaterialid = $2::text)
),
history_stats AS (
    SELECT
        SPLIT_PART(ticker, '.', 1) AS material_ticker,
        SPLIT_PART(ticker, '.', 2) AS exchange_code,
        ROUND(COALESCE(AVG(px) FILTER (WHERE ts >= NOW() - INTERVAL '7 days'), 0)::numeric, 2) AS avg_7d,
        ROUND(COALESCE(AVG(px) FILTER (WHERE ts >= NOW() - INTERVAL '30 days'), 0)::numeric, 2) AS avg_30d
    FROM (
        SELECT 
            ticker,
            COALESCE(priceaverage, askprice, bidprice, price) AS px,
            snapshot_at AS ts
        FROM cx_brokers_history
        WHERE snapshot_at >= NOW() - INTERVAL '30 days'
          AND COALESCE(priceaverage, askprice, bidprice, price) IS NOT NULL
          AND COALESCE(priceaverage, askprice, bidprice, price) > 0
          AND ($2::text IS NULL OR $2::text = '' OR SPLIT_PART(ticker, '.', 1) = SPLIT_PART($2::text, '.', 1) OR brokermaterialid = $2::text)
        
        UNION ALL
        
        SELECT 
            ticker,
            COALESCE(priceaverage, askprice, bidprice, price) AS px,
            COALESCE(xata_updatedat, NOW()) AS ts
        FROM cx_brokers
        WHERE COALESCE(priceaverage, askprice, bidprice, price) IS NOT NULL
          AND COALESCE(priceaverage, askprice, bidprice, price) > 0
          AND ($2::text IS NULL OR $2::text = '' OR SPLIT_PART(ticker, '.', 1) = SPLIT_PART($2::text, '.', 1) OR brokermaterialid = $2::text)
    ) combined
    GROUP BY 1, 2
),
mm_totals AS (
    SELECT 
        pb.material_ticker,
        COALESCE(SUM(b.priceamount), 0) AS mm_buy_sum,
        COALESCE(SUM(s.priceamount), 0) AS mm_sell_sum
    FROM parsed_brokers pb
    LEFT JOIN cx_brokers_buy_orders b 
        ON pb.brokermaterialid = b.brokermaterialid 
       AND b.tradername = 'Insitor Cooperative Market Maker'
    LEFT JOIN cx_brokers_sell_orders s 
        ON pb.brokermaterialid = s.brokermaterialid 
       AND s.tradername = 'Insitor Cooperative Market Maker'
    GROUP BY pb.material_ticker
)
SELECT
    pb.material_ticker AS "Ticker",
    COALESCE(mm.mm_buy_sum, 0) AS "MMBuy",
    COALESCE(mm.mm_sell_sum, 0) AS "MMSell",
    
    -- AI1
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.priceaverage END) AS "AI1-Average",
    MAX(CASE WHEN hs.exchange_code = 'AI1' THEN hs.avg_7d END) AS "AI1-7dAvg",
    MAX(CASE WHEN hs.exchange_code = 'AI1' THEN hs.avg_30d END) AS "AI1-30dAvg",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.askamount END) AS "AI1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.askprice END) AS "AI1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.askamount END) AS "AI1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.bidamount END) AS "AI1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.bidprice END) AS "AI1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.bidamount END) AS "AI1-BidAvail",
    
    -- CI1
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.priceaverage END) AS "CI1-Average",
    MAX(CASE WHEN hs.exchange_code = 'CI1' THEN hs.avg_7d END) AS "CI1-7dAvg",
    MAX(CASE WHEN hs.exchange_code = 'CI1' THEN hs.avg_30d END) AS "CI1-30dAvg",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.askamount END) AS "CI1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.askprice END) AS "CI1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.askamount END) AS "CI1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.bidamount END) AS "CI1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.bidprice END) AS "CI1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.bidamount END) AS "CI1-BidAvail",
    
    -- CI2
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.priceaverage END) AS "CI2-Average",
    MAX(CASE WHEN hs.exchange_code = 'CI2' THEN hs.avg_7d END) AS "CI2-7dAvg",
    MAX(CASE WHEN hs.exchange_code = 'CI2' THEN hs.avg_30d END) AS "CI2-30dAvg",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.askamount END) AS "CI2-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.askprice END) AS "CI2-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.askamount END) AS "CI2-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.bidamount END) AS "CI2-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.bidprice END) AS "CI2-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.bidamount END) AS "CI2-BidAvail",
    
    -- NC1
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.priceaverage END) AS "NC1-Average",
    MAX(CASE WHEN hs.exchange_code = 'NC1' THEN hs.avg_7d END) AS "NC1-7dAvg",
    MAX(CASE WHEN hs.exchange_code = 'NC1' THEN hs.avg_30d END) AS "NC1-30dAvg",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.askamount END) AS "NC1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.askprice END) AS "NC1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.askamount END) AS "NC1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.bidamount END) AS "NC1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.bidprice END) AS "NC1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.bidamount END) AS "NC1-BidAvail",
    
    -- NC2
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.priceaverage END) AS "NC2-Average",
    MAX(CASE WHEN hs.exchange_code = 'NC2' THEN hs.avg_7d END) AS "NC2-7dAvg",
    MAX(CASE WHEN hs.exchange_code = 'NC2' THEN hs.avg_30d END) AS "NC2-30dAvg",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.askamount END) AS "NC2-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.askprice END) AS "NC2-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.askamount END) AS "NC2-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.bidamount END) AS "NC2-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.bidprice END) AS "NC2-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.bidamount END) AS "NC2-BidAvail",
    
    -- IC1
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.priceaverage END) AS "IC1-Average",
    MAX(CASE WHEN hs.exchange_code = 'IC1' THEN hs.avg_7d END) AS "IC1-7dAvg",
    MAX(CASE WHEN hs.exchange_code = 'IC1' THEN hs.avg_30d END) AS "IC1-30dAvg",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.askamount END) AS "IC1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.askprice END) AS "IC1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.askamount END) AS "IC1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.bidamount END) AS "IC1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.bidprice END) AS "IC1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.bidamount END) AS "IC1-BidAvail",
    
    MAX(pb.xata_updatedat) AS "last_update"
FROM parsed_brokers pb
LEFT JOIN history_stats hs ON pb.material_ticker = hs.material_ticker
LEFT JOIN mm_totals mm ON pb.material_ticker = mm.material_ticker
WHERE ($1::text[] IS NULL OR cardinality($1::text[]) = 0 OR pb.material_ticker = ANY($1::text[]))
GROUP BY pb.material_ticker, mm.mm_buy_sum, mm.mm_sell_sum
ORDER BY pb.material_ticker;
"""


async def fetch_pivoted_market_data(
    db,
    tickers: Optional[List[str]] = None,
    brokermaterialids: Optional[str] = None,
) -> List[asyncpg.Record]:
    """Executes the pivoted market data query with lock timeout safety."""
    try:
        async with db.pool.acquire() as con:
            await con.execute("SET lock_timeout = '10s';")

            ticker_list = tickers if tickers else []
            brokermaterialid = brokermaterialids if brokermaterialids else ""

            records = await con.fetch(
                SQL_GET_PIVOTED_MARKET_DATA, ticker_list, brokermaterialid
            )
            return records

    except Exception as e:
        logger.error(
            f"Database error fetching pivoted market data: {e}", exc_info=True
        )
        raise