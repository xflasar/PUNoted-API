import logging

logger = logging.getLogger(__name__)

SQL_GET_PIVOTED_MARKET_DATA = """
WITH mm_buys AS (
    SELECT brokermaterialid, SUM(priceamount) AS mm_buy_sum
    FROM cx_brokers_buy_orders
    WHERE tradername = 'Insitor Cooperative Market Maker'
    GROUP BY brokermaterialid
),
mm_sells AS (
    SELECT brokermaterialid, SUM(priceamount) AS mm_sell_sum
    FROM cx_brokers_sell_orders
    WHERE tradername = 'Insitor Cooperative Market Maker'
    GROUP BY brokermaterialid
),
parsed_brokers AS (
    SELECT 
        SPLIT_PART(ticker, '.', 1) AS material_ticker,
        SPLIT_PART(ticker, '.', 2) AS exchange_code,
        priceaverage, askamount, askprice, bidamount, bidprice,
        brokermaterialid, xata_updatedat
    FROM cx_brokers
    WHERE ticker IS NOT NULL AND POSITION('.' IN ticker) > 0
)
SELECT
    pb.material_ticker AS "Ticker",
    COALESCE(SUM(b.mm_buy_sum), 0) AS "MMBuy",
    COALESCE(SUM(s.mm_sell_sum), 0) AS "MMSell",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.priceaverage END) AS "AI1-Average",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.askamount END) AS "AI1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.askprice END) AS "AI1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.askamount END) AS "AI1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.bidamount END) AS "AI1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.bidprice END) AS "AI1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'AI1' THEN pb.bidamount END) AS "AI1-BidAvail",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.priceaverage END) AS "CI1-Average",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.askamount END) AS "CI1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.askprice END) AS "CI1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.askamount END) AS "CI1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.bidamount END) AS "CI1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.bidprice END) AS "CI1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI1' THEN pb.bidamount END) AS "CI1-BidAvail",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.priceaverage END) AS "CI2-Average",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.askamount END) AS "CI2-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.askprice END) AS "CI2-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.askamount END) AS "CI2-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.bidamount END) AS "CI2-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.bidprice END) AS "CI2-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'CI2' THEN pb.bidamount END) AS "CI2-BidAvail",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.priceaverage END) AS "NC1-Average",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.askamount END) AS "NC1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.askprice END) AS "NC1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.askamount END) AS "NC1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.bidamount END) AS "NC1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.bidprice END) AS "NC1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC1' THEN pb.bidamount END) AS "NC1-BidAvail",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.priceaverage END) AS "NC2-Average",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.askamount END) AS "NC2-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.askprice END) AS "NC2-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.askamount END) AS "NC2-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.bidamount END) AS "NC2-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.bidprice END) AS "NC2-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'NC2' THEN pb.bidamount END) AS "NC2-BidAvail",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.priceaverage END) AS "IC1-Average",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.askamount END) AS "IC1-AskAmt",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.askprice END) AS "IC1-AskPrice",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.askamount END) AS "IC1-AskAvail",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.bidamount END) AS "IC1-BidAmt",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.bidprice END) AS "IC1-BidPrice",
    MAX(CASE WHEN pb.exchange_code = 'IC1' THEN pb.bidamount END) AS "IC1-BidAvail",
    MAX(pb.xata_updatedat) AS "last_update"
FROM parsed_brokers pb
LEFT JOIN mm_buys b ON pb.brokermaterialid = b.brokermaterialid
LEFT JOIN mm_sells s ON pb.brokermaterialid = s.brokermaterialid
GROUP BY pb.material_ticker
ORDER BY pb.material_ticker;
"""

async def fetch_pivoted_market_data(db) -> list:
    """
    Executes the market data query and returns a list of database records.
    """
    try:
        async with db.pool.acquire() as con:
            await con.execute("SET lock_timeout = '10s';")
            records = await con.fetch(SQL_GET_PIVOTED_MARKET_DATA)
            return records
    except Exception as e:
        logger.error(f"Database error fetching market data: {e}", exc_info=True)
        raise
