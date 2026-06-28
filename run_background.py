import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import Database
from app.services.background import scrape_and_save_data, scrape_prices_and_save_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("background_worker")

async def main():
    db = Database()
    await db.create_pool()
    logger.info("Database pool created.")

    # Run them once on startup to populate/sync data immediately
    logger.info("Running initial scraping tasks on startup...")
    try:
        await scrape_and_save_data(db.pool)
    except Exception as e:
        logger.error(f"Error during initial scrape_and_save_data: {e}", exc_info=True)

    try:
        await scrape_prices_and_save_data(db.pool)
    except Exception as e:
        logger.error(f"Error during initial scrape_prices_and_save_data: {e}", exc_info=True)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scrape_and_save_data, "interval", minutes=30, args=[db.pool])
    scheduler.add_job(scrape_prices_and_save_data, "interval", minutes=30, args=[db.pool])
    scheduler.start()
    logger.info("Background scheduler started (interval: 30 minutes).")

    try:
        # Keep the main thread/loop alive
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down background scheduler...")
        scheduler.shutdown()
        await db.close_pool()

if __name__ == "__main__":
    asyncio.run(main())
