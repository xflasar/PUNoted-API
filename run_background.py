import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db import Database
from app.services.background import scrape_and_save_data, scrape_prices_and_save_data
from repositories.banking_repo import BankingRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("background_worker")


async def process_weekly_bank_yields_task(db: Database):
    """
    Weekly banking cycle worker:
    Runs every Monday at 00:00 UTC.
    1. Runs repayment sync.
    2. Calculates and distributes time-weighted compounding yields and records weekly snapshots.
    """
    try:
        repo = BankingRepository(db)
        await repo.sync_loan_repayments()
        result = await repo.process_automatic_weekly_yields()
        processed_count = result.get("processed", 0)
        if processed_count > 0:
            logger.info(f"Weekly banking yield worker processed {processed_count} bank(s): {result.get('details')}")
    except Exception as e:
        logger.error(f"Error in process_weekly_bank_yields_task: {e}", exc_info=True)


async def main():
    db = Database()
    await db.create_pool()
    logger.info("Database pool created.")

    # Run tasks on startup to sync immediately
    logger.info("Running initial background tasks on startup...")
    try:
        await scrape_and_save_data(db.pool)
    except Exception as e:
        logger.error(f"Error during initial scrape_and_save_data: {e}", exc_info=True)

    try:
        await scrape_prices_and_save_data(db.pool)
    except Exception as e:
        logger.error(f"Error during initial scrape_prices_and_save_data: {e}", exc_info=True)

    # Check for any missed weekly cycles (e.g. after container restart)
    await process_weekly_bank_yields_task(db)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scrape_and_save_data, "interval", minutes=30, args=[db.pool])
    scheduler.add_job(scrape_prices_and_save_data, "interval", minutes=30, args=[db.pool])
    # Weekly yield distribution & snapshot runs every Monday at exactly 00:00 UTC
    scheduler.add_job(
        process_weekly_bank_yields_task,
        "cron",
        day_of_week="mon",
        hour=0,
        minute=0,
        timezone="UTC",
        args=[db]
    )
    scheduler.start()
    logger.info("Background scheduler started (scraping: 30m, bank yields: Mon 00:00 UTC).")

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
