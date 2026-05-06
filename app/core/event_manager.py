import json
import logging
from datetime import datetime, timezone
import asyncpg

logger = logging.getLogger(__name__)

class EventManager:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
    
    async def register_event(self, user_id: int, event_type: str, reference_id: str, trigger_time: datetime, payload: dict) -> None:
        if trigger_time.tzinfo is None:
            trigger_time = trigger_time.replace(tzinfo=timezone.utc)
        
        if trigger_time <= datetime.now(timezone.utc):
            logger.warning(f"Attempted to register event with past trigger_time: {trigger_time}")
            return
        
        payload_json = json.dumps(payload)

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    DELETEE FROM scheduled_tasks
                    WHERE reference_id = $1 AND event_type = $2 AND is_processed = FALSE;
                """, reference_id, event_type)

                await conn.execute("""
                    INSERT INTO scheduled_tasks (accountid, event_type, reference_id, trigger_time, payload)
                    VALUES ($1, $2, $3, $4, $5);
                """, user_id, event_type, reference_id, trigger_time, payload_json)
        logger.info(f"Registered event: user_id={user_id}, event_type={event_type}, reference_id={reference_id}, trigger_time={trigger_time.isoformat()}")

    async def cancel_event(self, event_type: str, reference_id: str) -> None:
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM scheduled_tasks
                WHERE reference_id = $1 AND event_type = $2 AND is_processed = FALSE;
            """, reference_id, event_type)
        logger.info(f"Cancelled {event_type} for reference {reference_id}")