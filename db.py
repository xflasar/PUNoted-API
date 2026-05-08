import asyncio
import logging
from typing import Any, Optional

import asyncpg

from config import XATA_DATABASE_URL

logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.poolInit = False
        self.timeout = 10

    async def no_op_reset(self, con):
        """A no-operation function to override asyncpg's default reset."""
        pass

    async def init_connection(self, con):
        """Sets a statement timeout on the connection."""
        await con.execute("SET statement_timeout = '15s'")

    async def create_pool(self):
        self.poolInit = True
        self.pool = await asyncpg.create_pool(
            dsn=XATA_DATABASE_URL,
            reset=self.no_op_reset,
            command_timeout=60,
            timeout=30,
            init=self.init_connection,
        )
        logger.debug("Database pool created successfully.")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
            logger.debug("Database pool closed.")
            self.pool = None

    async def executemany(self, query: str, args: list[list[Any]]) -> None:
        """
        Executes a query with multiple sets of arguments.
        """
        if self.pool and self.poolInit:
            async with self.pool.acquire() as con:
                await asyncio.wait_for(con.executemany(query, args), timeout=self.timeout)
        else:
            raise ConnectionError("Database pool not initialized.")

    # Not really needed
    async def transaction(self):
        """
        Returns the transaction context manager of the raw asyncpg connection.
        """
        if self.pool and self.poolInit:
            async with self.pool.acquire() as con:
                return con.transaction()
        else:
            raise ConnectionError("Database pool not initialized.")
