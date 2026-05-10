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

    async def execute(self, query: str, *args, timeout: Optional[float] = None) -> Any:
        """
        Executes a query with the provided arguments and returns the result.
        """
        if self.pool and self.poolInit:
            use_timeout = timeout if timeout is not None else self.timeout
            async with self.pool.acquire() as con:
                return await con.execute(query, *args, timeout=use_timeout)
        else:
            raise ConnectionError("Database pool not initialized.")
        
    async def fetch_one(self, query: str, *args, timeout: Optional[float] = None) -> Optional[asyncpg.Record]:
        """
        Executes a query and returns a single record, or None if no record is found.
        """
        if self.pool and self.poolInit:
            use_timeout = timeout if timeout is not None else self.timeout
            async with self.pool.acquire() as con:
                return await con.fetchrow(query, *args, timeout=use_timeout)
        else:
            raise ConnectionError("Database pool not initialized.")
        
    async def fetch_rows(self, query: str, *args, timeout: Optional[float] = None) -> list[asyncpg.Record]:
        """
        Executes a query and returns a list of records.
        """
        if self.pool and self.poolInit:
            use_timeout = timeout if timeout is not None else self.timeout
            async with self.pool.acquire() as con:
                return await con.fetch(query, *args, timeout=use_timeout)
        else:
            raise ConnectionError("Database pool not initialized.")

    async def executemany(self, query: str, args: list[list[Any]], timeout: Optional[float] = None) -> None:
        """
        Executes a query with multiple sets of arguments.
        """
        if self.pool and self.poolInit:
            use_timeout = timeout if timeout is not None else self.timeout
            async with self.pool.acquire() as con:
                await con.executemany(query, args, timeout=use_timeout)
        else:
            raise ConnectionError("Database pool not initialized.")

    async def transaction(self):
        """
        Returns the transaction context manager of the raw asyncpg connection.
        """
        if self.pool and self.poolInit:
            async with self.pool.acquire() as con:
                return con.transaction()
        else:
            raise ConnectionError("Database pool not initialized.")