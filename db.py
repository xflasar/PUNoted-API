# db.py
import os
import asyncpg
import logging
import asyncio
from typing import Any, List, Optional

from config import XATA_DATABASE_URL

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.poolInit = False
        self.conInit = False
        self.con: Optional[asyncpg.Connection] = None
        self.timeout = 10

    async def no_op_reset(self, con):
        """A no-operation function to override asyncpg's default reset."""
        pass

    def connect(self):
        self.conInit = True
        self.con = asyncpg.connect(XATA_DATABASE_URL)
        print("Connection created.")

    def close_connection(self):
        if self.con:
            self.con.close()
            print("Connection closed.")

    async def create_pool(self, loop):
        self.poolInit = True
        dsn = (XATA_DATABASE_URL)
        self.pool = await asyncpg.create_pool(dsn=dsn, reset=self.no_op_reset, loop=loop, timeout=15, init=lambda con: con.execute("SET statement_timeout = '15s'"))
        logger.info("Database pool created successfully.")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed.")
            self.pool = None

    async def fetch_rows(self, query: str, *params: Any, timeout: float = None) -> Optional[List[asyncpg.Record]]:
        timeout = self.timeout
        if self.conInit and self.con:
            try:
                result = await asyncio.wait_for(self.con.fetch(query, *params), timeout=timeout)
                self.con.close()
                return result
            except Exception as e:
                logger.error(f"Error fetching rows: {e}")
                return None
        elif self.pool and self.poolInit:
            async with self.pool.acquire() as con:
                try:
                    result = await asyncio.wait_for(con.fetch(query, *params), timeout=timeout)
                    return result
                except Exception as e:
                    logger.error(f"Error fetching rows: {e}")
                    return None
        else:
            logger.error("Database not initialized.")
            return None

    async def fetch_one(self, query: str, *params: Any, timeout: float = None) -> Optional[asyncpg.Record]:
        timeout = self.timeout
        rows = await self.fetch_rows(query, *params, timeout=timeout)
        return rows[0] if rows else None

    async def execute(self, query: str, *params: Any, timeout: float = None) -> Optional[str]:
        timeout = self.timeout
        if self.conInit and self.con:
            try:
                result = await asyncio.wait_for(self.con.execute(query, *params), timeout=timeout)
                self.con.close()
                return result
            except Exception as e:
                logger.error(f"Error executing command: {e}")
                return None
        elif self.pool and self.poolInit:
            async with self.pool.acquire() as con:
                try:
                    result = await asyncio.wait_for(con.execute(query, *params), timeout=timeout)
                    return result
                except Exception as e:
                    logger.error(f"Error executing command: {e}")
                    return None
        else:
            logger.error("Database not initialized.")
            return None
        
    async def executemany(self, query: str, args: list[list[Any]], timeout: float = None) -> None:
        timeout = self.timeout
        """
        Executes a query with multiple sets of arguments.
        """
        if self.pool and self.poolInit:
            async with self.pool.acquire() as con:
                try:
                    await asyncio.wait_for(con.executemany(query, args), timeout=timeout)
                except Exception as e:
                    logger.error(f"Error executing bulk command: {e}", exc_info=True)
                    raise
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