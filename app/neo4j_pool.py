# app/neo4j_pool.py
# Minimal async Neo4j driver wrapper with context-friendly session management.

from neo4j import AsyncGraphDatabase

class Neo4jAsyncPool:
    def __init__(self, uri: str, user: str, password: str, max_connection_pool_size: int = 10):
        # Initialize the async driver and connection pool.
        self.driver = AsyncGraphDatabase.driver(
            uri, auth=(user, password), max_connection_pool_size=max_connection_pool_size
        )

    def session(self):
        # Return an async session (usable with "async with").
        return self.driver.session()

    async def close(self):
        # Close the underlying driver and release resources.
        await self.driver.close()
