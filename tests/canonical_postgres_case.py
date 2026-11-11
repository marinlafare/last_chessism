"""One fresh database per test, on the explicitly selected disposable server."""
import os
import unittest
from uuid import uuid4

import asyncpg

from scripts.init_canonical_db import isolated_target


class CanonicalDatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        socket = os.environ['CHESSISM_CANONICAL_TEST_SOCKET']
        self.socket = isolated_target(socket, 'chessism_canonical_test')
        self.settings = dict(host=self.socket, port=55432, user='test', password='', timeout=10)
        self.admin = await asyncpg.connect(database='chessism_canonical_test', **self.settings)
        self.name = 'chessism_canonical_' + uuid4().hex
        self.connections = []
        await self.admin.execute('CREATE DATABASE ' + self.name)
        self.addAsyncCleanup(self.close_database)
        self.db = await self.connect()

    async def connect(self):
        conn = await asyncpg.connect(database=self.name, **self.settings)
        self.connections.append(conn)
        await conn.execute("SET search_path TO public, pg_catalog; SET statement_timeout TO '5s'")
        return conn

    async def close_database(self):
        try:
            for conn in self.connections:
                await conn.close()
            await self.admin.execute('DROP DATABASE ' + self.name)
        finally:
            await self.admin.close()
