"""Opt-in PostgreSQL regression tests using exclusively session-local tables.

CHESSISM_DB_INTEGRATION=1 python -m unittest tests.test_salience_database
No production rows or schemas are changed; no migrations are run.
"""

import os
import unittest
from collections import Counter

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from chessism_api.operations.player_salience_calculation import _full_rebuild, _incremental_update


@unittest.skipUnless(os.environ.get("CHESSISM_DB_INTEGRATION") == "1", "opt-in database test")
class SalienceDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        url = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+asyncpg://", 1)
        self.engine = create_async_engine(url)
        self.connection = await self.engine.connect()
        self.session = AsyncSession(bind=self.connection, expire_on_commit=False)
        # Shadow every table the calculation can read or write in this session.
        tables = {
            "game": "link bigint PRIMARY KEY, fens_done boolean",
            "game_player": "link bigint, color text, player_name text",
            "game_fen_association": "game_link bigint, n_move int, move_color text, fen_fen text",
            "player_salience_pending_game": "player_name text, game_link bigint, player_color text",
            "player_position_frequency": """player_name text, player_color text, fen_fen text,
                games_with_position bigint, total_occurrences bigint,
                PRIMARY KEY (player_name, player_color, fen_fen)""",
            "game_player_salience": """game_link bigint, player_name text, player_color text,
                salience double precision, weighted_numerator double precision,
                depth_weight_sum double precision, position_occurrence_count int,
                unique_position_count int, repeated_position_count int,
                PRIMARY KEY (game_link, player_color)""",
        }
        for name, definition in tables.items():
            await self.session.execute(text(f"CREATE TEMP TABLE {name} ({definition})"))
            is_temporary = await self.session.scalar(text(
                "SELECT relpersistence = 't' FROM pg_class WHERE oid = to_regclass(:name)"
            ), {"name": name})
            self.assertTrue(is_temporary, name)
        await self.session.commit()
        self.games = {}

    async def asyncTearDown(self):
        await self.session.close()
        await self.connection.close()
        await self.engine.dispose()

    async def add_game(self, link, color, positions):
        self.games[(link, color)] = positions
        await self.session.execute(text("INSERT INTO game VALUES (:id, true)"), {"id": link})
        await self.session.execute(text("INSERT INTO game_player VALUES (:id, :color, 'test')"),
                                   {"id": link, "color": color})
        await self.session.execute(text("INSERT INTO player_salience_pending_game VALUES ('test', :id, :color)"),
                                   {"id": link, "color": color})
        await self.session.execute(text("INSERT INTO game_fen_association VALUES (:id, :move, :color, :fen)"), [
            {"id": link, "move": index // 2 + 1, "color": "white" if index % 2 == 0 else "black", "fen": fen}
            for index, fen in enumerate(positions)
        ])
        await self.session.commit()

    async def rows(self):
        rows = (await self.session.execute(text("SELECT * FROM game_player_salience ORDER BY game_link"))).mappings().all()
        return [dict(row) for row in rows]

    async def assert_reference(self):
        frequencies = Counter((color, fen) for (_, color), fens in self.games.items() for fen in set(fens))
        expected_occurrences = Counter((color, fen) for (_, color), fens in self.games.items() for fen in fens)
        rows = await self.rows()
        self.assertEqual(len(rows), len(self.games))
        for row in rows:
            fens = self.games[(row["game_link"], row["player_color"])]
            seen = Counter()
            numerator = denominator = 0
            for ply, fen in enumerate(fens, 1):
                seen[fen] += 1
                depth = 0.25 + 0.75 * min(ply / 16, 1)
                numerator += depth / (frequencies[(row["player_color"], fen)] * seen[fen])
                denominator += depth
            self.assertAlmostEqual(row["salience"], numerator / denominator, places=12)
            self.assertAlmostEqual(row["weighted_numerator"], numerator, places=12)
            self.assertAlmostEqual(row["depth_weight_sum"], denominator, places=12)
            self.assertEqual(row["position_occurrence_count"], len(fens))
            self.assertEqual(row["repeated_position_count"], len(fens) - len(set(fens)))
        stored = (await self.session.execute(text("SELECT * FROM player_position_frequency"))).mappings().all()
        self.assertEqual(len(stored), sum(count > 1 for count in frequencies.values()))
        for row in stored:
            key = row["player_color"], row["fen_fen"]
            self.assertEqual(row["games_with_position"], frequencies[key])
            self.assertEqual(row["total_occurrences"], expected_occurrences[key])
        self.assertEqual(await self.session.scalar(text("SELECT COUNT(*) FROM player_salience_pending_game")), 0)
        await self.session.commit()
        return rows

    async def test_incremental_matches_full_and_occurrence_reference(self):
        # Include same-game repeats, corpus repeats, singleton->repeated, both
        # player colors, and a long game extending past the depth-weight cap.
        await self.add_game(1, "white", ["a", "b", "a", "solo", "a"])
        await self.add_game(2, "white", ["a", "c", "d"])
        await self.add_game(3, "black", ["a", "b", "a", "black-only"])
        await _full_rebuild(self.session, "test")
        await self.session.commit()
        await self.assert_reference()
        for link, color, fens in [
            (4, "white", ["solo", "c", "solo", "new", "solo"]),
            (5, "black", ["a", "other", "a"] * 9),
            (6, "white", ["unseen1", "unseen2"]),
        ]:
            await self.add_game(link, color, fens)
            await _incremental_update(self.session, "test")
            await self.session.commit()
            incremental = await self.assert_reference()
            await _full_rebuild(self.session, "test")
            await self.session.commit()
            rebuilt = await self.assert_reference()
            for old, new in zip(incremental, rebuilt):
                self.assertAlmostEqual(old["salience"], new["salience"], places=12)


if __name__ == "__main__":
    unittest.main()
