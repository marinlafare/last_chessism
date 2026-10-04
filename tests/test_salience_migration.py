"""Current salience schemas must not trigger expensive backfill scans."""

import unittest
from unittest.mock import AsyncMock, MagicMock

from chessism_api.database.engine import _reshape_player_salience_schema


class SalienceMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_current_schema_needs_no_writes_or_alter_locks(self):
        connection = MagicMock()
        connection.transaction.return_value = AsyncMock()
        connection.fetchval = AsyncMock(side_effect=[True, False])
        connection.fetch = AsyncMock(side_effect=[
            [{"column_name": key, "is_nullable": "NO"} for key in (
                "position_occurrence_count", "unique_position_count",
                "repeated_position_count", "weighted_numerator", "depth_weight_sum",
            )],
            [{"conname": name} for name in (
                "game_player_salience_occurrence_count", "game_player_salience_unique_count",
                "game_player_salience_repeated_count", "game_player_salience_components_positive",
            )],
        ])
        connection.execute = AsyncMock()
        self.assertFalse(await _reshape_player_salience_schema(connection))
        connection.execute.assert_not_awaited()

    async def test_nullable_components_are_backfilled_once(self):
        connection = MagicMock()
        connection.transaction.return_value = AsyncMock()
        connection.fetchval = AsyncMock(side_effect=[True, False])
        connection.fetch = AsyncMock(side_effect=[
            [{"column_name": key, "is_nullable": "YES"} for key in (
                "position_occurrence_count", "unique_position_count",
                "repeated_position_count", "weighted_numerator", "depth_weight_sum",
            )],
            [{"conname": name} for name in (
                "game_player_salience_occurrence_count", "game_player_salience_unique_count",
                "game_player_salience_repeated_count", "game_player_salience_components_positive",
            )],
        ])
        connection.execute = AsyncMock()
        await _reshape_player_salience_schema(connection)
        queries = [call.args[0] for call in connection.execute.await_args_list]
        self.assertEqual(sum("UPDATE game_player_salience" in sql for sql in queries), 2)
        self.assertEqual(sum("SET NOT NULL" in sql for sql in queries), 2)


if __name__ == "__main__":
    unittest.main()
