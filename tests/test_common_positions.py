import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
import chess
from chessism_api.operations import common_positions as cache
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY

NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)
E4 = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -"


def context(session):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


def result(rows=(), scalar=None):
    mock = MagicMock()
    mock.scalar.return_value = scalar
    mock.mappings.return_value.all.return_value = rows
    mock.mappings.return_value.first.return_value = rows[0] if rows else None
    return mock


class CommonPositionsTests(unittest.IsolatedAsyncioTestCase):
    async def test_compact_labels_and_example_do_not_reference_removed_move_id(self):
        session = AsyncMock()
        session.info = {SESSION_KEY: RelationalStorage('public', 'chessism_archive', 'chessism_reads', 'relational_v3')}
        session.execute.side_effect = [
            result([{'n_move': 1, 'move_color': 'white', 'san': 'e4', 'label_occurrences': 40, 'example_game_link': 7}]),
            result([{'n_move': 1, 'white_move': 'e4', 'black_move': 'e5'}]),
        ]
        rows = await cache.describe_positions(session, [{'fen': E4, 'fen_id': 123, 'appearances': 42}], start_rank=11)
        self.assertEqual(rows[0]['rank'], 11)
        self.assertEqual(rows[0]['example_line'], '1. e4')
        for call in session.execute.call_args_list:
            self.assertNotRegex(str(call.args[0]), r'\bid\b')
        self.assertNotIn('DISTINCT', str(session.execute.call_args_list[1].args[0]))
        sql = str(session.execute.call_args_list[0].args[0])
        self.assertNotIn('LATERAL', sql)
        self.assertIn('a.fen_id = :fen_id', sql)
        self.assertIn('a.is_white IS NOT NULL', sql)
        self.assertIn('ORDER BY label_occurrences DESC, a.n_move, move_color, san', sql)
        self.assertEqual(session.execute.call_args_list[0].args[1], {'fen_id': 123})

    def test_summary_lock_does_not_overlap_cloud_controller_or_histogram(self):
        self.assertNotIn(cache.LOCK_ID, (731_946_219, 731_946_218))

    def test_move_labels_distinguish_sides_and_missing_sources(self):
        self.assertEqual(cache.move_label(1, "white", "e4"), "1. e4")
        self.assertEqual(cache.move_label(1, "black", "e5"), "1... e5")
        self.assertEqual(cache.move_label(2, "white", "Nf3"), "2. Nf3")
        self.assertEqual(cache.move_label(None, None, None), "Position")

    def test_example_stops_at_arrival_and_verifies_actual_fen(self):
        moves = [{"n_move": 1, "white_move": "e4", "black_move": "e5"}]
        self.assertEqual(cache.verified_example_line(moves, 1, "white", E4), "1. e4")
        self.assertIsNone(cache.verified_example_line(moves, 1, "black", E4))
        self.assertIsNone(cache.verified_example_line([], 1, "white", E4))
        self.assertIsNone(cache.verified_example_line([{**moves[0], "n_move": 2}], 2, "white", E4))
        self.assertIsNone(cache.verified_example_line([{**moves[0], "white_move": "Qh9"}], 1, "white", E4))
        self.assertIsNone(cache.verified_example_line(moves, 101, "white", E4))

    def test_transpositions_keep_same_identity_but_allow_different_example_arrivals(self):
        board = chess.Board()
        for san in ("Nf3", "d5", "g3"): board.push_san(san)
        fen = " ".join(board.fen().split()[:4])
        rows = [{"n_move": 1, "white_move": "g3", "black_move": "d5"},
                {"n_move": 2, "white_move": "Nf3", "black_move": "--"}]
        self.assertEqual(cache.verified_example_line(rows, 2, "white", fen), "1. g3 d5 2. Nf3")

    async def test_get_reads_only_small_cache_and_missing_snapshot_queues_one_job(self):
        session, redis = AsyncMock(), AsyncMock()
        session.execute.return_value = result([{"refreshed_at": None, "rank": None}])
        with patch.object(cache, "AsyncDBSession", return_value=context(session)):
            payload = await cache.get_common_positions(redis, NOW)
        self.assertEqual(payload["rows"], [])
        self.assertTrue(payload["stale"])
        sql = str(session.execute.call_args.args[0])
        self.assertNotIn("JOIN moves", sql)
        self.assertNotIn("FROM fen ", sql)
        redis.enqueue_job.assert_awaited_once_with("run_commonness_refresh", _job_id=cache.JOB_ID, _queue_name="analysis_queue")

    async def test_fresh_snapshot_serializes_labels_without_queueing(self):
        session, redis = AsyncMock(), AsyncMock()
        session.execute.return_value = result([{"refreshed_at": NOW, "source_updated_at": NOW,
            "rank": 1, "fen": E4, "appearances": 42, "n_move": 1, "move_color": "white",
            "san": "e4", "label_occurrences": 40, "example_line": "1. e4"}])
        with patch.object(cache, "AsyncDBSession", return_value=context(session)):
            payload = await cache.get_common_positions(redis, NOW)
        self.assertFalse(payload["stale"])
        self.assertEqual(payload["rows"][0]["label"], "1. e4")
        self.assertEqual(payload["rows"][0]["appearances"], 42)
        self.assertEqual(payload["rows"][0]["label_occurrences"], 40)
        redis.enqueue_job.assert_not_awaited()

    async def test_rebuild_publishes_labels_only_after_all_top_positions_are_ready(self):
        session = AsyncMock()
        session.execute.side_effect = [result(scalar=NOW), result([{"fen": E4, "appearances": 42}]),
            result([{"n_move": 1, "move_color": "white", "san": "e4", "label_occurrences": 40, "example_game_link": 7}]),
            result([{"n_move": 1, "white_move": "e4", "black_move": "e5"}]), result(), result(), result()]
        await cache.rebuild_positions(session)
        calls = session.execute.call_args_list
        self.assertIn("ORDER BY n_games DESC, fen LIMIT 10", str(calls[1].args[0]))
        self.assertIn("GROUP BY a.n_move, a.move_color, san", str(calls[2].args[0]))
        self.assertIn("ORDER BY label_occurrences DESC", str(calls[2].args[0]))
        self.assertIn("ORDER BY id LIMIT 1", str(calls[2].args[0]))
        self.assertIn("DELETE FROM fen_commonness_position", str(calls[4].args[0]))
        self.assertEqual(calls[5].args[1][0]["example_line"], "1. e4")
        self.assertIn("last_error = NULL", str(calls[-1].args[0]))

    async def test_failed_or_contending_refresh_never_publishes_partial_snapshot(self):
        for locked in (True, False):
            mark, refresh, failed = AsyncMock(), AsyncMock(), AsyncMock()
            refresh.execute.return_value = result(scalar=locked)
            with patch.object(cache, "AsyncDBSession", side_effect=[context(mark), context(refresh), context(failed)]), \
                    patch.object(cache, "rebuild_positions", AsyncMock(side_effect=RuntimeError("failed"))) as rebuild:
                if locked:
                    with self.assertLogs(cache.LOG, level="ERROR"), self.assertRaises(RuntimeError):
                        await cache.run_commonness_refresh({})
                    failed.commit.assert_awaited_once()
                else:
                    self.assertEqual(await cache.run_commonness_refresh({}), {"status": "already_refreshing"})
                    rebuild.assert_not_awaited()
            refresh.commit.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
