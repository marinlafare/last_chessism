"""Opt-in synthetic timing check, only in the disposable PostgreSQL harness."""
import json
import time
import unittest
from uuid import uuid4
from sqlalchemy import select, text
import test_cloud_analysis_database as existing
import test_cloud_game_drafts_database as fixtures
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import CloudGameDraftEntry, CloudGameDraftGame
from chessism_api.operations.cloud_analysis import draft_cache, game_drafts


@unittest.skipUnless(existing.TEST_URL, 'requires explicitly isolated PostgreSQL')
class DraftCacheTiming(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.DraftDatabaseTests.asyncSetUp
    asyncTearDown = fixtures.DraftDatabaseTests.asyncTearDown
    add = fixtures.DraftDatabaseTests.add

    async def test_add_delta_against_full_union(self):
        async with AsyncDBSession() as s, s.begin():
            await s.execute(text("""INSERT INTO fen(fen,n_games,moves_counter)
                SELECT 'draft-benchmark-'||i,1,'{}' FROM generate_series(1,100500) i"""))
            # Repeated appearances make the old association scan substantially
            # larger than its unique set; these are synthetic, not chess games.
            await s.execute(text("""INSERT INTO game_fen_association(game_link,fen_id,n_move,move_color)
                SELECT 101,fen_id,repeat,'white' FROM fen CROSS JOIN generate_series(1,10) repeat
                WHERE fen LIKE 'draft-benchmark-%' AND fen_id <= 100003"""))
            await s.execute(text("""INSERT INTO game_fen_association(game_link,fen_id,n_move,move_color)
                SELECT 102,fen_id,1,'white' FROM fen
                WHERE fen LIKE 'draft-benchmark-%' AND fen_id > 99503"""))
            await s.execute(text('ANALYZE fen'))
            await s.execute(text('ANALYZE game_fen_association'))
        await self.add((101,))
        async with AsyncDBSession() as s, s.begin():
            draft = await game_drafts.locked_draft(s, self.draft['id'], self.draft['revision'])
            cache = await draft_cache.ensure(s, draft)
            entry = CloudGameDraftEntry(id=uuid4().hex, draft_id=draft.id, ordinal=2,
                player_name='magnuscarlsen', selection_mode='latest', selection_order='latest',
                requested_games=1, selected_games=1, preview_fens=1002)
            s.add(entry)
            await s.flush()
            s.add(CloudGameDraftGame(entry_id=entry.id, game_link=102, ordinal=0))
            await s.flush()
            started = time.monotonic()
            reference = await game_drafts.count_union(s, draft.id)
            full = time.monotonic() - started
            started = time.monotonic()
            await draft_cache.add_entry(s, cache, entry.id)
            game_drafts.changed(draft)
            await draft_cache.publish(s, draft, cache)
            incremental = time.monotonic() - started
            self.assertEqual(draft.preview_fens, reference['eligible'])
            self.assertEqual(draft.preview_games, reference['games'])
            print(json.dumps({'old_set_unique_fens': 100002, 'old_set_appearances': 1000002,
                'new_step_fens': 1002, 'full_recount_seconds': round(full, 4),
                'incremental_update_seconds': round(incremental, 4),
                'ratio': round(full / incremental, 2), 'identical_counts': True}), flush=True)


if __name__ == '__main__': unittest.main()
