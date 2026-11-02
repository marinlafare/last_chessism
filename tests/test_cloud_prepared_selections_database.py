"""Prepared Calculate/Add correctness on isolated PostgreSQL, no Google calls."""
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4
from fastapi import HTTPException
from sqlalchemy import select, text, func
import test_cloud_analysis_database as existing
from test_cloud_game_drafts_database import DraftDatabaseTests
from test_cloud_game_drafts import plan
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.ask_db import preview_player_games_for_analysis
from chessism_api.database.postgresql.models import (CloudGamePreparedSelection, CloudGamePreparedChunk,
    CloudGameDraftEntryFen, CloudGameDraftFen, CloudAnalysisRun, CloudFenClaim, Fen, GameAnalysisSummary)
from chessism_api.operations.cloud_analysis import prepared_selection as prepared, draft_cache
from chessism_api.operations.cloud_analysis.migrate_prepared_selections import upgrade
from chessism_api.routers.cloud_game_drafts import save_entry, remove_entry, current_draft, AddSelection, Revision


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL')
class PreparedSelectionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = DraftDatabaseTests.asyncSetUp
    asyncTearDown = DraftDatabaseTests.asyncTearDown
    available = DraftDatabaseTests.available
    submit = DraftDatabaseTests.submit

    async def calculate(self, links=(101,102)):
        ident = uuid4().hex
        async with AsyncDBSession() as s, s.begin():
            result = await prepared.build(s, ident, list(links))
        return ident, {**plan(links), **result}

    async def add(self, selected, replace=None):
        ident, payload = selected
        redis = AsyncMock(); redis.get.return_value = json.dumps(payload)
        self.draft = await save_entry(self.draft['id'], AddSelection(revision=self.draft['revision'],
            plan_id=ident, replace_entry_id=replace), redis)
        return self.draft

    async def test_calculate_add_reuses_ids_and_merges_overlaps(self):
        selected = await self.calculate((101,))
        self.assertEqual(selected[1]['fens_to_analyze'], 2)
        with patch.object(draft_cache, 'add_entry', side_effect=AssertionError('Add rescanned games')):
            await self.add(selected)
            await self.add(await self.calculate((102,103)))
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 3)
        self.assertEqual(self.draft['preview']['unique_games'], 3)
        async with AsyncDBSession() as s:
            self.assertEqual(sorted((await s.scalars(select(CloudGameDraftFen.n_entries))).all()), [1,2,2])
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGamePreparedChunk)), 0)
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGamePreparedSelection)), 0)

    async def test_only_missing_ids_retained_and_claims_counted(self):
        async with AsyncDBSession() as s, s.begin():
            (await s.get(Fen, self.fens[0])).score = 12
            s.add(CloudAnalysisRun(id='e'*32,job_id=self.job.id,positions=[],launch={},receipts={}))
            await s.flush(); s.add(CloudFenClaim(fen=self.fens[1],run_id='e'*32))
        selected = await self.calculate()
        self.assertEqual((selected[1]['fens_to_analyze'],selected[1]['claimed_fens']), (2,1))
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(text('SELECT sum(cardinality(fen_ids)) FROM cloud_game_prepared_chunk')), 2)
        await self.add(selected)
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 1)
        self.assertEqual(self.draft['preview']['claimed_fens'], 1)

    async def test_replace_remove_and_legacy_steps_mix(self):
        await DraftDatabaseTests.add(self, (101,))
        first = self.draft['entries'][0]['id']
        await self.add(await self.calculate((102,)))
        await self.add(await self.calculate((103,)), replace=first)
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 3)
        self.draft = await remove_entry(self.draft['id'], first, Revision(revision=self.draft['revision']))
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 2)

    async def test_expired_or_mismatched_plan_does_not_change_draft(self):
        selected = await self.calculate()
        async with AsyncDBSession() as s, s.begin():
            (await s.get(CloudGamePreparedSelection,selected[0])).expires_at = datetime.now(timezone.utc)-timedelta(seconds=1)
        for value in (selected, (await self.calculate((101,)))):
            before = await current_draft()
            if value is not selected: value[1]['game_links'] = [102]
            with self.assertRaises(HTTPException): await self.add(value)
            self.assertEqual(await current_draft(), before)

    async def test_failed_save_keeps_plan_for_retry(self):
        selected = await self.calculate()
        before = await current_draft()
        with patch.object(draft_cache,'publish',side_effect=RuntimeError('synthetic failed save')):
            with self.assertRaises(RuntimeError): await self.add(selected)
        self.assertEqual(await current_draft(), before)
        async with AsyncDBSession() as s:
            self.assertIsNotNone(await s.get(CloudGamePreparedSelection,selected[0]))
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftEntryFen)), 0)
        await self.add(selected)
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 3)

    async def test_live_launch_rechecks_new_scores_after_calculate(self):
        selected = await self.calculate()
        async with AsyncDBSession() as s, s.begin():
            (await s.get(Fen,self.fens[0])).score = 0
        await self.add(selected)
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 3)  # captured calculation
        await self.available()
        self.assertEqual((await self.submit())['target'], 2)  # live eligibility

    async def test_chunk_boundaries_deduplicate_and_expiration_cleanup(self):
        stale = await self.calculate()
        async with AsyncDBSession() as s, s.begin():
            (await s.get(CloudGamePreparedSelection,stale[0])).expires_at = datetime.now(timezone.utc)-timedelta(seconds=1)
        with patch.object(prepared,'GAME_BLOCK',1), patch.object(prepared,'FEN_BLOCK',1):
            selected = await self.calculate((101,102,103))
        async with AsyncDBSession() as s:
            self.assertIsNone(await s.get(CloudGamePreparedSelection,stale[0]))
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGamePreparedChunk)), 3)
        await self.add(selected)
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 3)

    async def test_migration_repeatable_and_empty_missing_set(self):
        async with self.engine.begin() as c:
            await c.run_sync(upgrade); await c.run_sync(upgrade)
        async with AsyncDBSession() as s, s.begin():
            await s.execute(text('UPDATE fen SET score=0'))
        selected = await self.calculate()
        self.assertEqual(selected[1]['fens_to_analyze'], 0)
        await self.add(selected)
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 0)

    async def test_game_selector_commits_preparation_for_subsequent_add(self):
        async with AsyncDBSession() as s, s.begin():
            s.add_all([GameAnalysisSummary(link=link, total_positions=2, unscored_positions=2)
                       for link in (101,102,103)])
        ident = uuid4().hex
        selected = await preview_player_games_for_analysis(
            'hikaru', order='latest', game_limit=2, prepared_plan_id=ident)
        self.assertEqual(selected['game_links'], [103,102])
        self.assertEqual(selected['fens_to_analyze'], 3)
        await self.add((ident, {**plan((103,102)), **selected}))
        self.assertEqual(self.draft['preview']['fens_to_analyze'], 3)


if __name__ == '__main__': unittest.main()
