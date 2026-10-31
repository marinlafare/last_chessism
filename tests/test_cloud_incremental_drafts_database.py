"""Real, disposable PostgreSQL; no live data, Google calls or billable launches."""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException
from sqlalchemy import func, select
import test_cloud_analysis_database as existing
import test_cloud_game_drafts_database as drafts_tests
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (
    CloudGameDraft, CloudGameDraftCache, CloudGameDraftFen, CloudGameDraftEntryFen,
    CloudGameDraftEntry, Fen, CloudFenClaim, CloudAnalysisRun, GameFenAssociation,
)
from chessism_api.operations.cloud_analysis import draft_cache, game_drafts
from chessism_api.operations.cloud_analysis.migrate_draft_cache import upgrade
from chessism_api.routers.cloud_game_drafts import (
    current_draft, remove_entry, submit_draft, Revision, SubmitDraft,
)


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL database')
class IncrementalDraftTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = drafts_tests.DraftDatabaseTests.asyncSetUp
    asyncTearDown = drafts_tests.DraftDatabaseTests.asyncTearDown
    add = drafts_tests.DraftDatabaseTests.add
    preview = drafts_tests.DraftDatabaseTests.preview
    available = drafts_tests.DraftDatabaseTests.available

    async def assert_counts(self, games, fens):
        loaded = await current_draft()
        self.assertEqual(loaded['preview']['unique_games'], games)
        self.assertEqual(loaded['preview']['fens_to_analyze'], fens)
        async with AsyncDBSession() as s:
            fresh = await game_drafts.count_union(s, self.draft['id'])
            cache = await s.get(CloudGameDraftCache, self.draft['id'])
            self.assertEqual(cache.revision, loaded['revision'])
            self.assertEqual(cache.missing - cache.claimed, fens)
        self.assertEqual(fresh['eligible'], fens)

    async def remove(self, ident):
        self.draft = await remove_entry(self.draft['id'], ident, Revision(revision=self.draft['revision']))

    async def test_add_reuses_old_membership_and_counts_shared_fens_once(self):
        await self.add((101,))
        first = self.draft['entries'][0]['id']
        with patch.object(draft_cache, 'add_entry', wraps=draft_cache.add_entry) as add:
            with patch.object(game_drafts, 'count_union', side_effect=AssertionError('Full union scanned')):
                await self.add((102, 103), 'magnuscarlsen')
            self.assertEqual(add.await_count, 1)
            self.assertNotEqual(add.await_args.args[2], first)
        await self.assert_counts(3, 3)
        await self.add((101,))  # Exact duplicate selection is another reference, not another FEN.
        await self.assert_counts(3, 3)
        async with AsyncDBSession() as s:
            refs = (await s.scalars(select(CloudGameDraftFen.n_entries))).all()
            self.assertEqual(sorted(refs), [1, 3, 3])

    async def test_replace_and_remove_adjust_only_the_changed_entry(self):
        await self.add((101,))
        first = self.draft['entries'][0]['id']
        await self.add((102,))
        second = self.draft['entries'][1]['id']
        with patch.object(draft_cache, 'add_entry', wraps=draft_cache.add_entry) as add:
            await self.add((103,), replace=first)
            self.assertEqual(add.await_count, 1)
            self.assertEqual(add.await_args.args[2], first)
        await self.assert_counts(2, 3)
        with patch.object(draft_cache, 'add_entry', side_effect=AssertionError('Removal rescanned a step')):
            await self.remove(second)
            await self.assert_counts(1, 2)
            await self.remove(first)
        await self.assert_counts(0, 0)
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftEntryFen)), 0)
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftFen)), 0)

    async def test_legacy_draft_cache_builds_once_and_survives_new_sessions(self):
        await self.add((101,))
        async with AsyncDBSession() as s, s.begin():
            await draft_cache.discard(s, self.draft['id'])
        with patch.object(draft_cache, 'add_entry', wraps=draft_cache.add_entry) as add:
            await self.add((102,))
            self.assertEqual(add.await_count, 2)  # One existing legacy step, one new step.
            await self.add((103,))
            self.assertEqual(add.await_count, 3)  # No old steps rebuilt on the next Add.
        await self.assert_counts(3, 3)

    async def test_failed_cache_update_rolls_back_step_revision_and_counts(self):
        await self.add((101,))
        before = await current_draft()
        original = draft_cache.add_entry
        async def fail(*args):
            await original(*args)
            raise RuntimeError('injected failure after merge')
        with patch.object(draft_cache, 'add_entry', side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                await self.add((102,), replace=before['entries'][0]['id'])
        self.assertEqual(await current_draft(), before)
        await self.assert_counts(1, 2)

    async def test_stale_cache_revision_rebuilds_before_applying_new_step(self):
        await self.add((101,))
        async with AsyncDBSession() as s, s.begin():
            (await s.get(CloudGameDraftCache, self.draft['id'])).revision -= 1
        with patch.object(draft_cache, 'add_entry', wraps=draft_cache.add_entry) as add:
            await self.add((102,))
            self.assertEqual(add.await_count, 2)
        await self.assert_counts(2, 3)

    async def test_launch_refuses_increased_live_membership_until_refresh(self):
        await self.add((101,))
        await self.available()
        async with AsyncDBSession() as s, s.begin():
            extra = await s.scalar(select(Fen.fen_id).where(Fen.fen == self.fens[2]))
            s.add(GameFenAssociation(game_link=101, fen_id=extra, n_move=3, move_color='white'))
        with self.assertRaises(HTTPException) as error:
            await submit_draft(self.draft['id'], SubmitDraft(revision=self.draft['revision']))
        self.assertIn('increased', error.exception.detail)
        async with AsyncDBSession() as s:
            self.assertIsNone((await s.get(CloudGameDraft, self.draft['id'])).submitted_job_id)
            self.assertIsNotNone(await s.get(CloudGameDraftCache, self.draft['id']))
        self.assertEqual((await self.preview())['fens_to_analyze'], 3)

    async def test_concurrent_tabs_cannot_double_apply_a_delta(self):
        outcomes = await asyncio.gather(self.add((101,)), self.add((102,)), return_exceptions=True)
        self.assertEqual(sum(isinstance(value, HTTPException) and value.status_code == 409 for value in outcomes), 1)
        self.assertEqual(sum(isinstance(value, dict) for value in outcomes), 1)
        await self.assert_counts(1, 2)

    async def test_refresh_rebuilds_current_membership_and_eligibility(self):
        await self.add((101,))
        async with AsyncDBSession() as s, s.begin():
            (await s.get(Fen, self.fens[0])).score = 1
            s.add(CloudAnalysisRun(id='e'*32, job_id=self.job.id, positions=[], launch={}, receipts={}))
            await s.flush()
            s.add(CloudFenClaim(fen=self.fens[1], run_id='e'*32))
            extra = await s.scalar(select(Fen.fen_id).where(Fen.fen == self.fens[2]))
            s.add(GameFenAssociation(game_link=101, fen_id=extra, n_move=3, move_color='white'))
        preview = await self.preview()
        self.assertEqual((preview['unique_missing_fens'], preview['claimed_fens'], preview['fens_to_analyze']), (2, 1, 1))
        await self.remove(self.draft['entries'][0]['id'])
        await self.assert_counts(0, 0)

    async def test_launch_rechecks_live_union_and_releases_only_cache_for_both_backends(self):
        await self.available()
        from chessism_api.routers.cloud_game_drafts import create_draft
        from chessism_api.operations.cloud_analysis.controller import save_job
        for backend in ('batch_spot', 'cloud_run'):
            self.draft = await create_draft()
            await self.add((101, 102))
            async with AsyncDBSession() as s, s.begin():
                (await s.get(Fen, self.fens[0])).score = 1
            result = await submit_draft(self.draft['id'], SubmitDraft(
                revision=self.draft['revision'], backend=backend, n_vms=6, n_cpus=100))
            self.assertEqual(result['target'], 2)
            async with AsyncDBSession() as s:
                self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftCache)), 0)
                self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftEntryFen)), 0)
                self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftFen)), 0)
                self.assertGreater(await s.scalar(select(func.count()).select_from(CloudGameDraftEntry)), 0)
            await save_job(result['id'], status='complete')

    async def test_migration_is_additive_repeatable_and_native_columns_only(self):
        async with self.engine.begin() as c:
            for model in (CloudGameDraftEntryFen, CloudGameDraftFen, CloudGameDraftCache):
                await c.run_sync(lambda conn, model=model: model.__table__.drop(conn))
            await c.run_sync(upgrade)
            await c.run_sync(upgrade)
        for model in (CloudGameDraftCache, CloudGameDraftEntryFen, CloudGameDraftFen):
            self.assertFalse(any('JSON' in str(c.type) for c in model.__table__.columns))
        await self.add((101,))
        await self.assert_counts(1, 2)


if __name__ == '__main__': unittest.main()
