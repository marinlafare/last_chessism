"""Draft API + real isolated PostgreSQL; cloud execution is always mocked."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
from fastapi import HTTPException
from sqlalchemy import func, select, update
import test_cloud_analysis_database as existing
import test_cloud_auto_sequences_database as auto_tests
import test_cloud_work_units_database as unit_tests
from test_cloud_game_drafts import plan
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.models import (CloudAnalysisJob, CloudAnalysisRun, CloudBatchSequence,
    CloudGameDraft, CloudGameDraftEntry, CloudGameDraftGame, CloudControllerHeartbeat,
    CloudFenClaim, Fen, Game, Player, GameFenAssociation, GameAnalysisSummary)
from chessism_api.routers.cloud_game_drafts import (create_draft, current_draft, save_entry,
    remove_entry, preview_draft, submit_draft, Revision, AddSelection, SubmitDraft)
from chessism_api.operations.cloud_analysis import controller, sequence, game_drafts
from chessism_api.operations.cloud_analysis.migrate_game_drafts import upgrade


@unittest.skipUnless(existing.TEST_URL, 'requires isolated PostgreSQL database')
class DraftDatabaseTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = existing.CloudDatabaseTests.asyncTearDown
    reload = unit_tests.WorkUnitDatabaseTests.reload
    objects = unit_tests.WorkUnitDatabaseTests.objects
    stubs = auto_tests.AutoSequenceDatabaseTests.stubs
    tick = auto_tests.AutoSequenceDatabaseTests.tick
    launch = auto_tests.AutoSequenceDatabaseTests.launch
    cloud_finishes = auto_tests.AutoSequenceDatabaseTests.cloud_finishes

    async def asyncSetUp(self):
        await existing.CloudDatabaseTests.asyncSetUp(self)
        async with AsyncDBSession() as s, s.begin():
            s.add_all([Player(player_name='hikaru'), Player(player_name='magnuscarlsen')])
            s.add(CloudControllerHeartbeat(id=1, seen_at=datetime.now(timezone.utc)))
            await s.flush()
            for link in (101, 102, 103):
                s.add(Game(link=link, white='hikaru', black='magnuscarlsen',
                    year=2026, month=10, day=1, hour=0, minute=0, second=0,
                    white_elo=2800, black_elo=2800, white_result=1, black_result=0,
                    white_str_result='win', black_str_result='resigned', time_control='180',
                    eco='B01', time_elapsed=60, n_moves=2, fens_done=True))
            await s.flush()
            fen_ids = dict((await s.execute(select(Fen.fen, Fen.fen_id))).all())
            for link, indexes in ((101, (0, 1)), (102, (0, 2)), (103, (1, 2))):
                for move, index in enumerate(indexes):
                    s.add(GameFenAssociation(game_link=link, fen_id=fen_ids[self.fens[index]], n_move=move, move_color='white'))
        self.draft = await create_draft()

    async def add(self, links=(101, 102), player='hikaru', replace=None):
        redis = AsyncMock()
        redis.get.return_value = json.dumps(plan(links, player))
        self.draft = await save_entry(self.draft['id'], AddSelection(revision=self.draft['revision'],
            plan_id='a'*32, replace_entry_id=replace), redis)
        return self.draft

    async def preview(self):
        self.draft = await preview_draft(self.draft['id'], Revision(revision=self.draft['revision']))
        return self.draft['preview']

    async def available(self):
        await controller.save_job(self.job.id, status='complete')

    async def submit(self, n_vms=1):
        return await submit_draft(self.draft['id'], SubmitDraft(revision=self.draft['revision'], n_vms=n_vms))

    async def test_draft_survives_sessions_and_preview_token_expiry(self):
        await self.add()
        await self.add((102, 103), 'magnuscarlsen')
        # New sessions only; no Redis calls are used to preview saved selections.
        loaded = await current_draft()
        self.assertEqual(len(loaded['entries']), 2)
        self.assertEqual((await create_draft())['id'], self.draft['id'])
        view = await self.preview()
        self.assertEqual(view['unique_games'], 3)
        self.assertEqual(view['fens_to_analyze'], 3)  # not 6, or four occurrences per source
        async with AsyncDBSession() as s:
            self.assertEqual(await game_drafts.game_links(s, self.draft['id']), [101, 102, 103])
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudAnalysisJob)), 1)
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudFenClaim)), 0)

    async def test_scored_and_reserved_positions_are_excluded(self):
        await self.add()
        async with AsyncDBSession() as s, s.begin():
            (await s.get(Fen, self.fens[0])).score = 1
            s.add(CloudAnalysisRun(id='f'*32, job_id=self.job.id, positions=[], launch={}, receipts={}))
            await s.flush()
            s.add(CloudFenClaim(fen=self.fens[1], run_id='f'*32))
        view = await self.preview()
        self.assertEqual((view['unique_missing_fens'], view['claimed_fens'], view['fens_to_analyze']), (2, 1, 1))

    async def test_draft_editing_allowed_but_submission_blocked_during_current_job(self):
        await self.add()
        await self.preview()
        with self.assertRaises(HTTPException) as error:
            await self.submit()
        self.assertEqual(error.exception.status_code, 409)
        async with AsyncDBSession() as s:
            self.assertIsNone((await s.get(CloudGameDraft, self.draft['id'])).submitted_job_id)
            self.assertEqual((await s.get(CloudAnalysisJob, self.job.id)).status, 'running')

    async def test_edit_remove_revision_conflicts_and_preview_invalidation(self):
        await self.add()
        stale = self.draft['revision']
        entry_id = self.draft['entries'][0]['id']
        await self.preview()
        await self.add((103,), replace=entry_id)
        self.assertEqual(len(self.draft['entries']), 1)
        self.assertIsNone(self.draft['preview'])
        async with AsyncDBSession() as s:
            self.assertEqual(await game_drafts.game_links(s, self.draft['id']), [103])
        with self.assertRaises(HTTPException):
            await remove_entry(self.draft['id'], entry_id, Revision(revision=stale))
        removed = await remove_entry(self.draft['id'], entry_id, Revision(revision=self.draft['revision']))
        self.assertEqual(removed['entries'], [])
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudGameDraftGame)), 0)

    async def test_submit_is_idempotent_rechecks_scores_and_freezes_existing_pipeline(self):
        await self.add()
        await self.add((102, 103), 'magnuscarlsen')
        await self.preview()
        await self.available()
        async with AsyncDBSession() as s, s.begin():
            (await s.get(Fen, self.fens[0])).score = 1  # completed since preview
        with patch('chessism_api.operations.cloud_analysis.schemas.MAX_BATCH_FENS_PER_VM', 1):
            first, second = await asyncio.gather(self.submit(), self.submit())
        self.assertEqual(first['id'], second['id'])
        self.assertEqual(sorted([first['already_submitted'], second['already_submitted']]), [False, True])
        async with AsyncDBSession() as s:
            job = await s.get(CloudAnalysisJob, first['id'])
            self.assertEqual(job.target, 2)
            self.assertEqual(job.selection['game_links'], [101, 102, 103])
            self.assertEqual(job.selection['execution_mode'], 'batch_sequence_v2')
            self.assertEqual(job.selection['nodes'], 100000)
            seq = await s.get(CloudBatchSequence, job.id)
            self.assertEqual(seq.repeat_count, 2)
        # Preparation uses the same real selector and claims as production.
        with patch('cloud_job.sequencing.LIMIT', 1):
            await sequence.initialize(job, seq)
            for _ in range(3):
                await sequence.select_next(type('Controller', (), {})(), job, await sequence.cycles(job.id))
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudFenClaim)), 2)
            self.assertIsNotNone((await s.get(CloudBatchSequence, job.id)).frozen_at)
        roots = await sequence.cycles(job.id)
        self.assertEqual([r.position_count for _, r in roots], [1, 1])
        self.assertIsNone(await current_draft())
        with self.assertRaises(HTTPException):
            await preview_draft(self.draft['id'], Revision(revision=self.draft['revision']))

    async def test_preview_required_and_empty_oversized_or_increased_counts_refused(self):
        await self.add()
        await self.available()
        with self.assertRaises(HTTPException):
            await self.submit()
        await self.preview()
        with patch('chessism_api.routers.cloud_game_drafts.MAX_BATCH_TOTAL_FENS', 2):
            with self.assertRaises(HTTPException):
                await self.submit()
        with patch.object(game_drafts, 'count_union', new=AsyncMock(return_value={'eligible': 4})):
            with self.assertRaises(HTTPException) as error:
                await self.submit()
            self.assertIn('increased', error.exception.detail)
        async with AsyncDBSession() as s, s.begin():
            await s.execute(update(Fen).values(score=0))
        with self.assertRaises(HTTPException):
            await self.submit()

    async def test_migration_repeatable_during_running_job_and_no_json_columns(self):
        for _ in range(2):
            async with self.engine.begin() as c:
                await c.run_sync(upgrade)
        async with AsyncDBSession() as s:
            self.assertEqual((await s.get(CloudAnalysisJob, self.job.id)).status, 'running')
        for model in (CloudGameDraft, CloudGameDraftEntry, CloudGameDraftGame):
            self.assertFalse(any('JSON' in str(c.type) for c in model.__table__.columns))

    async def test_expired_token_and_foreign_entry_cannot_mutate_draft(self):
        redis = AsyncMock()
        redis.get.return_value = None
        with self.assertRaises(HTTPException):
            await save_entry(self.draft['id'], AddSelection(revision=1, plan_id='a'*32), redis)
        await self.add()
        with self.assertRaises(HTTPException):
            await self.add(replace='b'*32)
        self.assertEqual(len((await current_draft())['entries']), 1)

    async def test_combined_two_vm_offline_completion_imports_once_and_completes_shared_games(self):
        await self.add()
        await self.add((102, 103), 'magnuscarlsen')
        await self.preview()
        await self.available()
        with patch('chessism_api.operations.cloud_analysis.schemas.MAX_BATCH_FENS_PER_VM', 2):
            result = await self.submit(n_vms=2)
        async with AsyncDBSession() as s:
            self.job = await s.get(CloudAnalysisJob, result['id'])
        self.cloud = Mock()
        self.data, self.states, self.remotes = {}, {}, {}
        self.remote, self.completed = 'RUNNING', 0
        def create(uri, raw):
            if uri in self.data:
                return False
            self.data[uri] = raw
            return True
        self.cloud.storage.return_value.create.side_effect = create
        self.cloud.storage.return_value.read.side_effect = lambda uri, *args: self.data.get(uri)
        self.cloud.publish.return_value = unit_tests.IMAGE
        self.cloud.start_sequence.return_value = auto_tests.EXECUTION
        self.cloud.sequence_snapshot.side_effect = lambda c: ({
            'version': 2, 'run_id': c.control_run_id, 'session': c.session, 'execution': auto_tests.EXECUTION,
            'plan_hash': c.plan_sha256, 'status': self.remote, 'completed_cycles': self.completed},
            {'state': 'ACTIVE' if self.remote == 'RUNNING' else 'SUCCEEDED'})
        self.cloud.recovery_document.side_effect = lambda ident, session, name: self.states.get((ident, session))
        self.cloud.job.side_effect = lambda name: deepcopy(self.remotes.get(name))
        with self.stubs():
            await self.launch()
            roots = await sequence.cycles(self.job.id)
            self.assertEqual(len(roots), 1)
            self.assertEqual([t['count'] for t in roots[0][1].launch['tasks']], [2, 1])
            # Google completes both VMs with no host ticks/imports in between.
            await self.cloud_finishes()
            for _ in range(40):
                job = await self.tick()  # a fresh controller each tick
                if self.clean.called:
                    self.assertEqual(job.imported, 3)
                if job.status == 'complete':
                    break
            self.assertEqual(job.status, 'complete')
        self.clean.assert_called_once()
        self.cloud.start_sequence.assert_called_once()
        async with AsyncDBSession() as s:
            self.assertEqual(await s.scalar(select(func.count()).select_from(CloudFenClaim)), 0)
            self.assertEqual(await s.scalar(select(func.count()).select_from(Fen).where(Fen.score.is_not(None))), 3)
            summaries = (await s.scalars(select(GameAnalysisSummary))).all()
            self.assertEqual(len(summaries), 3)
            self.assertTrue(all(row.is_fully_analyzed for row in summaries))
        from chessism_api.routers.cloud_analysis import cloud_jobs
        view = await cloud_jobs()
        saved = next(j for j in view['jobs'] if j['id'] == self.job.id)
        self.assertEqual(saved['combined_selection']['players'], ['hikaru', 'magnuscarlsen'])
        self.assertEqual(saved['combined_selection']['selections'], 2)
