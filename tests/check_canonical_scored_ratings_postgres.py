"""Scored-rating projection: real canonical storage, publication and failure paths."""
import asyncio
from itertools import combinations
import json
from unittest.mock import patch

import asyncpg
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError, NoResultFound
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, game_coverage
from chessism_api.database.postgresql import scored_rating_summary as storage
from chessism_api.database.postgresql.transactions import write_transaction
from chessism_api.operations import scored_rating_summary as summaries
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import seed, GAME_SQL, PLAYED


class CanonicalScoredRatingTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.ids = await seed(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host': self.socket}),
            connect_args={'server_settings': {'statement_timeout': '5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.children = []
        self.addAsyncCleanup(self.stop_children)

    async def stop_children(self):
        for task in self.children:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.children, return_exceptions=True)

    def start(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.children.append(task)
        return task

    async def refresh(self):
        return await summaries.refresh_scored_rating_summary(session_factory=self.factory)

    async def read(self, include_ratings=True):
        return await summaries.get_scored_advantage_by_rating(
            session_factory=self.factory, include_ratings=include_ratings)

    async def saved(self):
        return dict(await self.db.fetchrow('SELECT * FROM scored_rating_summary'))

    async def coverage(self, link, buckets, *, unscored=0):
        # Literal independent coverage inputs isolate chart math from its producer.
        analyzed = sum(buckets)
        await self.db.execute('''INSERT INTO game_analysis_coverage VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now()) ON CONFLICT(link) DO UPDATE SET
            total_positions=EXCLUDED.total_positions,analyzed_positions=EXCLUDED.analyzed_positions,
            unscored_positions=EXCLUDED.unscored_positions,is_fully_analyzed=EXCLUDED.is_fully_analyzed,
            equal_positions=EXCLUDED.equal_positions,small_positions=EXCLUDED.small_positions,
            clear_positions=EXCLUDED.clear_positions,decisive_positions=EXCLUDED.decisive_positions,
            mate_positions=EXCLUDED.mate_positions''',
            link, analyzed+unscored, analyzed, unscored, unscored == 0, *buckets)

    async def game(self, link, white, black, buckets, *, unscored=0):
        if not await self.db.fetchval('SELECT EXISTS(SELECT 1 FROM game WHERE link=$1)', link):
            await self.db.execute(GAME_SQL, link, self.ids['white'], self.ids['black'], PLAYED, 2, 'chess', None)
        await self.db.execute('UPDATE game SET white_elo=$2,black_elo=$3 WHERE link=$1', link, white, black)
        await self.coverage(link, buckets, unscored=unscored)

    async def wait_for_lock(self):
        observer = await self.connect()
        async def wait():
            while not await observer.fetchval('''SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                WHERE datname=current_database() AND wait_event_type='Lock'
                AND query LIKE '%scored_rating_summary%FOR UPDATE%')'''):
                await asyncio.sleep(.01)
        await asyncio.wait_for(wait(), 2)

    async def assert_lock_free(self):
        async with self.db.transaction():
            await self.db.fetchval('SELECT id FROM scored_rating_summary WHERE id=1 FOR UPDATE NOWAIT')
        self.assertEqual(self.engine.pool.checkedout(), 0)

    async def test_empty_initialization_and_refresh_preserve_three_group_public_shape(self):
        await self.db.execute('DELETE FROM game')
        before = await self.saved()
        with patch.object(summaries, 'refresh_scored_rating_summary', side_effect=AssertionError('read rebuilt')):
            value = await self.read()
            overview = await self.read(False)
        self.assertEqual(await self.saved(), before)
        self.assertEqual(value['rating_basis'], 'avg_elo')
        self.assertEqual(value['ratings'], [])
        self.assertEqual(overview, {k: value[k] for k in ('rating_basis', 'groups')})
        self.assertEqual([(g['key'], g['label'], g['min_rating'], g['max_rating'], g['games'], g['positions'])
                         for g in value['groups']],
                         [('bad', 'Bad', 0, 0, 0, 0), ('medium', 'Medium', 0, 0, 0, 0), ('great', 'Great', 0, 0, 0, 0)])
        for group in value['groups']:
            self.assertEqual(group['buckets'], [dict(key=k, label=k.title(), order=i, positions=0)
                for i,k in enumerate(('equal','small','clear','decisive','mate'), 1)])
        self.assertEqual(await self.refresh(), value)
        self.assertEqual(await self.read(), value)
        self.assertFalse(await canonical.initialize(self.db))
        self.assertEqual(json.loads((await self.saved())['groups_payload']), value['groups'])

    async def test_occurrences_rounding_self_play_eligibility_and_chart_order(self):
        await self.game(101,1000,1001,[1,2,3,4,5])
        await self.game(105,1001,1001,[2,0,0,0,0])  # Self-play is one game.
        await self.game(106,1500,1500,[0,4,0,0,0])
        await self.game(107,2000,2001,[0,0,1,0,0])
        for link in (102,103,104):  # Variant/custom/zero-move games cannot enter active analytics.
            await self.game(link,9999,9999,[99,0,0,0,0])
        await self.game(108,0,0,[99,0,0,0,0])
        await self.game(109,7000,7000,[99,0,0,0,0],unscored=1)
        value = await self.refresh()
        self.assertEqual(value, await self.read())
        self.assertEqual(value['ratings'], [dict(link=link,x_index=i,rating=rating,group=group)
            for i,(link,rating,group) in enumerate(((101,1001,'bad'),(105,1001,'bad'),
                                                   (106,1500,'medium'),(107,2001,'great')),1)])
        expected = [(1001,2,17,[3,2,3,4,5]),(1500,1,4,[0,4,0,0,0]),(2001,1,1,[0,0,1,0,0])]
        for group,(rating,games,positions,buckets) in zip(value['groups'],expected):
            self.assertEqual((group['min_rating'],group['max_rating'],group['games'],group['positions']),
                             (rating,rating,games,positions))
            self.assertEqual([b['positions'] for b in group['buckets']],buckets)
        row = await self.saved()
        self.assertEqual((row['source_full_games'],row['source_distinct_ratings']),(4,3))

    async def test_repeated_jenks_ranges_keep_all_group_counts_and_first_chart_assignment(self):
        await self.game(101,1000,1000,[2,1,0,0,0])
        value = await self.refresh()
        self.assertEqual([g['games'] for g in value['groups']],[1,1,1])
        self.assertEqual([g['positions'] for g in value['groups']],[3,3,3])
        self.assertEqual(value['ratings'][0]['group'],'bad')
        await self.game(105,2000,2000,[0,0,0,0,7])
        value = await self.refresh()
        self.assertEqual([g['positions'] for g in value['groups']],[3,7,7])
        self.assertEqual([p['group'] for p in value['ratings']],['bad','medium'])

    async def test_weighted_partition_matches_independent_exhaustive_game_oracle(self):
        # Enumerate every contiguous three-way partition; do not call production Jenks.
        histogram = {500: 1, 510: 2, 1000: 1, 1010: 2, 2000: 1, 2010: 2}
        ratings = list(histogram)
        candidates = []
        for a,b in combinations(range(1,len(ratings)),2):
            groups = (ratings[:a],ratings[a:b],ratings[b:])
            cost = 0
            for group in groups:
                members = [r for r in group for _ in range(histogram[r])]
                mean = sum(members)/len(members)
                cost += sum((r-mean)**2 for r in members)
            candidates.append((cost,groups))
        expected = min(candidates,key=lambda item:item[0])[1]
        link = 200
        for rating,count in histogram.items():
            for _ in range(count):
                await self.game(link,rating,rating,[1,0,0,0,0])
                link += 1
        value = await self.refresh()
        for group,partition in zip(value['groups'],expected):
            count = sum(histogram[r] for r in partition)
            self.assertEqual((group['min_rating'],group['max_rating'],group['games'],group['positions']),
                             (partition[0],partition[-1],count,count))
        self.assertEqual(len(value['ratings']),sum(histogram.values()))

    async def test_average_does_not_overflow_and_nonpositive_averages_are_excluded(self):
        await self.game(101,2147483647,2147483647,[1,0,0,0,0])
        await self.game(105,-2147483648,2147483647,[9,0,0,0,0])
        self.assertEqual((await self.refresh())['ratings'],
            [dict(link=101,x_index=1,rating=2147483647,group='bad')])

    async def test_coverage_bucket_thresholds_and_int32_minimum_for_scoped_and_full_refresh(self):
        # Actual associations and scores verify the producer, including repeated occurrences.
        values = (0,49,-50,149,-150,299,-300,8999,-9000,-2147483648)
        ids = self.ids['fens']
        for score in values:
            with self.subTest(score=score):
                await self.db.execute('''UPDATE fen SET score=$1,analysis_source='stockfish',analyzed_at=now()
                    WHERE fen_id=ANY($2::bigint[])''',score,ids[:3])
                bucket = 0 if abs(score)<50 else 1 if abs(score)<150 else 2 if abs(score)<300 else 3 if abs(score)<9000 else 4
                for scope in ([101,105], None):
                    async with write_transaction(self.factory) as session:
                        await game_coverage.refresh_coverage(session,scope)
                    value = await self.refresh()
                    for group in value['groups']:
                        self.assertEqual(group['positions'],6)
                        self.assertEqual([b['positions'] for b in group['buckets']],
                            [6 if index==bucket else 0 for index in range(5)])

    async def test_publication_is_atomic_on_failure_and_commit_error(self):
        await self.game(101,1500,1500,[1,0,0,0,0])
        await self.refresh()
        before = await self.saved()
        await self.coverage(101,[0,0,0,0,9])
        publish = storage.publish
        async def fail_after_write(*args):
            await publish(*args)
            raise RuntimeError('after publication')
        with patch.object(storage,'publish',fail_after_write), self.assertRaisesRegex(RuntimeError,'after publication'):
            await self.refresh()
        self.assertEqual(await self.saved(),before)
        def bad_commit_factory():
            session=self.factory()
            async def fail():
                raise OSError('commit failed')
            session.commit=fail
            return session
        with self.assertRaisesRegex(OSError,'commit failed'):
            await summaries.refresh_scored_rating_summary(session_factory=bad_commit_factory)
        self.assertEqual(await self.saved(),before)
        await self.assert_lock_free()
        self.assertEqual((await self.refresh())['groups'][0]['positions'],9)

    async def test_source_database_failure_preserves_committed_summary_and_propagates(self):
        before = await self.saved()
        await self.db.execute('ALTER TABLE game_analysis_coverage RENAME TO missing_coverage')
        with self.assertRaises(DBAPIError):
            await self.refresh()
        self.assertEqual(await self.saved(),before)
        await self.assert_lock_free()

    async def test_missing_singleton_fails_reads_and_refresh_without_rebuilding(self):
        await self.db.execute('DELETE FROM scored_rating_summary')
        for action in (self.read,self.refresh):
            with self.assertRaises(NoResultFound):
                await action()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM scored_rating_summary'),0)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_adapters_require_transaction_and_page_read_is_readonly(self):
        async with self.factory() as session:
            for action in (lambda:storage.lock_summary(session),
                           lambda:storage.publish(session,{}),
                           lambda:storage.read_summary(session,include_ratings=False)):
                with self.assertRaisesRegex(ValueError,'caller-owned'):
                    await action()
            with self.assertRaisesRegex(ValueError,'caller-owned'):
                await anext(storage.source_games(session))
        original = storage.read_summary
        async def write_attempt(session,**kwargs):
            await original(session,**kwargs)
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            await session.execute(text('DELETE FROM scored_rating_summary'))
        with patch.object(storage,'read_summary',write_attempt),self.assertRaises(DBAPIError):
            await self.read(False)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM scored_rating_summary'),1)
        await self.assert_lock_free()

    async def test_source_is_one_snapshot_across_concurrent_game_deletion(self):
        await self.game(101,1500,1500,[1,0,0,0,0])
        await self.game(105,2000,2000,[0,2,0,0,0])
        source=storage.source_games
        async def delete_after_first(session):
            first=True
            async for row in source(session):
                if first:
                    first=False
                    await self.db.execute('DELETE FROM game WHERE link=105')
                yield row
        with patch.object(storage,'source_games',delete_after_first):
            value=await self.refresh()
        self.assertEqual([p['link'] for p in value['ratings']],[101,105])
        self.assertEqual([g['positions'] for g in value['groups']],[1,2,2])
        self.assertEqual([p['link'] for p in (await self.refresh())['ratings']],[101])

    async def test_waiting_publisher_reads_fresh_source_after_predecessor_commits(self):
        await self.game(101,1500,1500,[1,0,0,0,0])
        entered,release=asyncio.Event(),asyncio.Event()
        source=storage.source_games
        first=True
        async def paused(session):
            nonlocal first
            pause=first;first=False
            async for row in source(session):
                if pause:
                    entered.set();await release.wait()
                yield row
        with patch.object(storage,'source_games',paused):
            a=self.start(self.refresh())
            try:
                await asyncio.wait_for(entered.wait(),2)
                await self.coverage(101,[0,0,0,0,9])
                b=self.start(self.refresh())
                await self.wait_for_lock()
                self.assertFalse(b.done())
            finally:
                release.set()
            first_value,second_value=await asyncio.wait_for(asyncio.gather(a,b),5)
        self.assertEqual(first_value['groups'][0]['positions'],1)
        self.assertEqual(second_value['groups'][0]['positions'],9)
        self.assertEqual(await self.read(),second_value)
        await self.assert_lock_free()

    async def test_cancellation_while_waiting_for_lock_does_not_publish(self):
        before=await self.saved()
        async with self.db.transaction():
            await self.db.fetchval('SELECT id FROM scored_rating_summary WHERE id=1 FOR UPDATE')
            task=self.start(self.refresh())
            await self.wait_for_lock()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task,2)
        self.assertEqual(await self.saved(),before)
        await self.assert_lock_free()

    async def test_repeated_cancellation_during_rollback_preserves_commits_and_finishes_close(self):
        await self.game(101,1500,1500,[1,0,0,0,0])
        await self.refresh()
        before=await self.saved()
        await self.coverage(101,[0,0,0,0,9])
        entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
        publish=storage.publish
        async def pause(*args):
            await publish(*args)
            # A reader during an uncommitted publication must still see the prior value.
            self.assertEqual(await self.read(),before_payload)
            entered.set();await asyncio.Future()
        def factory():
            session=self.factory();rollback,close=session.rollback,session.close
            async def delayed():
                rolling.set();await release.wait();await rollback()
            async def closing():
                try:await close()
                finally:closed.set()
            session.rollback=delayed;session.close=closing
            return session
        with patch.object(storage,'publish',pause):
            before_payload=await self.read()
            task=self.start(summaries.refresh_scored_rating_summary(session_factory=factory))
            try:
                await asyncio.wait_for(entered.wait(),2);task.cancel()
                await asyncio.wait_for(rolling.wait(),2)
                for _ in range(2):
                    task.cancel();await asyncio.sleep(0)
                    self.assertFalse(task.done())
            finally:
                release.set()
                if not task.done():task.cancel()
                await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(task.cancelled());self.assertTrue(closed.is_set())
        self.assertEqual(await self.saved(),before)
        await self.assert_lock_free()
        self.assertEqual((await self.refresh())['groups'][0]['positions'],9)

    async def test_singleton_and_payload_count_constraints_reject_inconsistent_publication(self):
        for sql in ("INSERT INTO scored_rating_summary(id) VALUES(2)",
                    "UPDATE scored_rating_summary SET source_full_games=-1",
                    "UPDATE scored_rating_summary SET source_distinct_ratings=1",
                    "UPDATE scored_rating_summary SET groups_payload='{}'",
                    "UPDATE scored_rating_summary SET ratings_payload='[{}]'",
                    "UPDATE scored_rating_summary SET source_full_games=1,ratings_payload='[{}]'"):
            with self.subTest(sql=sql),self.assertRaises(asyncpg.CheckViolationError):
                await self.db.execute(sql)
        self.assertEqual((await self.saved())['source_full_games'],0)
