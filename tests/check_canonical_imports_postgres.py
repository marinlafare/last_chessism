"""Real canonical import transactions; explicit isolated PostgreSQL is mandatory."""
import asyncio
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from chessism_api.database.postgresql import canonical, game_imports
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import START, FENS


def bundle():
    games, moves, openings = [], [], []
    for link, rules, setup, count, black in (
        (101, 'chess', None, 1, 'black'),
        (102, 'chess960', None, 1, 'black'),
        (103, 'chess', FENS[4], 1, 'black'),
        (104, 'chess', None, 0, 'black'),
        (105, 'chess', START + ' 0 1', 1, 'white'),
    ):
        games.append(dict(link=link, white='white', black=black, mode='rapid',
            played_at=datetime(2026, 1, 31, 23, 59, tzinfo=timezone.utc),
            white_elo=1500, black_elo=1600, white_result=1., black_result=0.,
            white_str_result='win', black_str_result='resigned', time_control='600',
            eco='https://www.chess.com/openings/Kings-Pawn-Game', time_elapsed=60.,
            n_moves=count, rules=rules, initial_setup=setup, fens_done=rules != 'chess'))
        if count:
            moves.append(dict(link=link, n_move=1, white_move='Ke4' if link==103 else 'e4',
                black_move='Kf2' if link==103 else 'e5',
                white_reaction_time=0., black_reaction_time=1., white_time_left=600., black_time_left=599.))
            if link != 103:
                openings.append(dict(link=link, n_moves=1, opening='1. e4 e5'))
    return games, moves, openings


class CanonicalImportTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)

    async def import_bundle(self, connection=None, data=None):
        conn = connection or self.db
        result = await game_imports.ingest_games(conn, *(data or bundle()))
        await game_imports.sync_month_counts(conn, 'white', {(2026, 1), (2026, 2)})
        return result

    async def counts(self, connection=None):
        rows = await (connection or self.db).fetch('SELECT year,month,n_games FROM months ORDER BY year,month')
        return [tuple(row) for row in rows]

    async def test_mixed_classifications_metadata_moves_openings_and_self_play(self):
        async with self.db.transaction():
            result = await self.import_bundle()
        self.assertEqual(result, {'inserted_links':[101,102,103,104,105], 'analyzable_links':[101,105]})
        self.assertEqual(await self.counts(), [(2026,1,2), (2026,2,0)])
        rows = await self.db.fetch('SELECT link,is_standard,n_moves,fens_done FROM game ORDER BY link')
        self.assertEqual([tuple(row) for row in rows],
            [(101,True,1,False),(102,False,1,False),(103,False,1,False),(104,True,0,False),(105,True,1,False)])
        self.assertEqual(await self.db.fetchval('SELECT initial_setup FROM game WHERE link=103'), FENS[4])
        zero = await self.db.fetchrow('SELECT played_at,white_elo,eco FROM game WHERE link=104')
        self.assertEqual(tuple(zero), (bundle()[0][3]['played_at'],1500,bundle()[0][3]['eco']))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_player WHERE game_link=105'),2)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM moves'),4)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_opening'),3)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM opening_sequence'),1)

    async def test_repeated_bundle_preserves_counts_and_analysis_state(self):
        async with self.db.transaction():
            await self.import_bundle()
        await self.db.execute('UPDATE game SET fens_done=true WHERE link=101')
        async with self.db.transaction():
            result = await self.import_bundle()
        self.assertEqual(result, {'inserted_links':[], 'analyzable_links':[]})
        self.assertEqual(await self.counts(), [(2026,1,2),(2026,2,0)])
        self.assertTrue(await self.db.fetchval('SELECT fens_done FROM game WHERE link=101'))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM moves'),4)

    async def test_identical_duplicates_within_bundle_are_not_counted_twice(self):
        games,moves,openings = bundle()
        async with self.db.transaction():
            result = await self.import_bundle(data=(games+games,moves+moves,openings+openings))
        self.assertEqual(len(result['inserted_links']),5)
        self.assertEqual(await self.counts(),[(2026,1,2),(2026,2,0)])

    async def test_conflicting_metadata_and_children_cannot_be_acknowledged_as_retries(self):
        async with self.db.transaction():
            await self.import_bundle()
        for conflict in ('player','classification','rating','move','opening','missing_opening'):
            games,moves,openings = bundle()
            if conflict=='player': games[0]['white']='different'
            if conflict=='classification': games[0]['rules']='chess960'
            if conflict=='rating': games[0]['white_elo']=1700
            if conflict=='move': moves[0]['white_move']='d4'
            if conflict=='opening': openings[0]['opening']='1. d4 d5'
            if conflict=='missing_opening': openings=openings[1:]
            with self.subTest(conflict=conflict), self.assertRaisesRegex(ValueError,'conflicts'):
                async with self.db.transaction():
                    await self.import_bundle(data=(games,moves,openings))
            self.assertEqual(await self.counts(),[(2026,1,2),(2026,2,0)])
        self.assertEqual(await self.db.fetchval('SELECT white_elo FROM game WHERE link=101'),1500)
        self.assertEqual(await self.db.fetchval('SELECT white_move FROM moves WHERE link=101'),'e4')

    async def test_late_chunk_conflict_rolls_back_earlier_chunk(self):
        async with self.db.transaction():
            await self.import_bundle()
        games,moves,openings = bundle()
        games[0]['link']=100
        moves[0]['link']=100
        openings[0]['link']=100
        games[-1]['white_elo']=1800
        with patch.object(game_imports,'CHUNK_SIZE',2),self.assertRaises(ValueError):
            async with self.db.transaction():
                await self.import_bundle(data=(games,moves,openings))
        self.assertFalse(await self.db.fetchval('SELECT EXISTS(SELECT 1 FROM game WHERE link=100)'))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),5)

    async def test_rollback_restores_whole_unit_then_retry_succeeds(self):
        with self.assertRaisesRegex(RuntimeError,'after accounting'):
            async with self.db.transaction():
                await self.import_bundle()
                await self.db.execute("""INSERT INTO ingestion_pipeline_run
                    (run_id,trigger,status,started_at) VALUES('synthetic','manual','complete',now())""")
                raise RuntimeError('after accounting')
        for table in ('player','game_mode','game','moves','opening_sequence','game_opening','months','ingestion_pipeline_run'):
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM '+table),0,table)
        async with self.db.transaction():
            result=await self.import_bundle()
        self.assertEqual(len(result['inserted_links']),5)
        self.assertEqual(await self.counts(),[(2026,1,2),(2026,2,0)])

    async def test_helpers_never_commit_the_callers_transaction(self):
        other=await self.connect()
        async with self.db.transaction():
            await self.import_bundle()
            self.assertTrue(self.db.is_in_transaction())
            self.assertEqual(await other.fetchval('SELECT count(*) FROM game'),0)
            self.assertEqual(await other.fetchval('SELECT count(*) FROM months'),0)
        self.assertEqual(await other.fetchval('SELECT count(*) FROM game'),5)

    async def test_helpers_refuse_autocommit_connections(self):
        with self.assertRaisesRegex(ValueError,'caller-owned transaction'):
            await game_imports.ingest_games(self.db,*bundle())
        with self.assertRaisesRegex(ValueError,'caller-owned transaction'):
            await game_imports.sync_month_counts(self.db,'white',{(2026,1)})
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player'),0)

    async def test_concurrent_identical_imports_and_month_updates_serialize(self):
        other=await self.connect()
        async def importer(conn):
            async with conn.transaction():
                return await self.import_bundle(connection=conn)
        results=await asyncio.gather(importer(self.db),importer(other))
        self.assertEqual(sorted(len(row['inserted_links']) for row in results),[0,5])
        self.assertEqual(await self.counts(),[(2026,1,2),(2026,2,0)])

    async def test_concurrent_distinct_bundles_do_not_lose_month_counts(self):
        other=await self.connect()
        first=bundle()
        second=bundle()
        for records in second:
            for row in records:
                row['link']+=1000
        async def importer(conn,data):
            async with conn.transaction():
                return await self.import_bundle(connection=conn,data=data)
        results=await asyncio.gather(importer(self.db,first),importer(other,second))
        self.assertEqual([len(row['inserted_links']) for row in results],[5,5])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),10)
        self.assertEqual(await self.counts(),[(2026,1,4),(2026,2,0)])

    async def test_month_membership_uses_either_side_without_double_counting(self):
        async with self.db.transaction():
            await self.import_bundle()
            await game_imports.sync_month_counts(self.db,'black',{(2026,1)})
        rows=await self.db.fetch('''SELECT p.player_name,m.n_games FROM months m
            JOIN player p ON p.id=m.player_id WHERE month=1 ORDER BY p.player_name''')
        self.assertEqual([tuple(row) for row in rows],[('black',1),('white',2)])

    async def test_utc_month_boundaries_do_not_depend_on_connection_timezone(self):
        games,moves,openings=bundle()
        games[0]['played_at']=datetime(2026,2,1,0,0,tzinfo=timezone.utc)
        async with self.db.transaction():
            await self.db.execute("SET LOCAL TIME ZONE 'America/Los_Angeles'")
            await self.import_bundle(data=(games,moves,openings))
        self.assertEqual(await self.counts(),[(2026,1,1),(2026,2,1)])

    async def test_empty_fetched_month_is_persisted_without_a_game(self):
        async with self.db.transaction():
            result=await game_imports.ingest_games(self.db,[],[],[])
            await game_imports.sync_month_counts(self.db,'empty_player',{(2026,2)})
        self.assertEqual(result['inserted_links'],[])
        self.assertEqual(await self.counts(),[(2026,2,0)])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),0)

    async def test_invalid_children_and_fractional_values_do_not_produce_partial_imports(self):
        for kind in ('orphan','missing_move','duplicate_move','zero_move_child','fractional','naive_date'):
            games,moves,openings=bundle()
            if kind=='orphan': moves[0]['link']=999
            if kind=='missing_move': moves=moves[1:]
            if kind=='duplicate_move': moves.append(dict(moves[0],white_move='d4'))
            if kind=='zero_move_child': moves.append(dict(moves[0],link=104))
            if kind=='fractional': games[0]['white_elo']=1500.5
            if kind=='naive_date': games[0]['played_at']=datetime(2026,1,1)
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                async with self.db.transaction():
                    await self.import_bundle(data=(games,moves,openings))
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player'),0)

    async def test_cancellation_waiting_for_import_lock_preserves_committed_results(self):
        async with self.db.transaction():
            await self.import_bundle()
        other=await self.connect()
        async def cancelled_import():
            async with other.transaction():
                await self.import_bundle(connection=other)
        async with self.db.transaction():
            await self.db.execute('SELECT pg_advisory_xact_lock($1)',game_imports.INGESTION_COMMIT_LOCK)
            task=asyncio.create_task(cancelled_import())
            try:
                for _ in range(100):
                    if await self.db.fetchval("SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",other.get_server_pid()):
                        break
                    await asyncio.sleep(.01)
                else:
                    self.fail('Importer never reached the transaction lock')
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertFalse(other.is_in_transaction())
        self.assertEqual(await self.counts(),[(2026,1,2),(2026,2,0)])
        async with other.transaction():
            result=await self.import_bundle(connection=other)
        self.assertEqual(result['inserted_links'],[])

    async def test_cancellation_after_writes_rolls_back_only_the_active_import(self):
        async with self.db.transaction():
            await self.import_bundle()
        other=await self.connect()
        data=bundle()
        for records in data:
            for row in records:
                row['link']+=1000
        written=asyncio.Event()
        async def worker():
            async with other.transaction():
                await self.import_bundle(connection=other,data=data)
                written.set()
                await asyncio.Future()
        task=asyncio.create_task(worker())
        try:
            await asyncio.wait_for(written.wait(),2)
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),5)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(other.is_in_transaction())
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),5)
        self.assertEqual(await self.counts(),[(2026,1,2),(2026,2,0)])
        async with other.transaction():
            retry=await self.import_bundle(connection=other,data=data)
        self.assertEqual(len(retry['inserted_links']),5)
        self.assertEqual(await self.counts(),[(2026,1,4),(2026,2,0)])


if __name__ == '__main__':
    unittest.main()
