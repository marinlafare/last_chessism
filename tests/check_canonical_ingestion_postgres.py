"""Canonical application import workflow; HTTP is synthetic, PostgreSQL is real."""
import asyncio
from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, patch

import asyncpg
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical
from chessism_api.operations.ingestion_pipeline import game_importer, jobs, months
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.canonical_schema_fixture import FENS


def archives():
    rows=[]
    for link,rules,setup,black in ((101,'chess',None,'black'),(102,'chess960',None,'black'),
            (103,'chess',FENS[4],'black'),(104,'chess',None,'black'),(105,'chess',None,'white')):
        moves=('1. Ke4 {[%clk 0:10:00]} Kf2 {[%clk 0:09:59]} 1-0' if link==103 else
               '1. e4 {[%clk 0:10:00]} e5 {[%clk 0:09:59]} 2. Nf3 {[%clk 0:09:58]} Nc6 {[%clk 0:09:57]} 1-0')
        if link==104:
            moves='1-0'
        rows.append({'url':f'https://www.chess.com/game/live/{link}', 'rules':rules,'initial_setup':setup,
            'time_control':'600','white':{'username':'white','rating':1500,'result':'win'},
            'black':{'username':black,'rating':1600,'result':'resigned'},
            'eco':'https://www.chess.com/openings/Synthetic',
            'pgn':'[Date "2026.01.31"]\n[StartTime "23:59:00"]\n[EndDate "2026.02.01"]\n[EndTime "00:00:00"]\n\n'+moves})
    return {2026:{1:rows,2:[]}}


class CanonicalIngestionTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        await self.db.execute("INSERT INTO player(player_name,joined) VALUES('white',1),('black',1)")
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',
            database=self.name,port=55432,query={'host':self.socket}),
            connect_args={'server_settings':{'statement_timeout':'5000'}})
        self.addAsyncCleanup(self.engine.dispose)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        for module in (game_importer,months):
            patcher=patch.object(module,'AsyncDBSession',self.factory)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def run_import(self,data=None,progress=None):
        with patch.object(jobs,'download_months',AsyncMock(return_value=data or archives())):
            return await jobs._download_format_and_insert('white',['2026-1','2026-2'],progress,
                no_games_message='no games')

    async def test_download_parse_import_months_and_pending_sides_commit_together(self):
        result=await self.run_import()
        self.assertEqual(result,'Successfully processed and inserted 4 games and recorded 1 zero-move games for white.')
        rows=await self.db.fetch('SELECT month,n_games FROM months ORDER BY month')
        self.assertEqual([tuple(r) for r in rows],[(1,2),(2,0)])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),5)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM moves'),7)
        self.assertEqual(await self.db.fetchval('SELECT initial_setup FROM game WHERE link=103'),FENS[4])
        zero=await self.db.fetchrow('SELECT played_at,white_elo,time_control,rules FROM game WHERE link=104')
        self.assertEqual(tuple(zero),(datetime(2026,1,31,23,59,tzinfo=timezone.utc),1500,'600','chess'))
        pending=await self.db.fetch('SELECT game_link,is_white FROM player_salience_pending_game ORDER BY 1,2')
        self.assertEqual([tuple(r) for r in pending],[(101,False),(101,True),(105,False),(105,True)])
        summary=await self.db.fetchrow('SELECT n_games_in_db,main_characters,rapid_games FROM database_summary')
        self.assertEqual(tuple(summary),(2,2,2))
        mode=await self.db.fetchrow('''SELECT s.n_games,s.as_white,s.as_black FROM main_character_mode_summary s
            JOIN player p ON p.id=s.player_id WHERE p.player_name='white' ''')
        self.assertEqual(tuple(mode),(3,2,1))

    async def test_retry_returns_existing_message_without_requeueing_ready_summaries(self):
        await self.run_import()
        await self.db.execute("DELETE FROM player_salience_pending_game; UPDATE player_salience_summary SET status='ready'")
        self.assertEqual(await self.run_import(),'All games already at DB')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_salience_pending_game'),0)
        self.assertTrue(await self.db.fetchval("SELECT bool_and(status='ready') FROM player_salience_summary"))
        self.assertEqual(await self.db.fetchval('SELECT n_games FROM months WHERE month=1'),2)

    async def test_changed_existing_game_payload_is_not_filtered_away(self):
        await self.run_import()
        changed=archives()
        changed[2026][1][0]['white']['rating']=1900
        with self.assertRaisesRegex(ValueError,'persisted metadata'):
            await self.run_import(changed)
        self.assertEqual(await self.db.fetchval('SELECT white_elo FROM game WHERE link=101'),1500)

    async def test_projection_failure_rolls_back_game_month_and_side_work(self):
        refresh=game_importer.refresh_import_summaries
        async def failed_refresh(*args):
            await refresh(*args)
            raise RuntimeError('projection failed after writes')
        progress=AsyncMock()
        with patch.object(game_importer,'refresh_import_summaries',failed_refresh):
            with self.assertRaisesRegex(RuntimeError,'projection failed'):
                await self.run_import(progress=progress)
        for table in ('game','moves','months','main_character_mode_summary','player_salience_summary','player_salience_pending_game'):
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM '+table),0,table)
        self.assertEqual(await self.db.fetchval('SELECT n_games_in_db FROM database_summary'),0)
        self.assertFalse(any(call.args[0]=='complete' for call in progress.await_args_list))
        self.assertIn('inserted 4 games',await self.run_import())

    async def test_missing_summary_fails_the_import_instead_of_committing_partial_accounting(self):
        await self.db.execute('DELETE FROM database_summary')
        with self.assertRaisesRegex(RuntimeError,'summaries are missing'):
            await self.run_import()
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM months'),0)

    async def test_cancel_after_projection_writes_rolls_back_before_return(self):
        entered,release=asyncio.Event(),asyncio.Event()
        refresh=game_importer.refresh_import_summaries
        async def blocked_refresh(*args):
            await refresh(*args)
            entered.set()
            await release.wait()
        with patch.object(game_importer,'refresh_import_summaries',blocked_refresh):
            task=asyncio.create_task(self.run_import())
            try:
                await asyncio.wait_for(entered.wait(),2)
                self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),0)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task,2)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM months'),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_salience_pending_game'),0)
        self.assertIn('inserted 4 games',await self.run_import())

    async def test_only_empty_downloads_record_months_and_complete_without_game_rows(self):
        self.assertEqual(await self.run_import({2026:{1:[],2:[]}}),'no games')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM months WHERE n_games=0'),2)

    async def test_side_pending_keys_and_cascade_follow_canonical_game_ownership(self):
        await self.run_import()
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO player_salience_pending_game VALUES(105,true)')
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute('INSERT INTO player_salience_pending_game VALUES(999,true)')
        await self.db.execute('DELETE FROM game WHERE link=105')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_salience_pending_game WHERE game_link=105'),0)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_salience_pending_game'),2)

    async def test_month_read_consumers_use_numeric_owner_keys(self):
        await self.run_import()
        with patch.object(months,'full_archive_months',AsyncMock(return_value=['2026-1','2026-2','2026-3'])):
            self.assertEqual(await months.missing_archive_months('white'),['2026-3'])
        result=await months.update_archive_months('white')
        self.assertEqual(result[0],'2026-2')

    async def test_import_refresh_preserves_existing_position_counters(self):
        await self.db.execute('UPDATE database_summary SET n_positions=12,analyzed_fens=5,unscored_fens=7')
        await self.run_import()
        row=await self.db.fetchrow('SELECT n_positions,analyzed_fens,unscored_fens FROM database_summary')
        self.assertEqual(tuple(row),(12,5,7))


if __name__=='__main__':
    unittest.main()
