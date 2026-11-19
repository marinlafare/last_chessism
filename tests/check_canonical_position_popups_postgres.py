"""FEN popup contracts on the actual canonical schema, without legacy identities."""
import asyncio
import base64
from datetime import datetime, timedelta, timezone
import json
import random
from unittest.mock import AsyncMock, patch

import chess
import httpx
from fastapi import FastAPI
from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical, player_position_reads as reads
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations import player_position_games as games, player_score_position_page as examples
from chessism_api.operations import player_score_distribution as distribution
from chessism_api.routers import players
from tests.canonical_postgres_case import CanonicalDatabaseTestCase


class CanonicalPositionPopupTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp(); await canonical.initialize(self.db)
        self.owners={name:await self.db.fetchval("INSERT INTO player(player_name,joined,timezone) VALUES($1,$2,'America/New_York') RETURNING id",
            name,0 if name=='shell' else 1) for name in ('alice','bob','carol','empty','shell')}
        self.modes={name:await self.db.fetchval('INSERT INTO game_mode(name) VALUES($1) RETURNING id',name)
                    for name in ('blitz','bullet','rapid','daily')}
        board=chess.Board();rng=random.Random(42);seen=set();self.fens=[]
        scores=[None,-9999,-9000,-8999,-550,-551,-150,-151,-50,-51,0,50,51,149,150,550,551,8999,9000,9999,-2**31,2**31-1]+[0]*34
        for index,score in enumerate(scores):
            while True:
                if board.is_game_over():board.reset()
                board.push(rng.choice(list(board.legal_moves)))
                fen=' '.join(board.fen().split()[:4])
                if fen not in seen:seen.add(fen);break
            source='tablebase' if index==22 else 'stockfish' if score is not None else None
            key=await self.db.fetchval('''INSERT INTO fen(fen,score,analysis_source,analyzed_at,tablebase_wdl)
                VALUES($1,$2,$3,$4,$5) RETURNING fen_id''',fen,score,source,datetime.now(timezone.utc) if score is not None else None,0 if source=='tablebase' else None)
            self.fens.append(dict(id=key,fen=fen,score=score,source=source))
        self.records=[];self.occurrences=[]
        for n in range(1,41):
            white,black=('alice','bob') if n%2 else ('bob','alice')
            if n%11==0:white,black='bob','carol'
            await self.add_game(n,white,black,mode=('blitz','bullet','rapid','daily',None)[n%5])
        await self.add_game(41,'alice','alice',mode='blitz')
        for game in self.records:
            for move,white,fen in ((0,True,10),(1,False,10),(5,True,10),(8,False,game['id']%len(self.fens)),(10,True,12),(10,False,9)):
                await self.add_occurrence(game['id'],move,white,fen)
        await self.add_occurrence(41,2,True,0)
        await self.add_occurrence(41,3,False,22)
        await self.add_game(60,'alice','bob',rules='chess960')
        await self.add_game(61,'alice','bob',moves=0)
        await self.add_game(62,'alice','bob',setup='8/8/8/8/8/4K3/8/6k1 w - -')
        self.engine=create_async_engine(URL.create('postgresql+asyncpg',username='test',database=self.name,port=55432,query={'host':self.socket}))
        self.addAsyncCleanup(self.engine.dispose);self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        slots=patch.object(distribution,'POSITION_READ_SLOTS',asyncio.Semaphore(1));slots.start();self.addCleanup(slots.stop)
        self.tasks=[];self.addAsyncCleanup(self.stop_tasks)

    async def add_game(self,link,white='alice',black='bob',*,mode='blitz',rules='chess',moves=10,setup=None):
        stamp=datetime(2025,3,8,tzinfo=timezone.utc)+timedelta(hours=link%7)
        row=dict(id=link,white=white,black=black,mode=mode,stamp=stamp,wr=1700+link,br=1900+link,result=(1,.5,0)[link%3],rules=rules,moves=moves,setup=setup)
        await self.db.execute('''INSERT INTO game(link,white_player_id,black_player_id,mode_id,played_at,white_elo,black_elo,
            white_result,black_result,white_str_result,black_str_result,time_control,eco,time_elapsed,n_moves,rules,initial_setup)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'recorded','recorded','600','no_eco',120,$10,$11,$12)''',
            link,self.owners[white],self.owners[black],self.modes.get(mode),stamp,row['wr'],row['br'],row['result'],1-row['result'],moves,rules,setup)
        self.records.append(row)

    async def add_occurrence(self,game,move,white,index):
        await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)',game,self.fens[index]['id'],move,white)
        self.occurrences.append(dict(game=game,move=move,white=white,fen=self.fens[index]))

    async def clear_occurrences(self):
        await self.db.execute('DELETE FROM game_fen_association');self.occurrences=[]

    async def stop_tasks(self):
        for task in self.tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)

    def start(self,work):
        task=asyncio.create_task(work);self.tasks.append(task);return task

    async def game_page(self,**changes):
        args=dict(fen_id=self.fens[10]['id'],color='white',modes=['blitz','bullet','rapid'],limit=3,session_factory=self.factory)
        args.update(changes);return await games.explore_position_games(args.pop('player','alice'),**args)

    async def example_page(self,**changes):
        args=dict(bin_id=6,modes=['blitz','bullet','rapid'],snapshot=None,session_factory=self.factory)
        args.update(changes);return await distribution.get_positions(args.pop('player','alice'),None,**args)

    def owned(self,color=None,modes=('blitz','bullet','rapid')):
        return [(g,c) for g in self.records if g['rules']=='chess' and not g['setup'] and g['moves']>0 and g['mode'] in modes
                for c in ('white','black') if g[c]=='alice' and color in (None,c)]

    def example_oracle(self,interval,color=None):
        records=[(g,c,o) for g,c in self.owned(color) for o in self.occurrences if o['game']==g['id']]
        records.sort(key=lambda r:(r[0]['id'],r[1]=='white',r[2]['move'],r[2]['white']),reverse=True)
        # This fixture has fewer than 32 eligible owner-sides and 512 occurrences.
        self.assertLessEqual(len(self.owned(color)),32);self.assertLessEqual(len(records),512)
        found=[];seen=set()
        for g,c,o in records:
            f=o['fen'];score=f['score'];identity=(f['id'],c)
            if score is None or abs(score)>=9000 or f['source']=='tablebase':continue
            score=score if c=='white' else -score
            low,high=interval['lower'],interval['upper']
            if low is not None and (score<low or score==low and not interval['lower_inclusive']):continue
            if high is not None and (score>high or score==high and not interval['upper_inclusive']):continue
            if identity in seen:continue
            seen.add(identity);found.append(dict(fen_id=str(f['id']),player_color=c,score_min=float(score),score_max=float(score),fen=f['fen']))
            if len(found)==10:break
        return found

    async def test_rare_and_common_game_paths_match_records_and_distinct_membership(self):
        for frequency in (0,10000,10001):
            await self.db.execute('UPDATE fen SET n_games=$1 WHERE fen_id=$2',frequency,self.fens[10]['id'])
            for color in ('white','black'):
                for modes in (['blitz'],['blitz','bullet','rapid']):
                    expected=sorted((g for g,c in self.owned(color,modes)),key=lambda g:(g['stamp'],g['id']),reverse=True)
                    actual=[];cursor=None
                    for _ in range(len(expected)+1):
                        page=await self.game_page(color=color,modes=modes,cursor=cursor)
                        self.assertLessEqual(len(page['games']),3);actual.extend(page['games']);cursor=page['pagination']['next_cursor']
                        self.assertEqual(page['pagination']['has_more'],bool(cursor))
                        if not cursor:break
                    else:self.fail('Game pagination did not finish')
                    self.assertEqual([r['game_id'] for r in actual],[g['id'] for g in expected])
                    self.assertEqual(len({r['game_id'] for r in actual}),len(actual))
                    for row,g in zip(actual,expected):
                        self.assertEqual(row['player_color'],color);self.assertEqual(row['opponent_name'],g['black' if color=='white' else 'white'])
                        self.assertEqual(row['player_rating'],g['wr' if color=='white' else 'br'])
                        self.assertEqual(row['result'],{1:'win',.5:'draw',0:'loss'}[g['result'] if color=='white' else 1-g['result']])
        # A position's ply color is not the requested player's color.
        await self.db.execute('DELETE FROM game_fen_association WHERE fen_id=$1 AND is_white',self.fens[10]['id'])
        self.assertTrue((await self.game_page(color='white'))['games'])

    async def test_example_score_bins_colors_and_special_values_match_independent_records(self):
        for index in (0,4,5,6,7,8,12):
            for color in (None,'white','black'):
                with self.subTest(bin=index,color=color):
                    result=await self.example_page(bin_id=index,color=color)
                    self.assertEqual(result['rows'],self.example_oracle(distribution.bins()[index],color))
                    self.assertLessEqual(result['scanned_positions'],512);self.assertEqual(result['sort'],'matching_examples')
                    self.assertTrue(all('appearances' not in row for row in result['rows']))

    async def test_native_cursor_keeps_both_plies_and_move_zero_across_pages(self):
        await self.clear_occurrences();await self.add_game(1000,moves=10)
        for n in range(21):await self.add_occurrence(1000,n//2,n%2==0,23+n)
        expected=[str(self.fens[23+n]['id']) for n in sorted(range(21),key=lambda n:(n//2,n%2==0),reverse=True)]
        first=await self.example_page(color='white',modes=['blitz']);self.assertEqual(len(first['rows']),10)
        self.assertEqual(examples.decode_cursor(first['next_cursor'],first['snapshot']),(1000,True,5,True))
        second=await self.example_page(color='white',modes=['blitz'],cursor=first['next_cursor'])
        self.assertEqual(examples.decode_cursor(second['next_cursor'],second['snapshot']),(1000,True,0,True))
        third=await self.example_page(color='white',modes=['blitz'],cursor=second['next_cursor'])
        self.assertIsNone(third['next_cursor']);self.assertEqual(len(third['rows']),1)
        self.assertEqual([r['fen_id'] for p in (first,second,third) for r in p['rows']],expected)

    async def test_sparse_512_occurrence_page_continues_without_live_recount(self):
        await self.clear_occurrences();await self.add_game(1000,moves=512)
        await self.db.copy_records_to_table('game_fen_association',columns=('game_link','fen_id','n_move','is_white'),
            records=[(1000,self.fens[17]['id'],n,True) for n in range(1,513)])
        await self.add_occurrence(1000,0,True,23);await self.add_occurrence(1000,0,False,24)
        original=reads.example_occurrences;sizes=[]
        async def counted(*args):
            rows=await original(*args);sizes.append(len(rows));return rows
        with patch.object(reads,'example_occurrences',counted),patch.object(reads,'boards',wraps=reads.boards) as boards:
            first=await self.example_page(color='white',modes=['blitz'])
            self.assertEqual(first['rows'],[]);self.assertEqual(first['scanned_positions'],512);boards.assert_not_awaited()
            self.assertEqual(examples.decode_cursor(first['next_cursor'],first['snapshot']),(1000,True,1,True))
            second=await self.example_page(color='white',modes=['blitz'],cursor=first['next_cursor'])
            self.assertEqual([r['fen_id'] for r in second['rows']],[str(self.fens[i]['id']) for i in (23,24)])
            self.assertIsNone(second['next_cursor']);self.assertEqual(sizes,[513,2]);boards.assert_awaited_once()

    async def test_empty_game_batches_advance_without_revisiting_completed_games(self):
        await self.clear_occurrences()
        for link in range(1000,1065):await self.add_game(link)
        cursor=None;boundaries=[]
        for _ in range(5):
            page=await self.example_page(color='white',modes=['blitz'],cursor=cursor)
            self.assertEqual(page['rows'],[]);self.assertEqual(page['scanned_positions'],0)
            cursor=page['next_cursor']
            if not cursor:break
            game,white,move,ply=examples.decode_cursor(cursor,page['snapshot']);boundaries.append(game)
            self.assertTrue(white);self.assertIsNone(move);self.assertIsNone(ply)
        else:self.fail('Empty batches did not terminate')
        self.assertEqual(boundaries,[1033,1001])

    async def test_selfplay_example_sides_are_distinct_and_repeat_fens_deduplicate_per_page(self):
        await self.clear_occurrences()
        for move in (0,1,2):await self.add_occurrence(41,move,True,10)
        result=await self.example_page(modes=['blitz'])
        self.assertEqual([(r['fen_id'],r['player_color']) for r in result['rows']],[(str(self.fens[10]['id']),c) for c in ('white','black')])
        self.assertEqual(result['scanned_positions'],6);self.assertIsNone(result['next_cursor'])

    async def test_game_cursors_bind_owner_fen_color_and_modes(self):
        cursor=(await self.game_page(limit=1))['pagination']['next_cursor'];self.assertIsNotNone(cursor)
        for changes in (dict(player='bob'),dict(fen_id=self.fens[12]['id']),dict(color='black'),dict(modes=['rapid'])):
            with self.assertRaises(ValueError):await self.game_page(cursor=cursor,**changes)
        # Reordered/duplicate mode inputs retain the same selection.
        page=await self.game_page(cursor=cursor,modes=['rapid','bullet','blitz','blitz'])
        self.assertTrue(page['games'])

    async def test_example_cursor_scope_structure_and_old_versions_are_rejected(self):
        scope=examples.scope_key('alice',['blitz'],6,'white')
        valid=[5,scope,41,True,0,False]
        for index,value in ((0,3),(0,4),(2,True),(2,2**63),(3,1),(4,-1),(4,2**31),(4,True),(5,None)):
            broken=valid.copy();broken[index]=value
            token=base64.urlsafe_b64encode(json.dumps(broken).encode()).decode()
            with self.assertRaises(ValueError):await self.example_page(cursor=token,color='white',modes=['blitz'])
        cursor=examples.encode_cursor(scope,41,True,0,False)
        for options in (dict(color='black',modes=['blitz']),dict(color='white',modes=['rapid']),dict(color='white',modes=['blitz'],bin_id=5)):
            with self.assertRaises(ValueError):await self.example_page(cursor=cursor,**options)
        self.assertEqual(examples.decode_cursor(cursor,scope),(41,True,0,False))

    async def test_empty_missing_shell_deleted_and_missing_fen_cases(self):
        self.assertEqual((await self.game_page(player='empty'))['games'],[])
        self.assertEqual((await self.example_page(player='empty'))['rows'],[])
        self.assertEqual((await self.game_page(fen_id=self.fens[-1]['id']))['games'],[])
        with self.assertRaises(LookupError):await self.game_page(fen_id=2**62)
        await self.db.execute("UPDATE player SET deleted_at=now() WHERE player_name='carol'")
        for work in (self.game_page,self.example_page):
            for player in ('absent','shell','carol'):
                with self.assertRaises(LookupError):await work(player=player)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_invalid_game_filters_reject_before_acquisition(self):
        factory=AsyncMock()
        for options in (dict(fen_id=True),dict(fen_id=0),dict(color='red'),dict(modes=['daily']),dict(modes=[]),dict(limit=31)):
            with self.assertRaises(ValueError):await self.game_page(session_factory=factory,**options)
        factory.assert_not_called()
        with self.assertRaises(ValueError):await self.example_page(color='red')
        self.assertEqual(distribution.POSITION_READ_SLOTS._value,1)

    async def test_profile_and_membership_use_one_snapshot_during_concurrent_changes(self):
        original=reads.fen_frequency;expected=(await self.game_page(limit=30))['games']
        async def concurrent(session,fen):
            value=await original(session,fen)
            await self.db.execute('DELETE FROM game_fen_association WHERE fen_id=$1',fen)
            return value
        with patch.object(reads,'fen_frequency',concurrent):actual=await self.game_page(limit=30)
        self.assertEqual(actual['games'],expected);self.assertEqual((await self.game_page())['games'],[])

    async def test_readonly_failures_and_timeouts_propagate_with_slots_released(self):
        async def write(session,*args):
            self.assertEqual(await session.scalar(text('SHOW transaction_read_only')),'on')
            await session.execute(text('DELETE FROM fen'))
        async def slow(session,*args):
            await session.execute(text("SET LOCAL statement_timeout='20ms'"));await session.execute(text('SELECT pg_sleep(0.2)'))
        for method,work in (('position_games',self.game_page),('example_games',self.example_page)):
            with patch.object(reads,method,write),self.assertRaises(DBAPIError):await work()
            expected=distribution.ScoreDistributionBusy if method=='example_games' else DBAPIError
            with patch.object(reads,method,slow),self.assertRaises(expected):await work()
            self.assertEqual(self.engine.pool.checkedout(),0);self.assertEqual(distribution.POSITION_READ_SLOTS._value,1)

    async def test_repeated_cancellation_finishes_owned_read_cleanup(self):
        for method,work in (('position_games',self.game_page),('example_games',self.example_page)):
            entered,rolling,release,closed=(asyncio.Event() for _ in range(4))
            async def pause(*args):entered.set();await asyncio.Future()
            def factory():
                session=self.factory();rollback,close=session.rollback,session.close
                async def delayed():rolling.set();await release.wait();await rollback()
                async def closing():
                    try:await close()
                    finally:closed.set()
                session.rollback=delayed;session.close=closing;return session
            with patch.object(reads,method,pause):
                task=self.start(work(session_factory=factory))
                try:
                    await asyncio.wait_for(entered.wait(),2);task.cancel();await asyncio.wait_for(rolling.wait(),2)
                    for _ in range(2):task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
                finally:
                    release.set()
                    if not task.done():task.cancel()
                    await asyncio.gather(task,return_exceptions=True)
            self.assertTrue(task.cancelled());self.assertTrue(closed.is_set());self.assertEqual(self.engine.pool.checkedout(),0)
            self.assertEqual(distribution.POSITION_READ_SLOTS._value,1)

    async def test_canonical_constraints_replace_null_dates_and_mutable_side_rows(self):
        import asyncpg
        with self.assertRaises(asyncpg.NotNullViolationError):await self.db.execute('UPDATE game SET played_at=NULL WHERE link=41')
        with self.assertRaises(asyncpg.ObjectNotInPrerequisiteStateError):await self.db.execute('DELETE FROM game_player WHERE game_link=41')
        with self.assertRaises(asyncpg.CheckViolationError):await self.db.execute('INSERT INTO game_fen_association VALUES(60,$1,0,true)',self.fens[10]['id'])
        with self.assertRaises(asyncpg.UniqueViolationError):await self.db.execute('INSERT INTO game_fen_association VALUES(41,$1,0,true)',self.fens[10]['id'])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_player WHERE game_link=41'),2)

    async def test_actual_routes_return_game_membership_and_bounded_examples(self):
        app=FastAPI();app.include_router(players.router,prefix='/players')
        async def redis():return None
        app.dependency_overrides[players.get_redis_pool]=redis
        with patch.object(games,'AsyncDBSession',self.factory),patch.object(distribution,'AsyncDBSession',self.factory):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                response=await client.post('/players/alice/analysis/measures/explore-games',json=dict(scope='position',fen_id=self.fens[10]['id'],color='white',modes=['blitz'],analyzed_only=False,limit=3))
                self.assertEqual(response.status_code,200);self.assertEqual(response.json()['games'],(await self.game_page(modes=['blitz']))['games'])
                response=await client.post('/players/alice/analysis/measures/score-distribution/positions',json=dict(bin_id=6,modes=['blitz','bullet','rapid'],color='white',snapshot=None))
                self.assertEqual(response.status_code,200);self.assertEqual(response.json()['rows'],self.example_oracle(distribution.bins()[6],'white'))
                self.assertEqual(response.headers['Cache-Control'],'private, no-store')
                response=await client.post('/players/absent/analysis/measures/explore-games',json=dict(scope='position',fen_id=self.fens[10]['id'],color='white',modes=['blitz'],analyzed_only=False))
                self.assertEqual(response.status_code,404)
