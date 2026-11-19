"""Native CUDA transport/publication checks on private PostgreSQL, using array test doubles.

These checks exercise real SQL/COPY/transactions. NumPy stands in for the device;
they do not claim real CUDA execution or change the production CUDA requirement.
"""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
import uuid
from unittest.mock import patch

import asyncpg
import chess
import numpy as np
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from chessism_api.database.postgresql import canonical
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.operations.canonical_blunders import registry, source, compute, publication, readers
from chessism_api.operations.canonical_blunders.benchmark_store import Scratch
from chessism_api.operations.cuda_summaries import (runs, score_runs, shared_source, score_compute,
    accuracy_store, accuracy_compute, score_publication, salience_store, salience_source,
    salience_compute, salience_publication, frequency_source, frequency_store, frequency_worker,
    frequency_compute, metadata_store, metadata_source, metadata_publication, behavior_compute, ending_compute)
from tests.canonical_postgres_case import CanonicalDatabaseTestCase
from tests.test_canonical_blunders import report


class ArrayDevice:
    """Explicit host test double for publication's existing reduction calls."""
    __name__ = 'cupy'
    asnumpy = staticmethod(np.asarray)
    def __getattr__(self, name): return getattr(np, name)


DEVICE = ArrayDevice()
ARRAY = np
if os.environ.get('CHESSISM_TEST_CUDA') == '1':
    from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
    ARRAY, hardware = cuda_backend(0)
    if hardware['name'] != 'NVIDIA GeForce RTX 4060':
        raise RuntimeError('Isolated CUDA verification requires the selected RTX 4060')
    DEVICE = ARRAY
    print('REAL CUDA verification: '+json.dumps(hardware), flush=True)


class CanonicalCudaTests(CanonicalDatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await canonical.initialize(self.db)
        self.engine = create_async_engine(URL.create('postgresql+asyncpg', username='test',
            database=self.name, port=55432, query={'host':self.socket}))
        self.addAsyncCleanup(self.engine.dispose)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.people = {p:await self.db.fetchval('INSERT INTO player(player_name,joined) VALUES($1,1) RETURNING id',p)
                       for p in ('alice','bob')}
        self.mode = await self.db.fetchval("INSERT INTO game_mode(name) VALUES('blitz') RETURNING id")
        self.links = (1,2**63-1)
        self.fens = {}
        for link, black in ((self.links[0],'bob'),(self.links[1],'alice')):
            await self.add_game(link,black)
        for link, moves, rules in ((3,0,'chess'),(4,1,'chess960')):
            await self.add_game(link,'bob',moves=moves,rules=rules)
        await self.db.execute('DELETE FROM fen_summary_refresh')
        self.model,self.config,self.evidence = registry.freeze(report(),registry.ACK)
        await self.db.execute('''INSERT INTO canonical_blunder_model(id,source_run,config,evidence,approved_by,approval)
            VALUES($1,'synthetic',$2::jsonb,$3::jsonb,'test','provisional_user_approved')''',
            self.model,json.dumps(self.config),json.dumps(self.evidence))
        await self.db.execute('UPDATE cuda_summary_control SET manual=true,scores=true,accuracy=true')
        async with write_transaction(self.factory) as s:await registry.select_target(s,self.model)

    async def add_game(self,link,black,moves=11,rules='chess'):
        await self.db.execute('''INSERT INTO game(link,white_player_id,black_player_id,mode_id,played_at,
            white_elo,black_elo,white_result,black_result,white_str_result,black_str_result,
            time_control,eco,time_elapsed,n_moves,rules,fens_done)
            VALUES($1,$2,$3,$4,'2026-01-01 12:00Z',1500,1600,1,0,'win','resigned','600','no_eco',120,$5,$6,$7)''',
            link,self.people['alice'],self.people[black],self.mode,moves,rules,rules=='chess' and moves>0)
        if not moves or rules!='chess': return
        board=chess.Board()
        for ply in range(1,moves*2+1):
            san=('Nf3','Nf6','Ng1','Ng8')[(ply-1)%4];board.push_san(san)
            fen=' '.join(board.fen().split()[:4])
            if fen not in self.fens:
                self.fens[fen]=await self.db.fetchval('''INSERT INTO fen(fen,score,analysis_source,analyzed_at)
                    VALUES($1,$2,'stockfish',now()) RETURNING fen_id''',fen,(len(self.fens)-2)*100)
            await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,$3,$4)',link,self.fens[fen],(ply+1)//2,ply%2==1)
        await self.db.executemany('INSERT INTO moves VALUES($1,$2,$3,$4,1,1,590,590)',
            [(link,n,'Nf3' if n%2 else 'Ng1','Nf6' if n%2 else 'Ng8') for n in range(1,moves+1)])
        await self.db.execute('''INSERT INTO game_analysis_coverage VALUES($1,$2,$2,0,true,$2,0,0,0,0,now())''',link,moves*2)
        for owner in {self.people['alice'],self.people[black]}:
            await self.db.execute("INSERT INTO player_salience_summary(player_id,status) VALUES($1,'stale') ON CONFLICT(player_id) DO UPDATE SET status='stale'",owner)

    async def request(self,**flags):
        for flag,value in flags.items():
            assert flag in ('salience','metadata','frequencies')
            await self.db.execute(f'UPDATE cuda_summary_control SET {flag}=$1',value)
        async with write_transaction(self.factory) as s:
            run=await runs.request_run(s,'synthetic')
            while await score_runs.plan(s,run): pass
            return await runs.next_batch(s)

    async def input(self,run):
        async with read_transaction(self.factory) as s:return await shared_source.load(s,run)

    async def stage(self,run):
        data=await self.input(run)
        cfg,items,games,owners,raw,eligible,unsupported,transitions=data
        records,info=compute.infer(ARRAY,transitions,eligible,cfg,require_cuda=False)
        records+=unsupported
        parts=score_compute.calculate(ARRAY,raw,games,owners,require_cuda=False)
        accuracy=accuracy_compute.calculate(ARRAY,raw,games,require_cuda=False)
        async with write_transaction(self.factory) as s:
            await publication.publish(s,self.model,[i for i in items if i['blunders']],records,info,refresh_snapshots=False)
            await accuracy_store.stage(s,run,accuracy)
            await score_runs.stage(s,run,items,parts,{})
        return data,records,accuracy

    async def test_shared_capture_keeps_two_self_play_sides_and_native_maximum_identity(self):
        run=await self.request();data=await self.input(run)
        self.assertEqual([g['game_link'] for g in data[2]],list(self.links))
        self.assertEqual(len(data[3]),4)
        self.assertEqual({o['is_white'] for o in data[3] if o['game_link']==self.links[1]},{True,False})
        self.assertEqual(len(data[4]),44)
        async with read_transaction(self.factory) as s:
            await source.prepare(s);scalar=await source.load(s)
        np.testing.assert_array_equal(data[7],scalar[4])
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_atomic_score_accuracy_and_blunder_publication_and_saved_readers(self):
        run=await self.request();data,records,accuracy=await self.stage(run)
        for _ in range(3):await score_publication.finalize(self.factory,run,DEVICE)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_player_engine_summary'),4)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cuda_accuracy_game'),4)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cuda_summary_pending'),0)
        self.assertEqual(await self.db.fetchval('SELECT status FROM cuda_summary_run'),'complete')
        from chessism_api.operations import player_stockfish_summary as scores, player_accuracy_distribution as charts
        async with read_transaction(self.factory) as s:
            saved=await scores.read_saved(s,'alice');blunders=await readers.saved(s,'alice')
            actual=(await s.execute(text(charts.summary_rows_sql(s,'accuracy')),{'player':'alice','modes':['blitz']})).mappings().all()
        self.assertEqual(len(actual),3);self.assertFalse(saved['stale']);self.assertFalse(blunders['stale'])
        self.assertEqual(saved['by_mode']['blitz']['eligible_games'],3)
        self.assertEqual(saved['by_mode']['blitz']['cp_positions'],66)
        expected={(r['game_link'],r['is_white']):r['payload']['game_efficiency'] for r in accuracy}
        for row in await self.db.fetch('SELECT game_link,is_white,game_efficiency FROM game_player_engine_summary'):
            self.assertAlmostEqual(row['game_efficiency'],expected[row['game_link'],row['is_white']],places=12)

    async def test_changed_revision_rejects_old_results_without_acknowledging_new_work(self):
        run=await self.request();data=await self.input(run)
        cfg,items,_,_,_,eligible,unsupported,transitions=data
        records,info=compute.infer(ARRAY,transitions,eligible,cfg,require_cuda=False);records+=unsupported
        await self.db.execute('UPDATE game SET white_elo=white_elo+1 WHERE link=$1',self.links[0])
        current=await self.db.fetchval('SELECT revision FROM canonical_blunder_dirty WHERE game_link=$1',self.links[0])
        async with write_transaction(self.factory) as s:
            result=await publication.publish(s,self.model,items,records,info)
        self.assertEqual(result['written'],1);self.assertEqual(result['remaining'],1)
        self.assertEqual(await self.db.fetchval('SELECT revision FROM canonical_blunder_dirty WHERE game_link=$1',self.links[0]),current)
        await score_publication.finalize(self.factory,run,DEVICE)
        self.assertEqual(await self.db.fetchval("SELECT status FROM cuda_score_scope WHERE player_name='alice'"),'superseded')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_stockfish_snapshot'),0)

    async def test_failed_publication_rolls_back_output_and_acknowledgment(self):
        run=await self.request();data=await self.input(run)
        cfg,items,_,_,_,eligible,unsupported,transitions=data
        records,info=compute.infer(ARRAY,transitions,eligible,cfg,require_cuda=False);records+=unsupported
        before=await self.db.fetch('SELECT * FROM canonical_blunder_dirty ORDER BY game_link')
        with self.assertRaisesRegex(RuntimeError,'after publication'):
            async with write_transaction(self.factory) as s:
                await publication.publish(s,self.model,items,records,info)
                raise RuntimeError('after publication')
        self.assertEqual(await self.db.fetch('SELECT * FROM canonical_blunder_dirty ORDER BY game_link'),before)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM canonical_blunder_game'),0)
        self.assertIsNone(await self.db.fetchval('SELECT active_model FROM canonical_blunder_control'))
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_frequency_copy_paginates_every_native_occurrence_without_overflow(self):
        cursor=[0,0,0,0];parts=[]
        with patch.object(frequency_source,'GAME_LIMIT',1),patch.object(frequency_source,'ROW_LIMIT',7):
            for _ in range(30):
                async with read_transaction(self.factory) as s:
                    found=await frequency_source.read(s,list(self.people.values()),cursor)
                if found is None:break
                raw,next_cursor=found;self.assertNotEqual(next_cursor,cursor);cursor=next_cursor;parts.append(raw)
            else:self.fail('Native keyset did not terminate')
        rows=np.concatenate(parts);self.assertEqual(len(rows),88)
        actual=Counter(map(tuple,rows.tolist()))
        expected=Counter((r['player_id'],int(r['is_white']),r['fen_id'],r['mode_id']) for r in await self.db.fetch('''
            SELECT gp.player_id,gp.is_white,a.fen_id,g.mode_id FROM game_player gp
            JOIN game g ON g.link=gp.game_link JOIN game_fen_association a ON a.game_link=g.link
            WHERE g.is_standard AND g.n_moves>0'''))
        self.assertEqual(actual,expected)
        counted=frequency_compute.calculate(ARRAY,rows,require_cuda=False)
        self.assertEqual({tuple(r[:4]):int(r[4]) for r in counted},dict(expected))

    async def test_salience_run_indexes_map_to_native_sides_and_publish_atomically(self):
        run=await self.request(salience=True);parts=[];cursor=0
        while True:
            async with read_transaction(self.factory) as s:found=await salience_source.read(s,run['id'],cursor,limit=1)
            if found is None:break
            raw,cursor,_=found;parts.append(raw)
        raw=np.concatenate(parts);self.assertEqual(set(raw[:,1]),{1,2,3,4})
        pieces,frequencies=salience_compute.calculate(ARRAY,raw,require_cuda=False)
        games=salience_compute.reduce_games(ARRAY,pieces,require_cuda=False)
        async with write_transaction(self.factory) as s:
            await salience_publication.stage(s,run,0,pieces,frequencies)
            await salience_publication.stage_games(s,run,games)
        while await salience_publication.publish(self.factory,run,DEVICE): pass
        rows=await self.db.fetch('SELECT game_link,is_white,salience FROM game_player_salience ORDER BY game_link,is_white')
        self.assertEqual(len(rows),4);self.assertEqual({r['game_link'] for r in rows},set(self.links))
        self.assertTrue(all(0<r['salience']<=1 for r in rows))
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cuda_salience_scope WHERE status='published'"),2)

    async def test_metadata_capture_uses_numeric_player_state_and_composite_cursor(self):
        run=await self.request(metadata=True);cursor=[0,False];rows=[]
        while True:
            async with read_transaction(self.factory) as s:page=await metadata_source.read(s,run['id'],cursor,limit=1)
            if not page:break
            rows+=page;cursor=page[-1]['cursor']
        self.assertEqual(len(rows),4);self.assertEqual(cursor,[self.links[1],True])
        profiles=[json.loads(r['profile']) for r in await self.db.fetch('SELECT profile FROM cuda_metadata_scope ORDER BY player_name')]
        raw,dictionaries=behavior_compute.encode(rows,profiles);extra=ending_compute.encode(rows)
        behavior=behavior_compute.partitions(ARRAY,raw,dictionaries,profiles,require_cuda=False)
        endings=ending_compute.partitions(ARRAY,raw,extra,dictionaries,profiles,require_cuda=False)
        async with write_transaction(self.factory) as s:await metadata_publication.stage(s,run,0,behavior,endings)
        await metadata_publication.reduce(self.factory,run,DEVICE)
        while await metadata_publication.publish(self.factory,run): pass
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_analytics_snapshot'),14)
        self.assertEqual(await self.db.fetchval("SELECT count(*) FROM cuda_metadata_scope WHERE status='published'"),2)
        self.assertFalse(await self.db.fetchval('SELECT EXISTS(SELECT 1 FROM player_analytics_state WHERE revision<>published_revision)'))

    async def test_benchmark_publisher_is_confined_to_owned_scratch_tables(self):
        run=await self.request();data=await self.input(run)
        cfg,items,_,_,_,eligible,unsupported,transitions=data
        records,_=compute.infer(ARRAY,transitions,eligible,cfg,require_cuda=False);records+=unsupported
        scratch=Scratch(uuid.uuid4(),factory=self.factory)
        try:
            await scratch.create(self.model,items,list(self.people))
            result,_=await scratch.write(self.model,items,records)
            self.assertEqual(result['written'],2);self.assertTrue(await scratch.snapshots(self.model))
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM canonical_blunder_game'),0)
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM canonical_blunder_dirty'),2)
        finally:await scratch.cleanup()
        self.assertIsNone(await self.db.fetchval('SELECT oid FROM pg_namespace WHERE nspname=$1',scratch.name))

    async def test_native_reader_game_detail_explorer_and_salience_report(self):
        from chessism_api.operations import player_game_explorer as explorer, player_move_salience as moves, player_salience as salience
        from chessism_api.operations import player_salience_calculation as cpu
        run=await self.request();await self.stage(run)
        for _ in range(3):await score_publication.finalize(self.factory,run,DEVICE)
        await self.db.execute('UPDATE cuda_summary_control SET manual=false')
        with patch.object(cpu,'AsyncDBSession',self.factory):
            result=await cpu.calculate_player_salience('alice')
        self.assertEqual(result['source_game_count'],3)
        with patch.object(explorer,'AsyncDBSession',self.factory):
            game=await explorer.get_game_score(self.links[1])
            page=await explorer.explore_player_games('alice',scope_kind='date',target_date='2026-01-01',modes=['blitz'],timezone_name='UTC')
        self.assertEqual(game['game']['game_id'],self.links[1])
        self.assertEqual(len(game['moves']),22)
        self.assertEqual({s['player_color'] for s in game['player_summaries']},{'white','black'})
        self.assertEqual(len(page['games']),3)
        with patch.object(salience,'AsyncDBSession',self.factory):
            report=await salience.get_player_salience_report('alice')
        self.assertEqual(report['distribution']['games'],3)
        with patch.object(moves,'AsyncDBSession',self.factory):
            detail=await moves.get_player_game_salience('alice',self.links[1])
        self.assertEqual(len(detail['moves']),22)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_cpu_incremental_salience_matches_full_and_occurrence_reference(self):
        from chessism_api.operations import player_salience_calculation as cpu
        await self.db.execute('UPDATE cuda_summary_control SET manual=false')
        await cpu.calculate(self.factory,'alice')
        await self.add_game(5,'bob')
        await self.db.execute('INSERT INTO player_salience_pending_game VALUES(5,true) ON CONFLICT DO NOTHING')
        async with write_transaction(self.factory) as s:stats=await cpu._incremental_update(s,'alice')
        incremental=await self.db.fetch('SELECT * FROM game_player_salience ORDER BY game_link,is_white')
        self.assertEqual(stats['source_game_count'],4)
        async with write_transaction(self.factory) as s:await cpu._full_rebuild(s,'alice')
        full=await self.db.fetch('SELECT * FROM game_player_salience ORDER BY game_link,is_white')
        for old,new in zip(incremental,full):
            self.assertEqual((old['game_link'],old['is_white']),(new['game_link'],new['is_white']))
            self.assertAlmostEqual(old['salience'],new['salience'],places=12)
        frequencies=Counter((r['is_white'],r['fen_id']) for r in await self.db.fetch('''
            SELECT DISTINCT gp.game_link,gp.is_white,a.fen_id FROM game_player gp
            JOIN game_fen_association a ON a.game_link=gp.game_link
            WHERE gp.player_id=$1 AND a.n_move>0''',self.people['alice']))
        for row in full:
            seen=Counter();numerator=denominator=0
            for occurrence in await self.db.fetch('SELECT * FROM game_fen_association WHERE game_link=$1 AND n_move>0 ORDER BY n_move,is_white DESC',row['game_link']):
                fen=occurrence['fen_id'];seen[fen]+=1
                ply=occurrence['n_move']*2-int(occurrence['is_white']);weight=.25+.75*min(ply/16,1)
                numerator+=weight/(frequencies[row['is_white'],fen]*seen[fen]);denominator+=weight
            self.assertAlmostEqual(row['salience'],numerator/denominator,places=12)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM player_salience_pending_game WHERE game_link=5 AND is_white'),0)

    async def test_cancelled_refresh_preserves_committed_salience_and_closes_session(self):
        import asyncio
        from chessism_api.operations import player_salience_calculation as cpu
        await self.db.execute('UPDATE cuda_summary_control SET manual=false')
        await cpu.calculate(self.factory,'alice')
        before=await self.db.fetch('SELECT * FROM game_player_salience ORDER BY game_link,is_white')
        entered=asyncio.Event()
        async def interrupted(session,player):
            await session.execute(text('DELETE FROM game_player_salience'))
            entered.set();await asyncio.Event().wait()
        with patch.object(cpu,'_full_rebuild',interrupted):
            task=asyncio.create_task(cpu.calculate(self.factory,'alice'))
            await asyncio.wait_for(entered.wait(),3);task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(await self.db.fetch('SELECT * FROM game_player_salience ORDER BY game_link,is_white'),before)
        self.assertEqual(self.engine.pool.checkedout(),0)

    async def test_frequency_reader_excludes_mate_encoded_integer_extremes_without_overflow(self):
        from chessism_api.operations.player_frequency_position_page import ranked_sql
        fen=next(iter(self.fens.values()))
        await self.db.execute('UPDATE fen SET score=$1 WHERE fen_id=$2',-(2**31),fen)
        await self.db.execute('INSERT INTO player_fen_frequency(player_id,is_white,fen_id,mode_id,appearances) VALUES($1,false,$2,$3,2)',self.people['alice'],fen,self.mode)
        params=dict(owner=self.people['alice'],modes=['blitz'],color=None,lower=-150,upper=-50,
            after_count=None,after_fen=None,after_white=None,limit=50)
        async with read_transaction(self.factory) as session:
            result=await session.execute(ranked_sql(dict(lower=-150,upper=-50,lower_inclusive=True,upper_inclusive=False)),params)
            self.assertEqual(result.all(),[])
