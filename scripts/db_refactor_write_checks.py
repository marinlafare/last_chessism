"""Rollback-only normalized writer/summary checks on the private restored data."""
from collections import Counter
from datetime import datetime, timezone
import math
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from scripts.db_refactor_candidate import C,A,V
from scripts.db_refactor_write_store import CandidateStore
from scripts.db_refactor_salience_bridge import calculate

PLAYER = '__refactor_probe_white__'
OTHER = '__refactor_probe_black__'
LINKS = [-910000000001,-910000000002,-910000000003,-910000000004]


def fixture_bundle():
    from chessism_api.operations.ingestion_pipeline.game_importer import _prepare_game_bundle
    rows=[]
    sequences=[(['Nf3','Ng1','Nf3','e4'],['Nf6','Ng8','Nf6','e5']),
               (['Nf3','Ng1','d4','c4'],['Nf6','Ng8','d5','e6']),
               (['e4','Nf3','Bb5','Ba4'],['e5','Nc6','a6','Nf6']),
               (['e4','Nf3','Bb5','Ba4'],['e5','Nc6','a6','Nf6'])]
    for i,(white,black) in enumerate(sequences):
        when=datetime(2025,1,i+1,tzinfo=timezone.utc)
        row={'link':LINKS[i], 'white':PLAYER if i!=2 else OTHER,'black':OTHER if i!=2 else PLAYER,
            'year':when.year,'month':when.month,'day':when.day,'hour':0,'minute':0,'second':0,
            'white_elo':2500,'black_elo':2400,'white_result':1.0,'black_result':0.0,
            'white_str_result':'win','black_str_result':'resigned','time_control':'180',
            'mode':'blitz','played_at':when,'avg_elo':2450.0,
            'eco':'https://www.chess.com/openings/Rehearsal-Only','time_elapsed':100.0,
            'n_moves':4,'fens_done':False,'rules':'chess' if i!=3 else 'chess960',
            'initial_setup':None if i!=3 else 'preserve-the-original-starting-position',
            'moves_data':{'link':LINKS[i],'white_moves':white,'black_moves':black,
                'white_time_left':[179.,178.,177.,176.],'black_time_left':[179.,178.,177.,176.],
                'white_reaction_times':[1.]*4,'black_reaction_times':[1.]*4}}
        rows.append(row)
    return _prepare_game_bundle(rows)


async def run_checks(socket, pgdata, database='chessism_db', publish=None):
    from chessism_api.operations.ingestion_pipeline.fen_core import process_single_game_sync
    from chessism_api.operations.analysis import _format_engine_results
    from chessism_api.operations.fen_results import stage_fen_results
    engine=create_async_engine(URL.create('postgresql+asyncpg',username='chessism_user',
        database=database,port=55432,query={'host':str(socket)}))
    report=[]
    async def passed(name, **detail):
        report.append({'check':name,'passed':True,**detail})
        if publish:
            await publish('checking_candidate_writes',current_check=name,checks_completed=len(report))
    try:
        async with engine.connect() as connection:
            outer=await connection.begin()
            # SQLAlchemy starts transactions lazily. Force the actual BEGIN
            # before borrowing the native driver, otherwise its writes autocommit.
            await connection.execute(text('SELECT 1'))
            session=AsyncSession(bind=connection,expire_on_commit=False)
            conn=(await connection.get_raw_connection()).driver_connection
            try:
                store=await CandidateStore.open(conn,pgdata)
                if await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM {C}.game WHERE link=ANY($1::bigint[]))',LINKS):
                    raise RuntimeError('Probe identities already exist; refusing to overwrite')
                await store.prepare_probe_moves()
                bundle=fixture_bundle()
                assert len(bundle['games'])==4 and len(bundle['moves'])==16
                # Start with two games; the third will exercise incremental salience.
                initial=[g for g in bundle['games'] if g['link']!=LINKS[2]]
                inserted=await store.ingest(initial,bundle['moves'],bundle['game_openings'])
                assert inserted=={'standard':LINKS[:2],'archived':[LINKS[3]]},inserted
                assert (await store.ingest(initial,bundle['moves'],bundle['game_openings']))=={'standard':[],'archived':[]}
                assert await conn.fetchval(f'SELECT count(*) FROM {C}.game_player WHERE game_link=ANY($1::bigint[])',LINKS)==4
                archive=await conn.fetchrow(f'SELECT rules,initial_setup,eco FROM {A}.game WHERE link=$1',LINKS[3])
                assert archive['rules']=='chess960' and archive['initial_setup']=='preserve-the-original-starting-position'
                assert archive['eco'].startswith('https://www.chess.com/openings/')
                assert await conn.fetchval(f'SELECT count(*) FROM {A}.moves WHERE link=$1',LINKS[3])==4
                assert not await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM {C}.game WHERE link=$1)',LINKS[3])
                assert await conn.fetchval(f'SELECT eco FROM {C}.game WHERE link=$1',LINKS[0])=='Rehearsal-Only'
                await passed('ingestion_retry_numeric_ownership_openings_and_variant_archive')
                # A failed bundle must roll back its earlier inserts as well.
                bad_link=-910000000005
                try:
                    async with connection.begin_nested():
                        good=dict(bundle['games'][0],link=bad_link)
                        bad=dict(good,link=bad_link-1,eco='unrecognized-format')
                        await store.ingest([good,bad],[],[])
                except ValueError:
                    pass
                else:
                    raise AssertionError('Expected invalid ECO rejection')
                assert not await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM {C}.game WHERE link=$1)',bad_link)
                await passed('failed_bundle_rolls_back_all_new_game_rows')

                associations={}
                async def extract(link):
                    rows,errors=process_single_game_sync((link,[m for m in bundle['moves'] if m['link']==link]))
                    assert not errors,errors
                    associations[link]=rows
                    await store.associations(rows)
                    await store.associations(rows)
                    assert await conn.fetchval(f'SELECT count(*) FROM {C}.game_fen_association WHERE game_link=$1',link)==len(rows)
                for link in LINKS[:2]:
                    await extract(link)
                await passed('shared_fen_extractor_numeric_associations_and_duplicate_retry')

                async def check_salience():
                    stored=await conn.fetch(f'''SELECT gp.game_link,gp.is_white,s.* FROM {C}.game_player_salience s
                        JOIN {C}.game_player gp ON gp.id=s.game_player_id JOIN {C}.player p ON p.id=gp.player_id
                        WHERE p.player_name=$1 ORDER BY gp.game_link''',PLAYER)
                    frequencies=Counter((link!=LINKS[2],fen) for link,rows in associations.items()
                                        for fen in {r['fen_fen'] for r in rows})
                    for saved in stored:
                        rows=associations[saved['game_link']]
                        seen=Counter(); numerator=denominator=0.
                        for row in rows:
                            fen=row['fen_fen']; seen[fen]+=1
                            ply=row['n_move']*2-(row['move_color']=='white')
                            weight=.25+.75*min(ply/16.,1.)
                            denominator+=weight
                            numerator+=weight/(seen[fen]*frequencies[(saved['is_white'],fen)])
                        assert math.isclose(saved['salience'],numerator/denominator,rel_tol=1e-12,abs_tol=1e-12)
                        assert saved['position_occurrence_count']==len(rows)
                        assert saved['unique_position_count']==len(seen)
                    assert len(stored)==len(associations)
                    return [dict(r) for r in stored]
                full=await calculate(session,store,PLAYER)
                await check_salience()
                await store.ingest([bundle['games'][2]],bundle['moves'],bundle['game_openings'])
                await extract(LINKS[2])
                incremental=await calculate(session,store,PLAYER,incremental=True)
                prior=await check_salience()
                await calculate(session,store,PLAYER)
                rebuilt=await check_salience()
                for left,right in zip(prior,rebuilt):
                    for field in ('salience','weighted_numerator','depth_weight_sum'):
                        assert math.isclose(left[field],right[field],rel_tol=1e-12,abs_tol=1e-12)
                repeated=await calculate(session,store,PLAYER,incremental=True)
                assert repeated['source_game_count']==incremental['source_game_count']==3
                await check_salience()
                await passed('full_incremental_salience_and_replay_match_independent_occurrence_math',
                             full_games=full['source_game_count'],incremental_games=incremental['source_game_count'])

                fens=sorted({r['fen_fen'] for rows in associations.values() for r in rows})
                # Only in this rollback-only transaction on private restored PG.
                await conn.execute('UPDATE public.fen SET score=NULL WHERE fen=ANY($1::varchar[])',fens)
                await store.coverage(LINKS[:3])
                assert await conn.fetchval(f'SELECT sum(analyzed_positions) FROM {C}.game_analysis_coverage WHERE link=ANY($1::bigint[])',LINKS[:3])==0
                output=[{'fen':fen,'is_valid':True,'analysis':[
                    {'score':float([0,50,-150,300,10000][i%5]),'pv':['e2e4'],'wdl':[200,700,100]},
                    {'score':10.,'pv':['d2d4']},{'score':5.,'pv':['g1f3']}]} for i,fen in enumerate(fens)]
                formatted,continuations=_format_engine_results(output)
                await stage_fen_results(session,formatted,continuations)
                await stage_fen_results(session,formatted,continuations)
                await store.coverage(LINKS[:3]); await store.coverage(LINKS[:3])
                coverage=await conn.fetch(f'SELECT * FROM {C}.game_analysis_coverage WHERE link=ANY($1::bigint[])',LINKS[:3])
                assert len(coverage)==3 and all(r['is_fully_analyzed'] and r['unscored_positions']==0 for r in coverage)
                assert all(sum(r[k+'_positions'] for k in ('equal','small','clear','decisive','mate'))==r['analyzed_positions'] for r in coverage)
                assert await conn.fetchval('''SELECT count(*) FROM public.fen_continuation c JOIN public.fen f
                    ON f.fen_id=c.fen_id WHERE f.fen=ANY($1::varchar[])''',fens)==2*len(fens)
                await passed('shared_local_cloud_result_writer_continuations_zero_scores_and_coverage_retry',fens=len(fens))
                try:
                    async with connection.begin_nested():
                        await conn.execute(f'''INSERT INTO {C}.game_fen_association(game_link,fen_id,n_move,move_color)
                            VALUES($1,-999999999,100,'white')''',LINKS[0])
                except Exception as error:
                    if getattr(error,'sqlstate',None)!='23503':
                        raise
                else:
                    raise AssertionError('Missing FEN foreign key was accepted')
                await passed('invalid_numeric_reference_rejected')
            finally:
                await session.close()
                await outer.rollback()
            # Fresh transaction confirms all candidate/archive probe writes vanished.
            assert not await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM {C}.game WHERE link=ANY($1::bigint[]))',LINKS)
            assert not await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM {A}.game WHERE link=ANY($1::bigint[]))',LINKS)
            await passed('outer_rollback_leaves_no_probe_games')
    finally:
        await engine.dispose()
    return {'checks':report,'application_deployed':False,'write_scope':'rollback-only private candidate',
            'limitations':['Candidate ingestion/coverage adapters are not connected to application jobs.',
                'Salience uses a player-scoped temporary sink adapter; bulk throughput is not accepted.',
                'No global homepage/engine-summary refresh or UI workflow acceptance is claimed.']}
