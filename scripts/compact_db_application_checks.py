"""Real v3 application writes against an empty clone of the candidate schema.

Factories use a private Unix socket; no engine service, Google call or Redis
queue is started. Synthetic legal analysis results exercise persistence only.
"""
import asyncio
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY

LAYOUT=RelationalStorage('public','chessism_archive','chessism_reads','relational_v3')


def factory_for(socket, database, *, readonly=False):
    settings={'search_path':LAYOUT.search_path,'statement_timeout':'60000','application_name':'compact-acceptance'}
    if readonly:
        settings['default_transaction_read_only']='on'
    url=URL.create('postgresql+asyncpg',username='chessism_user',host=str(socket),port=55432,database=database)
    engine=create_async_engine(url,connect_args={'server_settings':settings})
    return engine,async_sessionmaker(engine,expire_on_commit=False,info={SESSION_KEY:LAYOUT})


async def application_checks(conn, socket, database, passed):
    if not database.startswith('compact_acceptance_'):
        raise RuntimeError('Application probes require the owned empty schema clone')
    from scripts.db_refactor_write_checks import fixture_bundle,PLAYER,OTHER,LINKS
    from chessism_api.database import ask_db,db_interface
    from chessism_api.database.db_interface import DBInterface
    from chessism_api.database.models import Fen,GameFenAssociation,PlayerStats,CloudAnalysisJob,CloudAnalysisRun,DatabaseSummary
    from chessism_api.operations.ingestion_pipeline import game_repository,fen_repository
    from chessism_api.operations.ingestion_pipeline.fen_core import process_single_game_sync,aggregate_fen_data
    from chessism_api.operations import home_summary,player_game_scores
    from chessism_api.operations.cloud_analysis import controller,importer
    from chessism_api.operations.cloud_analysis.schemas import CloudJobRequest
    from chessism_api.operations.fen_results import stage_fen_results
    from chessism_api.operations.fen_backup import database as fen_backup
    from chessism_api.operations.fen_backup.archive import validate_record
    from chessism_api.operations.tablebase import _save_tablebase_batch
    from chessism_api.database.relational_storage import describe_contract,create_relational_engine
    engine,factory=factory_for(socket,database)
    try:
        async with engine.connect() as connection:
            contract=await describe_contract(connection,LAYOUT)
        # The migration must not activate itself simply by changing column types.
        try:
            released=await create_relational_engine(engine.url,LAYOUT)
        except RuntimeError as error:
            if 'release has not been approved' not in str(error):
                raise
        else:
            await released.dispose()
            raise AssertionError('Unapproved storage profile activated')
        await passed('complete_v3_contract_and_release_guard')
        bundle=fixture_bundle()
        bundle['moves'][0]['white_time_left']=178.5
        bundle['moves'][0]['white_reaction_time']=1.5
        async def ingest():
            await game_repository.insert_game_bundle(bundle['games'],bundle['moves'],bundle['game_players'],
                bundle['game_openings'],[],player_name=PLAYER,affected_months={(2025,1),(2025,2)},affected_players={PLAYER,OTHER})
        with ExitStack() as patches:
            for module in (ask_db,db_interface,game_repository,fen_repository,home_summary,player_game_scores,controller,importer,fen_backup):
                patches.enter_context(patch.object(module,'AsyncDBSession',factory))
            await asyncio.wait_for(asyncio.gather(ingest(),ingest()),60)
            assert await conn.fetchval('SELECT count(*) FROM public.game')==3
            assert await conn.fetchval('SELECT count(*) FROM public.moves')==12
            assert await conn.fetchval('SELECT count(*) FROM chessism_archive.moves')==4
            assert await conn.fetchval('SELECT count(*) FROM public.game_player')==6
            first=bundle['moves'][0]
            assert await conn.fetchval('SELECT white_time_left FROM public.moves WHERE link=$1 AND n_move=$2',first['link'],first['n_move'])==178.5
            assert await conn.fetchval('SELECT n_games FROM chessism_reads.months WHERE player_name=$1 AND month=1',PLAYER)==3
            await passed('concurrent_ingestion_idempotency_archive_and_fractional_clocks')
            bad_link=-910000000099
            try:
                await game_repository.insert_game_bundle([dict(bundle['games'][0],link=bad_link,eco='invalid')],[],[],[],[],
                    player_name=PLAYER,affected_months={(2025,1)},affected_players={PLAYER})
            except ValueError:
                pass
            else:
                raise AssertionError('Invalid ingestion accepted')
            assert not await conn.fetchval('SELECT EXISTS(SELECT 1 FROM public.game WHERE link=$1)',bad_link)
            await passed('ingestion_failure_rolls_back')
            all_fens=set()
            for link in LINKS[:3]:
                rows,errors=process_single_game_sync((link,[m for m in bundle['moves'] if m['link']==link]))
                assert not errors,errors
                fens,associations=aggregate_fen_data(rows)
                all_fens.update(r['fen'] for r in fens)
                async with factory.begin() as session:
                    await DBInterface(Fen).create_all_with_session(session,fens)
                    await DBInterface(GameFenAssociation).create_all_with_session(session,associations)
                    await DBInterface(GameFenAssociation).create_all_with_session(session,associations)
                    await fen_repository.set_game_fen_state(session,[link],done=True,processing=False)
                assert await conn.fetchval('SELECT count(*) FROM public.game_fen_association WHERE game_link=$1',link)==len(rows)
            await fen_repository.refresh_fen_occurrence_counts(sorted(all_fens))
            await passed('real_fen_extraction_numeric_associations_and_retry')
            await conn.execute('UPDATE public.player SET joined=1,title=$1 WHERE player_name=$2','GM',PLAYER)
            stats=DBInterface(PlayerStats)
            await stats.create({'player_name':PLAYER,'chess_blitz_last_rating':2500})
            assert (await stats.update(PLAYER,{'chess_blitz_last_rating':2501}))['chess_blitz_last_rating']==2501
            assert (await stats.read(player_name=PLAYER))[0]['chess_blitz_last_rating']==2501
            await ask_db.refresh_main_character_mode_summary_for_players({PLAYER,OTHER})
            assert await conn.fetchval('SELECT sum(n_games) FROM chessism_reads.main_character_mode_summary WHERE player_name=$1',PLAYER)==3
            await passed('numeric_player_stats_and_mode_summaries')
            # Reserve/import real columnar control records, but never tick a cloud
            # controller or create its Google client.
            job=CloudAnalysisJob(id=uuid4().hex,selection=CloudJobRequest().model_dump(),target=1000,imported=0,status='running')
            async with factory.begin() as session:
                session.add(job)
                session.add(DatabaseSummary(id=1,n_positions=len(all_fens),unscored_fens=len(all_fens)))
            await controller.reserve(job,2)
            async with factory() as session:
                run=(await session.scalars(select(CloudAnalysisRun))).one()
            assert len(run.positions)==2
            from cloud_job.launch import configuration,expected_contract
            from stockfish_batch.checkpoints import BatchCheckpoints,encode
            import chess
            config=configuration(run.id,2,100000,300)
            cloud_contract=expected_contract(b''.join(encode(row) for row in run.positions),run.positions,config,
                {'engine_sha':'a'*64,'worker_version':'1.4.0','chess_version':'1.11.2'})
            checkpoint=BatchCheckpoints(None,'',cloud_contract,run.positions,500)
            records=[]
            for row in run.positions:
                board=chess.Board(row['fen'])
                lines=[{'multipv':i,'nodes':100000,'score':20-i,'wdl':[200,700,100],'pv':[move.uci()]}
                    for i,move in enumerate(list(board.legal_moves)[:cloud_contract['settings']['multipv']],1)]
                records.append(checkpoint.prepare(row,{'fen':row['fen'],'is_valid':True,'analysis':lines},1))
            await controller.save_run(run.id,contract=cloud_contract)
            raw=encode({'fingerprint':cloud_contract['fingerprint'],'records':records})
            original=importer.stage_fen_results
            async def interrupted(*args,**kwargs):
                await original(*args,**kwargs)
                raise RuntimeError('intentional isolated import rollback')
            with patch.object(importer,'stage_fen_results',interrupted):
                try:
                    await importer.import_batch(run.id,raw,0)
                except RuntimeError as error:
                    assert str(error)=='intentional isolated import rollback'
                else:
                    raise AssertionError('Import failure injection did not run')
            assert await conn.fetchval('SELECT count(*) FROM public.cloud_fen_claim')==2
            assert await conn.fetchval('SELECT count(*) FROM public.fen WHERE score IS NOT NULL')==0
            assert await importer.import_batch(run.id,raw,0)==2
            assert await importer.import_batch(run.id,raw,0)==0
            assert await conn.fetchval('SELECT count(*) FROM public.cloud_fen_claim')==0
            await passed('actual_cloud_import_atomicity_idempotency_and_integer_results')
            remaining=all_fens-{row['fen'] for row in run.positions}
            async with factory.begin() as session:
                await stage_fen_results(session,[{'fen':fen,'score':0.0,'next_moves':'',
                    'wdl_win':0.0,'wdl_draw':1000.0,'wdl_loss':0.0} for fen in remaining],[])
            assert (await ask_db.refresh_game_analysis_summary(tuple(LINKS[:3]),strict=True))['games']==3
            assert await conn.fetchval('SELECT count(*) FROM public.game_analysis_coverage WHERE is_fully_analyzed')==3
            async with factory() as session:
                exported=[row for batch in [b async for b in fen_backup.export_batches(session,datetime(2020,1,1,tzinfo=timezone.utc))] for row in batch]
            assert len(exported)==len(all_fens)
            for row in exported:
                validate_record(row)
            await fen_backup.restore_batch(exported)
            async with factory() as session:
                restored=[row for batch in [b async for b in fen_backup.export_batches(session)] for row in batch]
            assert {r['fen']:r for r in restored}=={r['fen']:r for r in exported}
            await passed('local_result_writer_coverage_and_exact_fen_archive_replay')
            for bad in (0.5,float('nan'),float('inf')):
                try:
                    async with factory.begin() as session:
                        await stage_fen_results(session,[{'fen':exported[0]['fen'],'score':bad,'next_moves':''}],[])
                except ValueError:
                    pass
                else:
                    raise AssertionError('Noninteger engine score accepted')
            terminal='8/8/8/8/8/8/4k3/6K1 w - -'
            async with factory.begin() as session:
                await DBInterface(Fen).create_all_with_session(session,[{'fen':terminal,'n_games':0,'moves_counter':'#0_1'}])
                await _save_tablebase_batch(session,[{'fen':terminal,'piece_count':2,'score':0.0,'next_moves':'',
                    'wdl_win':0.0,'wdl_draw':1000.0,'wdl_loss':0.0,'tablebase_wdl':0,'tablebase_dtz':0}],[])
            await passed('strict_integer_rejection_and_tablebase_writer')
            await conn.execute('INSERT INTO public.home_summary_source(id,revision) VALUES(1,1) ON CONFLICT DO NOTHING')
            home=await home_summary.refresh_home_summary(session_factory=factory)
            assert home['titled_games']==3 and home['titled_players']==1,home
            assert await conn.fetchval('SELECT games FROM chessism_reads.home_titled_player_summary WHERE player_name=$1',PLAYER)==3
            await passed('homepage_numeric_titled_summary')
        return contract
    finally:
        await engine.dispose()
