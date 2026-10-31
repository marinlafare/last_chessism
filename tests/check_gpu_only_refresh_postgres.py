"""Disposable DB: retain accuracy exactly, never compute/read CPU blunders.

Run in the pinned pre-retirement backup test image. Its /app module supplies an
old-query oracle ONLY inside this network-isolated synthetic test. It is never
loaded by the application or against live data.
"""
import asyncio
from contextlib import asynccontextmanager
import importlib.util
import inspect
import json
import os
from unittest.mock import AsyncMock, patch
import numpy as np
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from tests import check_canonical_blunders_postgres as base
from tests.test_canonical_blunders import report
from chessism_api.operations import player_game_scores as scores
from chessism_api.operations import player_accuracy_distribution as distribution
from chessism_api.operations.canonical_blunders import schema, registry, source, compute, publication, readers
from chessism_api.operations.canonical_blunders.retired_counter import repair


async def check(factory, connection, layout):
    await connection.execute('''
        ALTER TABLE candidate.game_player_engine_summary ADD PRIMARY KEY(game_player_id);
        ALTER TABLE candidate.game_player_engine_summary ALTER COLUMN blunder_count SET NOT NULL;
        ALTER TABLE candidate.game_player_engine_summary ALTER COLUMN blunder_count DROP DEFAULT;
        ALTER TABLE candidate.game_player_engine_summary
          ADD COLUMN own_move_cp_gain float8,ADD COLUMN own_move_cp_loss float8,
          ADD COLUMN mean_win_percent_loss float8,ADD COLUMN median_win_percent_loss float8,
          ADD COLUMN mate_for_positions int,ADD COLUMN mate_against_positions int,
          ADD COLUMN final_player_cp float8,ADD COLUMN result text,ADD COLUMN end_by text;
        CREATE VIEW reads.game AS SELECT g.*,m.name AS mode FROM candidate.game g
            JOIN candidate.game_mode m ON m.id=g.mode_id;
        CREATE VIEW reads.player AS SELECT * FROM candidate.player;
        CREATE VIEW reads.fen AS SELECT * FROM candidate.fen;
        CREATE VIEW reads.game_player AS SELECT gp.game_link AS link,p.player_name,
            CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS color,
            CASE WHEN gp.is_white THEN g.white_result ELSE g.black_result END AS result,
            m.name AS mode,g.played_at
            FROM candidate.game_player gp JOIN candidate.player p ON p.id=gp.player_id
            JOIN candidate.game g ON g.link=gp.game_link JOIN candidate.game_mode m ON m.id=g.mode_id;
        CREATE VIEW reads.game_analysis_summary AS SELECT * FROM candidate.game_analysis_coverage;
        CREATE VIEW reads.game_fen_association AS SELECT game_link,n_move,fen_id,
            CASE WHEN is_white THEN 'white' ELSE 'black' END AS move_color
            FROM candidate.game_fen_association;
        DROP VIEW reads.game_player_engine_summary;
        CREATE VIEW reads.game_player_engine_summary AS SELECT gp.game_link,p.player_name,
            CASE WHEN gp.is_white THEN 'white' ELSE 'black' END AS player_color,s.*
            FROM candidate.game_player_engine_summary s JOIN candidate.game_player gp ON gp.id=s.game_player_id
            JOIN candidate.player p ON p.id=gp.player_id;
    ''')
    @asynccontextmanager
    async def sessions():
        async with factory() as session:
            await session.execute(text('SET search_path=reads,candidate,public'))
            yield session

    spec=importlib.util.spec_from_file_location('isolated_old_accuracy_oracle','/app/chessism_api/operations/player_game_scores.py')
    old=importlib.util.module_from_spec(spec); spec.loader.exec_module(old)
    assert 'AS blunder_count' in inspect.getsource(old.refresh_game_player_engine_summaries), 'Use the pinned pre-retirement image'
    assert 'blunder_count' not in inspect.getsource(scores.refresh_game_player_engine_summaries)
    async def numeric_rows():
        return await connection.fetchval('''SELECT jsonb_agg(to_jsonb(s)-'blunder_count' ORDER BY game_player_id)
            FROM candidate.game_player_engine_summary s''')
    with patch.object(old,'AsyncDBSession',sessions):
        await old.refresh_game_player_engine_summaries()
    expected=await numeric_rows()
    await connection.execute('UPDATE candidate.game_player_engine_summary SET blunder_count=777,game_efficiency=-99')
    # Match the actual normalized production schema, not a synthetic default.
    # PostgreSQL checks NOT NULL even before ON CONFLICT can update an old row.
    with patch.object(scores,'AsyncDBSession',sessions):
        try:
            await scores.refresh_game_player_engine_summaries()
        except IntegrityError as error:
            assert error.orig.sqlstate=='23502' and 'blunder_count' in str(error.orig)
        else:
            raise AssertionError('Expected the production missing-default failure')
    assert await connection.fetchval('SELECT bool_and(game_efficiency=-99) FROM candidate.game_player_engine_summary')
    try:
        async with factory() as s,s.begin(): await repair(s,layout)
    except ValueError as error:
        assert 'CUDA-only' in str(error)
    else:
        raise AssertionError('Repair accepted legacy CPU readers')
    async with factory() as s,s.begin(): await schema.install(s,layout)
    contract=await connection.fetchrow('''SELECT is_nullable,column_default FROM information_schema.columns
        WHERE table_schema='candidate' AND table_name='game_player_engine_summary' AND column_name='blunder_count' ''')
    assert contract['is_nullable']=='YES' and contract['column_default'] is None
    async with factory() as s,s.begin(): assert not (await repair(s,layout))['changed']
    # Existing deployments already have CUDA views, but can retain the old
    # NOT NULL contract. Test the standalone repair, not only a fresh install.
    await connection.execute('ALTER TABLE candidate.game_player_engine_summary ALTER COLUMN blunder_count SET NOT NULL')
    async with factory() as s,s.begin(): assert (await repair(s,layout))['changed']
    # A legacy default must not fabricate zero counts either; existing values
    # are preserved when removing it. Repeat repairs do not recalculate rows.
    await connection.execute('ALTER TABLE candidate.game_player_engine_summary ALTER COLUMN blunder_count SET DEFAULT 0')
    async with factory() as s,s.begin(): assert (await repair(s,layout))['changed']
    async with factory() as s,s.begin(): assert not (await repair(s,layout))['changed']
    with patch.object(scores,'AsyncDBSession',sessions):
        await scores.refresh_game_player_engine_summaries()
        assert await numeric_rows()==expected, 'Accuracy/efficiency or other retained projection changed'
        assert await connection.fetchval('SELECT bool_and(blunder_count=777) FROM candidate.game_player_engine_summary')
        # Repeated refresh, and a newly inserted summary, cannot write CPU counts.
        await scores.refresh_game_player_engine_summaries()
        await connection.execute('DELETE FROM candidate.game_player_engine_summary WHERE game_player_id=1')
        await scores.refresh_game_player_engine_summaries((1,))
        assert await numeric_rows()==expected
        assert await connection.fetchval('SELECT blunder_count IS NULL FROM candidate.game_player_engine_summary WHERE game_player_id=1')
        assert await connection.fetchval('SELECT bool_and(blunder_count=777) FROM candidate.game_player_engine_summary WHERE game_player_id<>1')

    async with factory() as s,s.begin(): await schema.install(s,layout)
    assert await connection.fetchval('SELECT bool_and(blunder_count IS NULL) FROM reads.game_player_engine_summary')
    redis=AsyncMock()
    redis.get.side_effect=AssertionError('Legacy blunder cache must not be consulted')
    with patch.object(distribution,'AsyncDBSession',sessions):
        pending=await distribution.get_blunder_distribution('alice',redis)
        assert pending['summary_pending'] and pending['model_id'] is None
        assert pending['source']=='canonical_cuda_blunders'
        assert (await distribution.get_summary_distribution('alice',redis,metric='blunders'))['summary_pending']
    redis.get.assert_not_awaited()

    mid,config,evidence=registry.freeze(report(),registry.ACK)
    async with factory() as s,s.begin():
        await s.execute(text('''INSERT INTO candidate.canonical_blunder_model(id,source_run,config,evidence,approved_by,approval)
            VALUES(:id,'fixture',CAST(:config AS jsonb),CAST(:evidence AS jsonb),'test','provisional_user_approved')'''),
            {'id':mid,'config':json.dumps(config),'evidence':json.dumps(evidence)})
        await registry.select_target(s,mid)
    async with factory() as s:
        await source.prepare(s)
        model,cfg,items,(eligible,unsupported),rows=await source.load(s,position_reader='binary_v1')
    records,info=compute.infer(np,rows,eligible,cfg,require_cuda=False)
    async with factory() as s,s.begin():
        await publication.publish(s,model,items,records+unsupported,info)
    with patch.dict(os.environ,{'CHESSISM_CANONICAL_BLUNDERS':'0'}),patch.object(distribution,'AsyncDBSession',sessions):
        actual=await distribution.get_blunder_distribution('alice',redis)
        assert not actual['summary_pending'] and actual['model_id']==mid
        assert actual['source']=='canonical_cuda_blunders'
    assert await connection.fetchval('SELECT max(blunder_count)<777 FROM reads.game_player_engine_summary')
    # No active/missing GPU output is unknown, never the stored sentinel or zero.
    async with connection.transaction():
        await connection.execute('DELETE FROM candidate.canonical_blunder_game WHERE game_link=1')
        assert await connection.fetchval('SELECT bool_and(blunder_count IS NULL) FROM reads.game_player_engine_summary WHERE game_link=1')
        async with factory() as s:
            # A different connection cannot see this uncommitted deletion;
            # the view assertion above tests visibility in the same snapshot.
            assert readers.engine_table(layout)==schema.table(layout,'engine')
    print('PASS: production NOT NULL/no-default failure reproduced and repaired, repeat repair is inert, legacy readers rejected, exact non-blunder parity, new retired counts stay NULL, historical values unchanged, CUDA-only publication')
    await automatic_refresh_check(factory,connection,layout)


async def automatic_refresh_check(factory,connection,layout):
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from chessism_api.database import ask_db
    from chessism_api.operations.fen_summary_recovery import recover_batch
    await connection.execute('''ALTER TABLE candidate.game_analysis_coverage ADD PRIMARY KEY(link),
        ADD COLUMN analyzed_positions int,ADD COLUMN unscored_positions int,
        ADD COLUMN equal_positions int,ADD COLUMN small_positions int,ADD COLUMN clear_positions int,
        ADD COLUMN decisive_positions int,ADD COLUMN mate_positions int,ADD COLUMN refreshed_at timestamptz;
        UPDATE candidate.fen SET score=score+25 WHERE fen_id=1;''')
    engine=create_async_engine(factory.kw['bind'].url,
        connect_args={'server_settings':{'search_path':layout.search_path}})
    sessions=async_sessionmaker(engine,info={'relational_storage_v2':layout})
    try:
        assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh')>0
        with patch.object(ask_db,'AsyncDBSession',sessions),patch.object(scores,'AsyncDBSession',sessions):
            result=await recover_batch(factory=sessions)
        assert result['engine_refreshed']>0 and result['pending']==0 and result['error'] is None
        assert await connection.fetchval('SELECT bool_and(blunder_count=777) FROM candidate.game_player_engine_summary')
        assert await connection.fetchval('SELECT count(*) FROM candidate.canonical_blunder_dirty')>0
        async with sessions() as s:
            await source.prepare(s)
            model,cfg,items,(eligible,unsupported),rows=await source.load(s,position_reader='binary_v1')
        records,info=compute.infer(np,rows,eligible,cfg,require_cuda=False)
        async with sessions() as s,s.begin():
            result=await publication.publish(s,model,items,records+unsupported,info)
        assert result['status']=='ready' and result['remaining']==0
        assert await connection.fetchval('SELECT max(blunder_count)<777 FROM reads.game_player_engine_summary')
        assert await connection.fetchval('SELECT bool_and(blunder_count=777) FROM candidate.game_player_engine_summary')
        assert await connection.fetchval('SELECT blunder_count IS NULL FROM candidate.game_player_engine_summary WHERE game_player_id=1')
    finally:
        await engine.dispose()
    print('PASS: real new-score trigger -> coverage/accuracy refresh -> canonical queue -> publication, without a CPU blunder write')


if __name__=='__main__':
    base.check=check
    asyncio.run(base.main())
