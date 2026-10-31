"""Network-disabled disposable PostgreSQL; no production URL or cloud access."""
import asyncio
import json
from unittest.mock import patch

import chess
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations import fen_summary_recovery
from chessism_api.operations.evaluation_repair import legacy_policy as policy, legacy_batch as db
from chessism_api.operations.evaluation_repair import aggregates, publish, rating_delta


async def check(factory, connection, layout):
    await connection.execute('''CREATE TABLE ingestion_pipeline_run(status text);
        CREATE TABLE algorithm_run(status text);
        CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY);
        ALTER TABLE candidate.game ADD COLUMN avg_elo numeric DEFAULT 1500;
        ALTER TABLE candidate.fen ALTER COLUMN score TYPE int;
        ALTER TABLE candidate.fen ADD COLUMN fen text UNIQUE, ADD COLUMN n_games bigint DEFAULT 3,
            ADD COLUMN next_moves text, ADD COLUMN piece_count smallint,
            ADD COLUMN wdl_win smallint, ADD COLUMN wdl_draw smallint, ADD COLUMN wdl_loss smallint,
            ADD COLUMN analyzed_at timestamptz, ADD COLUMN tablebase_wdl smallint, ADD COLUMN tablebase_dtz int;
        CREATE TABLE candidate.fen_continuation(id bigserial,fen_id bigint,rank int,move text,score int);
        DELETE FROM candidate.game_fen_association;
        INSERT INTO candidate.game_fen_association VALUES(1,1,true,1),(1,2,false,1),(2,1,true,1),(4,1,true,1),(1,3,true,2);
        INSERT INTO candidate.fen_continuation(fen_id,rank,move,score) VALUES(1,2,'c5b6',10006),(2,2,'d2d4',24);
        UPDATE candidate.fen SET analysis_source=NULL;
        UPDATE candidate.fen SET analysis_source='stockfish' WHERE fen_id=4;
        UPDATE candidate.fen SET score=NULL WHERE fen_id=5;
        UPDATE candidate.fen SET score=10007 WHERE fen_id=1;
        UPDATE candidate.fen SET score=24 WHERE fen_id=2;
        UPDATE candidate.fen SET score=9997 WHERE fen_id=3;
        CREATE TABLE database_summary(id int,analyzed_fens bigint,unscored_fens bigint,
            scored_fens bigint,nonzero_scored_fens bigint,refreshed_at timestamptz DEFAULT now());
        INSERT INTO database_summary VALUES(1,4,1,4,4,now());
        CREATE TABLE scored_rating_summary(id int,groups_payload jsonb,ratings_payload jsonb,refreshed_at timestamptz);
        ''')
    from chessism_api.database.models import ScoredPositionSummary
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.dialects.postgresql import dialect
    await connection.execute(str(CreateTable(ScoredPositionSummary.__table__).compile(dialect=dialect())))
    for fen_id in range(1, 6):
        board = chess.Board()
        if fen_id > 1:
            board.push(list(board.legal_moves)[fen_id])
        fen = '8/8/8/2k5/4Q3/4K3/8/8 b - -' if fen_id == 1 else board.fen()
        await connection.execute('UPDATE candidate.fen SET fen=$1 WHERE fen_id=$2', fen, fen_id)
    fields = []
    for name, condition in (('equal','abs(score)<50'),('small','abs(score)>=50 AND abs(score)<150'),
                           ('clear','abs(score)>=150 AND abs(score)<300'),
                           ('decisive','abs(score)>=300 AND abs(score)<9000'),('mate','abs(score)>=9000')):
        fields.extend((f'count(*) FILTER(WHERE {condition}) AS {name}_positions',
                       f'COALESCE(sum(n_games) FILTER(WHERE {condition}),0) AS {name}_appearances',
                       f'COALESCE(sum(abs(score)) FILTER(WHERE {condition}),0) AS {name}_abs_score_sum'))
    fields.extend(('count(*) FILTER(WHERE score>50) AS white_better','count(*) FILTER(WHERE score< -50) AS black_better',
                   'count(*) FILTER(WHERE abs(score)<=50) AS balanced','sum(score) AS score_sum',
                   'sum(abs(score)) AS abs_score_sum','count(*) FILTER(WHERE score<>0) AS nonzero_scored_fens',
                   'count(*) FILTER(WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL) AS wdl_positions'))
    fields.extend(f'COALESCE(sum(wdl_{key}) FILTER(WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL),0) AS wdl_{key}_sum' for key in ('win','draw','loss'))
    async def oracle():
        return dict(await connection.fetchrow('SELECT '+','.join(fields)+' FROM candidate.fen WHERE score IS NOT NULL'))
    values = await oracle()
    await connection.execute('INSERT INTO scored_position_summary(id,analyzed_fens,scored_positions) VALUES(1,4,4)')
    for key, value in values.items():
        await connection.execute(f'UPDATE scored_position_summary SET {key}=$1', value)
    groups = [{'key':'bad','min_rating':1400,'max_rating':1600,'positions':4,'games':2,
               'buckets':[{'key':key,'positions':3 if key=='mate' else 1 if key=='equal' else 0} for key in aggregates.BINS]}]
    await connection.execute('INSERT INTO scored_rating_summary VALUES(1,$1,\'[]\',now())', json.dumps(groups))
    async with factory() as s, s.begin(): await fen_summary_recovery.install(s, layout)
    async with factory() as s, s.begin(): await publish.install(s)
    async with factory() as s:
        plan = await db.discover(s)
        assert plan['counts'] == {'tablebase':1,'assumed':1,'held_mate':1}, plan
    async with factory() as s: rows = await db.load(s,[1,2])
    solved = {'fen':rows[0]['fen'],'piece_count':3,'score':1000.0,'next_moves':'c5b6',
              'wdl_win':1000.0,'wdl_draw':0.0,'wdl_loss':0.0,'tablebase_wdl':-2,'tablebase_dtz':-3}
    with patch('chessism_api.operations.tablebase._probe_batch_sync', return_value=([solved], [])):
        candidates, skipped = policy.prepare(rows, object())
    assert not skipped
    async def run():
        async with factory() as s, s.begin():
            return await db.publish(s,candidates,run_id='isolated',actor='test',acknowledgement=policy.ACK)
    async def state():
        return [await connection.fetchval(f'SELECT jsonb_agg(to_jsonb(t)) FROM {name} t') for name in (
            'candidate.fen','candidate.fen_continuation','candidate.fen_summary_refresh',
            'candidate.evaluation_repair_receipt','scored_position_summary','database_summary','scored_rating_summary')]
    async def fails(expected):
        before=await state()
        try: await run()
        except (ValueError, DBAPIError) as error: assert expected.lower() in str(error).lower(), str(error)
        else: raise AssertionError('Unsafe repair accepted')
        assert before==await state(), 'Failure was not atomic'
    await connection.execute("INSERT INTO ingestion_pipeline_run VALUES('running')")
    await fails('active')
    await connection.execute('DELETE FROM ingestion_pipeline_run')
    await connection.execute('INSERT INTO cloud_fen_claim SELECT fen FROM candidate.fen WHERE fen_id=1')
    await fails('claimed')
    await connection.execute('DELETE FROM cloud_fen_claim')
    await connection.execute('UPDATE candidate.fen SET score=25 WHERE fen_id=2')
    await fails('stale')
    await connection.execute('UPDATE candidate.fen SET score=24 WHERE fen_id=2')
    await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt ADD CONSTRAINT synthetic_failure CHECK(false)')
    await fails('synthetic_failure')
    await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt DROP CONSTRAINT synthetic_failure')
    async with connection.transaction():
        await connection.execute('SELECT * FROM candidate.fen WHERE fen_id=1 FOR UPDATE')
        await fails('lock')
    result = await run()
    assert result['changed']==2 and result['tablebase']==result['assumed']==1, result
    assert (await run())['status']=='already_published'
    actual=dict(await connection.fetchrow('SELECT * FROM scored_position_summary WHERE id=1'))
    assert all(actual[key]==value for key,value in (await oracle()).items())
    updated=json.loads(await connection.fetchval('SELECT groups_payload FROM scored_rating_summary WHERE id=1'))
    assert [b['positions'] for b in updated[0]['buckets']]==[1,0,0,3,0], updated
    assert await connection.fetchval('SELECT analyzed_fens FROM database_summary')==4
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_continuation WHERE fen_id=1')==0
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_continuation WHERE fen_id=2')==1
    assert await connection.fetchval('SELECT analyzed_at IS NULL FROM candidate.fen WHERE fen_id=2')
    assert await connection.fetchval('SELECT analysis_source FROM candidate.fen WHERE fen_id=2')==policy.SOURCE
    assert await connection.fetchval('SELECT analysis_source IS NULL FROM candidate.fen WHERE fen_id=3')
    assert await connection.fetchval('SELECT count(*) FROM candidate.fen_summary_refresh')==3
    receipt=json.loads(await connection.fetchval('SELECT payload FROM candidate.evaluation_repair_receipt'))
    assert len(receipt['before'])==len(receipt['after'])==2 and len(receipt['removed_continuations'])==1
    assert receipt['rating_delta'] and receipt['global_delta'] and receipt['processed_ids']==[1,2]
    print('PASS: atomic repair, rollback after writes, stale/claimed/active protection, row lock safety, idempotency, original values, held mates, real refresh queues, global/rating delta parity', flush=True)


if __name__=='__main__':
    base.check=check
    asyncio.run(base.main())
