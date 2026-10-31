"""Disposable PostgreSQL: real shared writer, durable triggers and atomic repair."""
import asyncio
import json
from unittest.mock import patch

import chess
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from tests import check_canonical_blunders_postgres as base
from tests.test_evaluation_repair_publication import matching_shadow, approval
from chessism_api.operations.evaluation_repair import plan, publish
from chessism_api.operations import fen_summary_recovery
from chessism_api.operations.evaluation_repair import aggregates, artifacts
from tests.test_evaluation_repair_changes import reviewed_args


async def check(factory, connection, layout):
    await connection.execute('''CREATE TABLE ingestion_pipeline_run(status text);
        CREATE TABLE algorithm_run(status text);
        CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY);
        CREATE TABLE database_summary(id int,analyzed_fens bigint,unscored_fens bigint,scored_fens bigint);
        INSERT INTO database_summary VALUES(1,5,100,5);
        ALTER TABLE candidate.fen ALTER COLUMN score TYPE int;
        ALTER TABLE candidate.fen ADD COLUMN fen text UNIQUE, ADD COLUMN n_games bigint DEFAULT 1,
            ADD COLUMN next_moves text, ADD COLUMN piece_count smallint,
            ADD COLUMN wdl_win smallint, ADD COLUMN wdl_draw smallint, ADD COLUMN wdl_loss smallint,
            ADD COLUMN analyzed_at timestamptz, ADD COLUMN tablebase_wdl int, ADD COLUMN tablebase_dtz int;
        CREATE TABLE candidate.fen_continuation(id bigserial,fen_id bigint,rank int,move text,score int);
        DELETE FROM candidate.game_fen_association;
        INSERT INTO candidate.game_fen_association VALUES(1,1,true,1),(2,1,true,2),(3,1,true,3);
        UPDATE candidate.fen SET analysis_source=NULL;
        INSERT INTO candidate.fen_continuation(fen_id,rank,move,score) VALUES(1,2,'a7a5',10);''')
    for fen_id, move in enumerate(('e4', 'd4', 'c4', 'Nf3', 'f4'), 1):
        board = chess.Board(); board.push_san(move)
        await connection.execute('UPDATE candidate.fen SET fen=$1 WHERE fen_id=$2', board.fen(), fen_id)
    async with factory() as s, s.begin():
        await fen_summary_recovery.install(s, layout)
    async with factory() as s, s.begin():
        await publish.install(s)
    async def saved():
        async with factory() as s:
            await plan.prepare(s)
            p = await plan.preview(s)
            return p, matching_shadow(p)
    async def run(p, sh, ids=(1, 2)):
        async with factory() as s, s.begin():
            return await publish.publish(s, p, sh, list(ids), actor='isolated test', **approval(p, sh))
    async def state():
        return [await connection.fetchval(f'SELECT jsonb_agg(to_jsonb(t)) FROM {name} t') for name in (
            'candidate.fen', 'candidate.fen_continuation', 'candidate.fen_summary_refresh',
            'candidate.evaluation_repair_receipt', 'database_summary')]
    async def fails(p, sh, expected, ids=(1, 2)):
        before = await state()
        try:
            await run(p, sh, ids)
        except (ValueError, DBAPIError) as error:
            assert expected.lower() in str(error).lower(), str(error)
        else:
            raise AssertionError(f'Unsafe publication accepted: {expected}')
        assert before == await state(), 'Failed publication changed data'
    p, sh = await saved()
    await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt DISABLE TRIGGER evaluation_repair_immutable')
    await fails(p, sh, 'receipt protection')
    await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt ENABLE TRIGGER evaluation_repair_immutable')
    for table in ('ingestion_pipeline_run', 'cloud_analysis_job', 'algorithm_run'):
        await connection.execute(f"INSERT INTO {table} VALUES('running')")
        await fails(p, sh, 'active')
        await connection.execute(f'DELETE FROM {table}')
    await connection.execute('INSERT INTO cloud_fen_claim SELECT fen FROM candidate.fen WHERE fen_id=1')
    await fails(p, sh, 'claimed')
    await connection.execute('DELETE FROM cloud_fen_claim')
    for mutation in (
        'UPDATE candidate.fen SET n_games=n_games+1 WHERE fen_id=1',
        'UPDATE candidate.fen_continuation SET score=score+1 WHERE fen_id=1',
        'INSERT INTO candidate.game_fen_association VALUES(3,2,false,1)',
        "UPDATE candidate.player SET joined=joined+1 WHERE player_name='alice'",
    ):
        p, sh = await saved()
        await connection.execute(mutation)
        await connection.execute('DELETE FROM candidate.fen_summary_refresh')
        await fails(p, sh, 'stale')
    p, sh = await saved()
    await connection.execute('ALTER TABLE candidate.fen DISABLE TRIGGER fen_summary_refresh')
    await fails(p, sh, 'trigger')
    await connection.execute('ALTER TABLE candidate.fen ENABLE TRIGGER fen_summary_refresh')
    # An enabled-but-broken trigger must not silently omit downstream refreshes.
    await connection.execute('''CREATE OR REPLACE FUNCTION candidate.queue_fen_summary_refresh()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$''')
    await fails(p, sh, 'durably queued')
    async with factory() as s, s.begin():
        await fen_summary_recovery.install(s, layout)
    # Concurrent association changes / row locks refuse immediately, not wait.
    for sql in ('LOCK TABLE candidate.game_fen_association IN ROW EXCLUSIVE MODE',
                'SELECT fen_id FROM candidate.fen WHERE fen_id=1 FOR UPDATE'):
        async with connection.transaction():
            await connection.execute(sql)
            await fails(p, sh, 'lock')
    # A failure AFTER real writes must roll back evaluations, alternatives and queue.
    original = publish.stage_fen_results
    async def fail_after_write(*args, **kwargs):
        await original(*args, **kwargs)
        raise ValueError('synthetic interruption after writes')
    with patch.object(publish, 'stage_fen_results', fail_after_write):
        await fails(p, sh, 'synthetic interruption')
    await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt ADD CONSTRAINT synthetic_failure CHECK(false)')
    await fails(p, sh, 'synthetic_failure')
    await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt DROP CONSTRAINT synthetic_failure')
    held = await connection.fetchval('SELECT to_jsonb(f) FROM candidate.fen f WHERE fen_id=3')
    totals = await connection.fetchval('SELECT to_jsonb(t) FROM database_summary t')
    async def check_races(*args, **kwargs):
        # New job/claim/association writes cannot slip between check and commit.
        for table in ('public.cloud_analysis_job', 'public.cloud_fen_claim',
                      'candidate.game_fen_association', 'candidate.game_player'):
            try:
                async with connection.transaction():
                    await connection.execute(f'LOCK TABLE {table} IN ROW EXCLUSIVE MODE NOWAIT')
            except Exception as error:
                assert 'lock' in str(error)
            else:
                raise AssertionError('Concurrent admission was not protected')
        await original(*args, **kwargs)
    with patch.object(publish, 'stage_fen_results', check_races):
        result = await run(p, sh)
    assert result['evaluations_replaced'] == 2 and result['numeric_scores_changed'] == 0, result
    assert result['affected_games'] == [1, 2, 3]
    assert held == await connection.fetchval('SELECT to_jsonb(f) FROM candidate.fen f WHERE fen_id=3')
    assert totals == await connection.fetchval('SELECT to_jsonb(t) FROM database_summary t')
    receipt = json.loads(await connection.fetchval('SELECT payload FROM candidate.evaluation_repair_receipt'))
    assert len(receipt['before']) == len(receipt['after']) == 2
    assert receipt['plan'] == p and receipt['shadow'] == sh
    for item in receipt['after']:
        row = item['before']; replacement = next(c['replacement'] for c in sh['candidates'] if c['fen_id'] == row['fen_id'])
        assert row['analysis_source'] == 'stockfish'
        assert row['analyzed_at'] == replacement['analyzed_at']
        assert row['next_moves'] == replacement['next_moves']
        assert len(item['continuations']) == 1
    before_retry = await state()
    assert (await run(p, sh))['status'] == 'already_published'
    assert before_retry == await state()
    for sql in ('UPDATE candidate.evaluation_repair_receipt SET actor=actor',
                'DELETE FROM candidate.evaluation_repair_receipt',
                'TRUNCATE candidate.evaluation_repair_receipt'):
        try:
            await connection.execute(sql)
        except Exception as error:
            assert 'immutable' in str(error)
        else:
            raise AssertionError('Receipt mutation succeeded')
    # Queue consumption follows the existing durable recovery API, not a new path.
    async def refresh(games, *, strict):
        assert games == (1, 2, 3) and strict
    recovered = await fen_summary_recovery.recover_batch(factory=factory, refresh=refresh)
    assert recovered == {'processed': 3, 'pending': 0, 'error': None}, recovered
    # Test changed-score/WDL publication with genuine global summary tables.
    from chessism_api.database.models import ScoredPositionSummary
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.dialects.postgresql import dialect
    await connection.execute(str(CreateTable(ScoredPositionSummary.__table__).compile(dialect=dialect())))
    await connection.execute('''ALTER TABLE database_summary ADD COLUMN nonzero_scored_fens bigint DEFAULT 5,
        ADD COLUMN refreshed_at timestamptz DEFAULT now();
        UPDATE candidate.fen SET analysis_source=NULL,analyzed_at=NULL WHERE fen_id=1;
        DELETE FROM candidate.fen_summary_refresh;''')
    # Independent SQL oracle, matching production's full-refresh SQL filters.
    fields = []
    bounds = (('equal', 'abs(score)<50'), ('small', 'abs(score)>=50 AND abs(score)<150'),
              ('clear', 'abs(score)>=150 AND abs(score)<300'),
              ('decisive', 'abs(score)>=300 AND abs(score)<9000'), ('mate', 'abs(score)>=9000'))
    for name, condition in bounds:
        fields += [f'count(*) FILTER(WHERE {condition}) AS {name}_positions',
                   f'COALESCE(sum(n_games) FILTER(WHERE {condition}),0) AS {name}_appearances',
                   f'COALESCE(sum(abs(score)) FILTER(WHERE {condition}),0) AS {name}_abs_score_sum']
    fields += ['count(*) FILTER(WHERE score>50) AS white_better',
        'count(*) FILTER(WHERE score< -50) AS black_better', 'count(*) FILTER(WHERE abs(score)<=50) AS balanced',
        'sum(score) AS score_sum', 'sum(abs(score)) AS abs_score_sum', 'count(*) FILTER(WHERE score<>0) AS nonzero_scored_fens',
        'count(*) FILTER(WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL) AS wdl_positions']
    fields += [f'COALESCE(sum(wdl_{k}) FILTER(WHERE wdl_win IS NOT NULL AND wdl_draw IS NOT NULL AND wdl_loss IS NOT NULL),0) AS wdl_{k}_sum' for k in ('win','draw','loss')]
    async def oracle():
        return dict(await connection.fetchrow('SELECT '+','.join(fields)+' FROM candidate.fen WHERE score IS NOT NULL'))
    full = await oracle()
    await connection.execute('INSERT INTO scored_position_summary(id,analyzed_fens,scored_positions) VALUES(1,5,5)')
    await connection.execute('UPDATE scored_position_summary SET '+','.join(f'{k}=${i}' for i,k in enumerate(full,1)), *full.values())
    async def changed_run(p, sh):
        async with factory() as s, s.begin():
            return await publish.publish(s, p, sh, [1], actor='changed fixture', **reviewed_args(p,sh))
    # Includes negative/positive, exact 50 boundary, WDL NULL->known and known->NULL,
    # and scored-zero transitions, without changing the advantage category.
    for old, new, wdl in ((100, -100, (100,800,100)), (-100, 50, (None,None,None)),
                           (32, 0, (0,1000,0)), (0, 32, (None,None,None))):
        await connection.execute('UPDATE candidate.fen SET score=$1,analysis_source=NULL,analyzed_at=NULL WHERE fen_id=1',old)
        await connection.execute('DELETE FROM candidate.fen_summary_refresh')
        full = await oracle()
        await connection.execute('UPDATE scored_position_summary SET '+','.join(f'{k}=${i}' for i,k in enumerate(full,1)),*full.values())
        await connection.execute('UPDATE database_summary SET scored_fens=$1,nonzero_scored_fens=$1',full['nonzero_scored_fens'])
        async with factory() as s:
            await plan.prepare(s); p = await plan.preview_ids(s,[1])
        sh = matching_shadow(p); sh['candidates'][0]['replacement'].update(score=new,**dict(zip(publish.WDL,wdl)))
        sh = artifacts.seal(sh)
        before_all = await state()
        before_sps = await connection.fetchval('SELECT to_jsonb(s) FROM scored_position_summary s')
        # Failure after delta writes must roll back all global totals, too.
        await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt ADD CONSTRAINT synthetic_failure CHECK(false) NOT VALID')
        try: await changed_run(p,sh)
        except DBAPIError as error: assert 'synthetic_failure' in str(error)
        else: raise AssertionError('Injected final receipt failure did not abort')
        await connection.execute('ALTER TABLE candidate.evaluation_repair_receipt DROP CONSTRAINT synthetic_failure')
        assert before_all == await state()
        assert before_sps == await connection.fetchval('SELECT to_jsonb(s) FROM scored_position_summary s')
        result = await changed_run(p,sh)
        actual = dict(await connection.fetchrow('SELECT * FROM scored_position_summary'))
        for field,value in (await oracle()).items(): assert actual[field] == value, (field,actual[field],value)
        assert actual['analyzed_fens'] == actual['scored_positions'] == 5
        assert await connection.fetchval('SELECT analyzed_fens FROM database_summary') == 5
        assert await connection.fetchval('SELECT scored_fens FROM database_summary') == actual['nonzero_scored_fens']
        assert (await changed_run(p,sh))['status'] == 'already_published'
        assert actual == dict(await connection.fetchrow('SELECT * FROM scored_position_summary'))
        await connection.execute('DELETE FROM candidate.fen_summary_refresh')
    print('PASS changed repair: SQL-oracle aggregate parity, signed/zero/50-cp boundaries, NULL WDL transitions, delta+receipt rollback and idempotent replay', flush=True)
    print('PASS: matching publication, real shared writer/trigger, claims, active jobs, stale inputs, locks, rollback after writes, immutable audit, idempotence, unchanged totals/held row and durable recovery', flush=True)


if __name__ == '__main__':
    base.check = check
    asyncio.run(base.main())
