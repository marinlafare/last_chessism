"""Disposable PostgreSQL only: bounded legacy snapshots and stale-plan checks."""
import asyncio
import json
import chess
from sqlalchemy import text
from tests import check_canonical_blunders_postgres as base
from chessism_api.operations.evaluation_repair import plan


async def check(factory, connection, layout):
    await connection.execute('''CREATE TABLE ingestion_pipeline_run(status text);
        CREATE TABLE algorithm_run(status text);
        CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY);
        ALTER TABLE candidate.fen ADD COLUMN fen text,ADD COLUMN n_games bigint DEFAULT 1,
            ADD COLUMN analyzed_at timestamptz,ADD COLUMN tablebase_wdl int,ADD COLUMN tablebase_dtz int;
        CREATE TABLE candidate.fen_continuation(fen_id bigint,rank int,move text,score int);
        DELETE FROM candidate.game_fen_association;
        UPDATE candidate.fen SET analysis_source=NULL;
        INSERT INTO candidate.game_fen_association VALUES(1,1,true,1),(2,1,true,2);
        INSERT INTO candidate.game_fen_association
            SELECT i,1,true,3 FROM generate_series(1,21) i;
        UPDATE candidate.fen SET analysis_source='stockfish' WHERE fen_id=4;
        UPDATE candidate.fen SET score=NULL WHERE fen_id=5;
        INSERT INTO candidate.fen_continuation VALUES(1,2,'e2e4',24);''')
    for fen_id, move in enumerate(('e4', 'd4', 'c4', 'Nf3', 'f4'), 1):
        board = chess.Board(); board.push_san(move)
        await connection.execute('UPDATE candidate.fen SET fen=$1 WHERE fen_id=$2', board.fen(), fen_id)
    before = await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id) FROM candidate.fen f')
    async def preview(**kwargs):
        async with factory() as s:
            await plan.prepare(s)
            return await plan.preview(s, **kwargs)
    async def recheck(saved):
        async with factory() as s:
            await plan.prepare(s)
            return await plan.recheck(s, saved)
    saved = await preview()
    assert [i['before']['fen_id'] for i in saved['items']] == [1, 2]
    async with factory() as s:
        await plan.prepare(s)
        explicit = await plan.preview_ids(s, [2, 1])
        assert [i['before']['fen_id'] for i in explicit['items']] == [1, 2]
    assert [i['before']['fen_id'] for i in (await preview(cp_only=True))['items']] == [1, 2]
    assert saved['affected_games'] == [1, 2]
    assert saved['items'][0]['continuations'] == [{'rank': 2, 'move': 'e2e4', 'score': 24}]
    assert saved['skipped_ids'] == [3]  # Cached appearance count understated fanout.
    assert (await recheck(saved))['unchanged']
    assert before == await connection.fetchval('SELECT jsonb_agg(to_jsonb(f) ORDER BY fen_id) FROM candidate.fen f')
    await connection.execute('UPDATE candidate.fen SET score=score+1 WHERE fen_id=1')
    assert (await recheck(saved))['changed_ids'] == [1]
    await connection.execute("INSERT INTO cloud_fen_claim SELECT fen FROM candidate.fen WHERE fen_id=2")
    assert (await recheck(saved))['changed_ids'] == [1, 2]
    assert [i['before']['fen_id'] for i in (await preview())['items']] == [1]
    await connection.execute('DELETE FROM cloud_fen_claim')
    saved = await preview()
    await connection.execute("UPDATE candidate.fen_continuation SET score=0 WHERE fen_id=1")
    assert (await recheck(saved))['changed_ids'] == [1]
    saved = await preview()
    await connection.execute('INSERT INTO candidate.game_fen_association VALUES(3,1,true,2)')
    assert (await recheck(saved))['changed_ids'] == [2]
    for table in ('cloud_analysis_job', 'ingestion_pipeline_run', 'algorithm_run'):
        await connection.execute(f"INSERT INTO {table}(status) VALUES('running')")
        try:
            await preview()
        except ValueError as error:
            assert 'active' in str(error)
        else:
            raise AssertionError('Active work was not protected')
        await connection.execute(f'DELETE FROM {table}')
    for kwargs in ({'limit': 17}, {'window': 100001}, {'after_id': -1}):
        try:
            await preview(**kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError('Unbounded selection accepted')
    for ids in ([1, 1], [False], [4], [5], [3], []):
        try:
            async with factory() as s:
                await plan.prepare(s)
                await plan.preview_ids(s, ids)
        except ValueError:
            pass
        else:
            raise AssertionError('Unsafe explicit IDs accepted')
    async with factory() as s:
        try:
            await plan.preview(s)
        except ValueError as error:
            assert 'read-only' in str(error)
        else:
            raise AssertionError('Read/write session accepted')
    print('PASS: read-only snapshot, bounded ID/fanout limits, active-work/claim guards, original continuations, score/association/continuation drift', flush=True)


if __name__ == '__main__':
    base.check = check
    asyncio.run(base.main())
