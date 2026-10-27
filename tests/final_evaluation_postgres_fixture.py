"""Private database proof: frozen replay and durable exactly-once evaluation."""
import io
import json
from unittest.mock import AsyncMock


async def verify(connection, engine, training_config, frozen_layouts, expected_bytes):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from chessism_api.operations.research_algorithms import blunder_final_evaluation as final
    from chessism_api.operations.research_algorithms.repository import RunCancelled
    from tests.test_blunder_final_evaluation import parent_row, PARENT_ID
    parent = parent_row(frozen_layouts[0], training_config)
    config = final.configuration(PARENT_ID, parent)
    await connection.execute("INSERT INTO algorithm_run(id,status,config,result) VALUES($1,'complete',$2::jsonb,$3::jsonb)",
        PARENT_ID, json.dumps(training_config), json.dumps(parent['result']))
    run_id = '00000000-0000-4000-8000-000000000078'
    retry_id = '00000000-0000-4000-8000-000000000079'
    await connection.execute("INSERT INTO algorithm_run(id,status,config) VALUES($1,'running',$3::jsonb),($2,'running',$3::jsonb)",
        run_id, retry_id, json.dumps(config))
    for layout, frozen in zip((None, RelationalStorage('candidate', 'archive', 'reads', 'relational_v3')), frozen_layouts):
        parent = parent_row(frozen, training_config)
        config = final.configuration(PARENT_ID, parent)
        await connection.execute('UPDATE algorithm_run SET result=$2::jsonb WHERE id=$1', PARENT_ID, json.dumps(parent['result']))
        await connection.execute('UPDATE algorithm_run SET config=$2::jsonb WHERE id=ANY($1::text[])', [run_id, retry_id], json.dumps(config))
        stream = io.BytesIO()
        replayed, meta, games, actual = await final.freeze(config, AsyncMock(), stream,
            session_factory=async_sessionmaker(engine, info={'relational_storage_v2': layout}))
        assert replayed == parent['result']
        assert stream.getvalue() == expected_bytes
        assert actual['sha256'] == frozen['sha256']
        assert actual['game_ids_by_split'] == frozen['game_ids_by_split']
        assert actual['replay_verified']
    reporter = AsyncMock(); reporter.run_id = run_id
    # Active cloud work stops even a fixed replay.
    await connection.execute("INSERT INTO cloud_analysis_job VALUES('running')")
    try: await final.freeze(config, reporter, io.BytesIO(), session_factory=async_sessionmaker(engine))
    except ValueError as error: assert 'active cloud' in str(error)
    else: raise AssertionError('Active cloud was not protected')
    await connection.execute('DELETE FROM cloud_analysis_job')
    await connection.execute('UPDATE algorithm_run SET cancel_requested=true WHERE id=$1', run_id)
    try: await final.claim(config, parent['result'], reporter, session_factory=async_sessionmaker(engine))
    except RunCancelled: pass
    else: raise AssertionError('Cancelled run consumed the test')
    assert await connection.fetchval('SELECT result IS NULL FROM algorithm_run WHERE id=$1', run_id)
    await connection.execute('UPDATE algorithm_run SET cancel_requested=false WHERE id=$1', run_id)
    await final.claim(config, parent['result'], reporter, session_factory=async_sessionmaker(engine))
    saved = json.loads(await connection.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', run_id))
    assert saved['final_evaluation']['test_started'] and not saved['final_evaluation']['test_completed']
    await connection.execute("UPDATE algorithm_run SET status='failed' WHERE id=$1", run_id)
    reporter.run_id = retry_id
    try: await final.claim(config, parent['result'], reporter, session_factory=async_sessionmaker(engine))
    except ValueError as error: assert 'already been consumed' in str(error)
    else: raise AssertionError('Failed consumed test was allowed to rerun')
    unchanged = json.loads(await connection.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', PARENT_ID))
    assert unchanged == parent['result']
    # Only fixture-owned records; preserve the enclosing historical-role test.
    await connection.execute('DELETE FROM algorithm_run WHERE id=ANY($1::text[])', [PARENT_ID, run_id, retry_id])
    print('PASS: final frozen native/legacy replay, original hash/partitions, cloud guard, cancellation, exactly-once consumption across failure and immutable parent')
