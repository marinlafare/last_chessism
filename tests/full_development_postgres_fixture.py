"""Disposable PostgreSQL verification of large development source and reservation."""
import json
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock, patch


async def verify(connection, engine):
    import numpy as np
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage
    from chessism_api.operations.research_algorithms import blunder_full_source as source
    from chessism_api.operations.research_algorithms import blunder_development_reuse as history
    from chessism_api.operations.research_algorithms.repository import RunCancelled
    roles = {'training': [], 'validation': [], 'protected': [2]}
    policy = {'training_before': '2020-01-06T00:00:00+00:00', 'test_from': '2020-01-10T00:00:00+00:00'}
    config = {'sampling_policy': policy, 'history_sha256': history.digest(roles)}
    # Existing fixtures have two owners, below the real ten-owner prerequisite;
    # bypass selection caps ONLY here to exercise both physical SQL layouts.
    def selected(meta, policy, check=lambda: None):
        for g in meta: g['split'] = 0 if g['played_at'].isoformat() < policy['training_before'] else 1
        return meta, []
    before = await connection.fetchval('SELECT count(*) FROM game')
    outputs = []
    for layout in (None, RelationalStorage('candidate', 'archive', 'reads', 'relational_v3')):
        factory = async_sessionmaker(engine, info={'relational_storage_v2': layout})
        async with factory() as session:
            await session.execute(text('SET TRANSACTION READ ONLY'))
            with patch.object(history, 'history', AsyncMock(return_value=(roles, {2}, []))), patch.object(source, 'choose_maximum_pool', selected):
                games, frozen = await source.select(session, config, AsyncMock())
            assert {g['link'] for g in games} == {1, 3, 8}, games
            assert all('white_result' not in g for g in games)
            assert frozen['reserved_roles']['training'] == [1, 3]
            assert frozen['reserved_roles']['validation'] == [8]
            with tempfile.TemporaryDirectory() as folder:
                await source.extract(session, games, Path(folder), frozen, AsyncMock())
                with patch.object(source, 'choose_maximum_pool', selected):
                    groups, diagnostics = source.finalize_inputs(games, Path(folder), frozen, policy)
                raw = (Path(folder)/'0-blitz.bin').read_bytes()
                values = np.frombuffer(raw, dtype='<f8').reshape(-1, 5)
                assert len(values) == 44 and abs(values[:, 4].sum()-2) < 1e-12
                assert sum(g['cp_rows'] for g in groups) == 44
                assert next(g for g in groups if g['split'] == 1 and g['mode'] == 'blitz')['no_cp_games'] == 1
                assert frozen['selected_games'] == 2 and frozen['reserved_games'] == 3
                assert frozen['no_cp_games'] == 1 and frozen['balancing_order'] == 'cp_eligibility_then_both_player_cap_v2'
                outputs.append(raw)
    assert outputs[0] == outputs[1]
    await connection.execute('''CREATE TABLE IF NOT EXISTS algorithm_run(
        id text PRIMARY KEY,status text,config jsonb,result jsonb,cancel_requested bool DEFAULT false,
        created_at timestamptz DEFAULT now())''')
    run = '00000000-0000-4000-8000-000000000090'
    await connection.execute("INSERT INTO algorithm_run(id,status,config) VALUES($1,'running',$2::jsonb)", run, json.dumps({'operation': source.OPERATION}))
    reporter = AsyncMock(); reporter.run_id = run
    factory = async_sessionmaker(engine)
    await source.reserve(frozen, reporter, factory)
    await connection.execute("UPDATE algorithm_run SET status='failed' WHERE id=$1", run)
    async with factory() as session:
        assert await source.additional_roles(session) == ({1, 3}, {8})
    await connection.execute('UPDATE algorithm_run SET result=NULL,cancel_requested=true,status=\'running\' WHERE id=$1', run)
    try: await source.reserve(frozen, reporter, factory)
    except RunCancelled: pass
    else: raise AssertionError('Cancelled run reserved development identities')
    assert await connection.fetchval('SELECT result IS NULL FROM algorithm_run WHERE id=$1', run)
    assert before == await connection.fetchval('SELECT count(*) FROM game')
    await connection.execute('DELETE FROM algorithm_run WHERE id=$1', run)
    print('PASS: full-development native/legacy SQL, no test outcomes, exact CP weights, mate exclusion, durable failed reservation, cancellation, unchanged source data')
