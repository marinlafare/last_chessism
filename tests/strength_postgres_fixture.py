"""Private PostgreSQL only: shared sample lookup, reservations and no live writes."""
import io
import json
from unittest.mock import AsyncMock, patch


async def verify(connection, engine):
    import numpy as np
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
    from chessism_api.operations.research_algorithms import blunder_strength_source as source, blunder_strength_compute as compute
    from chessism_api.operations.research_algorithms import blunder_shadow_source as extraction, blunder_cohort_compute as cohort
    from chessism_api.operations.research_algorithms.blunder_fixed import DTYPE
    from chessism_api.operations.research_algorithms.repository import RunCancelled
    from tests.test_blunder_shadow import FITS
    from sqlalchemy import text
    saved = await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
    await connection.execute('''ALTER TABLE algorithm_run ADD COLUMN config jsonb DEFAULT '{}',ADD COLUMN created_at timestamptz DEFAULT now();
        ALTER TABLE native.game ADD COLUMN white_player_id bigint,ADD COLUMN black_player_id bigint,
            ADD COLUMN white_result float8,ADD COLUMN black_result float8;
        UPDATE native.game n SET white_player_id=w.id,black_player_id=b.id,white_result=g.white_result,black_result=g.black_result
            FROM game g JOIN native.player w ON w.player_name=g.white JOIN native.player b ON b.player_name=g.black WHERE n.link=g.link;''')
    outputs = []
    for i, layout in enumerate((None, RelationalStorage('native', 'archive', 'reads', 'relational_v3'))):
        factory = async_sessionmaker(engine, info={SESSION_KEY: layout})
        async with factory() as session:
            await source.cohort.prepare(session)
            initial = await extraction.select_games(session, ['alice', 'bob'], AsyncMock())
            stream = io.BytesIO(); await extraction.extract(session, initial, AsyncMock(), stream)
            original_rows = np.frombuffer(stream.getvalue(), dtype=DTYPE)
            selection = cohort.calculate(np, original_rows, initial, FITS)['records']
            with patch.object(source, 'held_player', return_value=False):
                games, inspected, dropped = await source.load_metadata(session, selection, AsyncMock())
            assert len(games) == 3 and not dropped and inspected == [1, 2, 3]
            assert all(g['outcome'] == (g['white_result'] if g['is_white'] else g['black_result']) for g in games)
            mixed = io.BytesIO(); await extraction.extract(session, games, AsyncMock(), mixed)
            result = compute.calculate(np, np.frombuffer(mixed.getvalue(), dtype=DTYPE), games, FITS)
            assert result['status'] == 'passed', result['checks']
            await extraction.attach_examples(session, result['examples'])
            assert all(e['legal_transition_verified'] for e in result['examples'])
            outputs.append((result['records'], result['groups']))
            with patch.object(source, 'held_player', return_value=True):
                try: await source.load_metadata(session, selection, AsyncMock())
                except ValueError as e: assert 'empty' in str(e)
                else: raise AssertionError('Held-out players leaked into development')
        run_id = f'strength-fixture-{i}'
        await connection.execute("INSERT INTO algorithm_run(id,status,config) VALUES($1,'running',$2::jsonb)", run_id, json.dumps({'operation': source.OPERATION}))
        frozen = {'inspected_game_ids': [1, 2, 3], 'inspected_game_ids_sha256': source.history.digest([1, 2, 3])}
        reporter = AsyncMock(); reporter.run_id = run_id
        with patch('chessism_api.database.engine.AsyncDBSession', factory):
            await source.reserve(frozen, reporter)
            await connection.execute("UPDATE algorithm_run SET status='failed' WHERE id=$1", run_id)
            async with factory() as session: assert await source.exploration_ids(session) == {1, 2, 3}
            try: await source.reserve(frozen, reporter)
            except RunCancelled: pass
            else: raise AssertionError('Terminal run reservation overwritten')
    assert outputs[0] == outputs[1], 'Native/legacy strength comparison differs'
    assert saved == await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
    print('PASS: shared native/legacy sampling, all eight candidates, legal examples, durable failed-run exclusions, held-player guard; production fixtures unchanged')
