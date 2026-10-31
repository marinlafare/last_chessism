"""Disposable database only: source parity, atomic updates and crash recovery."""
import io
import json
from unittest.mock import AsyncMock


async def verify(connection, engine):
    import chess
    import numpy as np
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
    from chessism_api.operations.research_algorithms import blunder_shadow_source as source
    from chessism_api.operations.research_algorithms import blunder_shadow_compute as compute
    from chessism_api.operations.research_algorithms import blunder_shadow_store as store
    from chessism_api.operations.research_algorithms.blunder_fixed import DTYPE
    from chessism_api.operations.research_algorithms.repository import RunCancelled
    from tests.test_blunder_shadow import FITS, record
    factory = async_sessionmaker(engine)
    saved_live = await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
    await connection.execute("ALTER TABLE game ADD COLUMN rules text DEFAULT 'chess',ADD COLUMN initial_setup text; ALTER TABLE fen ADD COLUMN fen text; ALTER TABLE native.fen ADD COLUMN fen text;")
    board = chess.Board()
    for i, san in enumerate(('e4', 'e5', 'Nf3', 'Nc6', 'Bb5', 'a6', 'Ba4', 'Nf6'), 1):
        board.push_san(san)
        await connection.execute('UPDATE fen SET fen=$1 WHERE fen_id=$2', board.fen(), i)
        await connection.execute('UPDATE native.fen SET fen=$1 WHERE fen_id=$2', board.fen(), i)
    await connection.execute('''ALTER TABLE native.game ADD COLUMN white_elo int,ADD COLUMN black_elo int,ADD COLUMN played_at timestamptz;
        UPDATE native.game n SET white_elo=g.white_elo,black_elo=g.black_elo,played_at=g.played_at FROM game g WHERE g.link=n.link;
        CREATE TABLE algorithm_run(id text PRIMARY KEY,status text,cancel_requested boolean DEFAULT false,result jsonb);''')
    outputs, calculated = [], []
    for layout in (None, RelationalStorage('native', 'archive', 'reads', 'relational_v3')):
        selected_factory = async_sessionmaker(engine, info={SESSION_KEY: layout})
        async with selected_factory() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
            games = await source.select_games(session, ['alice', 'bob'], AsyncMock())
            stream = io.BytesIO()
            n, _ = await source.extract(session, games, AsyncMock(), stream)
            rows = np.frombuffer(stream.getvalue(), dtype=DTYPE)
            assert len(games) == 3 and n == 12
            result = compute.calculate(np, rows, games, FITS)
            assert result['status'] == 'passed', result['checks']
            await source.attach_examples(session, result['examples'])
            assert all(r['legal_transition_verified'] for r in result['examples'])
            outputs.append((stream.getvalue(), games))
            calculated.append(result)
    assert outputs[0] == outputs[1], 'Native and legacy shadow input differ'
    # Real source-score update inside the isolated fixture: refresh the actual
    # legacy writer, recompute the frozen candidate, replace only its game.
    actual_id = 'shadow-score-update'
    await connection.execute("INSERT INTO algorithm_run(id,status) VALUES($1,'running')", actual_id)
    actual_reporter = AsyncMock(); actual_reporter.run_id = actual_id
    before_update, _ = await store.publish(actual_reporter, 'actual-model', calculated[0]['records'], 0, session_factory=factory)
    await connection.execute("INSERT INTO fen(fen_id,score,analysis_source,fen) SELECT 99,-500,'stockfish',fen FROM fen WHERE fen_id=1; UPDATE game_fen_association SET fen_id=99 WHERE game_link=1 AND n_move=1 AND move_color='white';")
    from unittest.mock import patch
    from chessism_api.operations import player_game_scores as production
    with patch.object(production, 'AsyncDBSession', factory):
        await production.refresh_game_player_engine_summaries((1,))
    async with factory() as session:
        games = await source.select_games(session, ['alice', 'bob'], AsyncMock())
        stream = io.BytesIO(); await source.extract(session, games, AsyncMock(), stream)
        recomputed = compute.calculate(np, np.frombuffer(stream.getvalue(), dtype=DTYPE), games, FITS)
    assert recomputed['status'] == 'passed'
    changed = [r for r in recomputed['records'] if r not in calculated[0]['records']]
    assert len(changed) == 1 and changed[0]['game'] == 1
    after_update, update_info = await store.publish(actual_reporter, 'actual-model', changed, 1, session_factory=factory)
    rebuilt, _ = store.merge(None, recomputed['records'], 'actual-model')
    assert after_update['groups'] == rebuilt['groups'] and update_info['changed_games'] == 1
    await connection.execute("UPDATE game_fen_association SET fen_id=1 WHERE game_link=1 AND n_move=1 AND move_color='white'; DELETE FROM fen WHERE fen_id=99;")
    with patch.object(production, 'AsyncDBSession', factory):
        await production.refresh_game_player_engine_summaries((1,))
    run_id = 'shadow-test'
    await connection.execute("INSERT INTO algorithm_run(id,status) VALUES($1,'running')", run_id)
    reporter = AsyncMock(); reporter.run_id = run_id
    initial, _ = await store.publish(reporter, 'm', [record()], 0, session_factory=factory)
    retry, info = await store.publish(reporter, 'm', [record()], 1, session_factory=factory)
    assert retry == initial and info['changed_games'] == 0
    async def crash(): raise RuntimeError('simulated crash before commit')
    try: await store.publish(reporter, 'm', [record(2, 2)], 1, session_factory=factory, before_commit=crash)
    except RuntimeError as error: assert 'simulated crash' in str(error)
    else: raise AssertionError('Injected failure did not stop publication')
    reloaded = json.loads(await connection.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', run_id))
    assert reloaded == initial, 'Failed publication damaged prior snapshot'
    # A restarted caller reloads generation 1 and retries exactly the pending batch.
    recovered, _ = await store.publish(reporter, 'm', [record(2, 2)], reloaded['generation'], session_factory=factory)
    assert len(recovered['records']) == 2 and recovered['generation'] == 2
    # A synthetic changed FEN causes only its game record to be replaced.
    updated, info = await store.publish(reporter, 'm', [record(1, 3)], 2, session_factory=factory)
    assert info['changed_games'] == 1 and updated['groups'][0]['candidate20'] == 5
    assert updated['groups'][0]['games'] == 2
    try: await store.publish(reporter, 'm', [record(1, 2)], 1, session_factory=factory)
    except ValueError as error: assert 'Stale' in str(error)
    else: raise AssertionError('Stale update overwrote a newer generation')
    await connection.execute('UPDATE algorithm_run SET cancel_requested=true WHERE id=$1', run_id)
    try: await store.publish(reporter, 'm', [record(1, 4)], 3, session_factory=factory)
    except RunCancelled: pass
    else: raise AssertionError('Cancelled update was published')
    assert json.loads(await connection.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', run_id)) == updated
    assert saved_live == await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
    print('PASS: shadow native/legacy source, saved/SQL/scalar parity, legal example boards, retry idempotency, one-game replacement, rollback, restarted-caller recovery, stale/cancel guards and unchanged live tables')
