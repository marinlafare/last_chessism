"""Disposable-only combined cohort stream, artifacts and checkpoint safety."""
from copy import deepcopy
import gzip
import json
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock, patch


async def verify(connection, engine):
    import numpy as np
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
    from chessism_api.operations.research_algorithms import blunder_cohort_job as job, blunder_cohort_source as source
    from chessism_api.operations.research_algorithms.repository import RunCancelled
    from tests.test_blunder_shadow import FITS
    saved = await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
    for schema in ('public', 'native'):
        await connection.execute(f'''CREATE TABLE {schema}.player_stockfish_state(player_name text PRIMARY KEY,revision bigint,published_revision bigint);
            INSERT INTO {schema}.player_stockfish_state VALUES('alice',1,1),('bob',1,1);
            UPDATE {schema}.game SET white_elo=0 WHERE link=3;''')
    outputs = []
    for i, layout in enumerate((None, RelationalStorage('native', 'archive', 'reads', 'relational_v3'))):
        factory = async_sessionmaker(engine, info={SESSION_KEY: layout})
        async with factory() as session:
            await source.prepare(session); inventory = await source.inventory(session)
        assert len(inventory) == 2 and all(sum(g['unsupported_rating_games'] for g in p['scope']) == 1 for p in inventory)
        assert all(sum(g['eligible_games'] for g in p['scope']) == 2 for p in inventory)
        run_id = f'cohort-fixture-{i}'
        await connection.execute("INSERT INTO algorithm_run(id,status) VALUES($1,'running')", run_id)
        reporter = AsyncMock(); reporter.run_id = run_id
        config = {'model_id': 'frozen', 'parent_report_sha256': 'f'*64, 'threshold': .15}
        result = {'format': job.CHECKPOINT, 'config': config, 'inventory': inventory, 'players': []}
        with tempfile.TemporaryDirectory(dir='/restore-test', prefix='cohort-') as directory:
            work = Path(directory)/'algorithms'/'tmp'; work.mkdir(parents=True)
            folder = work/'test'; folder.mkdir()
            with patch.object(job, 'AsyncDBSession', factory), patch.object(job, 'WORK_ROOT', work):
                await job.checkpoint(reporter, result, 0)
                runner = job.CohortRunner(np, FITS, config, reporter, folder, result)
                try:
                    for player in inventory: await runner.read_player(player)
                    # Both players were accumulated into ONE GPU contract batch.
                    assert not result['players']
                    await runner.flush()
                finally: runner.close()
                assert len(result['players']) == 2 and len(result['gpu_batches']) == 1
                assert result['gpu_batches'][0]['players'] == ['alice','bob']
                assert not any(result['checks'].values())
                assert sum(p['games'] for p in result['players']) == 4
                for player in result['players']:
                    path = work.parent/player['artifact']['relative_path']
                    assert job.file_sha(path) == player['artifact']['sha256']
                    with gzip.open(path, 'rt') as stream: records = [json.loads(line) for line in stream]
                    assert len(records) == 3 and records[0]['threshold'] == .15
                    assert all(r['player'] == player['player'] for r in records)
                    assert player['saved_histogram_agreement']
                original = json.loads(await connection.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', run_id))
                assert original == result
                async def crash(): raise RuntimeError('injected checkpoint failure')
                try: await job.checkpoint(reporter, result, 2, before_commit=crash)
                except RuntimeError: pass
                else: raise AssertionError('Missing checkpoint rollback')
                assert json.loads(await connection.fetchval('SELECT result::text FROM algorithm_run WHERE id=$1', run_id)) == original
                try: await job.checkpoint(reporter, result, 0)
                except ValueError: pass
                else: raise AssertionError('Stale checkpoint accepted')
                await connection.execute('UPDATE algorithm_run SET cancel_requested=true WHERE id=$1', run_id)
                try: await job.checkpoint(reporter, result, 2)
                except RunCancelled: pass
                else: raise AssertionError('Cancelled checkpoint accepted')
            outputs.append([{'player': p['player'], 'scope': p['scope'], 'by_mode': p['by_mode'],
                'current': p['current_ui_by_mode'], 'paired': p['paired_saved_by_mode']} for p in result['players']])
    assert outputs[0] == outputs[1], 'Native and legacy full cohort differ'
    for schema in ('public', 'native'):
        await connection.execute(f'UPDATE {schema}.game SET white_elo=1500 WHERE link=3')
    assert saved == await connection.fetch('SELECT * FROM game_player_engine_summary ORDER BY game_link,player_color')
    print('PASS: full native/legacy histories, shared-player GPU batch, unsupported-rating exclusions, saved histograms, artifact checksums, atomic checkpoints/rollback/cancel guards; live fixtures unchanged')
