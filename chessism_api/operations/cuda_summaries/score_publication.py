"""Atomic per-player publication after all shared batches have been staged."""
import json
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from ..player_stockfish_summary import empty_modes
from ..matrix_constructor.artifact_files import file_operation
from . import score_runs as scope, score_compute
from . import accuracy_store


async def finalize(factory, run, xp):
    async with factory() as session:
        layout = storage_for(session); t = lambda n: summary_table(layout, n)
        row = (await session.execute(text(f'''SELECT * FROM {scope.SCOPE} WHERE run_id=:id AND status='pending'
            ORDER BY player_name LIMIT 1'''), {'id': run['id']})).mappings().first()
        if not row:
            await session.rollback()
            from . import salience_worker
            if await salience_worker.tick(factory,run,xp):
                return {'status':'building','remaining':1}
            from . import metadata_worker
            if await metadata_worker.tick(factory,run,xp):
                return {'status':'building','remaining':1}
            async with session.begin():
                await scope.complete(session, run)
            return {'status': 'ready', 'remaining': 0}
        params = {'id': run['id'], 'player': row['player_name'], 'revision': row['revision']}
        days = (await session.execute(text(f'SELECT day::text FROM {scope.DAY} WHERE run_id=:id AND player_name=:player'), params)).scalars().all()
        entries = [(r[0], r[1]) for r in (await session.execute(text(f'''SELECT day::text,payload FROM {scope.PART}
            WHERE run_id=:id AND player_name=:player ORDER BY day,batch'''), params)).all()]
        entries.extend((day, empty_modes()) for day in days)
        existing = [] if row['full_rebuild'] else (await session.execute(text(f'''SELECT payload FROM {t('partition')} p
            WHERE player_name=:player AND NOT EXISTS(SELECT 1 FROM {scope.DAY} d
                WHERE d.run_id=:id AND d.player_name=p.player_name AND d.day=p.day)'''), params)).scalars().all()
        accuracy = await accuracy_store.prepare(session, run, row, xp) if 'accuracy' in run['metrics'] else None
        from . import blunder_summary
        blunders = await blunder_summary.prepare(session,run,row['player_name'],xp)
    partitions = await file_operation(score_compute.reduce, xp, entries)
    totals = await file_operation(score_compute.reduce, xp,
        [('total', payload) for payload in [*existing, *partitions.values(), empty_modes()]])
    async with factory() as session, session.begin():
        await session.execute(text("SET LOCAL lock_timeout='2s'"))
        revision = await session.scalar(text(f'SELECT revision FROM {t("state")} WHERE player_name=:player FOR UPDATE'), params)
        preparing = await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {layout.table('fen_summary_refresh')} f
            JOIN {layout.table('game_player')} gp ON gp.game_link=f.game_link
            JOIN {layout.table('player')} p ON p.id=gp.player_id WHERE p.player_name=:player)'''), params)
        if revision != row['revision'] or preparing:
            await session.execute(text(f"UPDATE {scope.SCOPE} SET status='superseded' WHERE run_id=:id AND player_name=:player"), params)
            return {'status': 'building', 'remaining': 1}
        eligible = await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {layout.table('player')}
            WHERE player_name=:player AND joined>0 AND deleted_at IS NULL)'''), params)
        if accuracy is not None:
            await accuracy_store.publish(session, run, row, accuracy, eligible)
        if eligible:
            await blunder_summary.publish(session,run,row['player_name'],blunders)
        if not eligible or row['full_rebuild']:
            await session.execute(text(f'DELETE FROM {t("partition")} WHERE player_name=:player'), params)
        if not eligible:
            await session.execute(text(f'DELETE FROM {t("snapshot")} WHERE player_name=:player'), params)
        else:
            if partitions:
                from .bulk import temp_rows,json_rows
                values = await json_rows(lambda: [(day,json.dumps(payload)) for day,payload in partitions.items()])
                await temp_rows(session,'cuda_partitions_upload','day text,payload jsonb',('day','payload'),values)
                await session.execute(text(f'''INSERT INTO {t('partition')}(player_name,day,payload)
                    SELECT :player,day::date,payload FROM pg_temp.cuda_partitions_upload
                    ON CONFLICT(player_name,day) DO UPDATE SET payload=EXCLUDED.payload'''),params)
            await session.execute(text(f'''INSERT INTO {t('snapshot')}(player_name,revision,payload)
                VALUES(:player,:revision,CAST(:payload AS jsonb)) ON CONFLICT(player_name) DO UPDATE SET
                revision=EXCLUDED.revision,payload=EXCLUDED.payload,generated_at=clock_timestamp()'''),
                {**params, 'payload': json.dumps(totals['total'])})
        await session.execute(text(f'DELETE FROM {t("dirty")} WHERE player_name=:player'), params)
        await session.execute(text(f'''UPDATE {t('state')} SET published_revision=:revision,full_rebuild=:rebuild,
            attempted_at=clock_timestamp(),error=NULL WHERE player_name=:player'''), {**params, 'rebuild': not eligible})
        await session.execute(text(f"UPDATE {scope.SCOPE} SET status='published' WHERE run_id=:id AND player_name=:player"), params)
    return {'status': 'building', 'remaining': 1}
