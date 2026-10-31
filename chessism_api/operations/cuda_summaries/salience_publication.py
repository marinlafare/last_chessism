"""Publish CUDA salience and its saved hero daily cache in one transaction."""
import json
import numpy as np
from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from ..matrix_constructor.artifact_files import file_operation
from ..player_timezone import resolve_player_timezone
from . import salience_store as store, salience_compute as compute, accuracy_store, accuracy_compute
from . import salience_daily
from .bulk import temp_rows, json_rows


async def stage(session, run, batch, parts, frequencies):
    values = await json_rows(lambda:[(int(g),float(n),int(c),int(u)) for g,n,c,u in parts])
    await temp_rows(session,'salience_parts_upload','game_player_id bigint,numerator float8,positions int,unique_positions int',
        ('game_player_id','numerator','positions','unique_positions'),values)
    await session.execute(text(f'''INSERT INTO {store.PART} SELECT :id,:batch,u.* FROM pg_temp.salience_parts_upload u'''),
        {'id':run['id'],'batch':batch})
    values = await json_rows(lambda:[tuple(map(int,r)) for r in frequencies])
    await temp_rows(session,'salience_frequency_upload','owner bigint,fen_id bigint,games bigint,occurrences bigint',
        ('owner','fen_id','games','occurrences'),values)
    await session.execute(text(f'''INSERT INTO {store.FREQUENCY} SELECT :id,u.* FROM pg_temp.salience_frequency_upload u'''),{'id':run['id']})


async def read_parts(session, run, after, limit):
    rows = (await session.execute(text(f'''WITH selected AS MATERIALIZED (
        SELECT DISTINCT game_player_id FROM {store.PART} WHERE run_id=:id AND game_player_id>:after
        ORDER BY game_player_id LIMIT :limit)
        SELECT p.game_player_id,p.numerator,p.positions,p.unique_positions FROM {store.PART} p
        JOIN selected USING(game_player_id) WHERE run_id=:id ORDER BY p.game_player_id,p.batch'''),
        {'id':run['id'],'after':after,'limit':limit})).all()
    return await file_operation(lambda:np.asarray(rows,dtype=np.float64).reshape(-1,4))


async def stage_games(session, run, rows):
    values = await json_rows(lambda:[(int(g),float(n),int(c),int(u),float(s)) for g,n,c,u,s in rows])
    await temp_rows(session,'salience_games_upload',
        'game_player_id bigint,numerator float8,positions int,unique_positions int,salience float8',
        ('game_player_id','numerator','positions','unique_positions','salience'),values)
    await session.execute(text(f'INSERT INTO {store.GAME} SELECT :id,u.* FROM pg_temp.salience_games_upload u'), {'id':run['id']})


async def publish(factory, run, xp):
    async with factory() as session:
        layout = storage_for(session); t = layout.table
        scope = (await session.execute(text(f'''SELECT * FROM {store.SCOPE} WHERE run_id=:id
            AND status='pending' ORDER BY player_id LIMIT 1'''), {'id':run['id']})).mappings().first()
        if not scope:return False
        p = {'id':run['id'],'player':scope['player_name'],'owner':scope['player_id'],'revision':scope['revision']}
        profile = (await session.execute(text(f'SELECT * FROM {t("player")} WHERE id=:owner'),p)).mappings().first()
        zone = resolve_player_timezone(dict(profile or {}))
        # Only saved game accuracy scalars are needed. Do not rerun accuracy,
        # blunders or read their position matrices to refresh daily salience.
        rows = (await session.execute(text(f'''SELECT g.salience,g.positions,source.played_at,mode.name AS mode
            FROM {store.GAME} g JOIN {t('game_player')} gp ON gp.id=g.game_player_id
            JOIN {t('game')} source ON source.link=gp.game_link
            LEFT JOIN {t('game_mode')} mode ON mode.id=source.mode_id
            WHERE g.run_id=:id AND gp.player_id=:owner'''),p)).mappings().all()
        old_accuracy = (await session.execute(text(f'SELECT * FROM {accuracy_store.STATE} WHERE player_name=:player'),p)).mappings().first()
        # Include assessed games without salience as unknown, never as zero.
        daily_rows = (await session.execute(text(f'''SELECT a.*,s.salience FROM {accuracy_store.GAME} a
            LEFT JOIN {t('game_player')} gp ON gp.game_link=a.game_link AND gp.player_id=:owner
            LEFT JOIN {store.GAME} s ON s.run_id=:id AND s.game_player_id=gp.id
            WHERE a.player_name=:player AND a.moves>10 AND a.accuracy BETWEEN 0 AND 100
                AND a.played_at IS NOT NULL'''),p)).mappings().all() if old_accuracy else []
    raw = await file_operation(lambda:np.asarray([(r['salience'],r['positions']) for r in rows],dtype=np.float64).reshape(-1,2))
    stats = await file_operation(compute.totals,xp,raw)
    salience_input = await file_operation(lambda:[{'day':zone.local_datetime(r['played_at']).date().isoformat(),
        'mode_bit':{'bullet':1,'blitz':2,'rapid':4}[r['mode']], 'salience':r['salience']}
        for r in rows if r['played_at'] and r['mode'] in ('bullet','blitz','rapid')])
    salience_days = await file_operation(salience_daily.calculate,xp,salience_input)
    daily_input = await file_operation(lambda:[{'day':zone.local_datetime(r['played_at']).date().isoformat(),
        'mode_bit':{'bullet':1,'blitz':2,'rapid':4}[r['mode']],
        'accuracy':r['accuracy'],'salience':r['salience']} for r in daily_rows])
    daily = await file_operation(accuracy_compute.daily,xp,daily_input) if old_accuracy else []
    async with factory() as session,session.begin():
        await session.execute(text("SET LOCAL lock_timeout='2s'"))
        # Same lock order as ingestion: old summary -> our revision state.
        await session.execute(text(f'SELECT player_id FROM {t("player_salience_summary")} WHERE player_id=:owner FOR UPDATE'),p)
        revision = await session.scalar(text(f'SELECT revision FROM {store.STATE} WHERE player_id=:owner FOR UPDATE'),p)
        score_revision = await session.scalar(text(f'SELECT revision FROM {summary_table(layout,"state")} WHERE player_name=:player FOR UPDATE'),p)
        valid = await session.scalar(text(f'''SELECT EXISTS(SELECT 1 FROM {t('player')} WHERE id=:owner
            AND joined>0 AND deleted_at IS NULL) AND NOT EXISTS(SELECT 1 FROM {t('game_player')} gp
            JOIN {t('game')} g ON g.link=gp.game_link WHERE gp.player_id=:owner AND NOT g.fens_done)'''),p)
        accuracy_stamp = await session.scalar(text(f'SELECT generated_at FROM {accuracy_store.STATE} WHERE player_name=:player FOR UPDATE'),p)
        if (revision or 0) != scope['revision'] or score_revision != scope['score_revision'] or not valid \
                or accuracy_stamp != (old_accuracy['generated_at'] if old_accuracy else None):
            await session.execute(text(f"UPDATE {store.SCOPE} SET status='superseded' WHERE run_id=:id AND player_id=:owner"),p)
            return True
        await session.execute(text(f'''DELETE FROM {t('game_player_salience')} s USING {t('game_player')} gp
            WHERE s.game_player_id=gp.id AND gp.player_id=:owner'''),p)
        await session.execute(text(f'''INSERT INTO {t('game_player_salience')}
            (game_player_id,salience,weighted_numerator,depth_weight_sum,position_occurrence_count,unique_position_count)
            SELECT s.game_player_id,s.salience,s.numerator,s.positions,s.positions,s.unique_positions
            FROM {store.GAME} s JOIN {t('game_player')} gp ON gp.id=s.game_player_id
            WHERE run_id=:id AND gp.player_id=:owner'''),p)
        await session.execute(text(f'DELETE FROM {t("player_position_frequency")} WHERE player_id=:owner'),p)
        await session.execute(text(f'''INSERT INTO {t('player_position_frequency')}
            (player_id,is_white,fen_id,games_with_position,total_occurrences)
            SELECT CAST(:owner AS bigint),(owner%2)=1,fen_id,games,occurrences FROM {store.FREQUENCY}
            WHERE run_id=:id AND owner IN (CAST(:owner AS bigint)*2,CAST(:owner AS bigint)*2+1)'''),p)
        await session.execute(text(f'''INSERT INTO {t('player_salience_summary')}
            (player_id,status,source_game_count,source_position_count,effective_game_count,error)
            VALUES(:owner,'ready',:source_game_count,:source_position_count,:effective_game_count,NULL)
            ON CONFLICT(player_id) DO UPDATE SET status='ready',source_game_count=EXCLUDED.source_game_count,
                source_position_count=EXCLUDED.source_position_count,effective_game_count=EXCLUDED.effective_game_count,error=NULL'''),{**p,**stats})
        await session.execute(text(f'''DELETE FROM {t('player_salience_pending_game')} s USING {t('game_player')} gp
            WHERE gp.id=s.game_player_id AND gp.player_id=:owner'''),p)
        await session.execute(text(f'DELETE FROM {store.DAILY} WHERE player_id=:owner'),p)
        values = await json_rows(lambda:[(r['day'],r['mode_mask'],json.dumps(r)) for r in salience_days])
        await temp_rows(session,'salience_bands_upload','day text,mode_mask smallint,payload jsonb',('day','mode_mask','payload'),values)
        await session.execute(text(f'''INSERT INTO {store.DAILY}(player_id,day,mode_mask,payload)
            SELECT :owner,day::date,mode_mask,payload FROM pg_temp.salience_bands_upload'''),p)
        await session.execute(text(f'''INSERT INTO {store.STATE}
            (player_id,revision,published_revision,version,daily_version,timezone,generated_at)
            VALUES(:owner,:revision,:revision,:version,:daily_version,CAST(:zone AS jsonb),clock_timestamp())
            ON CONFLICT(player_id) DO UPDATE SET published_revision=EXCLUDED.published_revision,
                version=EXCLUDED.version,daily_version=EXCLUDED.daily_version,timezone=EXCLUDED.timezone,
                generated_at=EXCLUDED.generated_at'''),
            {**p,'version':compute.VERSION,'daily_version':salience_daily.VERSION,'zone':json.dumps(zone.payload())})
        if old_accuracy:
            await session.execute(text(f'DELETE FROM {accuracy_store.DAILY} WHERE player_name=:player'),p)
            values = await json_rows(lambda:[(r['day'],r['mode_mask'],json.dumps(r)) for r in daily])
            await temp_rows(session,'salience_daily_upload','day text,mode_mask smallint,payload jsonb',('day','mode_mask','payload'),values)
            await session.execute(text(f'''INSERT INTO {accuracy_store.DAILY}(player_name,day,mode_mask,payload)
                SELECT :player,day::date,mode_mask,payload FROM pg_temp.salience_daily_upload'''),p)
            await session.execute(text(f'''UPDATE {accuracy_store.STATE} SET timezone=CAST(:zone AS jsonb),
                generated_at=clock_timestamp() WHERE player_name=:player'''),{**p,'zone':json.dumps(zone.payload())})
        await session.execute(text(f"UPDATE {store.SCOPE} SET status='published' WHERE run_id=:id AND player_id=:owner"),p)
    return True
