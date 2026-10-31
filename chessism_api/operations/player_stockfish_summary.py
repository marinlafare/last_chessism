"""Durable score histograms, recomputed only for changed UTC-day partitions.

No blunder/accuracy/salience formula changes. No engine runs. Readers never
build summaries. Publication is atomic; concurrent changes cannot be lost.
"""
from datetime import timezone
import hashlib
import json
import logging
import os

from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table
from .player_analytics_summary import day_params, source_rows_sql, NULL_DAY
from .player_score_distribution import MODES, bins, games_sql, positions_sql, endings_sql
from .player_score_histogram import BATCH_SELECTION, BATCH_GAMES
from .mate_distance import mate_distance_sql

LOG = logging.getLogger(__name__)


def enabled():
    return os.environ.get('CHESSISM_STOCKFISH_SUMMARIES', '0') == '1'


def empty_modes():
    return {mode: {'counts': [0]*13, 'cp_positions': 0, 'mate_positions': 0,
                  'tablebase_positions': 0, 'unscored_positions': 0,
                  'mates': [], 'endings': {}, 'eligible_games': 0} for mode in MODES}


def combine(partitions):
    result = empty_modes()
    mates = {mode: {} for mode in MODES}
    for payload in partitions:
        for mode, row in payload.items():
            target = result[mode]
            for i, count in enumerate(row['counts']):
                target['counts'][i] += count
            for key in ('cp_positions', 'mate_positions', 'tablebase_positions', 'unscored_positions', 'eligible_games'):
                target[key] += row[key]
            for key, count in row['endings'].items():
                target['endings'][key] = target['endings'].get(key, 0) + count
            for mate in row['mates']:
                key = mate['side'], mate['moves']
                mates[mode][key] = mates[mode].get(key, 0) + mate['positions']
    for mode, values in mates.items():
        result[mode]['mates'] = [{'side': side, 'moves': moves, 'positions': count}
            for (side, moves), count in sorted(values.items(), key=lambda r: (r[0][0], r[0][1] if r[0][1] is not None else 10001))]
    return result


async def read_saved(session, player):
    table = lambda name: summary_table(storage_for(session), name)
    row = (await session.execute(text(f'''SELECT s.payload,s.revision,s.generated_at,state.revision AS current_revision
        FROM {table('snapshot')} s JOIN {table('state')} state USING(player_name)
        WHERE s.player_name=:player'''), {'player': player})).mappings().first()
    if not row:
        return {'version': 1, 'player_name': player, 'summary_pending': True, 'summary_managed': True,
                'stale': True, 'bins': bins(), 'by_mode': empty_modes(), 'snapshot': None}
    return {'version': 1, 'player_name': player, 'summary_managed': True, 'summary_pending': False,
            'stale': row['revision'] != row['current_revision'],
            'snapshot': hashlib.sha256(f'stockfish-v1:{player}:{row["revision"]}'.encode()).hexdigest(), 'bins': bins(),
            'generated_at': row['generated_at'].isoformat(), 'by_mode': row['payload'],
            'counting': 'position_occurrences', 'score_perspective': 'player', 'score_unit': 'centipawns'}


async def get_readiness(player_name):
    payload = await _get_readiness(player_name)
    from .cuda_summaries.runs import status
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        payload['cuda_summaries'] = await status(session, player=str(player_name).strip().lower())
    return payload


async def _get_readiness(player_name):
    player = str(player_name or '').strip().lower()
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        if not await session.scalar(text('SELECT 1 FROM player WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'), {'player': player}):
            raise LookupError('Player was not found.')
        if enabled():
            saved = await read_saved(session, player)
            if not saved['summary_pending']:
                return {'summary_pending': False, 'summary_stale': saved['stale'],
                    'by_mode': {mode: {'coverage': {'eligible_games': row['eligible_games']}}
                                for mode, row in saved['by_mode'].items()}}
        # Initial-backfill fallback: only existing saved coverage, never a
        # move/FEN scan or synchronous repair. Keeps Accuracy available.
        layout = storage_for(session)
        if layout:
            query = f'''SELECT m.name,COUNT(*) FROM {layout.table('game_player')} gp
                JOIN {layout.table('game')} g ON g.link=gp.game_link
                JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
                JOIN {layout.table('game_analysis_coverage')} c ON c.link=g.link
                WHERE gp.player_id=(SELECT id FROM {layout.table('player')} WHERE player_name=:player)
                AND c.is_fully_analyzed AND c.total_positions>0 GROUP BY m.name'''
        else:
            query = '''SELECT gp.mode, COUNT(*) AS games FROM game_player gp
                JOIN game_analysis_summary c ON c.link=gp.link WHERE gp.player_name=:player
                AND c.is_fully_analyzed AND c.total_positions>0 GROUP BY gp.mode'''
        rows = (await session.execute(text(query), {'player': player})).all()
        counts = dict(rows)
        return {'summary_pending': False, 'summary_stale': False,
                'by_mode': {mode: {'coverage': {'eligible_games': counts.get(mode, 0)}} for mode in MODES}}


def day_games_sql(session):
    """Use player/date indexes instead of re-reading the whole player's history per day."""
    layout = storage_for(session)
    if not layout:
        return games_sql(session) + ''' AND ((:null_day AND g.played_at IS NULL) OR
            (NOT :null_day AND g.played_at>=:start AND g.played_at<:end))'''
    branches = []
    for side, white in (('white', 'true'), ('black', 'false')):
        branches.append(f'''SELECT g.link AS game_link,{white} AS is_white,m.name AS mode,
            g.n_moves,g.white_result,g.black_result,g.white_str_result,g.black_str_result
            FROM {layout.table('game')} g JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
            WHERE g.{side}_player_id=(SELECT id FROM owner) AND m.name=ANY(CAST(:modes AS text[]))
            AND ((:null_day AND g.played_at IS NULL) OR
                (NOT :null_day AND g.played_at>=:start AND g.played_at<:end))
            AND EXISTS(SELECT 1 FROM {layout.table('game_player')} gp WHERE gp.game_link=g.link
                AND gp.player_id=(SELECT id FROM owner) AND gp.is_white={white})''')
    return f'''WITH owner AS (SELECT id FROM {layout.table('player')} WHERE player_name=:player)
        {' UNION ALL '.join(branches)}'''


async def calculate_day(session, player, day):
    selection = day_games_sql(session)
    layout = storage_for(session)
    coverage = layout.table('game_analysis_coverage') if layout else 'game_analysis_summary'
    params = {**day_params(player, day), 'modes': list(MODES)}
    by_mode = empty_modes()
    # Reuse the exact recorded-ending rules with this partition's game set.
    ending_query = str(endings_sql(session)).replace(games_sql(session), selection)
    for row in (await session.execute(text(ending_query), params)).mappings():
        by_mode[row['mode']]['endings'] = {k: int(v) for k, v in row.items() if k != 'mode'}
    for mode, count in (await session.execute(text(f'''SELECT s.mode,COUNT(*) FROM ({selection}) s
        JOIN {coverage} c ON c.link=s.game_link WHERE c.is_fully_analyzed AND c.total_positions>0 GROUP BY s.mode'''), params)):
        by_mode[mode]['eligible_games'] = int(count)
    query = text(positions_sql(session, selected_games=BATCH_SELECTION) + f'''
        SELECT mode,score_kind,bin_id,
            CASE WHEN score_kind='mate' THEN {mate_distance_sql()} END AS mate_moves,
            CASE WHEN score_kind='mate' THEN CASE WHEN player_score>0 THEN 'for' ELSE 'against' END END AS mate_side,
            COUNT(*)::bigint AS positions FROM classified
        GROUP BY mode,score_kind,bin_id,mate_moves,mate_side''')
    stream = await session.stream(text(selection), params)
    chunks = []
    try:
        async for games in stream.mappings().partitions(BATCH_GAMES):
            chunk = empty_modes()
            for row in (await session.execute(query, {'game_links': [g['game_link'] for g in games],
                'player_sides': [g['is_white'] for g in games], 'game_modes': [g['mode'] for g in games]})).mappings():
                target = chunk[row['mode']]
                count = int(row['positions'])
                target[row['score_kind'] + '_positions'] += count
                if row['score_kind'] == 'cp':
                    target['counts'][int(row['bin_id'])] += count
                elif row['score_kind'] == 'mate':
                    target['mates'].append({'moves': row['mate_moves'], 'side': row['mate_side'], 'positions': count})
            chunks.append(chunk)
    finally:
        await stream.close()
    return combine([by_mode, *chunks])


async def refresh_player(player, *, batch_days=32, session_factory=None):
    factory = session_factory or AsyncDBSession
    async with factory() as session:
        from .cuda_summaries.schema import enabled as manual_summaries
        if await manual_summaries(session):
            return {'status': 'awaiting_cuda_migration'}
        await session.rollback()
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
        await session.execute(text("SET LOCAL statement_timeout='25s'"))
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
        await session.execute(text("SET LOCAL work_mem='32MB'"))
        await session.execute(text('SET LOCAL jit=off'))
        if not await session.scalar(text('SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))'), {'key': 'stockfish-summary:' + player}):
            return {'status': 'busy'}
        table = lambda name: summary_table(storage_for(session), name)
        state = (await session.execute(text(f'SELECT * FROM {table("state")} WHERE player_name=:player'), {'player': player})).mappings().first()
        if not state or state['revision'] == state['published_revision']:
            return {'status': 'ready'}
        eligible = await session.scalar(text('SELECT 1 FROM player WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'), {'player': player})
        if not eligible:
            await session.execute(text(f'''UPDATE {table('state')} SET published_revision=revision,
                full_rebuild=true,error=NULL WHERE player_name=:player'''), {'player': player})
            for name in ('snapshot', 'partition', 'dirty'):
                await session.execute(text(f'DELETE FROM {table(name)} WHERE player_name=:player'), {'player': player})
            await session.commit()
            return {'status': 'ineligible'}
        rebuild_days = None
        if state['full_rebuild']:
            dates = (await session.execute(source_rows_sql(session, dates_only=True), day_params(player, all_days=True))).scalars()
            rebuild_days = {stamp.astimezone(timezone.utc).date() if stamp else NULL_DAY for stamp in dates}
            for name in ('partition', 'dirty'):
                rebuild_days.update((await session.execute(text(f'SELECT day FROM {table(name)} WHERE player_name=:player'), {'player': player})).scalars())
            rebuild_days = sorted(rebuild_days or {NULL_DAY})
            days = rebuild_days[:batch_days]
        else:
            days = (await session.execute(text(f'SELECT day FROM {table("dirty")} WHERE player_name=:player ORDER BY day LIMIT :limit'), {'player': player, 'limit': batch_days})).scalars().all()
        # All expensive reads precede queue locks. Producers and this worker
        # always lock state before dirty rows; ingestion never waits behind
        # several day calculations or deadlocks on a deleted dirty key.
        calculated = [(day, await calculate_day(session, player, day)) for day in days]
        await session.execute(text(f'''UPDATE {table('state')} SET full_rebuild=false,
            attempted_at=clock_timestamp(),error=NULL WHERE player_name=:player'''), {'player': player})
        # Under REPEATABLE READ that update raises on any concurrent invalidation:
        # roll back all work and retry, never publish an older revision as current.
        if rebuild_days is not None:
            await session.execute(text(f'''INSERT INTO {table('dirty')}(player_name,day) VALUES(:player,:day)
                ON CONFLICT DO NOTHING'''), [{'player': player, 'day': day} for day in rebuild_days])
        for day, payload in calculated:
            await session.execute(text(f'''INSERT INTO {table('partition')}(player_name,day,payload) VALUES(:player,:day,CAST(:payload AS jsonb))
                ON CONFLICT(player_name,day) DO UPDATE SET payload=EXCLUDED.payload'''), {'player': player, 'day': day, 'payload': json.dumps(payload)})
            await session.execute(text(f'DELETE FROM {table("dirty")} WHERE player_name=:player AND day=:day'), {'player': player, 'day': day})
        remaining = await session.scalar(text(f'SELECT COUNT(*) FROM {table("dirty")} WHERE player_name=:player'), {'player': player})
        if not remaining:
            parts = (await session.execute(text(f'SELECT payload FROM {table("partition")} WHERE player_name=:player'), {'player': player})).scalars()
            await session.execute(text(f'''INSERT INTO {table('snapshot')}(player_name,revision,payload) VALUES(:player,:revision,CAST(:payload AS jsonb))
                ON CONFLICT(player_name) DO UPDATE SET revision=EXCLUDED.revision,payload=EXCLUDED.payload,generated_at=clock_timestamp()'''),
                {'player': player, 'revision': state['revision'], 'payload': json.dumps(combine(parts))})
        await session.execute(text(f'''UPDATE {table('state')} SET attempted_at=clock_timestamp(),error=NULL,
            published_revision=CASE WHEN :ready THEN :revision ELSE published_revision END WHERE player_name=:player'''),
            {'player': player, 'revision': state['revision'], 'ready': not remaining})
        await session.commit()
        return {'status': 'ready' if not remaining else 'building', 'days': len(days), 'remaining': remaining}


async def refresh_pending():
    async with AsyncDBSession() as session:
        from .cuda_summaries.schema import enabled as manual_summaries
        if await manual_summaries(session):
            return None
        table = summary_table(storage_for(session), 'state')
        player = await session.scalar(text(f'''SELECT player_name FROM {table}
            WHERE revision<>published_revision AND (error IS NULL OR attempted_at<now()-interval '60 seconds')
            ORDER BY attempted_at NULLS FIRST,player_name LIMIT 1'''))
    if not player:
        return None
    try:
        return {'player': player, **await refresh_player(player)}
    except Exception:
        LOG.exception('Retaining previous Stockfish summary for %s', player)
        async with AsyncDBSession.begin() as session:
            await session.execute(text(f'''UPDATE {table} SET attempted_at=clock_timestamp(),
                error='Refresh will retry; previous complete summary retained.' WHERE player_name=:player'''), {'player': player})
        return {'player': player, 'status': 'retrying'}
