"""Player-perspective histograms and saved-frequency or bounded FEN pages.

Histograms and rankings use saved CUDA summaries when available. Before the
first frequency publication, popups retain bounded matching examples. Requests
never count or rank the raw occurrence corpus.
"""
import asyncio
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.relational_storage import storage_for
from chessism_api.operations.player_score_histogram import BATCH_SELECTION, read_histogram_batches
from chessism_api.operations.player_score_position_page import read_position_page

MODES = ('bullet', 'blitz', 'rapid')
CACHE_SECONDS = 900
HISTOGRAM_RETENTION_SECONDS = 86400
PAGE_SIZE = 10
READ_SLOTS = asyncio.Semaphore(1)
POSITION_READ_SLOTS = asyncio.Semaphore(1)
IN_FLIGHT = {}
PROGRESS = {}
MAX_IN_FLIGHT = 8
CALCULATION_SECONDS = 55
HISTOGRAM_CALCULATION_SECONDS = 240
LOG = logging.getLogger(__name__)


class ScoreDistributionBusy(RuntimeError):
    pass


class ScoreDistributionChanged(RuntimeError):
    pass


def bins():
    result = [{'id': 0, 'lower': None, 'upper': -550, 'lower_inclusive': False, 'upper_inclusive': False, 'label': '< −550'}]
    for index in range(1, 6):
        lower = -650 + index * 100
        result.append({'id': index, 'lower': lower, 'upper': lower + 100,
                       'lower_inclusive': True, 'upper_inclusive': False, 'label': str(lower + 50)})
    result.append({'id': 6, 'lower': -50, 'upper': 50, 'lower_inclusive': True, 'upper_inclusive': True, 'label': '0'})
    for index in range(7, 12):
        lower = 50 + (index - 7) * 100
        result.append({'id': index, 'lower': lower, 'upper': lower + 100,
                       'lower_inclusive': False, 'upper_inclusive': True, 'label': f'+{lower + 50}'})
    result.append({'id': 12, 'lower': 550, 'upper': None, 'lower_inclusive': False, 'upper_inclusive': False, 'label': '> +550'})
    return result


def bin_sql(column):
    # Zero is a real score. The two central boundary values belong to bin 6.
    return f"""CASE WHEN {column} < -550 THEN 0 WHEN {column} > 550 THEN 12
        WHEN {column} BETWEEN -50 AND 50 THEN 6
        WHEN {column} < -50 THEN FLOOR(({column} + 550) / 100.0)::int + 1
        ELSE CEIL(({column} - 50) / 100.0)::int + 6 END"""


def normalize(player_name, modes=MODES):
    player = str(player_name or '').strip().lower()
    if not player or len(player) > 128:
        raise ValueError('A player name of 1–128 characters is required.')
    if not modes or set(modes) - set(MODES):
        raise ValueError('Select bullet, blitz and/or rapid.')
    return player, tuple(mode for mode in MODES if mode in modes)


def games_sql(session):
    layout = storage_for(session)
    if layout is None:
        return """SELECT gp.link AS game_link, gp.color='white' AS is_white,
            gp.mode, g.n_moves, g.white_result, g.black_result,
            g.white_str_result, g.black_str_result
            FROM game_player gp JOIN game g ON g.link=gp.link
            WHERE gp.player_name=:player AND gp.mode=ANY(CAST(:modes AS text[]))"""
    return f"""SELECT gp.game_link, gp.is_white, m.name AS mode,
        g.n_moves, g.white_result, g.black_result, g.white_str_result, g.black_str_result
        FROM {layout.table('game_player')} gp
        JOIN {layout.table('game')} g ON g.link=gp.game_link
        JOIN {layout.table('game_mode')} m ON m.id=g.mode_id
        WHERE gp.player_id=(SELECT id FROM {layout.table('player')} WHERE player_name=:player)
          AND m.name=ANY(CAST(:modes AS text[]))"""


def positions_sql(session, *, selected_games=None):
    layout = storage_for(session)
    association = layout.table('game_fen_association') if layout else 'game_fen_association'
    fen = layout.table('fen') if layout else 'fen'
    return f"""WITH selected_games AS MATERIALIZED ({selected_games or games_sql(session)}),
        occurrences AS (
            SELECT g.mode, g.is_white, a.fen_id
            FROM selected_games g JOIN {association} a ON a.game_link=g.game_link
        ), scored AS (
            SELECT o.*, f.analysis_source, CASE WHEN o.is_white THEN f.score ELSE -f.score END AS player_score,
                CASE WHEN f.score IS NULL THEN 'unscored'
                     WHEN f.analysis_source='tablebase' THEN 'tablebase'
                     WHEN ABS(f.score)>=9000 THEN 'mate' ELSE 'cp' END AS score_kind
            FROM occurrences o JOIN {fen} f ON f.fen_id=o.fen_id
        ), classified AS (
            SELECT *, CASE WHEN score_kind='cp' THEN {bin_sql('player_score')} END AS bin_id
            FROM scored
        )"""


def endings_sql(session):
    layout = storage_for(session)
    moves = layout.table('moves') if layout else 'moves'
    # Only recorded stalemates need a move lookup. The final full-move number
    # must match the game; '--' is the ingestion pipeline's absent black ply.
    return text(f"""WITH selected_games AS MATERIALIZED ({games_sql(session)}),
        outcomes AS (
            SELECT *,
                CASE WHEN is_white THEN white_result ELSE black_result END AS result,
                LOWER(CASE WHEN is_white THEN white_str_result ELSE black_str_result END) AS own_reason,
                LOWER(CASE WHEN is_white THEN black_str_result ELSE white_str_result END) AS opponent_reason,
                COALESCE(LOWER(white_str_result)='stalemate', FALSE)
                    OR COALESCE(LOWER(black_str_result)='stalemate', FALSE) AS stalemate
            FROM selected_games
        )
        SELECT g.mode, COUNT(*)::bigint AS games,
            COUNT(*) FILTER (WHERE g.result=1 AND g.opponent_reason IN ('checkmate','checkmated'))::bigint AS checkmates_for,
            COUNT(*) FILTER (WHERE g.result=0 AND g.own_reason IN ('checkmate','checkmated'))::bigint AS checkmates_against,
            COUNT(*) FILTER (WHERE g.result=0.5 AND g.stalemate AND last_move.is_white=g.is_white)::bigint AS stalemates_by_player,
            COUNT(*) FILTER (WHERE g.result=0.5 AND g.stalemate AND last_move.is_white<>g.is_white)::bigint AS stalemates_by_opponent,
            COUNT(*) FILTER (WHERE g.result=0.5 AND g.stalemate AND last_move.is_white IS NULL)::bigint AS stalemates_unattributed,
            COUNT(*) FILTER (WHERE g.result=0.5 AND NOT g.stalemate)::bigint AS other_draws
        FROM outcomes g LEFT JOIN LATERAL (
            SELECT CASE
                WHEN LOWER(BTRIM(m.white_move)) NOT IN ('','--','none','null') THEN
                    CASE WHEN LOWER(BTRIM(m.black_move)) NOT IN ('','--','none','null') THEN FALSE
                         WHEN BTRIM(m.black_move)='--' THEN TRUE END
                END AS is_white
            FROM {moves} m WHERE g.stalemate AND g.result=0.5
                AND m.link=g.game_link AND m.n_move=g.n_moves AND g.n_moves>0 LIMIT 1
        ) last_move ON TRUE GROUP BY g.mode
    """)


async def prepare(session):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='25s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL work_mem='64MB'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=2'))
    await session.execute(text('SET LOCAL jit=off'))


async def source_state(session, player):
    exists = await session.scalar(text('SELECT player_name FROM player WHERE player_name=:player AND deleted_at IS NULL'), {'player': player})
    if not exists:
        raise LookupError(f'Player {player!r} was not found.')
    # A totals refresh changes refreshed_at even when no data changed. Do not
    # throw away expensive analytics (or reject an open list) for that event.
    counts = (await session.execute(text('''SELECT n_games_in_db, n_positions,
        analyzed_fens, unscored_fens, nonzero_scored_fens
        FROM database_summary WHERE id=1'''))).first()
    layout = storage_for(session)
    fen = layout.table('fen') if layout else 'fen'
    # Uses the existing partial (analyzed_at, fen_id) backup index. This also
    # detects re-analysis where the number of scored positions stays constant.
    stamp = await session.scalar(text(f'''SELECT analyzed_at FROM {fen}
        WHERE score IS NOT NULL AND analyzed_at IS NOT NULL
        ORDER BY analyzed_at DESC, fen_id DESC LIMIT 1'''))
    values = [list(counts) if counts else None, stamp.isoformat() if stamp else None]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def key_for(kind, *values):
    return 'chessism:score-distribution:v2:' + kind + ':' + hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


async def read_cache(redis, key):
    try:
        raw = await redis.get(key)
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict) and data.get('version') == 1:
                return data
    except (RedisError, ValueError, TypeError):
        LOG.warning('Score distribution cache unavailable; using database reads.')
    return None


async def save_cache(redis, key, value, *, seconds=CACHE_SECONDS):
    try:
        await redis.set(key, json.dumps(value), ex=seconds)
    except RedisError:
        LOG.warning('Could not cache score distribution results.')


@asynccontextmanager
async def read_slot(*, wait_seconds=30, slots=None):
    slots = READ_SLOTS if slots is None else slots
    try:
        await asyncio.wait_for(slots.acquire(), timeout=wait_seconds)
    except asyncio.TimeoutError as error:
        raise ScoreDistributionBusy('Position calculations are busy. Please retry shortly.') from error
    try:
        yield
    except DBAPIError as error:
        if getattr(error.orig, 'sqlstate', None) in ('57014', '55P03'):
            raise ScoreDistributionBusy('Position calculation took too long. Please retry shortly.') from error
        raise
    finally:
        slots.release()


async def shared_calculation(key, factory, *, timeout=None, wait_seconds=None):
    """A React remount/retry must not queue behind its own abandoned request.

    Keep a strong reference and shield shared work from one disconnected
    caller. Its own deadline bounds both queue time and database work.
    """
    task = IN_FLIGHT.get(key)
    if task is None:
        if sum(not pending.done() for pending in IN_FLIGHT.values()) >= MAX_IN_FLIGHT:
            raise ScoreDistributionBusy('Position calculations are busy. Please retry shortly.')

        async def run():
            try:
                return await asyncio.wait_for(factory(), timeout=timeout or CALCULATION_SECONDS)
            except asyncio.TimeoutError as error:
                raise ScoreDistributionBusy('Position calculation took too long. Please retry shortly.') from error

        task = asyncio.create_task(run())
        IN_FLIGHT[key] = task
        if wait_seconds is not None:
            PROGRESS[key] = {'phase': 'queued', 'completed_games': 0, 'total_games': None}

        def forget(done):
            if IN_FLIGHT.get(key) is done:
                IN_FLIGHT.pop(key, None)
                PROGRESS.pop(key, None)

        def finished(done):
            # A client may have disconnected while the calculation finished.
            error = None if done.cancelled() else done.exception()
            if wait_seconds is None:
                forget(done)
            else:
                PROGRESS[key]['phase'] = 'failed' if error or done.cancelled() else 'complete'
                # Keep the finished result long enough for the next poll,
                # including when incoming scores immediately change freshness.
                asyncio.get_running_loop().call_later(5 if error else 60, forget, done)

        task.add_done_callback(finished)
    if wait_seconds is None:
        return await asyncio.shield(task)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=wait_seconds)
    except asyncio.TimeoutError:
        return None


def histogram_is_fresh(payload, snapshot):
    if not payload or payload.get('snapshot') != snapshot:
        return False
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(payload['generated_at'])).total_seconds()
        return 0 <= age < CACHE_SECONDS
    except (KeyError, ValueError, TypeError):
        return False


async def get_distribution(player_name, redis, *, refresh=False):
    player, modes = normalize(player_name)
    from .player_stockfish_summary import enabled, read_saved
    from .cuda_summaries.schema import enabled as manual_summaries
    async with AsyncDBSession() as session:
        manual = await manual_summaries(session)
    if enabled() or manual:
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            if not await session.scalar(text('SELECT 1 FROM player WHERE player_name=:player AND joined>0 AND deleted_at IS NULL'), {'player': player}):
                raise LookupError('Player was not found.')
            saved = await read_saved(session, player)
        if saved['summary_pending']:
            # Preserve a previously viewed chart during the one-time backfill.
            # Missing snapshots never start the old expensive calculation.
            cached = await read_cache(redis, key_for('histogram', player))
            if cached:
                return {**cached, 'summary_managed': True, 'summary_pending': False,
                        'stale': True, 'mate_distances_pending': True}
        return saved
    async with AsyncDBSession() as session:
        await prepare(session)
        snapshot = await source_state(session, player)
    key = key_for('histogram', player)
    cached = await read_cache(redis, key)
    fresh = histogram_is_fresh(cached, snapshot)
    # Serve the last complete chart immediately. The UI explicitly requests
    # a refresh in the background, keeping those bars usable in the meantime.
    if cached and (fresh or not refresh):
        return {**cached, 'stale': not fresh}
    payload = await shared_calculation(key, lambda: calculate_distribution(player, modes, redis),
        timeout=HISTOGRAM_CALCULATION_SECONDS, wait_seconds=0.2)
    if payload is not None:
        return payload
    return {'status': 'calculating', 'player_name': player, 'retry_after_seconds': 1,
            'progress': dict(PROGRESS.get(key, {}))}


async def calculate_distribution(player, modes, redis):
    from .cuda_summaries.schema import enabled as manual_summaries
    from .player_stockfish_summary import read_saved
    async with AsyncDBSession() as session:
        if await manual_summaries(session):
            return await read_saved(session, player)
    async with read_slot(wait_seconds=120):
        async with AsyncDBSession() as session:
            await prepare(session)
            snapshot = await source_state(session, player)
            key = key_for('histogram', player)
            cached = await read_cache(redis, key)
            if histogram_is_fresh(cached, snapshot):
                return {**cached, 'stale': False}
            params = {'player': player, 'modes': list(modes)}
            # There are at most 48 groups. Avoid sorting millions of position
            # occurrences into an on-disk merge just to produce those groups.
            await session.execute(text('SET LOCAL enable_sort=off'))
            endings = (await session.execute(endings_sql(session), params)).mappings().all()
            total_games = sum(int(row['games']) for row in endings)
            rows = await read_histogram_batches(
                session, games_sql(session), positions_sql(session, selected_games=BATCH_SELECTION),
                params, total_games, lambda progress: PROGRESS.__setitem__(key, progress))
        by_mode = {mode: {'counts': [0] * 13, 'cp_positions': 0, 'mate_positions': 0,
                          'tablebase_positions': 0, 'unscored_positions': 0, 'endings': {}} for mode in MODES}
        for row in rows:
            target = by_mode[row['mode']]
            total = int(row['positions'])
            target[row['score_kind'] + '_positions'] += total
            if row['score_kind'] == 'cp':
                target['counts'][int(row['bin_id'])] += total
        for row in endings:
            by_mode[row['mode']]['endings'] = {key: int(value) for key, value in row.items() if key != 'mode'}
        payload = {'version': 1, 'player_name': player, 'snapshot': snapshot,
                   'generated_at': datetime.now(timezone.utc).isoformat(), 'max_age_seconds': CACHE_SECONDS,
                   'score_unit': 'centipawns', 'score_perspective': 'player',
                   'counting': 'position_occurrences', 'endings_scope': 'all_recorded_games_in_selected_modes',
                   'stale': False,
                   'bins': bins(), 'by_mode': by_mode}
        await save_cache(redis, key, payload, seconds=HISTOGRAM_RETENTION_SECONDS)
        return payload


async def get_positions(player_name, redis, *, bin_id, modes, snapshot, cursor=None, color=None):
    player, modes = normalize(player_name, modes)
    if type(bin_id) is not int or not 0 <= bin_id <= 12:
        raise ValueError('Choose one of the thirteen score intervals.')
    # Live examples do not require rebuilding or validating a corpus snapshot.
    # Bound the source BEFORE score filtering; no shielded long-running task.
    async with read_slot(wait_seconds=2, slots=POSITION_READ_SLOTS):
        async with AsyncDBSession() as session:
            await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
            await session.execute(text("SET LOCAL statement_timeout='3s'"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            await session.execute(text("SET LOCAL work_mem='8MB'"))
            await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
            await session.execute(text('SET LOCAL jit=off'))
            from .player_frequency_position_page import read_ranked_page, FrequencySnapshotChanged
            try:
                ranked = await read_ranked_page(session, player, modes, bins()[bin_id], cursor, color=color)
            except FrequencySnapshotChanged as error:
                raise ScoreDistributionChanged(str(error)) from error
            if ranked and not ranked.get('ranking_pending'):
                return ranked
            page = await read_position_page(session, player, modes, bins()[bin_id], cursor, color=color)
            return {**page, **(ranked or {})}
