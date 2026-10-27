"""Full eligible player histories, bounded source reads and revision guards."""
from sqlalchemy import text

from chessism_api.database.relational_storage import storage_for
from chessism_api.database.stockfish_summary_schema import summary_table

MAX_PLAYERS = 100
MAX_PLAYER_POSITIONS = 20_000_000
MAX_COHORT_POSITIONS = 100_000_000
MODES = ('bullet', 'blitz', 'rapid')


async def prepare(session):
    await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    await session.execute(text("SET LOCAL statement_timeout='30s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
    await session.execute(text('SET LOCAL max_parallel_workers_per_gather=0'))
    await session.execute(text("SET LOCAL work_mem='32MB'"))
    await session.execute(text('SET LOCAL jit=off'))
    if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM cloud_analysis_job WHERE status NOT IN ('complete','cancelled','limit_reached','failed'))")):
        raise ValueError('Active cloud workflow: wait before reading cohort inputs.')


async def profiles(session):
    layout = storage_for(session)
    player = layout.table('player') if layout else 'player'
    state = summary_table(layout, 'state')
    rows = [dict(r) for r in (await session.execute(text(f'''SELECT p.player_name,s.revision,s.published_revision
        FROM {player} p LEFT JOIN {state} s USING(player_name)
        WHERE p.joined>0 AND p.deleted_at IS NULL ORDER BY p.player_name LIMIT :limit'''),
        {'limit': MAX_PLAYERS+1})).mappings()]
    if not 1 <= len(rows) <= MAX_PLAYERS:
        raise ValueError('Empty or oversized cohort; no player truncation is allowed.')
    if any(r['revision'] is None or r['revision'] != r['published_revision'] for r in rows):
        raise ValueError('Finish existing player summary refreshes before the cohort comparison.')
    return rows


def raw_sql(session):
    layout = storage_for(session)
    if layout:
        t = layout.table
        joins = f'''FROM {t('player')} p JOIN {t('game_player')} gp ON gp.player_id=p.id
            JOIN {t('game')} g ON g.link=gp.game_link JOIN {t('game_mode')} m ON m.id=g.mode_id
            LEFT JOIN {t('game_analysis_coverage')} c ON c.link=g.link
            LEFT JOIN {t('game_player_engine_summary')} e ON e.game_player_id=gp.id'''
        white, mode, standard = 'gp.is_white', 'm.name', 'TRUE'
    else:
        joins = '''FROM player p JOIN game_player gp USING(player_name) JOIN game g ON g.link=gp.link
            LEFT JOIN game_analysis_summary c ON c.link=g.link
            LEFT JOIN game_player_engine_summary e ON e.game_link=g.link AND e.player_color=gp.color'''
        white, mode = "gp.color='white'", 'gp.mode'
        standard = "g.rules='chess' AND (g.initial_setup IS NULL OR g.initial_setup IN ('','startpos','rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'))"
    return f'''SELECT p.player_name AS player,g.link,{white} AS is_white,{mode} AS mode,g.played_at,
        c.total_positions,COALESCE(c.is_fully_analyzed AND c.total_positions>0,FALSE) AS analyzed,
        e.blunder_count AS saved_blunders,e.analyzed_player_moves AS saved_moves,
        CASE WHEN {white} THEN g.white_elo ELSE g.black_elo END AS player_rating,
        CASE WHEN {white} THEN g.black_elo ELSE g.white_elo END AS opponent_rating,
        COALESCE(g.white_elo BETWEEN 1 AND 4000 AND g.black_elo BETWEEN 1 AND 4000,FALSE) AS rated,
        COALESCE({standard},FALSE) AS standard {joins}
        WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL
        AND {mode}=ANY(CAST(:modes AS text[]))'''


ELIGIBLE = 'analyzed AND standard AND rated'


async def scope(session, player):
    raw = raw_sql(session)
    rows = (await session.execute(text(f'''SELECT mode,COUNT(*) AS all_games,
        COUNT(*) FILTER(WHERE analyzed) AS analyzed_games,
        COUNT(*) FILTER(WHERE analyzed AND NOT standard) AS nonstandard_games,
        COUNT(*) FILTER(WHERE analyzed AND standard AND NOT rated) AS unsupported_rating_games,
        COUNT(*) FILTER(WHERE {ELIGIBLE}) AS eligible_games,
        COALESCE(SUM(total_positions) FILTER(WHERE {ELIGIBLE}),0) AS source_positions,
        COUNT(*) FILTER(WHERE analyzed AND (saved_blunders IS NULL OR saved_blunders<0 OR saved_moves IS NULL OR saved_moves<0)) AS missing_summaries
        FROM ({raw}) s GROUP BY mode ORDER BY mode'''), {'player': player, 'modes': list(MODES)})).mappings().all()
    result = [dict(r) for r in rows]
    if sum(r['source_positions'] for r in result) > MAX_PLAYER_POSITIONS:
        raise ValueError(f'{player} exceeds the full-player position bound; no truncation allowed.')
    if any(r['missing_summaries'] for r in result):
        raise ValueError(f'{player} has missing saved CPU references; no synchronous live repair is performed.')
    return result


async def saved_histograms(session, player, *, paired=False):
    condition = ELIGIBLE if paired else 'analyzed'
    rows = (await session.execute(text(f'''SELECT mode,LEAST(6,saved_blunders) AS bin_id,COUNT(*) AS games,
        SUM(saved_blunders) AS blunders FROM ({raw_sql(session)}) s
        WHERE {condition} AND saved_blunders>=0 AND saved_moves>0
        GROUP BY mode,bin_id ORDER BY mode,bin_id'''), {'player': player, 'modes': list(MODES)})).mappings()
    result = {mode: {'games': 0, 'blunders': 0, 'counts': [0]*7} for mode in MODES}
    for r in rows:
        g = result[r['mode']]; g['games'] += r['games']; g['blunders'] += r['blunders']; g['counts'][r['bin_id']] += r['games']
    return result


async def inventory(session):
    result = []
    for profile in await profiles(session):
        counts = await scope(session, profile['player_name'])
        result.append({**profile, 'scope': counts, 'source_positions': sum(r['source_positions'] for r in counts)})
    if sum(r['source_positions'] for r in result) > MAX_COHORT_POSITIONS:
        raise ValueError('Cohort exceeds the declared position budget; no player is skipped.')
    return sorted(result, key=lambda r: (r['source_positions'], r['player_name']))


async def current_revision(session, player):
    layout = storage_for(session)
    p = layout.table('player') if layout else 'player'
    return await session.scalar(text(f'''SELECT s.revision FROM {summary_table(layout, 'state')} s
        JOIN {p} p USING(player_name) WHERE p.player_name=:player AND p.joined>0 AND p.deleted_at IS NULL'''), {'player': player})
