"""Atomic active-corpus summary finalization, initially PRIVATE rehearsal only.

Reuse application refreshes with explicit sessions/savepoints. Their commits
must not commit the caller's transaction. No startup hooks or cloud clients.
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from chessism_api.database.relational_storage import SESSION_KEY, require_storage_contract
from scripts.db_refactor_candidate import guard
from scripts.db_refactor_switch_rehearsal import LAYOUT

SUMMARY_TABLES = (
    'database_summary', 'fen_pipeline_summary', 'scored_position_summary',
    'main_character_mode_summary', 'player_salience_summary',
    'scored_rating_summary', 'home_titled_summary', 'home_titled_player_summary',
)
STAGES = ('database_counts', 'pipeline_counts', 'scored_counts', 'player_modes',
          'salience_totals', 'rating_groups', 'homepage_union', 'validation')


class InjectedFinalizationFailure(RuntimeError):
    pass


async def snapshot(connection):
    """Small-cache fingerprint only; never serialize the game/FEN corpus."""
    result = {}
    for table in SUMMARY_TABLES:
        row = (await connection.execute(text(f"""SELECT count(*) AS rows,
            md5(coalesce(string_agg(fingerprint,'' ORDER BY fingerprint),'')) AS fingerprint
            FROM (SELECT md5(to_jsonb(t)::text) AS fingerprint FROM public.{table} t) s"""))).mappings().one()
        result[table] = dict(row)
    return result


async def check_private_corpus(connection, pgdata):
    await connection.execute(text('SELECT 1'))
    driver = (await connection.get_raw_connection()).driver_connection
    await guard(driver, pgdata)
    await require_storage_contract(connection, LAYOUT)
    from scripts.db_refactor_live_target import LiveMigrationTarget
    private = not isinstance(pgdata, LiveMigrationTarget)
    if not await connection.scalar(text("""SELECT EXISTS(SELECT 1
        FROM public.relational_storage_release WHERE version='relational_v2'
        AND rehearsal_only=:private)"""), {'private':private}):
        raise RuntimeError('Summary finalization requires a matching private/live release marker')
    # Existing per-FEN frequencies and per-game analyses are preserved only
    # when archival removes no analyzed positions. Fail closed otherwise.
    if await connection.scalar(text("""SELECT EXISTS(SELECT 1
        FROM chessism_archive.game g JOIN chessism_legacy.game_fen_association a
        ON a.game_link=g.link)""")):
        raise RuntimeError('Archived games contributed FENs; a full frequency rebuild is required')
    return driver


async def refresh_salience_totals(factory):
    async with factory.begin() as session:
        # Per-game values and frequency rows were losslessly copied. Recount
        # only their active ownership; never manufacture new analysis scores.
        await session.execute(text("""WITH totals AS (
            SELECT p.id, count(s.game_player_id)::bigint AS games,
                coalesce(sum(s.position_occurrence_count),0)::bigint AS positions,
                coalesce(sum(s.salience),0)::float8 AS effective
            FROM public.player p LEFT JOIN public.game_player gp ON gp.player_id=p.id
            LEFT JOIN public.game_player_salience s ON s.game_player_id=gp.id GROUP BY p.id
        ) UPDATE public.player_salience_summary s SET source_game_count=t.games,
            source_position_count=t.positions,effective_game_count=t.effective,
            status=CASE WHEN s.status IN ('queued','running') OR EXISTS(
                SELECT 1 FROM public.player_salience_pending_game pending
                JOIN public.game_player gp ON gp.id=pending.game_player_id WHERE gp.player_id=s.player_id
            ) OR EXISTS(
                SELECT 1 FROM public.game_player gp
                JOIN public.game g ON g.link=gp.game_link AND g.fens_done
                LEFT JOIN public.game_player_salience a ON a.game_player_id=gp.id
                WHERE gp.player_id=s.player_id AND a.game_player_id IS NULL AND EXISTS(
                    SELECT 1 FROM public.game_fen_association f WHERE f.game_link=gp.game_link)
            ) THEN 'stale' ELSE s.status END
            FROM totals t WHERE s.player_id=t.id"""))


async def validate_summaries(connection):
    """Independent typed base-table counts, not the compatibility read views."""
    expected = dict((await connection.execute(text("""SELECT
        (SELECT count(*) FROM public.game) AS games,
        (SELECT count(*) FROM chessism_archive.game) AS archived_games,
        (SELECT count(*) FROM public.game_analysis_coverage WHERE total_positions>0) AS extracted,
        (SELECT count(*) FROM public.game_analysis_coverage c JOIN public.game g ON g.link=c.link
            WHERE c.total_positions>0 AND c.is_fully_analyzed AND g.avg_elo>0) AS rated_games,
        (SELECT count(*) FROM public.game_player gp JOIN public.player p ON p.id=gp.player_id
            JOIN public.game g ON g.link=gp.game_link JOIN public.game_mode m ON m.id=g.mode_id
            WHERE coalesce(p.joined,0)<>0 AND m.name IN ('bullet','blitz','rapid')) AS player_games
        """))).mappings().one())
    db = (await connection.execute(text('SELECT * FROM public.database_summary WHERE id=1'))).mappings().one()
    pipeline = (await connection.execute(text('SELECT * FROM public.fen_pipeline_summary WHERE id=1'))).mappings().one()
    scored = (await connection.execute(text('SELECT * FROM public.scored_position_summary WHERE id=1'))).mappings().one()
    home = (await connection.execute(text('SELECT * FROM public.home_titled_summary WHERE id=1'))).mappings().one()
    modes = await connection.scalar(text('SELECT coalesce(sum(n_games),0) FROM public.main_character_mode_summary'))
    rated = await connection.scalar(text('SELECT source_full_games FROM public.scored_rating_summary WHERE id=1'))
    revision = await connection.scalar(text('SELECT revision FROM public.home_summary_source WHERE id=1'))
    half_units = await connection.scalar(text('SELECT coalesce(sum(game_half_units),0) FROM public.home_titled_player_summary'))
    fen = (await connection.execute(text("""SELECT count(*) AS total,
        count(score) AS scored,count(*) FILTER(WHERE score<>0) AS nonzero FROM public.fen"""))).mappings().one()
    salience_bad = await connection.scalar(text("""SELECT count(*) FROM public.player_salience_summary s
        LEFT JOIN (SELECT gp.player_id,count(a.game_player_id) AS games,
            coalesce(sum(a.position_occurrence_count),0) AS positions,coalesce(sum(a.salience),0)::float8 AS effective
            FROM public.game_player gp JOIN public.game_player_salience a ON a.game_player_id=gp.id
            GROUP BY gp.player_id) t ON t.player_id=s.player_id
        WHERE s.source_game_count<>coalesce(t.games,0) OR s.source_position_count<>coalesce(t.positions,0)
            OR abs(s.effective_game_count-coalesce(t.effective,0))>0.000001*greatest(1,abs(coalesce(t.effective,0)))
            OR (s.status='ready' AND EXISTS(SELECT 1 FROM public.player_salience_pending_game p
                JOIN public.game_player gp ON gp.id=p.game_player_id WHERE gp.player_id=s.player_id))"""))
    checks = {
        'active_game_count': db['n_games_in_db'] == expected['games'],
        'pipeline_scope': pipeline['parsed_games'] == pipeline['analyzable_games'] == expected['games']
            and pipeline['excluded_games'] == 0 and pipeline['fen_extracted_games'] == expected['extracted']
            and 0 <= pipeline['tablebase_marked_games'] <= expected['extracted'],
        'database_fens': db['n_positions'] == fen['total'] and db['analyzed_fens'] == fen['scored']
            and db['unscored_fens'] == fen['total']-fen['scored'] and db['nonzero_scored_fens'] == fen['nonzero'],
        'scored_fens': scored['total_positions'] == fen['total'] and scored['scored_positions'] == fen['scored']
            and sum(scored[k+'_positions'] for k in ('equal','small','clear','decisive','mate')) == fen['scored'],
        'mode_counts': modes == expected['player_games'],
        'rating_count': rated == expected['rated_games'],
        'salience_totals': salience_bad == 0,
        'homepage': home['total_fens'] == fen['total'] and home['source_revision'] == revision
            and home['last_error'] is None and 0 <= home['titled_fens'] <= fen['total']
            and half_units == 2*home['titled_games'],
    }
    if not all(checks.values()):
        raise RuntimeError('Summary validation failed: '+', '.join(k for k,v in checks.items() if not v))
    return {'checks': checks, 'counts': {**expected, 'fens': fen['total'], 'scored_fens': fen['scored']}}


async def finalize(connection, pgdata, publish, *, inject=False):
    if not connection.in_transaction():
        raise RuntimeError('Finalization requires a caller-owned transaction')
    await check_private_corpus(connection, pgdata)
    if await connection.scalar(text('SHOW transaction_isolation')) != 'repeatable read':
        raise RuntimeError('Finalization requires REPEATABLE READ')
    await connection.execute(text("SET LOCAL lock_timeout='5s'"))
    await connection.execute(text("SET LOCAL statement_timeout='2h'"))
    await connection.execute(text(f'SET LOCAL search_path={LAYOUT.search_path}'))
    factory = sessionmaker(bind=connection, class_=AsyncSession, expire_on_commit=False,
        join_transaction_mode='create_savepoint', info={SESSION_KEY: LAYOUT})
    from chessism_api.database import ask_db
    from chessism_api.operations.home_summary import refresh_home_summary

    await publish('refreshing_database_counts', steps_completed=0, steps_total=len(STAGES))
    database = await ask_db.refresh_database_summary(session_factory=factory)
    if inject:
        raise InjectedFinalizationFailure('Injected failure after first refreshed summary')
    actions = (
        ('pipeline_counts', lambda: ask_db.refresh_fen_pipeline_summary(session_factory=factory)),
        ('scored_counts', lambda: ask_db.refresh_scored_position_summary(session_factory=factory,database_summary=database)),
        ('player_modes', lambda: ask_db.refresh_main_character_mode_summary(session_factory=factory)),
        ('salience_totals', lambda: refresh_salience_totals(factory)),
        ('rating_groups', lambda: ask_db.refresh_scored_rating_summary(session_factory=factory)),
        ('homepage_union', lambda: refresh_home_summary(session_factory=factory,transaction_managed=True)),
    )
    for index,(name,action) in enumerate(actions,1):
        await publish('refreshing_'+name,steps_completed=index,steps_total=len(STAGES))
        result = await action()
        if name == 'homepage_union' and result.get('status') != 'complete':
            raise RuntimeError('Homepage summary refresh did not complete')
    await publish('validating_corpus_summaries',steps_completed=7,steps_total=len(STAGES))
    result = await validate_summaries(connection)
    await publish('corpus_summaries_validated',steps_completed=len(STAGES),steps_total=len(STAGES))
    return result
