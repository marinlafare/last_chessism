"""Caller-owned canonical game coverage and ingestion totals."""
from sqlalchemy import text


FIELDS = ("total_positions", "analyzed_positions", "unscored_positions", "is_fully_analyzed",
          "equal_positions", "small_positions", "clear_positions", "decisive_positions", "mate_positions")
# Large cloud result sets/whole-corpus repairs can legitimately benefit from a
# grouped scan. Small ingestion and endgame batches must remain index-bounded.
INDEXED_SCOPE_MAX_GAMES = 10_000


def coverage_source_sql(*, scoped):
    """Scoped refreshes must visit selected games, not scan the entire corpus.

    A correlated aggregate keeps the indexed game->positions lookup bounded
    even when the catalog's row estimates are stale. Whole-database repair
    retains the set-based aggregate, where scanning the corpus is intentional.
    """
    associations, positions = 'public.game_fen_association', 'public.fen'
    aggregates = """count(*)::int AS total_positions,
        count(f.score)::int AS analyzed_positions,
        (count(*)-count(f.score))::int AS unscored_positions,
        count(*)=count(f.score) AS is_fully_analyzed,
        count(*) FILTER(WHERE abs(f.score::bigint)<50)::int AS equal_positions,
        count(*) FILTER(WHERE abs(f.score::bigint)>=50 AND abs(f.score::bigint)<150)::int AS small_positions,
        count(*) FILTER(WHERE abs(f.score::bigint)>=150 AND abs(f.score::bigint)<300)::int AS clear_positions,
        count(*) FILTER(WHERE abs(f.score::bigint)>=300 AND abs(f.score::bigint)<9000)::int AS decisive_positions,
        count(*) FILTER(WHERE abs(f.score::bigint)>=9000)::int AS mate_positions"""
    if scoped:
        return f"""SELECT t.link,{','.join('s.'+field for field in FIELDS)},CURRENT_TIMESTAMP
            FROM pg_temp.canonical_coverage_links t
            CROSS JOIN LATERAL (
                SELECT {aggregates} FROM {associations} a
                JOIN {positions} f ON f.fen_id=a.fen_id
                WHERE a.game_link=t.link HAVING count(*)>0
            ) s ORDER BY t.link"""
    return f"""SELECT a.game_link,{aggregates},CURRENT_TIMESTAMP
        FROM pg_temp.canonical_coverage_links t JOIN {associations} a ON a.game_link=t.link
        JOIN {positions} f ON f.fen_id=a.fen_id GROUP BY a.game_link ORDER BY a.game_link"""


async def refresh_coverage(session, links=None, *, fens=None):
    if not session.in_transaction():
        raise ValueError("Coverage requires a caller-owned transaction")
    # None means an intentional full repair; an empty selection means no work.
    if (fens is not None and not fens) or (fens is None and links is not None and not links):
        return []
    associations = 'public.game_fen_association'
    coverage = 'public.game_analysis_coverage'
    # Lock game rows before reading counts, in stable order. Separate overlapping
    # refreshes then read a fresh READ COMMITTED snapshot instead of overwriting
    # a newer count with an older snapshot. No arithmetic deltas on replays.
    await session.execute(text("""CREATE TEMP TABLE IF NOT EXISTS canonical_coverage_links
        (link bigint PRIMARY KEY) ON COMMIT DROP"""))
    await session.execute(text("TRUNCATE pg_temp.canonical_coverage_links"))
    if fens is not None:
        await session.execute(text(f"""INSERT INTO canonical_coverage_links
            SELECT DISTINCT a.game_link FROM {associations} a JOIN public.fen f ON f.fen_id=a.fen_id
            WHERE f.fen=ANY(CAST(:fens AS text[])) ON CONFLICT DO NOTHING"""), {'fens': list(fens)})
    elif links is None:
        await session.execute(text("INSERT INTO canonical_coverage_links SELECT link FROM public.game"))
    else:
        await session.execute(text("""INSERT INTO canonical_coverage_links
            SELECT unnest(CAST(:links AS bigint[])) ON CONFLICT DO NOTHING"""), {'links': list(links)})
    # Autovacuum cannot analyze session-local scratch tables. Without statistics
    # even a 1,000-game batch can choose full association/FEN-table hash joins.
    await session.execute(text("ANALYZE pg_temp.canonical_coverage_links"))
    scoped = links is not None or fens is not None
    if scoped:
        count = await session.scalar(text("SELECT count(*) FROM pg_temp.canonical_coverage_links"))
        if not count:
            return []
        scoped = count <= INDEXED_SCOPE_MAX_GAMES
    await session.execute(text("""SELECT g.link FROM public.game g
        JOIN canonical_coverage_links t USING(link) ORDER BY g.link FOR UPDATE OF g"""))
    await session.execute(text(f"""DELETE FROM {coverage} c USING canonical_coverage_links t
        WHERE c.link=t.link AND NOT EXISTS(SELECT 1 FROM {associations} a WHERE a.game_link=c.link)"""))
    source = coverage_source_sql(scoped=scoped)
    result = await session.execute(text(f"""INSERT INTO {coverage}(link,{','.join(FIELDS)},refreshed_at)
        {source}
        ON CONFLICT(link) DO UPDATE SET
            {','.join(field+'=EXCLUDED.'+field for field in FIELDS)},refreshed_at=CURRENT_TIMESTAMP
        RETURNING link,is_fully_analyzed"""))
    return list(result.mappings())


async def refresh_pipeline_summary(connection, *, mark_tablebase_current=False):
    """Serialize absolute counts, then read a fresh snapshot in the caller's unit."""
    if not connection.is_in_transaction():
        raise ValueError('Pipeline totals require a caller-owned transaction')
    if await connection.fetchval(
            'SELECT id FROM public.fen_pipeline_summary WHERE id=1 FOR UPDATE') != 1:
        raise RuntimeError('Required fen_pipeline_summary is missing')
    return await connection.fetchrow('''
        UPDATE public.fen_pipeline_summary SET
            parsed_games=counts.games,analyzable_games=counts.games,excluded_games=0,
            fen_extracted_games=counts.extracted,
            tablebase_marked_games=CASE WHEN $1 THEN counts.extracted
                ELSE least(tablebase_marked_games,counts.extracted) END,refreshed_at=now()
        FROM (SELECT count(*)::bigint AS games,
            count(*) FILTER(WHERE c.total_positions>0)::bigint AS extracted
            FROM public.game g LEFT JOIN public.game_analysis_coverage c ON c.link=g.link
            WHERE g.is_standard AND g.n_moves>0) counts WHERE id=1
        RETURNING parsed_games,analyzable_games,excluded_games,fen_extracted_games,
                  tablebase_marked_games,refreshed_at
    ''', mark_tablebase_current)
