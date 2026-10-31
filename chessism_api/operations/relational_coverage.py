"""Absolute, retry-safe analysis coverage in the normalized layout."""
from sqlalchemy import text

from chessism_api.database.relational_storage import storage_for


FIELDS = ("total_positions", "analyzed_positions", "unscored_positions", "is_fully_analyzed",
          "equal_positions", "small_positions", "clear_positions", "decisive_positions", "mate_positions")
# Large cloud result sets/whole-corpus repairs can legitimately benefit from a
# grouped scan. Small ingestion and endgame batches must remain index-bounded.
INDEXED_SCOPE_MAX_GAMES = 10_000


def coverage_source_sql(layout, *, scoped):
    """Scoped refreshes must visit selected games, not scan the entire corpus.

    A correlated aggregate keeps the indexed game->positions lookup bounded
    even when the catalog's row estimates are stale. Whole-database repair
    retains the set-based aggregate, where scanning the corpus is intentional.
    """
    associations, positions = layout.table('game_fen_association'), layout.table('fen')
    aggregates = """count(*)::int AS total_positions,
        count(f.score)::int AS analyzed_positions,
        (count(*)-count(f.score))::int AS unscored_positions,
        count(*)=count(f.score) AS is_fully_analyzed,
        count(*) FILTER(WHERE abs(f.score)<50)::int AS equal_positions,
        count(*) FILTER(WHERE abs(f.score)>=50 AND abs(f.score)<150)::int AS small_positions,
        count(*) FILTER(WHERE abs(f.score)>=150 AND abs(f.score)<300)::int AS clear_positions,
        count(*) FILTER(WHERE abs(f.score)>=300 AND abs(f.score)<9000)::int AS decisive_positions,
        count(*) FILTER(WHERE abs(f.score)>=9000)::int AS mate_positions"""
    if scoped:
        return f"""SELECT t.link,{','.join('s.'+field for field in FIELDS)},CURRENT_TIMESTAMP
            FROM pg_temp.relational_coverage_links t
            CROSS JOIN LATERAL (
                SELECT {aggregates} FROM {associations} a
                JOIN {positions} f ON f.fen_id=a.fen_id
                WHERE a.game_link=t.link HAVING count(*)>0
            ) s ORDER BY t.link"""
    return f"""SELECT a.game_link,{aggregates},CURRENT_TIMESTAMP
        FROM pg_temp.relational_coverage_links t JOIN {associations} a ON a.game_link=t.link
        JOIN {positions} f ON f.fen_id=a.fen_id GROUP BY a.game_link ORDER BY a.game_link"""


async def refresh_coverage(session, links=None, *, fens=None):
    layout = storage_for(session)
    if layout is None:
        raise RuntimeError("Coverage requires the explicit relational storage contract")
    # None means an intentional full repair; an empty selection means no work.
    if (fens is not None and not fens) or (fens is None and links is not None and not links):
        return []
    associations = layout.table('game_fen_association')
    coverage = layout.table('game_analysis_coverage')
    # Lock game rows before reading counts, in stable order. Separate overlapping
    # refreshes then read a fresh READ COMMITTED snapshot instead of overwriting
    # a newer count with an older snapshot. No arithmetic deltas on replays.
    await session.execute(text("""CREATE TEMP TABLE IF NOT EXISTS relational_coverage_links
        (link bigint PRIMARY KEY) ON COMMIT DROP"""))
    await session.execute(text("TRUNCATE pg_temp.relational_coverage_links"))
    if fens is not None:
        await session.execute(text(f"""INSERT INTO relational_coverage_links
            SELECT DISTINCT a.game_link FROM {associations} a JOIN {layout.table('fen')} f ON f.fen_id=a.fen_id
            WHERE f.fen=ANY(CAST(:fens AS text[])) ON CONFLICT DO NOTHING"""), {'fens': list(fens)})
    elif links is None:
        await session.execute(text(f"INSERT INTO relational_coverage_links SELECT link FROM {layout.table('game')}"))
    else:
        await session.execute(text("""INSERT INTO relational_coverage_links
            SELECT unnest(CAST(:links AS bigint[])) ON CONFLICT DO NOTHING"""), {'links': list(links)})
    # Autovacuum cannot analyze session-local scratch tables. Without statistics
    # even a 1,000-game batch can choose full association/FEN-table hash joins.
    await session.execute(text("ANALYZE pg_temp.relational_coverage_links"))
    scoped = links is not None or fens is not None
    if scoped:
        count = await session.scalar(text("SELECT count(*) FROM pg_temp.relational_coverage_links"))
        if not count:
            return []
        scoped = count <= INDEXED_SCOPE_MAX_GAMES
    await session.execute(text(f"""SELECT g.link FROM {layout.table('game')} g
        JOIN relational_coverage_links t USING(link) ORDER BY g.link FOR UPDATE OF g"""))
    await session.execute(text(f"""DELETE FROM {coverage} c USING relational_coverage_links t
        WHERE c.link=t.link AND NOT EXISTS(SELECT 1 FROM {associations} a WHERE a.game_link=c.link)"""))
    source = coverage_source_sql(layout, scoped=scoped)
    result = await session.execute(text(f"""INSERT INTO {coverage}(link,{','.join(FIELDS)},refreshed_at)
        {source}
        ON CONFLICT(link) DO UPDATE SET
            {','.join(field+'=EXCLUDED.'+field for field in FIELDS)},refreshed_at=CURRENT_TIMESTAMP
        RETURNING link,is_fully_analyzed"""))
    return list(result.mappings())
