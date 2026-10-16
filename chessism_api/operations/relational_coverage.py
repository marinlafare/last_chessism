"""Absolute, retry-safe analysis coverage in the normalized layout."""
from sqlalchemy import text

from chessism_api.database.relational_storage import storage_for


FIELDS = ("total_positions", "analyzed_positions", "unscored_positions", "is_fully_analyzed",
          "equal_positions", "small_positions", "clear_positions", "decisive_positions", "mate_positions")


async def refresh_coverage(session, links=None, *, fens=None):
    layout = storage_for(session)
    if layout is None:
        raise RuntimeError("Coverage requires the explicit relational storage contract")
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
            SELECT DISTINCT a.game_link FROM {associations} a JOIN fen f ON f.fen_id=a.fen_id
            WHERE f.fen=ANY(CAST(:fens AS text[])) ON CONFLICT DO NOTHING"""), {'fens': list(fens)})
    elif links is None:
        await session.execute(text(f"INSERT INTO relational_coverage_links SELECT link FROM {layout.table('game')}"))
    else:
        await session.execute(text("""INSERT INTO relational_coverage_links
            SELECT unnest(CAST(:links AS bigint[])) ON CONFLICT DO NOTHING"""), {'links': list(links)})
    await session.execute(text(f"""SELECT g.link FROM {layout.table('game')} g
        JOIN relational_coverage_links t USING(link) ORDER BY g.link FOR UPDATE OF g"""))
    await session.execute(text(f"""DELETE FROM {coverage} c USING relational_coverage_links t
        WHERE c.link=t.link AND NOT EXISTS(SELECT 1 FROM {associations} a WHERE a.game_link=c.link)"""))
    result = await session.execute(text(f"""INSERT INTO {coverage}(link,{','.join(FIELDS)},refreshed_at)
        SELECT a.game_link,count(*)::int,count(f.score)::int,(count(*)-count(f.score))::int,
            count(*)=count(f.score),
            count(*) FILTER(WHERE abs(f.score)<50)::int,
            count(*) FILTER(WHERE abs(f.score)>=50 AND abs(f.score)<150)::int,
            count(*) FILTER(WHERE abs(f.score)>=150 AND abs(f.score)<300)::int,
            count(*) FILTER(WHERE abs(f.score)>=300 AND abs(f.score)<9000)::int,
            count(*) FILTER(WHERE abs(f.score)>=9000)::int,CURRENT_TIMESTAMP
        FROM relational_coverage_links t JOIN {associations} a ON a.game_link=t.link
        JOIN fen f ON f.fen_id=a.fen_id GROUP BY a.game_link ORDER BY a.game_link
        ON CONFLICT(link) DO UPDATE SET
            {','.join(field+'=EXCLUDED.'+field for field in FIELDS)},refreshed_at=CURRENT_TIMESTAMP
        RETURNING link,is_fully_analyzed"""))
    return list(result.mappings())
