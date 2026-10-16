"""Cached titled-player/FEN union for the homepage; heavy work stays off HTTP.

Reads enqueue a debounced rebuild when a committed structural change invalidates
the snapshot. Score imports do not invalidate it. Existing data stays visible
while rebuilding. The published counts and all pie slices share one snapshot.
"""
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.compact_storage import compact_for
from chessism_api.player_titles import OFFICIAL_PLAYER_TITLES as TITLES

LOG = logging.getLogger(__name__)
JOB_ID = "home-titled-summary-v1"
LOCK_ID = 731_946_229
REFRESH_SECONDS = 300


def needs_refresh(meta, now=None):
    now = now or datetime.now(timezone.utc)
    previous = meta.get("attempted_at")
    if previous and (now - previous).total_seconds() < REFRESH_SECONDS:
        return False
    return (meta.get("refreshed_at") is None or meta.get("last_error") is not None
            or meta.get("source_revision") != meta.get("current_revision"))


async def get_home_summary(redis):
    # Counts stay cached; profile lookups and the tiny per-player/mode summary
    # supply avatars and untitled-player game totals without scanning game/FEN rows.
    async with AsyncDBSession() as session:
        rows = (await session.execute(text("""
            SELECT m.*, s.revision AS current_revision,
                   p.player_name, p.title, p.games, p.game_half_units, profile.avatar
            FROM (SELECT 1 AS id) singleton
            LEFT JOIN home_summary_source s ON s.id = singleton.id
            LEFT JOIN home_titled_summary m ON m.id = singleton.id
            LEFT JOIN home_titled_player_summary p ON TRUE
            LEFT JOIN player profile ON profile.player_name = p.player_name AND profile.deleted_at IS NULL
            ORDER BY p.game_half_units DESC, p.player_name
        """))).mappings().all()
        untitled = (await session.execute(text("""
            SELECT p.player_name, p.avatar, SUM(s.n_games) AS games
            FROM main_character_mode_summary s
            JOIN player p ON p.player_name = s.player_name
            WHERE p.deleted_at IS NULL
              AND NOT (UPPER(TRIM(COALESCE(p.title, ''))) = ANY(CAST(:titles AS text[])))
            GROUP BY p.player_name, p.avatar
            HAVING SUM(s.n_games) > 0
            ORDER BY games DESC, p.player_name
        """), {"titles": list(TITLES)})).mappings().all()
    meta = dict(rows[0])
    error = meta.get("last_error")
    if needs_refresh(meta):
        try:
            await redis.enqueue_job("run_home_summary_refresh", _job_id=JOB_ID,
                                    _queue_name="analysis_queue")
        except Exception:
            LOG.exception("Could not enqueue homepage summary")
            error = "Summary refresh unavailable; try again later."
    refreshed = meta.get("refreshed_at")
    return {
        "total_fens": int(meta.get("total_fens") or 0) if refreshed else None,
        "titled_players": int(meta.get("titled_players") or 0) if refreshed else None,
        "titled_fens": int(meta.get("titled_fens") or 0) if refreshed else None,
        "titled_games": int(meta.get("titled_games") or 0) if refreshed else None,
        "refreshed_at": refreshed.isoformat() if refreshed else None,
        "stale": not refreshed or meta.get("source_revision") != meta.get("current_revision"),
        "refresh_error": error,
        "players": [{"player_name": row["player_name"], "title": row["title"],
                     "games": int(row["games"]), "game_half_units": int(row["game_half_units"]),
                     "avatar": row["avatar"]}
                    for row in rows if row["player_name"] is not None],
        "untitled_players": [{"player_name": row["player_name"], "title": "",
                              "games": int(row["games"]), "avatar": row["avatar"]}
                             for row in untitled],
    }


async def run_home_summary_refresh(ctx):
    return await refresh_home_summary()


async def refresh_home_summary(*, session_factory=None, transaction_managed=False):
    """Allow migration tooling to own the transaction without changing workers.

    A managed caller must supply sessions bound to one REPEATABLE READ (or
    SERIALIZABLE) transaction. Its sessions use savepoints, never commit the
    caller's transaction. Ordinary queue workers retain their existing path.
    """
    if transaction_managed and session_factory is None:
        raise ValueError('Managed summary refresh requires an explicit session factory')
    factory = session_factory or AsyncDBSession
    if transaction_managed:
        async with factory() as session:
            isolation = await session.scalar(text('SHOW transaction_isolation'))
            if isolation not in ('repeatable read', 'serializable'):
                raise RuntimeError('Managed homepage refresh requires a stable snapshot')
    async with factory.begin() as session:
        await session.execute(text("""
            INSERT INTO home_titled_summary (id, attempted_at) VALUES (1, clock_timestamp())
            ON CONFLICT (id) DO UPDATE SET attempted_at = EXCLUDED.attempted_at
        """))
    try:
        async with factory() as session:
            if not transaction_managed:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await session.execute(text("SET LOCAL statement_timeout = '900s'"))
            await session.execute(text("SET LOCAL lock_timeout = '5s'"))
            await session.execute(text("SET LOCAL work_mem = '64MB'"))
            await session.execute(text("SET LOCAL max_parallel_workers_per_gather = 2"))
            if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_ID}):
                return {"status": "already_refreshing"}
            revision = await session.scalar(text("SELECT revision FROM home_summary_source WHERE id = 1"))
            compact = compact_for(session)
            owner_projection = 'id AS player_id,' if compact else ''
            await session.execute(text(f"""
                CREATE TEMP TABLE home_titles ON COMMIT DROP AS
                SELECT {owner_projection} player_name, UPPER(TRIM(title)) AS title FROM player
                WHERE deleted_at IS NULL AND UPPER(TRIM(title)) = ANY(CAST(:titles AS text[]))
            """), {"titles": list(TITLES)})
            await session.execute(text("CREATE UNIQUE INDEX ON home_titles (player_name)"))
            await session.execute(text("ANALYZE home_titles"))
            # A game row is unique by link; joining both participants cannot
            # duplicate it. Read game directly, not potentially partial caches.
            await session.execute(text("""
                CREATE TEMP TABLE home_titled_games ON COMMIT DROP AS
                SELECT g.link, w.player_name AS white, b.player_name AS black
                FROM game g LEFT JOIN home_titles w ON w.player_name = g.white
                LEFT JOIN home_titles b ON b.player_name = g.black
                WHERE w.player_name IS NOT NULL OR b.player_name IS NOT NULL
            """))
            await session.execute(text("CREATE UNIQUE INDEX ON home_titled_games (link)"))
            await session.execute(text("ANALYZE home_titled_games"))
            counts = (await session.execute(text("""
                SELECT (SELECT count(*) FROM fen) AS total_fens,
                       (SELECT count(*) FROM home_titles) AS titled_players,
                       (SELECT count(*) FROM home_titled_games) AS titled_games
            """))).mappings().one()
            titled_fens = await session.scalar(text("""
                SELECT count(*) FROM (
                    SELECT a.fen_id FROM game_fen_association a
                    JOIN home_titled_games g ON g.link = a.game_link
                    GROUP BY a.fen_id
                ) distinct_titled_fens
            """))
            target = compact.table('home_titled_player_summary') if compact else 'home_titled_player_summary'
            owner_key = 'player_id' if compact else 'player_name'
            await session.execute(text(f"DELETE FROM {target}"))
            await session.execute(text(f"""
                INSERT INTO {target} ({owner_key}, title, games, game_half_units)
                SELECT p.{owner_key}, p.title, count(c.player_name), coalesce(sum(c.units), 0)
                FROM home_titles p LEFT JOIN (
                    SELECT white AS player_name, CASE WHEN black IS NULL THEN 2 ELSE 1 END AS units
                    FROM home_titled_games WHERE white IS NOT NULL
                    UNION ALL
                    SELECT black, CASE WHEN white IS NULL THEN 2 ELSE 1 END
                    FROM home_titled_games WHERE black IS NOT NULL
                ) c ON c.player_name = p.player_name
                GROUP BY p.{owner_key}, p.title
            """))
            half_units = await session.scalar(text("SELECT coalesce(sum(game_half_units),0) FROM home_titled_player_summary"))
            if half_units != 2 * counts["titled_games"]:
                raise ValueError("Titled game contributions do not match distinct games")
            await session.execute(text("""
                UPDATE home_titled_summary SET total_fens=:total_fens, titled_players=:titled_players,
                    titled_games=:titled_games, titled_fens=:titled_fens, source_revision=:revision,
                    refreshed_at=clock_timestamp(), last_error=NULL WHERE id=1
            """), {**counts, "titled_fens": titled_fens, "revision": revision})
            await session.commit()
    except Exception:
        LOG.exception("Homepage summary rebuild failed; retaining previous snapshot")
        async with factory.begin() as session:
            await session.execute(text("""
                UPDATE home_titled_summary SET last_error=
                    'Summary refresh failed; the previous snapshot is retained.' WHERE id=1
            """))
        raise
    return {"status": "complete", **dict(counts), "titled_fens": titled_fens}
