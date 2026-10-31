"""Bounded COPY staging and retry-safe position persistence for compact storage."""
from contextlib import asynccontextmanager
import json
import logging
from time import perf_counter

from chessism_api.database.compact_storage import compact_for
from chessism_api.database.relational_storage import driver_connection

LOG = logging.getLogger(__name__)


@asynccontextmanager
async def _step(reporter, name, total, unit):
    if reporter:
        async with reporter.step(name, total, unit):
            yield
    else:
        yield


async def persist_positions(session, fens, associations, reporter=None, *, timings=None):
    """Caller commits game state, links and exact counter deltas together.

    PostgreSQL owns the scratch tables; rollback/connection loss removes them.
    Existing scores and cloud claims are never replaced. New and existing FENs
    acquire locks in one textual order, matching cloud reservation/import order.
    """
    layout = compact_for(session)
    if not layout:
        raise RuntimeError("Bulk ingestion requires compact storage")
    if any(row["move_color"] not in ("white", "black") for row in associations):
        raise ValueError("Invalid move side")
    identities = {row['fen']: key for key, row in enumerate(fens)}
    if len(identities) != len(fens):
        raise ValueError('Duplicate canonical FEN input')
    # Every association must belong to this transaction's locked FEN set.
    # Transport a small batch-local key, not a repeated full board string.
    try:
        link_records = [(r['game_link'], r['n_move'], r['move_color'] == 'white', identities[r['fen_fen']])
                        for r in associations]
    except KeyError as error:
        raise ValueError('A game-position link has no canonical FEN input') from error
    timings = timings if timings is not None else {}
    conn = await driver_connection(session)
    fen = layout.table("fen")
    links = layout.table("game_fen_association")
    await conn.execute("""
        CREATE TEMP TABLE ingestion_fen_input (
            fen_key integer PRIMARY KEY, fen text UNIQUE NOT NULL, piece_count smallint, moves_counter text
        ) ON COMMIT DROP;
        CREATE TEMP TABLE ingestion_link_input (
            game_link bigint, n_move integer, is_white boolean, fen_key integer,
            PRIMARY KEY(game_link,n_move,is_white)
        ) ON COMMIT DROP;
    """)
    async with _step(reporter, "save_positions", len(fens), "positions"):
        started = perf_counter()
        await conn.copy_records_to_table("ingestion_fen_input",
            columns=["fen_key", "fen", "piece_count", "moves_counter"],
            records=[(key, r["fen"], r["piece_count"], r["moves_counter"]) for key, r in enumerate(fens)])
        await conn.execute("ANALYZE pg_temp.ingestion_fen_input")
        timings['copy_fens_seconds'] = perf_counter() - started
        # Resolve new AND existing rows in a single ordered pass. A separate
        # SELECT FOR UPDATE followed by INSERT misses concurrently created rows
        # and conflicts with the KEY SHARE locks used by association FK checks.
        # ON CONFLICT locks the row even when WHERE false skips its update.
        # Targeting a non-key column takes NO KEY UPDATE; existing rows are not
        # rewritten, and RETURNING counts only genuine inserts. Keep this lock
        # through appearance deltas so a concurrent score import cannot race it.
        started = perf_counter()
        new_count = await conn.fetchval(f"""WITH inserted AS (
            INSERT INTO {fen} AS stored(fen,piece_count,moves_counter,n_games)
            SELECT fen,piece_count,moves_counter,0 FROM ingestion_fen_input ORDER BY fen
            ON CONFLICT(fen) DO UPDATE SET n_games=stored.n_games WHERE false
            RETURNING fen_id
        ) SELECT COUNT(*) FROM inserted""")
        timings['insert_lock_seconds'] = perf_counter() - started
        started = perf_counter()
        # Resolve text to IDs once after locks are acquired. The fresh statement
        # sees rows that a concurrent writer committed while INSERT was waiting.
        # Scores cannot change under these locks, so reuse them for exact deltas.
        await conn.execute(f"""CREATE TEMP TABLE ingestion_fen_resolved ON COMMIT DROP AS
            SELECT i.fen_key,f.fen_id,i.piece_count,i.moves_counter,f.score
            FROM ingestion_fen_input i JOIN {fen} f USING(fen);
            ALTER TABLE ingestion_fen_resolved ADD PRIMARY KEY(fen_key);
            CREATE UNIQUE INDEX ON ingestion_fen_resolved(fen_id);
            ANALYZE pg_temp.ingestion_fen_resolved""")
        if await conn.fetchval('SELECT count(*) FROM ingestion_fen_resolved') != len(fens):
            raise ValueError('A canonical FEN could not be resolved')
        timings['resolve_ids_seconds'] = perf_counter() - started
        started = perf_counter()
        await conn.execute(f"""UPDATE {fen} f SET
            piece_count=COALESCE(f.piece_count,i.piece_count),
            moves_counter=COALESCE(f.moves_counter,'') ||
                CASE WHEN strpos(COALESCE(f.moves_counter,''),i.moves_counter)=0
                     THEN i.moves_counter ELSE '' END
            FROM ingestion_fen_resolved i WHERE f.fen_id=i.fen_id AND
                (f.piece_count IS NULL OR strpos(COALESCE(f.moves_counter,''),i.moves_counter)=0)""")
        timings['metadata_seconds'] = perf_counter() - started
    async with _step(reporter, "link_positions", len(associations), "links"):
        started = perf_counter()
        await conn.copy_records_to_table("ingestion_link_input",
            columns=["game_link", "n_move", "is_white", "fen_key"], records=link_records)
        await conn.execute("ANALYZE pg_temp.ingestion_link_input")
        await conn.execute(f"""CREATE TEMP TABLE ingestion_link_delta ON COMMIT DROP AS
            WITH inserted AS (
                INSERT INTO {links}(game_link,n_move,is_white,fen_id)
                SELECT i.game_link,i.n_move,i.is_white,f.fen_id
                FROM ingestion_link_input i JOIN ingestion_fen_resolved f USING(fen_key)
                ORDER BY i.game_link,i.n_move,i.is_white
                ON CONFLICT(game_link,n_move,is_white) DO NOTHING RETURNING fen_id
            ) SELECT fen_id,COUNT(*)::bigint AS occurrences FROM inserted GROUP BY fen_id""")
        await conn.execute("ANALYZE pg_temp.ingestion_link_delta")
        conflict = await conn.fetchval(f"""SELECT EXISTS(
            SELECT 1 FROM ingestion_link_input i
            JOIN ingestion_fen_resolved f USING(fen_key)
            JOIN {links} a USING(game_link,n_move,is_white)
            WHERE a.fen_id<>f.fen_id)""")
        if conflict:
            raise ValueError("Existing game/move/side has a different FEN; refusing to overwrite")
        timings['links_seconds'] = perf_counter() - started
    async with _step(reporter, "appearance_counts", len(fens), "positions"):
        started = perf_counter()
        await conn.execute(f"""UPDATE {fen} f SET n_games=COALESCE(f.n_games,0)+d.occurrences
            FROM ingestion_link_delta d WHERE d.fen_id=f.fen_id""")
        delta = await conn.fetchrow(f"""SELECT
            COALESCE(SUM(d.occurrences),0)::bigint AS added_links,
            COALESCE(SUM(d.occurrences) FILTER(WHERE ABS(f.score)<50),0)::bigint AS equal,
            COALESCE(SUM(d.occurrences) FILTER(WHERE ABS(f.score)>=50 AND ABS(f.score)<150),0)::bigint AS small,
            COALESCE(SUM(d.occurrences) FILTER(WHERE ABS(f.score)>=150 AND ABS(f.score)<300),0)::bigint AS clear,
            COALESCE(SUM(d.occurrences) FILTER(WHERE ABS(f.score)>=300 AND ABS(f.score)<9000),0)::bigint AS decisive,
            COALESCE(SUM(d.occurrences) FILTER(WHERE ABS(f.score)>=9000),0)::bigint AS mate
            FROM ingestion_link_delta d JOIN ingestion_fen_resolved f USING(fen_id)""")
        # Fail closed if the required singleton caches are missing, rather than
        # committing links with silently absent counter deltas.
        for table in ("database_summary", "scored_position_summary"):
            if not await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {layout.table(table)} WHERE id=1)"):
                raise RuntimeError(f"Missing {table}; initialize summaries before ingestion")
        await conn.execute(f"""UPDATE {layout.table('database_summary')}
            SET n_positions=n_positions+$1,unscored_fens=unscored_fens+$1,
                refreshed_at=CURRENT_TIMESTAMP WHERE id=1""", new_count)
        await conn.execute(f"""UPDATE {layout.table('scored_position_summary')}
            SET total_positions=total_positions+$1,unscored_fens=unscored_fens+$1,
                equal_appearances=equal_appearances+$2,small_appearances=small_appearances+$3,
                clear_appearances=clear_appearances+$4,decisive_appearances=decisive_appearances+$5,
                mate_appearances=mate_appearances+$6,refreshed_at=CURRENT_TIMESTAMP WHERE id=1""",
            new_count, delta["equal"], delta["small"], delta["clear"], delta["decisive"], delta["mate"])
        timings['counters_seconds'] = perf_counter() - started
    LOG.info('FEN persistence timings: %s', json.dumps({'fens':len(fens),'links':len(associations),**timings}))
    return {"new_positions": new_count, "new_links": delta["added_links"]}
