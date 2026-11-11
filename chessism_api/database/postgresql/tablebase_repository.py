"""Canonical tablebase candidates, leases and publication; no engine execution."""
import struct
import tempfile
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from chessism_api.evaluations import TABLEBASE_MAX_PIECES, integer, validate_batch
from .engine import AsyncDBSession
from .analysis_cleanup import finish_analysis_cleanup
from .analysis_selection import release_analysis_session
from .fen_claims import unclaimed_locked_fens
from .transactions import driver_connection, write_transaction

PIECE_COUNT_SQL = (
    "COALESCE(f.piece_count, "
    "char_length(translate(split_part(f.fen, ' ', 1), '12345678/', '')))"
)
TABLEBASE_CANDIDATE_SQL = f"""
    f.score IS NULL
    AND NOT EXISTS (SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen = f.fen)
    AND COALESCE(f.analysis_source, '') <> 'tablebase_unavailable'
    AND {PIECE_COUNT_SQL} BETWEEN 2 AND {TABLEBASE_MAX_PIECES}
"""


class CandidateSpool:
    """Only numeric IDs on temporary disk; no large Python list/Redis payload."""
    def __init__(self):
        self.file = tempfile.TemporaryFile(prefix="chessism-endgames-")
        self.count = 0

    async def discover(self, game_links, limit, advance):
        params = {"limit": limit}
        if game_links is not None:
            query = f"""SELECT DISTINCT f.fen_id FROM public.game_fen_association a
                JOIN public.fen f ON f.fen_id=a.fen_id
                WHERE a.game_link=ANY(CAST(:links AS bigint[])) AND {TABLEBASE_CANDIDATE_SQL} LIMIT :limit"""
            params["links"] = list(game_links)
        else:
            query = f"SELECT f.fen_id FROM public.fen f WHERE {TABLEBASE_CANDIDATE_SQL} LIMIT :limit"
        # No ordering requirement for endgame results: each FEN is independent.
        async with write_transaction(AsyncDBSession) as session:
            await session.execute(text('SET TRANSACTION READ ONLY'))
            result = await session.stream(text(query), params)
            try:
                async for rows in result.partitions(5000):
                    identifiers = [int(row[0]) for row in rows]
                    self.file.write(struct.pack(f"!{len(identifiers)}q", *identifiers))
                    self.count += len(identifiers)
                    await advance(len(identifiers))
            finally:
                await finish_analysis_cleanup(result.close())
        self.file.seek(0)

    def read(self, size):
        data = self.file.read(size * 8)
        return list(struct.unpack(f"!{len(data) // 8}q", data)) if data else []

    def close(self):
        self.file.close()


def clean_game_links(game_links: Sequence[int] | None) -> list[int]:
    return list(dict.fromkeys(
        int(link) for link in (game_links or []) if link is not None
    ))


async def count_tablebase_candidates(game_links: Sequence[int] | None = None) -> int:
    clean_links = clean_game_links(game_links)
    if game_links is not None and not clean_links:
        return 0

    if clean_links:
        query = f"""
            SELECT COUNT(DISTINCT f.fen)::bigint
            FROM public.game_fen_association gfa
            JOIN public.fen f ON f.fen_id = gfa.fen_id
            WHERE gfa.game_link = ANY(CAST(:game_links AS BIGINT[]))
              AND {TABLEBASE_CANDIDATE_SQL};
        """
        params = {"game_links": clean_links}
    else:
        query = f"SELECT COUNT(*)::bigint FROM public.fen f WHERE {TABLEBASE_CANDIDATE_SQL};"
        params = {}

    async with write_transaction(AsyncDBSession) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        return int(await session.scalar(text(query), params) or 0)


async def has_tablebase_candidates(game_links=None):
    """Cheap admission check; the worker's single discovery determines the count."""
    clean_links = clean_game_links(game_links)
    if game_links is not None and not clean_links: return False
    scope = ('JOIN public.game_fen_association a ON a.fen_id=f.fen_id '
             'WHERE a.game_link=ANY(CAST(:links AS bigint[])) AND ') if clean_links else 'WHERE '
    async with write_transaction(AsyncDBSession) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        return bool(await session.scalar(text(
            f'SELECT EXISTS(SELECT 1 FROM public.fen f {scope} {TABLEBASE_CANDIDATE_SQL})'), {'links':clean_links}))


async def read_tablebase_batch(identifiers):
    """Close the read transaction before probing. Publication rechecks under locks."""
    async with write_transaction(AsyncDBSession) as session:
        await session.execute(text('SET TRANSACTION READ ONLY'))
        rows = (await session.execute(text(f'''SELECT f.fen,{PIECE_COUNT_SQL}::smallint AS piece_count
            FROM public.fen f WHERE f.fen_id=ANY(CAST(:ids AS bigint[])) AND {TABLEBASE_CANDIDATE_SQL}
            ORDER BY f.fen'''), {'ids':identifiers})).mappings().all()
        return [dict(row) for row in rows]


async def lease_tablebase_batch(
    game_links: Sequence[int] | None,
    limit: int,
    candidate_ids: list[int] | None = None,
) -> tuple[AsyncSession | None, list[dict[str, Any]]]:
    clean_links = clean_game_links(game_links)
    if game_links is not None and not clean_links:
        return None, []

    session = AsyncDBSession()
    try:
        await session.begin()
        if candidate_ids is not None:
            query = f"""SELECT f.fen, {PIECE_COUNT_SQL}::smallint AS piece_count
                FROM public.fen f WHERE f.fen_id=ANY(CAST(:ids AS bigint[]))
                  AND {TABLEBASE_CANDIDATE_SQL}
                ORDER BY f.fen FOR UPDATE OF f"""
            params = {"ids": candidate_ids}
        elif clean_links:
            query = f"""
                WITH candidate_fens AS MATERIALIZED (
                    SELECT f.fen, MIN(gfa.n_move) AS first_move
                    FROM public.game_fen_association gfa
                    JOIN public.fen f ON f.fen_id = gfa.fen_id
                    WHERE gfa.game_link = ANY(CAST(:game_links AS BIGINT[]))
                      AND {TABLEBASE_CANDIDATE_SQL}
                    GROUP BY f.fen
                )
                SELECT f.fen, {PIECE_COUNT_SQL}::smallint AS piece_count
                FROM candidate_fens candidate
                JOIN public.fen f ON f.fen = candidate.fen
                ORDER BY candidate.first_move, f.fen
                LIMIT :limit
                FOR UPDATE OF f SKIP LOCKED;
            """
            params = {"game_links": clean_links, "limit": max(1, int(limit))}
        else:
            query = f"""
                SELECT f.fen, {PIECE_COUNT_SQL}::smallint AS piece_count
                FROM public.fen f
                WHERE {TABLEBASE_CANDIDATE_SQL}
                ORDER BY f.n_games DESC, f.fen
                LIMIT :limit
                FOR UPDATE OF f SKIP LOCKED;
            """
            params = {"limit": max(1, int(limit))}

        rows = [dict(row) for row in (await session.execute(text(query), params)).mappings()]
        unclaimed = set(await unclaimed_locked_fens(session, [row["fen"] for row in rows]))
        rows = [row for row in rows if row["fen"] in unclaimed]
    except BaseException:
        try:
            await release_analysis_session(session)
        except BaseException as error:
            print(f"Failed to release tablebase selection session: {error!r}", flush=True)
        raise
    if not rows:
        await release_analysis_session(session)
        return None, []
    return session, rows


async def stage_tablebase_results(session, solved, unavailable):
    """Stage one bounded probe batch; the application commits scores and counts."""
    keys = validate_batch(solved, source='tablebase')
    for row in unavailable:
        fen = row.get('fen')
        if not isinstance(fen, str) or not fen or fen in keys:
            raise ValueError('Invalid or duplicate unavailable FEN')
        keys.add(fen)
    if len(keys) > 5000:
        raise ValueError('Tablebase publication exceeds the bounded probe batch')
    for row in [*solved, *unavailable]:
        integer(row.get('piece_count'), 'piece_count', 2, TABLEBASE_MAX_PIECES)
    if not keys:
        return

    connection = await driver_connection(session)
    previous = await connection.fetch('''SELECT fen,score FROM public.fen
        WHERE fen=ANY($1::text[]) ORDER BY fen FOR UPDATE''', list(keys))
    if len(previous) != len(keys) or any(row['score'] is not None for row in previous):
        raise ValueError('Tablebase FENs disappeared or are already scored')
    if set(await unclaimed_locked_fens(session, list(keys))) != keys:
        raise ValueError('Tablebase FENs are claimed; refusing to overwrite analysis')

    # Preserve the existing bounded COPY/set-based update. The temporary input
    # belongs to this transaction and can be reused for another staged batch.
    await connection.execute('''CREATE TEMP TABLE IF NOT EXISTS tablebase_result_input(
        fen text PRIMARY KEY,piece_count smallint,score integer,next_moves text,
        wdl_win smallint,wdl_draw smallint,wdl_loss smallint,
        tablebase_wdl smallint,tablebase_dtz integer,available boolean
    ) ON COMMIT DROP;
    TRUNCATE pg_temp.tablebase_result_input''')
    numeric = ('piece_count', 'score', 'wdl_win', 'wdl_draw', 'wdl_loss', 'tablebase_wdl', 'tablebase_dtz')
    fields = ('fen', 'piece_count', 'score', 'next_moves', 'wdl_win', 'wdl_draw',
              'wdl_loss', 'tablebase_wdl', 'tablebase_dtz')
    rows = [tuple(int(row[name]) if name in numeric and row.get(name) is not None else row.get(name)
                  for name in fields) + (True,) for row in solved]
    rows.extend((row['fen'], int(row['piece_count']), None, None, None, None, None, None, None, False)
                for row in unavailable)
    await connection.copy_records_to_table('tablebase_result_input', schema_name='pg_temp',
        columns=(*fields, 'available'), records=rows)
    await connection.execute('ANALYZE pg_temp.tablebase_result_input')
    status = await connection.execute('''UPDATE public.fen f SET piece_count=i.piece_count,
        score=CASE WHEN i.available THEN i.score ELSE f.score END,
        next_moves=CASE WHEN i.available THEN i.next_moves ELSE f.next_moves END,
        wdl_win=CASE WHEN i.available THEN i.wdl_win ELSE f.wdl_win END,
        wdl_draw=CASE WHEN i.available THEN i.wdl_draw ELSE f.wdl_draw END,
        wdl_loss=CASE WHEN i.available THEN i.wdl_loss ELSE f.wdl_loss END,
        tablebase_wdl=CASE WHEN i.available THEN i.tablebase_wdl ELSE f.tablebase_wdl END,
        tablebase_dtz=CASE WHEN i.available THEN i.tablebase_dtz ELSE f.tablebase_dtz END,
        analyzed_at=CASE WHEN i.available THEN CURRENT_TIMESTAMP ELSE f.analyzed_at END,
        analysis_source=CASE WHEN i.available THEN 'tablebase' ELSE 'tablebase_unavailable' END
        FROM pg_temp.tablebase_result_input i WHERE f.fen=i.fen''')
    if status != f'UPDATE {len(keys)}':
        raise ValueError('Not every tablebase result matched; refusing a partial batch')

