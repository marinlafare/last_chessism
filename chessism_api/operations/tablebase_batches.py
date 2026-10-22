"""One streamed candidate discovery and bounded, set-based endgame writes."""
import struct
import tempfile

from sqlalchemy import text
from chessism_api.database.engine import AsyncDBSession
from chessism_api.database.compact_storage import compact_for, integer_result
from chessism_api.database.relational_storage import driver_connection


class CandidateSpool:
    """Only numeric IDs on temporary disk; no large Python list/Redis payload."""
    def __init__(self):
        self.file = tempfile.TemporaryFile(prefix="chessism-endgames-")
        self.count = 0

    async def discover(self, game_links, limit, predicate, reporter):
        params = {"limit": limit}
        if game_links is not None:
            query = f"""SELECT DISTINCT f.fen_id FROM game_fen_association a
                JOIN fen f ON f.fen_id=a.fen_id
                WHERE a.game_link=ANY(CAST(:links AS bigint[])) AND {predicate} LIMIT :limit"""
            params["links"] = list(game_links)
        else:
            query = f"SELECT f.fen_id FROM fen f WHERE {predicate} LIMIT :limit"
        # No ordering requirement for endgame results: each FEN is independent.
        async with AsyncDBSession() as session:
            result = await session.stream(text(query), params)
            async for rows in result.partitions(5000):
                identifiers = [int(row[0]) for row in rows]
                self.file.write(struct.pack(f"!{len(identifiers)}q", *identifiers))
                self.count += len(identifiers)
                await reporter.advance(len(identifiers))
        self.file.seek(0)

    def read(self, size):
        data = self.file.read(size * 8)
        return list(struct.unpack(f"!{len(data) // 8}q", data)) if data else []

    def close(self):
        self.file.close()


async def save_compact_batch(session, solved, unavailable):
    layout = compact_for(session)
    conn = await driver_connection(session)
    target = layout.table("fen")
    await conn.execute("""CREATE TEMP TABLE tablebase_result_input(
        fen text PRIMARY KEY,piece_count smallint,score integer,next_moves text,
        wdl_win smallint,wdl_draw smallint,wdl_loss smallint,
        tablebase_wdl smallint,tablebase_dtz integer,available boolean
    ) ON COMMIT DROP""")
    rows = []
    for original in solved:
        row = integer_result(original)
        rows.append((row["fen"], row["piece_count"], row["score"], row["next_moves"],
                     row["wdl_win"], row["wdl_draw"], row["wdl_loss"],
                     row["tablebase_wdl"], row["tablebase_dtz"], True))
    for row in unavailable:
        rows.append((row["fen"], row["piece_count"], None, None, None, None, None, None, None, False))
    await conn.copy_records_to_table("tablebase_result_input",
        columns=["fen", "piece_count", "score", "next_moves", "wdl_win", "wdl_draw",
                 "wdl_loss", "tablebase_wdl", "tablebase_dtz", "available"], records=rows)
    await conn.execute("ANALYZE pg_temp.tablebase_result_input")
    # Caller owns FEN row locks and has rechecked cloud claims.
    await conn.execute(f"""UPDATE {target} f SET piece_count=i.piece_count,
        score=CASE WHEN i.available THEN i.score ELSE f.score END,
        next_moves=CASE WHEN i.available THEN i.next_moves ELSE f.next_moves END,
        wdl_win=CASE WHEN i.available THEN i.wdl_win ELSE f.wdl_win END,
        wdl_draw=CASE WHEN i.available THEN i.wdl_draw ELSE f.wdl_draw END,
        wdl_loss=CASE WHEN i.available THEN i.wdl_loss ELSE f.wdl_loss END,
        tablebase_wdl=CASE WHEN i.available THEN i.tablebase_wdl ELSE f.tablebase_wdl END,
        tablebase_dtz=CASE WHEN i.available THEN i.tablebase_dtz ELSE f.tablebase_dtz END,
        analyzed_at=CASE WHEN i.available THEN CURRENT_TIMESTAMP ELSE f.analyzed_at END,
        analysis_source=CASE WHEN i.available THEN 'tablebase' ELSE 'tablebase_unavailable' END
        FROM tablebase_result_input i WHERE f.fen=i.fen""")
