"""Network-isolated PostgreSQL checks; never mount a production PG directory."""
import asyncio
from contextlib import suppress
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

import asyncpg
import chess
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects.postgresql import dialect

from chessism_api.database.models import Fen
from chessism_api.database.relational_storage import RelationalStorage, SESSION_KEY
from chessism_api.operations import database_restore_tests as restore
from chessism_api.operations.ingestion_pipeline.fen_bulk import persist_positions
from chessism_api.operations.ingestion_pipeline.fen_core import process_single_game_sync, aggregate_fen_data
from chessism_api.operations import tablebase
from chessism_api.operations.tablebase_batches import CandidateSpool


async def fixture(conn):
    await conn.execute(str(CreateTable(Fen.__table__).compile(dialect=dialect())))
    await conn.execute("""
        ALTER TABLE fen ALTER COLUMN score TYPE integer;
        CREATE TABLE game(link bigint PRIMARY KEY,fens_done boolean DEFAULT true,
            fens_processing boolean DEFAULT false);
        CREATE TABLE game_player(game_link bigint);
        CREATE TABLE moves(link bigint,n_move int,white_move text,black_move text,
            PRIMARY KEY(link,n_move));
        CREATE TABLE game_fen_association(
            id bigint GENERATED ALWAYS AS IDENTITY,
            game_link bigint REFERENCES game(link), n_move integer, is_white boolean,
            fen_id bigint REFERENCES fen(fen_id), UNIQUE(game_link,n_move,is_white));
        CREATE INDEX ON game_fen_association(fen_id);
        CREATE TABLE cloud_fen_claim(fen text PRIMARY KEY);
        CREATE TABLE game_analysis_coverage(
            link bigint PRIMARY KEY,total_positions int,analyzed_positions int,
            unscored_positions int,is_fully_analyzed boolean,equal_positions int,
            small_positions int,clear_positions int,decisive_positions int,mate_positions int,
            refreshed_at timestamptz);
        CREATE TABLE database_summary(id integer PRIMARY KEY,n_positions bigint,
            unscored_fens bigint,refreshed_at timestamptz);
        CREATE TABLE scored_position_summary(id integer PRIMARY KEY,total_positions bigint,
            unscored_fens bigint,equal_appearances bigint DEFAULT 0,small_appearances bigint DEFAULT 0,
            clear_appearances bigint DEFAULT 0,decisive_appearances bigint DEFAULT 0,
            mate_appearances bigint DEFAULT 0,refreshed_at timestamptz);
        INSERT INTO database_summary VALUES(1,0,0,now());
        INSERT INTO scored_position_summary(id,total_positions,unscored_fens) VALUES(1,0,0);
        INSERT INTO game(link) SELECT generate_series(1,1000);
    """)


async def exercise(conn, factory):
    rows, errors = process_single_game_sync((1, [
        {"n_move": 1, "white_move": "e4", "black_move": "e5"},
        {"n_move": 2, "white_move": "Nf3", "black_move": "Nc6"},
    ]))
    assert not errors
    fens, links = aggregate_fen_data(rows)
    async def save(fens=fens, links=links):
        async with factory.begin() as session:
            return await persist_positions(session, fens, links)
    first = await save()
    assert first == {"new_positions": 4, "new_links": 4}, first
    assert await save() == {"new_positions": 0, "new_links": 0}
    await asyncio.gather(save(), save())
    assert await conn.fetchval("SELECT SUM(n_games) FROM fen") == 4
    assert await conn.fetchval("SELECT n_positions FROM database_summary") == 4
    # New occurrences of a scored and cloud-reserved FEN retain its analysis.
    chosen = fens[0]["fen"]
    await conn.execute("UPDATE fen SET score=25,next_moves='e7e5' WHERE fen=$1", chosen)
    await conn.execute("INSERT INTO cloud_fen_claim VALUES($1)", chosen)
    second_links = [{**row, "game_link": 2} for row in links]
    assert (await save(links=second_links))["new_links"] == 4
    assert await conn.fetchval("SELECT score FROM fen WHERE fen=$1", chosen) == 25
    assert await conn.fetchval("SELECT equal_appearances FROM scored_position_summary") == 1
    assert await conn.fetchval("SELECT count(*) FROM cloud_fen_claim") == 1
    before = await conn.fetchval("SELECT SUM(n_games) FROM fen")
    bad_links = [{**links[0], "fen_fen": fens[1]["fen"]}]
    try:
        await save(links=bad_links)
        raise AssertionError("Conflicting position accepted")
    except ValueError:
        pass
    assert await conn.fetchval("SELECT SUM(n_games) FROM fen") == before
    for invalid in ({**links[0], "move_color": "unknown"},
                    {**links[0], "fen_fen": "not-a-stored-fen"}):
        try:
            await save(links=[invalid])
            raise AssertionError("Invalid link accepted")
        except ValueError:
            pass
    # The old string looked up a globally existing row outside the locked batch.
    # Refuse that malformed input even when the target FEN exists in the database.
    try:
        await save(fens=fens[:1],links=[{**links[-1],'game_link':3}])
        raise AssertionError('Association outside locked identity set accepted')
    except ValueError:pass
    try:
        async with factory.begin() as session:
            await persist_positions(session, fens, [{**row, "game_link": 3} for row in links])
            raise RuntimeError("simulated interruption")
    except RuntimeError:
        pass
    assert not await conn.fetchval("SELECT count(*) FROM game_fen_association WHERE game_link=3")
    assert await conn.fetchval("SELECT SUM(n_games) FROM fen") == before

    endgame = "8/8/8/8/8/2k5/8/K7 w - -"
    await conn.execute("""INSERT INTO fen(fen,n_games,moves_counter,piece_count)
        VALUES($1,0,'#0_1',2)""", endgame)
    identity = await conn.fetchval("SELECT fen_id FROM fen WHERE fen=$1", endgame)
    with patch.object(tablebase, "AsyncDBSession", factory):
        session, selected = await tablebase._lease_tablebase_batch(None, 1, candidate_ids=[identity])
        assert len(selected) == 1
        solved = {"fen": endgame, "piece_count": 2, "score": 0, "next_moves": "a1b1",
                  "wdl_win": 0, "wdl_draw": 1000, "wdl_loss": 0,
                  "tablebase_wdl": 0, "tablebase_dtz": 0}
        await tablebase._save_tablebase_batch(session, [solved], [])
        await session.commit()
        await session.close()
        assert await conn.fetchval("SELECT analysis_source FROM fen WHERE fen=$1", endgame) == "tablebase"
        await conn.execute("UPDATE fen SET score=NULL,analysis_source=NULL WHERE fen=$1", endgame)
        await conn.execute("INSERT INTO cloud_fen_claim VALUES($1)", endgame)
        session, selected = await tablebase._lease_tablebase_batch(None, 1, candidate_ids=[identity])
        assert session is None and selected == []
        await conn.execute("DELETE FROM cloud_fen_claim WHERE fen=$1", endgame)
        # Reads hold no FEN lock while probing. A concurrent score/claim wins at
        # publication, and is not mislabeled or counted as our tablebase result.
        for changed in ('claim','score'):
            await conn.execute("UPDATE fen SET score=NULL,analysis_source=NULL WHERE fen=$1",endgame)
            initial=await tablebase._read_tablebase_batch([identity])
            assert len(initial)==1
            async with conn.transaction():
                await conn.execute('SELECT fen_id FROM fen WHERE fen_id=$1 FOR UPDATE NOWAIT',identity)
                if changed=='claim':await conn.execute('INSERT INTO cloud_fen_claim VALUES($1)',endgame)
                else:await conn.execute("UPDATE fen SET score=77,analysis_source='stockfish' WHERE fen_id=$1",identity)
            session,selected=await tablebase._lease_tablebase_batch(None,1,candidate_ids=[identity])
            assert session is None and selected==[]
            if changed=='claim':await conn.execute('DELETE FROM cloud_fen_claim WHERE fen=$1',endgame)
            else:assert await conn.fetchval('SELECT score FROM fen WHERE fen_id=$1',identity)==77
        await conn.execute("UPDATE fen SET score=NULL,analysis_source=NULL WHERE fen=$1",endgame)
        # Reservation commits while selection waits for the FEN lock. The
        # second, fresh snapshot must reject it rather than overwrite a claim.
        async with factory.begin() as reserver:
            await reserver.execute(text("SELECT fen FROM fen WHERE fen=:fen FOR UPDATE"), {"fen": endgame})
            waiting = asyncio.create_task(tablebase._lease_tablebase_batch(None, 1, candidate_ids=[identity]))
            await asyncio.sleep(0.05)
            await reserver.execute(text("INSERT INTO cloud_fen_claim VALUES(:fen)"), {"fen": endgame})
        session, selected = await asyncio.wait_for(waiting, 5)
        assert session is None and selected == []
        await conn.execute("DELETE FROM cloud_fen_claim WHERE fen=$1", endgame)
    from chessism_api.operations import tablebase_batches
    with patch.object(tablebase_batches, "AsyncDBSession", factory):
        spool = CandidateSpool()
        try:
            await spool.discover(None, 1000, tablebase.TABLEBASE_CANDIDATE_SQL, AsyncMock())
            assert spool.count == 1
            assert spool.read(10) == [identity]
            assert spool.read(10) == []
        finally:
            spool.close()

    # A modest throughput check, not a production load test.
    many = [{**row, "game_link": game} for game in range(10, 310) for row in links]
    started = time.monotonic()
    result = await save(links=many)
    assert result["new_links"] == 1200
    print(f"PASS 1,200-link batch in {time.monotonic()-started:.3f}s; "
          "duplicate/concurrent retries, rollback, conflict and cloud claims", flush=True)
    from chessism_api.operations.ingestion_pipeline import fen_batches
    await conn.execute("""UPDATE game SET fens_done=false WHERE link=800;
        INSERT INTO moves VALUES(800,1,'e4','e5'),(800,2,'Nf3','Nc6')""")
    with patch.object(fen_batches, "AsyncDBSession", factory):
        result = await fen_batches.run_fen_batch_job({"job_id": "isolated-worker"}, 1)
        assert result["successful_game_links"] == [800]
        assert result["positions"] == 4 and "associations" not in result
        before = await conn.fetchval("SELECT SUM(n_games) FROM fen")
        # Simulate coordinator restart before publishing fens_done.
        await conn.execute("UPDATE game SET fens_processing=false WHERE link=800")
        retried = await fen_batches.run_fen_batch_job({"job_id": "isolated-retry"}, 1)
        assert retried["new_links"] == 0
        assert await conn.fetchval("SELECT SUM(n_games) FROM fen") == before
        assert await conn.fetchval("SELECT fens_processing FROM game WHERE link=800")
    print("PASS bounded SAN worker and coordinator-restart replay", flush=True)
    from chessism_api.operations.ingestion_pipeline import fen_repository
    from chessism_api.operations.relational_coverage import refresh_coverage
    await conn.execute("""UPDATE game SET fens_done=true,fens_processing=false WHERE link=800;
        UPDATE game SET fens_done=false WHERE link IN(801,802);
        INSERT INTO moves VALUES(801,1,'e4','e5'),(801,2,'Nf3','Nc6'),
            (802,1,'e4','e5'),(802,2,'Nf3','Nc6')""")
    redis = AsyncMock()
    redis.eval.return_value = [2, 2, 0]
    tasks = []
    async def enqueue(function, **kwargs):
        assert function == "run_fen_batch_job"
        kwargs.pop("_queue_name")
        task = asyncio.create_task(fen_batches.run_fen_batch_job(
            {"job_id": f"child-{len(tasks)}", "redis": redis}, **kwargs))
        tasks.append(task)
        return SimpleNamespace(result=lambda timeout=None: task)
    redis.enqueue_job.side_effect = enqueue
    async def coverage(links):
        async with factory.begin() as session:
            return await refresh_coverage(session, list(links))
    with patch.object(fen_batches, "AsyncDBSession", factory), \
            patch.object(fen_repository, "AsyncDBSession", factory), \
            patch.object(fen_batches, "refresh_game_analysis_summary", side_effect=coverage), \
            patch.object(fen_batches, "refresh_fen_pipeline_summary", new_callable=AsyncMock):
        result = await fen_batches.run_bounded_pipeline(
            {"job_id": "parent", "redis": redis}, 2, 1, 2)
        assert result["successful_game_links"] == [801, 802]
    assert all(task.done() for task in tasks)
    assert await conn.fetchval("SELECT bool_and(fens_done AND NOT fens_processing) FROM game WHERE link IN(801,802)")
    rows = await conn.fetch("SELECT total_positions,analyzed_positions FROM game_analysis_coverage ORDER BY link")
    assert [(r[0], r[1]) for r in rows] == [(4, 1), (4, 1)]
    print("PASS parallel coordinator, committed coverage and final game state", flush=True)


async def main():
    if Path("/var/lib/postgresql/data/PG_VERSION").exists():
        raise RuntimeError("Production PostgreSQL data must not be mounted")
    root = Path(tempfile.mkdtemp(prefix="ingestion-check-", dir="/restore-test"))
    pgdata, socket = root / "pgdata", root / "socket"
    pgdata.mkdir(); socket.mkdir()
    conn = engine = None
    try:
        await restore._run_command_capture(str(Path(restore.PG_CTL).with_name("initdb")),
            "-D", str(pgdata), "--username=chessism_user", "--auth=trust", "--locale=C")
        await restore._run_command_capture(restore.PG_CTL, "-D", str(pgdata), "-l",
            str(root / "postgres.log"), "-w", "-o", restore._postgres_start_options(socket), "start")
        conn = await asyncpg.connect(host=str(socket), port=55432, user="chessism_user", database="postgres")
        await fixture(conn)
        engine = create_async_engine(URL.create("postgresql+asyncpg", username="chessism_user",
            host=str(socket), port=55432, database="postgres"))
        factory = async_sessionmaker(engine, expire_on_commit=False,
            info={SESSION_KEY: RelationalStorage("public", "archive", "reads", "relational_v3")})
        # Exercise real ingestion with the production refresh triggers installed.
        # Their empty conflict-update statements must remain cheap and idempotent.
        from chessism_api.operations.fen_summary_recovery import install
        async with factory.begin() as session:
            await install(session, session.info[SESSION_KEY])
        await asyncio.wait_for(exercise(conn, factory), 180)
        await asyncio.wait_for(exercise_coverage(conn, factory), 90)
    finally:
        if engine:
            await engine.dispose()
        if conn:
            await conn.close()
        with suppress(Exception):
            await restore._run_command_capture(restore.PG_CTL, "-D", str(pgdata), "-m", "fast", "-w", "stop")


async def exercise_coverage(conn, factory):
    from chessism_api.operations.relational_coverage import FIELDS, coverage_source_sql, refresh_coverage
    layout = RelationalStorage('public', 'archive', 'reads', 'relational_v3')
    scores = [None, 0, -49, 50, -149, 150, -299, 300, -8999, 9000, -10000]
    identities = []
    for index, score in enumerate(scores):
        identities.append(await conn.fetchval("""INSERT INTO fen(fen,n_games,moves_counter,score)
            VALUES($1,1,'fixture',$2) RETURNING fen_id""", f'coverage-fixture-{index}', score))
    await conn.executemany("""INSERT INTO game_fen_association(game_link,n_move,is_white,fen_id)
        VALUES($1,$2,true,$3)""", [(901,index+1,identity) for index,identity in enumerate(identities)]
        + [(902,index+1,identities[1]) for index in range(10)]
        + [(904,1,identities[0]), (905,1,identities[1])])
    # An obsolete empty-game cache row must be removed, not turned into 0/0=100%.
    await conn.execute('INSERT INTO game_analysis_coverage(link) VALUES(903)')
    async def refresh(links=None, **kwargs):
        async with factory.begin() as session:
            return await refresh_coverage(session, links, **kwargs)
    async def snapshot():
        return [tuple(row) for row in await conn.fetch(
            f"SELECT link,{','.join(FIELDS)} FROM game_analysis_coverage WHERE link>=901 ORDER BY link")]
    async with factory.begin() as session:
        rows = await refresh_coverage(session, [901, 902, 903, 904, 905, 901])
        assert [row['link'] for row in rows] == [901,902,904,905]
        scoped = (await session.execute(text(coverage_source_sql(layout, scoped=True)))).all()
        grouped = (await session.execute(text(coverage_source_sql(layout, scoped=False)))).all()
        assert scoped == grouped, 'Scoped and whole-corpus formulas disagree'
    before = await snapshot()
    assert before[0] == (901,11,10,1,False,2,2,2,2,2), before[0]
    assert before[1] == (902,10,10,0,True,10,0,0,0,0), before[1]
    assert await refresh([]) == []
    assert await refresh(fens=[]) == []
    assert await snapshot() == before
    await asyncio.gather(refresh([901,902,904]), refresh([902,901,904]))
    assert await snapshot() == before, 'Concurrent replay changed absolute counters'
    # Shared FENs must refresh other players' games too; repeated occurrences
    # within one game must still count separately.
    await conn.execute('UPDATE fen SET score=100 WHERE fen_id=$1', identities[1])
    changed = await refresh(fens=['coverage-fixture-1'])
    assert [row['link'] for row in changed] == [901,902,905]
    after = await snapshot()
    assert after[1] == (902,10,10,0,True,0,10,0,0,0), after[1]
    assert after[-1] == (905,1,1,0,True,0,1,0,0,0), after[-1]
    await refresh(fens=['coverage-fixture-1'])
    assert await snapshot() == after
    try:
        async with factory.begin() as session:
            await session.execute(text('UPDATE fen SET score=NULL WHERE fen_id=:id'), {'id':identities[1]})
            await refresh_coverage(session, [901,902,905])
            raise RuntimeError('interrupted transaction')
    except RuntimeError:
        pass
    assert await snapshot() == after, 'Rollback published uncommitted coverage'
    await refresh()
    assert await snapshot() == after
    print('PASS scoped/full parity, score boundaries, shared FENs, empty scopes, concurrent replay and rollback', flush=True)


if __name__ == "__main__":
    asyncio.run(main())
