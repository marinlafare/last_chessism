"""Small PostgreSQL integration test. Run as postgres in a network-none container.

No production mounts or credentials required; all data lives in tempfile dirs.
"""
import asyncio
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

import asyncpg
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from chessism_api.operations import database_backups as b
from chessism_api.operations import database_restore_tests as r
from chessism_api.operations import database_snapshot as s
from chessism_api.operations.database_snapshot_restore import prepare_cluster, restore_archive


async def main():
    with tempfile.TemporaryDirectory(prefix="snapshot-test-") as temp, ExitStack() as stack:
        root = Path(temp)
        volume, workspace = root / "backup", root / "restore"
        app = volume / "chessism"
        db = app / "database"
        db.mkdir(parents=True)
        source, target = root / "source", workspace / "pgdata"
        source_socket, target_socket = root / "src-socket", root / "dst-socket"
        for path in (source, target, source_socket, target_socket):
            path.mkdir(parents=True, mode=0o700)
        for key, value in {
            "BACKUP_VOLUME_ROOT": volume, "APP_BACKUP_ROOT": app, "DATABASE_BACKUP_ROOT": db,
            "BACKUP_CATALOG_PATH": db / "catalog.json", "MANIFEST_DIRECTORY": db / "manifests",
            "BACKUP_STATUS_PATH": app / "backup-status.json", "BACKUP_USAGE_PATH": app / "backup-usage.json",
            "PGBACKREST_BACKUP_DIRECTORY": db / "pgbackrest" / "backup",
            "PGBACKREST_ARCHIVE_DIRECTORY": db / "pgbackrest" / "archive",
            "FEN_ANALYSIS_BACKUP_ROOT": app / "fen-analysis", "RESEARCH_BACKUP_ROOT": app / "research",
            "FREE_FLOOR_BYTES": 0, "STOP_MARGIN_BYTES": 0,
        }.items():
            stack.enter_context(patch.object(b, key, value))
        stack.enter_context(patch.object(b, "require_storage", return_value={"application_bytes": 0}))
        stack.enter_context(patch.object(b, "storage_snapshot", return_value={"marker_matches": True}))
        stack.enter_context(patch.object(r, "RESTORE_TEST_ROOT", workspace))
        engine = None
        source_connection = restored = None
        try:
            await r._run_command_capture(str(Path(r.PG_CTL).with_name("initdb")), "-D", str(source),
                                         "--username=chessism_user", "--auth=trust", "--locale=C")
            await r._run_command_capture(r.PG_CTL, "-D", str(source), "-l", str(root / "src.log"),
                "-o", f"-c listen_addresses='' -c unix_socket_directories='{source_socket}' -c port=55433", "-w", "start")
            source_connection = await asyncpg.connect(host=str(source_socket), port=55433, user="chessism_user", database="postgres")
            await source_connection.execute("CREATE DATABASE chessism_db TEMPLATE template0")
            await source_connection.close()
            source_connection = await asyncpg.connect(host=str(source_socket), port=55433, user="chessism_user", database="chessism_db")
            await source_connection.execute("""
                CREATE ROLE chessism_export_reader NOLOGIN NOINHERIT;
                CREATE ROLE chessism_export_view_owner NOLOGIN NOINHERIT;
                CREATE SCHEMA chessism_archive;
                CREATE SCHEMA chessism_reads;
                CREATE TABLE public.player(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text NOT NULL UNIQUE);
                CREATE TABLE public.game(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    player_id bigint REFERENCES public.player(id), payload text NOT NULL);
                CREATE INDEX game_player ON public.game(player_id);
                INSERT INTO public.player(name) VALUES ('sample');
                INSERT INTO public.game(player_id,payload) SELECT 1,repeat('compressible data ',50) FROM generate_series(1,1000);
                CREATE VIEW chessism_reads.games AS SELECT g.*, p.name FROM public.game g JOIN public.player p ON p.id=g.player_id;
                CREATE TABLE chessism_archive.game(id bigint PRIMARY KEY, rules text NOT NULL);
                INSERT INTO chessism_archive.game VALUES (1,'chess960');
                CREATE SCHEMA exports_data_v1 AUTHORIZATION chessism_export_view_owner;
                GRANT USAGE ON SCHEMA public TO chessism_export_view_owner;
                GRANT SELECT ON public.game TO chessism_export_view_owner;
                CREATE VIEW exports_data_v1.games WITH(security_barrier=true) AS SELECT id FROM public.game;
                ALTER VIEW exports_data_v1.games OWNER TO chessism_export_view_owner;
                GRANT USAGE ON SCHEMA exports_data_v1 TO chessism_export_reader;
                GRANT SELECT ON exports_data_v1.games TO chessism_export_reader;
            """)
            url = URL.create("postgresql+asyncpg", host=str(source_socket), port=55433,
                             username="chessism_user", database="chessism_db")
            engine = create_async_engine(url)

            async def metadata(session, publish):
                from chessism_api.operations.exports.backup_roles import role_manifest
                # pg_export_snapshot was already called. This concurrent write
                # must not leak into the archive or its snapshot-bound metadata.
                await source_connection.execute("INSERT INTO public.player(name) VALUES ('after_snapshot')")
                count = await session.scalar(text("SELECT count(*) FROM public.player"))
                assert count == 1
                locale = (await session.execute(text("""SELECT datname AS name,pg_encoding_to_char(encoding) AS encoding,
                    datcollate AS collate,datctype AS ctype FROM pg_database WHERE datname=current_database()"""))).mappings().one()
                return {"database": {**dict(locale), "owner": "chessism_user"},
                        "database_support_roles": await role_manifest(session,'chessism_user'),
                        "database_bytes": int(await session.scalar(text("SELECT pg_database_size(current_database())"))),
                        "matrices": {"count": 0, "bytes_added": 0}, "matrix_definitions": {"count": 0}}

            stack.enter_context(patch.object(s, "snapshot_metadata", side_effect=metadata))
            async with AsyncSession(engine) as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                result = await s.run_snapshot_backup({"job_id": "isolated-snapshot-test", "redis": None}, session)
            manifest = b._read_json(Path(result["manifest"]), {})
            assert s.snapshot_path(result["backup_id"]).stat().st_size < 100_000, "Fixture should compress strongly"
            async def progress(detail):
                pass
            await prepare_cluster(result, manifest, target, progress)
            await r._run_command_capture(r.PG_CTL, "-D", str(target), "-l", str(root / "dst.log"),
                                         "-o", r._postgres_start_options(target_socket), "-w", "start")
            await restore_archive(result["backup_id"], target_socket, target, progress)
            restored = await asyncpg.connect(host=str(target_socket), port=55432, user="chessism_user", database="chessism_db")
            assert await restored.fetchval("SELECT count(*) FROM public.player") == 1, "Consistent exported snapshot"
            assert await restored.fetchval("SELECT count(*) FROM chessism_reads.games") == 1000
            assert await restored.fetchval("SELECT count(*) FROM exports_data_v1.games") == 1000
            assert await restored.fetchval("SELECT bool_and(NOT rolcanlogin) FROM pg_roles WHERE rolname IN ('chessism_export_reader','chessism_export_view_owner')")
            assert await restored.fetchval("SELECT has_table_privilege('chessism_export_reader','exports_data_v1.games','SELECT')")
            assert await restored.fetchval("SELECT rules FROM chessism_archive.game") == "chess960"
            assert await restored.fetchval("SELECT count(*) FROM pg_indexes WHERE indexname='game_player'") == 1
            identity = await restored.fetchval("INSERT INTO public.player(name) VALUES ('new') RETURNING id")
            assert identity >= 2, "Sequence state preserved"
            try:
                await restored.execute("INSERT INTO public.game(player_id,payload) VALUES (999,'bad')")
                raise AssertionError("Foreign key was not restored")
            except asyncpg.ForeignKeyViolationError:
                pass
            assert await source_connection.fetchval("SELECT count(*) FROM public.player") == 2, "Source unchanged by restore"
            print(json.dumps({"state": "succeeded", "compressed_bytes": result["repository_bytes"],
                              "checks": ["snapshot consistency", "compression", "restore", "views", "archive schema", "indexes", "sequences", "foreign keys", "source isolation"]}))
        finally:
            if restored:
                await restored.close()
            if source_connection:
                await source_connection.close()
            if engine:
                await engine.dispose()
            for data in (source, target):
                await r._stop_restored_postgres(data)


asyncio.run(main())
