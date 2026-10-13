"""Read-only archive diagnosis: restore schema (not data) in a private cluster.

Run in a network-isolated backup-worker container with the backup volume
mounted read-only. Never connects to the live database or changes its catalog.
"""
import argparse
import asyncio
import json
from pathlib import Path
import tempfile

from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from chessism_api.database.relational_storage import RelationalStorage, describe_contract
from chessism_api.database.backup_contract import contract_differences
from chessism_api.operations import database_restore_tests as r


def differences(expected, actual):
    result = {}
    keys = {"columns": ("table_schema", "table_name", "column_name"),
            "constraints": ("schema", "table", "kind", "definition"),
            "views": ("table_name",), "indexes": ("schemaname", "tablename", "indexname")}
    for section in expected.keys() | actual.keys():
        if expected.get(section) == actual.get(section):
            continue
        if section not in keys:
            result[section] = {"expected": expected.get(section), "actual": actual.get(section)}
            continue
        identity = keys[section]
        before = {tuple(row[k] for k in identity): row for row in expected[section]}
        after = {tuple(row[k] for k in identity): row for row in actual[section]}
        changed = [{"key": key, "expected": before.get(key), "actual": after.get(key)}
                   for key in sorted(before.keys() | after.keys()) if before.get(key) != after.get(key)]
        result[section] = {"expected_count": len(expected[section]), "actual_count": len(actual[section]),
                           "order_only": not changed, "changed_count": len(changed), "examples": changed[:12]}
    return result


async def main(args):
    manifest = json.loads(args.manifest.read_text())
    expected = manifest["relational_storage"]
    layout = RelationalStorage(expected["core"], expected["archive"], expected["reads"])
    database = manifest["database"]
    with tempfile.TemporaryDirectory(prefix="schema-check-") as temporary:
        root = Path(temporary)
        pgdata, socket = root / "pgdata", root / "socket"
        pgdata.mkdir(mode=0o700)
        socket.mkdir(mode=0o700)
        engine = None
        try:
            await r._run_command_capture(str(Path(r.PG_CTL).with_name("initdb")), "-D", str(pgdata),
                f"--username={database['owner']}", "--auth-local=trust", "--auth-host=reject",
                f"--encoding={database['encoding']}", f"--lc-collate={database['collate']}", f"--lc-ctype={database['ctype']}")
            await r._run_command_capture(r.PG_CTL, "-D", str(pgdata), "-l", str(root / "postgres.log"),
                                         "-o", r._postgres_start_options(socket), "-w", "start")
            await r._run_command_capture("pg_restore", "--schema-only", "--create", "--exit-on-error",
                f"--host={socket}", "--port=55432", f"--username={database['owner']}", "--dbname=postgres", str(args.archive))
            url = URL.create("postgresql+asyncpg", username=database["owner"], database=database["name"],
                             port=55432, query={"host": str(socket)})
            engine = create_async_engine(url, connect_args={"server_settings": {
                "search_path": layout.search_path, "default_transaction_read_only": "on"}})
            async with engine.connect() as connection:
                actual = await describe_contract(connection, layout)
            print(json.dumps({"backup_id": manifest["backup_id"], "schema_matches": actual == expected,
                              "canonical_schema_matches": not contract_differences(expected, actual),
                              "remaining_differences": contract_differences(expected, actual),
                              "differences": differences(expected, actual)}, indent=2))
        finally:
            if engine:
                await engine.dispose()
            await r._stop_restored_postgres(pgdata)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    asyncio.run(main(parser.parse_args()))
