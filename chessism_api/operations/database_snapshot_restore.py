"""Restore custom-format snapshots only into a disposable, private cluster."""
import asyncio
from pathlib import Path
import os

from .database_snapshot import run_progress_command, snapshot_path


async def prepare_cluster(backup, manifest, pgdata, progress):
    from . import database_restore_tests as r
    from . import database_backups as b
    archive = snapshot_path(backup["backup_id"])
    if manifest.get("format") != "chessism-pg-dump-custom" or manifest.get("archive_file") != archive.name:
        raise RuntimeError("Missing or inconsistent compressed snapshot manifest")
    if not archive.is_file() or archive.stat().st_size != int(backup["repository_bytes"]):
        raise RuntimeError("Compressed snapshot is missing or has the wrong size")
    await progress("Verifying the compressed archive checksum before restoring")
    checksum = await asyncio.to_thread(b._sha256, archive)
    if checksum != manifest.get("archive_sha256") or checksum != backup.get("archive_sha256"):
        raise RuntimeError("Compressed snapshot checksum mismatch")
    database = manifest["database"]
    if database["name"] != r.RESTORE_DATABASE or database["owner"] != r.RESTORE_USER:
        raise RuntimeError("Snapshot database/owner differs from the restore-test configuration")
    await r._run_command_capture(
        str(Path(r.PG_CTL).with_name("initdb")), "-D", str(pgdata),
        f"--username={r.RESTORE_USER}", "--auth-local=trust", "--auth-host=reject",
        f"--encoding={database['encoding']}", f"--lc-collate={database['collate']}",
        f"--lc-ctype={database['ctype']}",
    )


async def restore_archive(backup_id, socket_dir, pgdata, progress):
    from . import database_restore_tests as r
    # Never use DATABASE_URL/PGHOST for a restore. The private socket is
    # mandatory and pgdata is checked against the owned rehearsal workspace.
    if not pgdata.resolve().is_relative_to(r.RESTORE_TEST_ROOT.resolve()):
        raise RuntimeError("Restore target is outside the disposable workspace")
    if not (socket_dir / ".s.PGSQL.55432").exists():
        raise RuntimeError("The private restore PostgreSQL socket is missing")
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env["PGOPTIONS"] = "-c maintenance_work_mem=256MB -c statement_timeout=0"
    def capacity():
        import shutil
        if shutil.disk_usage(r.RESTORE_TEST_ROOT).free < 2_000_000_000:
            raise r.RestoreTestCapacityError("Restore stopped to preserve local disk headroom")
    await run_progress_command(
        "pg_restore", "--create", "--exit-on-error", "--verbose", "--jobs=2", "--no-password",
        f"--host={socket_dir}", "--port=55432", f"--username={r.RESTORE_USER}",
        "--dbname=postgres", str(snapshot_path(backup_id)), env=env,
        progress=progress, capacity_check=capacity,
    )
