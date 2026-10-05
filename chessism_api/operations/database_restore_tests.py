"""Manual, disposable restore rehearsals for pgBackRest recovery points."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chessism_api.operations.backup_coordination import (
    ensure_backup_reservation,
    release_backup,
)
from chessism_api.operations.matrix_backups import validate_matrix_bundle
from chessism_api.operations.matrix_constructor.artifact_files import file_operation
from chessism_api.operations.matrix_constructor.definition_backups import validate_definition_backup


APP_ID = os.getenv("DATABASE_BACKUP_APP_ID", "chessism")
BACKUP_VOLUME_ROOT = Path(os.getenv("BACKUP_VOLUME_ROOT", "/backup-volume"))
EXPECTED_VOLUME_UUID = os.getenv(
    "BACKUP_VOLUME_UUID",
    "1ebffac6-4b7a-4906-9e07-9586060d3825",
)
VOLUME_MARKER = BACKUP_VOLUME_ROOT / ".chessism-backup-volume"
APP_BACKUP_ROOT = BACKUP_VOLUME_ROOT / APP_ID
DATABASE_BACKUP_ROOT = APP_BACKUP_ROOT / "database"
BACKUP_CATALOG_PATH = DATABASE_BACKUP_ROOT / "catalog.json"
BACKUP_STATUS_PATH = APP_BACKUP_ROOT / "backup-status.json"
MANIFEST_DIRECTORY = DATABASE_BACKUP_ROOT / "manifests"
RESTORE_TEST_STATUS_PATH = APP_BACKUP_ROOT / "restore-test-status.json"
RESTORE_TEST_ROOT = Path(os.getenv("DATABASE_RESTORE_TEST_ROOT", "/restore-test"))
RESTORE_TEST_HEADROOM_BYTES = int(os.getenv(
    "DATABASE_RESTORE_TEST_HEADROOM_BYTES",
    "15000000000",
))
RESTORE_TEST_TIMEOUT_SECONDS = int(os.getenv(
    "DATABASE_RESTORE_TEST_TIMEOUT_SECONDS",
    str(6 * 60 * 60),
))
PGBACKREST_STANZA = os.getenv("PGBACKREST_STANZA", "chessism")
RESTORE_DATABASE = os.getenv("RESTORE_TEST_DB_NAME", "chessism_db")
RESTORE_USER = os.getenv("RESTORE_TEST_DB_USER", "chessism_user")
PG_CTL = os.getenv("RESTORE_TEST_PG_CTL", "/usr/lib/postgresql/15/bin/pg_ctl")
PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60
_SAFE_BACKUP_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class RestoreTestUnavailableError(RuntimeError):
    """Raised when a recovery point cannot be rehearsed safely."""


class RestoreTestCapacityError(RuntimeError):
    """Raised when the local disposable restore volume is too small."""


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _default_restore_status() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "not_tested",
        "job_id": None,
        "backup_id": None,
        "started_at": None,
        "completed_at": None,
        "elapsed_seconds": None,
        "database_bytes": None,
        "required_local_bytes": None,
        "local_free_bytes": None,
        "validation": None,
        "error_code": None,
        "error_message": None,
        "detail": "No database recovery point has been restored and tested yet.",
    }


def database_restore_test_status() -> dict[str, Any]:
    payload = _read_json(RESTORE_TEST_STATUS_PATH, {})
    return payload if isinstance(payload, dict) and payload else _default_restore_status()


def cleanup_stale_restore_workspaces() -> list[str]:
    """Remove interrupted restore workspaces without touching unrelated data."""
    RESTORE_TEST_ROOT.mkdir(parents=True, exist_ok=True)
    removed: list[str] = []
    for candidate in RESTORE_TEST_ROOT.iterdir():
        if not candidate.name.startswith("rehearsal-"):
            continue
        if candidate.is_symlink() or not candidate.is_dir():
            candidate.unlink(missing_ok=True)
        else:
            shutil.rmtree(candidate)
        removed.append(candidate.name)
    return sorted(removed)


def latest_backup_for_restore_test() -> dict[str, Any]:
    marker = None
    try:
        marker = VOLUME_MARKER.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    if marker != EXPECTED_VOLUME_UUID:
        raise RestoreTestUnavailableError(
            "The dedicated backup volume is unavailable or has the wrong UUID marker."
        )

    catalog = _read_json(BACKUP_CATALOG_PATH, {"backups": []})
    backups = catalog.get("backups", []) if isinstance(catalog, dict) else []
    if not isinstance(backups, list) or not backups:
        raise RestoreTestUnavailableError("No verified database backup is available to test.")
    latest = backups[0]
    backup_id = str(latest.get("backup_id") or "")
    if not backup_id or not _SAFE_BACKUP_ID.fullmatch(backup_id):
        raise RestoreTestUnavailableError("The latest backup has an invalid recovery-point ID.")
    return dict(latest)


def mark_restore_test_queued(*, job_id: str, backup: dict[str, Any]) -> None:
    _atomic_write_json(RESTORE_TEST_STATUS_PATH, {
        **_default_restore_status(),
        "status": "queued",
        "job_id": job_id,
        "backup_id": backup.get("backup_id"),
        "database_bytes": int(backup.get("database_bytes") or 0),
        "detail": f"Recovery test queued for {backup.get('backup_id')}.",
    })


def mark_restore_test_queue_failed(*, job_id: str, backup_id: str, error: Exception) -> None:
    _atomic_write_json(RESTORE_TEST_STATUS_PATH, {
        **_default_restore_status(),
        "status": "failed",
        "job_id": job_id,
        "backup_id": backup_id,
        "completed_at": _iso_now(),
        "error_code": error.__class__.__name__,
        "error_message": str(error),
        "detail": str(error),
    })


async def _write_progress(
    redis: Any,
    job_id: str,
    *,
    phase: str,
    detail: str,
    result: dict[str, Any] | None = None,
) -> None:
    if redis is None:
        return
    await redis.set(
        f"chessism:job_progress:{job_id}",
        json.dumps({
            "job_id": job_id,
            "kind": "database_restore_test",
            "phase": phase,
            "total": 0,
            "processed": 0,
            "detail": detail,
            "result": result,
            "updated_at": time.time(),
        }),
        ex=PROGRESS_TTL_SECONDS,
    )


async def _run_command_capture(
    *arguments: str,
    allow_failure: bool = False,
) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        stdout, _ = await asyncio.wait_for(
            process.communicate(),
            timeout=RESTORE_TEST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        process.terminate()
        await process.wait()
        raise RuntimeError(f"{arguments[0]} exceeded the restore-test timeout.")
    output = stdout.decode("utf-8", errors="replace") if stdout else ""
    if process.returncode and not allow_failure:
        raise RuntimeError(
            output[-4000:].strip()
            or f"Command {arguments[0]} failed with exit code {process.returncode}."
        )
    return int(process.returncode or 0), output


async def _run_restore(
    *,
    backup_id: str,
    pgdata: Path,
    redis: Any,
    job_id: str,
) -> None:
    process = await asyncio.create_subprocess_exec(
        "pgbackrest",
        f"--stanza={PGBACKREST_STANZA}",
        f"--set={backup_id}",
        f"--pg1-path={pgdata}",
        "--type=immediate",
        "--target-action=promote",
        "--archive-mode=off",
        "--process-max=1",
        "--log-level-console=info",
        "restore",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    started = time.monotonic()
    recent_lines: list[str] = []
    assert process.stdout is not None
    while True:
        try:
            raw_line = await asyncio.wait_for(process.stdout.readline(), timeout=2.0)
        except asyncio.TimeoutError:
            raw_line = b""
        if raw_line:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if line:
                recent_lines.append(line)
                recent_lines = recent_lines[-40:]
                await _write_progress(
                    redis,
                    job_id,
                    phase="restoring",
                    detail=line[-500:],
                )
        if time.monotonic() - started > RESTORE_TEST_TIMEOUT_SECONDS:
            process.terminate()
            await process.wait()
            raise RuntimeError("The pgBackRest restore exceeded the restore-test timeout.")
        if process.returncode is not None:
            break
        if not raw_line:
            try:
                await asyncio.wait_for(process.wait(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

    return_code = await process.wait()
    if return_code:
        raise RuntimeError(
            "\n".join(recent_lines[-15:])
            or f"pgBackRest restore failed with exit code {return_code}."
        )


def _validation_query() -> str:
    return """
WITH required(name, relation_name) AS (
    VALUES
        ('player', 'public.player'),
        ('game', 'public.game'),
        ('moves', 'public.moves'),
        ('fen', 'public.fen'),
        ('game_fen_association', 'public.game_fen_association'),
        ('database_summary', 'public.database_summary'),
        ('fen_pipeline_summary', 'public.fen_pipeline_summary'),
        ('scored_position_summary', 'public.scored_position_summary'),
        ('account', 'public.account')
), missing AS (
    SELECT COALESCE(json_agg(name ORDER BY name), '[]'::json) AS names
    FROM required
    WHERE to_regclass(relation_name) IS NULL
)
SELECT json_build_object(
    'database', current_database(),
    'server_version_num', current_setting('server_version_num')::int,
    'database_bytes', pg_database_size(current_database()),
    'missing_relations', missing.names,
    'fen_primary_key_valid', COALESCE((
        SELECT indisprimary AND indisunique AND indisvalid
        FROM pg_index
        WHERE indexrelid = to_regclass('public.fen_pkey')
    ), false),
    'redundant_fen_index_present', to_regclass('public.ix_fen_fen') IS NOT NULL,
    'database_summary', (
        SELECT json_build_object(
            'games', n_games_in_db,
            'positions', n_positions,
            'analyzed_fens', analyzed_fens,
            'scored_fens', scored_fens
        )
        FROM database_summary WHERE id = 1
    ),
    'pipeline_summary', (
        SELECT json_build_object(
            'parsed_games', parsed_games,
            'analyzable_games', analyzable_games,
            'fen_extracted_games', fen_extracted_games,
            'tablebase_marked_games', tablebase_marked_games
        )
        FROM fen_pipeline_summary WHERE id = 1
    ),
    'scored_summary', (
        SELECT json_build_object(
            'total_positions', total_positions,
            'analyzed_fens', analyzed_fens,
            'scored_positions', scored_positions
        )
        FROM scored_position_summary WHERE id = 1
    )
)
FROM missing;
"""


def _validate_probe(probe: dict[str, Any]) -> None:
    if probe.get("database") != RESTORE_DATABASE:
        raise RuntimeError("The restored server opened the wrong database.")
    if int(probe.get("server_version_num") or 0) // 10000 != 15:
        raise RuntimeError("The restored database is not running PostgreSQL 15.")
    missing = probe.get("missing_relations") or []
    if missing:
        raise RuntimeError("The restored schema is missing: " + ", ".join(map(str, missing)))
    if not probe.get("fen_primary_key_valid"):
        raise RuntimeError("The restored fen primary key is missing or invalid.")
    if probe.get("redundant_fen_index_present"):
        raise RuntimeError("The restored database unexpectedly contains ix_fen_fen.")

    database_summary = probe.get("database_summary") or {}
    pipeline_summary = probe.get("pipeline_summary") or {}
    scored_summary = probe.get("scored_summary") or {}
    if int(database_summary.get("games") or 0) <= 0:
        raise RuntimeError("The restored database summary contains no games.")
    if int(database_summary.get("positions") or 0) <= 0:
        raise RuntimeError("The restored database summary contains no positions.")
    parsed = int(pipeline_summary.get("parsed_games") or 0)
    extracted = int(pipeline_summary.get("fen_extracted_games") or 0)
    tablebase = int(pipeline_summary.get("tablebase_marked_games") or 0)
    if parsed <= 0 or extracted > parsed or tablebase > extracted:
        raise RuntimeError("The restored ingestion summary violates its coverage invariants.")
    if int(scored_summary.get("total_positions") or 0) <= 0:
        raise RuntimeError("The restored scored-position summary is empty.")


def _postgres_start_options(socket_dir: Path) -> str:
    # Recovery refuses to start if WAL was generated with higher shared-memory
    # limits than the temporary server. Keep those settings from the restored
    # postgresql.conf instead of overriding them with smaller rehearsal values.
    return (
        "-c listen_addresses='' "
        f"-c unix_socket_directories='{socket_dir}' "
        "-c unix_socket_permissions=0700 "
        "-c port=55432 "
        "-c archive_mode=off"
    )


def _catalog_with_restore_result(
    *,
    backup_id: str,
    status: str,
    completed_at: str,
    elapsed_seconds: float,
    error_message: str | None,
) -> list[dict[str, Any]]:
    catalog = _read_json(BACKUP_CATALOG_PATH, {"backups": []})
    if not isinstance(catalog, dict):
        raise RuntimeError("The backup catalog is invalid.")
    rows = catalog.get("backups", [])
    if not isinstance(rows, list):
        raise RuntimeError("The backup catalog has no valid backup list.")

    found = False
    for row in rows:
        if str(row.get("backup_id") or "") != backup_id:
            continue
        found = True
        row.update({
            "restore_tested": status == "success",
            "restore_test_status": status,
            "restore_tested_at": completed_at,
            "restore_test_elapsed_seconds": round(elapsed_seconds, 2),
            "restore_test_error": error_message,
        })
        break
    if not found:
        raise RuntimeError(f"Backup {backup_id} disappeared from the catalog.")

    catalog["updated_at"] = completed_at
    _atomic_write_json(BACKUP_CATALOG_PATH, catalog)
    return rows


def _update_manifest(
    *,
    backup_id: str,
    status: str,
    completed_at: str,
    elapsed_seconds: float,
    validation: dict[str, Any] | None,
    error_message: str | None,
) -> None:
    path = MANIFEST_DIRECTORY / f"{backup_id}.json"
    manifest = _read_json(path, {})
    if not isinstance(manifest, dict) or not manifest:
        return
    manifest.update({
        "restore_tested": status == "success",
        "restore_test_status": status,
        "restore_tested_at": completed_at,
        "restore_test_elapsed_seconds": round(elapsed_seconds, 2),
        "restore_test_validation": validation,
        "restore_test_error": error_message,
    })
    _atomic_write_json(path, manifest)
    backup_status = _read_json(BACKUP_STATUS_PATH, {})
    if (
        isinstance(backup_status, dict)
        and str(backup_status.get("backup_id") or "") == backup_id
    ):
        backup_status.update({
            "manifest_sha256": _sha256(path),
            "restore_tested": status == "success",
            "restore_tested_at": completed_at,
        })
        _atomic_write_json(BACKUP_STATUS_PATH, backup_status)


async def _stop_restored_postgres(pgdata: Path) -> None:
    if not (pgdata / "postmaster.pid").exists():
        return
    code, _output = await _run_command_capture(
        PG_CTL,
        "-D",
        str(pgdata),
        "-m",
        "fast",
        "-w",
        "-t",
        "120",
        "stop",
        allow_failure=True,
    )
    if code and (pgdata / "postmaster.pid").exists():
        await _run_command_capture(
            PG_CTL,
            "-D",
            str(pgdata),
            "-m",
            "immediate",
            "-w",
            "-t",
            "60",
            "stop",
            allow_failure=True,
        )


async def run_database_restore_test_job(
    ctx: dict[str, Any],
    *,
    backup_id: str,
    **_kwargs: Any,
) -> dict[str, Any]:
    """Restore one selected recovery point, validate it, and remove the copy."""
    job_id = str(ctx.get("job_id") or uuid.uuid4())
    redis = ctx.get("redis")
    started_at = _iso_now()
    started_monotonic = time.monotonic()
    workspace = RESTORE_TEST_ROOT / f"rehearsal-{job_id}"
    pgdata = workspace / "pgdata"
    socket_dir = workspace / "socket"
    postgres_log = workspace / "postgres.log"
    validation: dict[str, Any] | None = None
    backup: dict[str, Any] | None = None
    running_status: dict[str, Any] | None = None
    await ensure_backup_reservation(redis, job_id)

    try:
        if not _SAFE_BACKUP_ID.fullmatch(str(backup_id or "")):
            raise RestoreTestUnavailableError("The recovery-point ID is invalid.")
        latest = latest_backup_for_restore_test()
        if str(latest.get("backup_id")) != backup_id:
            raise RestoreTestUnavailableError(
                "A newer backup exists. Refresh the page and test the latest recovery point."
            )
        backup = latest

        RESTORE_TEST_ROOT.mkdir(parents=True, exist_ok=True)
        database_bytes = int(backup.get("database_bytes") or 0)
        required_local_bytes = database_bytes + RESTORE_TEST_HEADROOM_BYTES
        local_free_bytes = shutil.disk_usage(RESTORE_TEST_ROOT).free
        if local_free_bytes < required_local_bytes:
            raise RestoreTestCapacityError(
                "Restore test needs "
                f"{required_local_bytes:,} local bytes but only {local_free_bytes:,} are free."
            )

        running_status = {
            **_default_restore_status(),
            "status": "running",
            "job_id": job_id,
            "backup_id": backup_id,
            "started_at": started_at,
            "database_bytes": database_bytes,
            "required_local_bytes": required_local_bytes,
            "local_free_bytes": local_free_bytes,
            "detail": f"Preparing disposable restore for {backup_id}.",
        }
        _atomic_write_json(RESTORE_TEST_STATUS_PATH, running_status)
        await _write_progress(
            redis,
            job_id,
            phase="preparing_restore",
            detail=running_status["detail"],
        )

        if workspace.exists():
            shutil.rmtree(workspace)
        pgdata.mkdir(parents=True, mode=0o700)
        socket_dir.mkdir(parents=True, mode=0o700)

        await _write_progress(
            redis,
            job_id,
            phase="restoring",
            detail=f"Restoring {backup_id} into an isolated local volume.",
        )
        await _run_restore(
            backup_id=backup_id,
            pgdata=pgdata,
            redis=redis,
            job_id=job_id,
        )

        await _write_progress(
            redis,
            job_id,
            phase="starting_restore",
            detail="Starting the restored PostgreSQL cluster on a private socket.",
        )
        postgres_options = _postgres_start_options(socket_dir)
        await _run_command_capture(
            PG_CTL,
            "-D",
            str(pgdata),
            "-l",
            str(postgres_log),
            "-w",
            "-t",
            "300",
            "-o",
            postgres_options,
            "start",
        )

        await _write_progress(
            redis,
            job_id,
            phase="validating_restore",
            detail="Validating the restored schema, summaries, and FEN primary key.",
        )
        _code, output = await _run_command_capture(
            "psql",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--set=ON_ERROR_STOP=1",
            f"--host={socket_dir}",
            "--port=55432",
            f"--username={RESTORE_USER}",
            f"--dbname={RESTORE_DATABASE}",
            f"--command={_validation_query()}",
        )
        try:
            validation = json.loads(output.strip())
        except json.JSONDecodeError as error:
            raise RuntimeError("The restored database returned an invalid validation result.") from error
        if not isinstance(validation, dict):
            raise RuntimeError("The restored database validation result is not an object.")
        _validate_probe(validation)

        backup_manifest = _read_json(MANIFEST_DIRECTORY / f"{backup_id}.json", {})
        if "matrix_definitions" in backup_manifest:
            await _write_progress(redis, job_id, phase="validating_definitions",
                                  detail="Verifying matrix instructions restored inside PostgreSQL.")
            _code, definitions_output = await _run_command_capture(
                "psql", "--no-psqlrc", "--tuples-only", "--no-align", "--set=ON_ERROR_STOP=1",
                f"--host={socket_dir}", "--port=55432", f"--username={RESTORE_USER}",
                f"--dbname={RESTORE_DATABASE}",
                "--command=SELECT COALESCE(json_agg(json_build_object('id', id, 'name', name, "
                "'row_type', row_type, 'config', config) ORDER BY id), '[]'::json) FROM matrix_definition",
            )
            validation["matrix_definitions"] = validate_definition_backup(
                backup_manifest["matrix_definitions"], json.loads(definitions_output.strip()),
            )
        elif int(backup_manifest.get("schema_version") or 0) >= 4:
            raise RuntimeError("This backup is missing its matrix definition fingerprint.")
        else:
            validation["matrix_definitions"] = {"status": "not_recorded", "detail": "Backup predates matrix definitions."}
        if "matrices" in backup_manifest:
            await _write_progress(redis, job_id, phase="validating_matrices",
                                  detail="Verifying matrix backup files against the restored database.")
            _code, matrix_output = await _run_command_capture(
                "psql", "--no-psqlrc", "--tuples-only", "--no-align", "--set=ON_ERROR_STOP=1",
                f"--host={socket_dir}", "--port=55432", f"--username={RESTORE_USER}",
                f"--dbname={RESTORE_DATABASE}",
                "--command=SELECT COALESCE(json_agg(id ORDER BY id), '[]'::json) "
                "FROM matrix_artifact WHERE status = 'complete'",
            )
            validation["matrices"] = await file_operation(
                validate_matrix_bundle, backup_manifest["matrices"],
                APP_BACKUP_ROOT / "research" / "matrices", json.loads(matrix_output.strip()),
            )
        elif int(backup_manifest.get("schema_version") or 0) >= 3:
            raise RuntimeError("This backup is missing its matrix companion manifest.")
        else:
            validation["matrices"] = {"status": "not_recorded", "detail": "Legacy backup: external matrices were not included."}

        await _write_progress(
            redis,
            job_id,
            phase="cleaning_restore",
            detail="Validation passed; removing the disposable restored database.",
        )
        await _stop_restored_postgres(pgdata)
        elapsed_seconds = time.monotonic() - started_monotonic
        completed_at = _iso_now()
        detail = f"Restored and validated backup {backup_id}; temporary data was removed."
        _catalog_with_restore_result(
            backup_id=backup_id,
            status="success",
            completed_at=completed_at,
            elapsed_seconds=elapsed_seconds,
            error_message=None,
        )
        _update_manifest(
            backup_id=backup_id,
            status="success",
            completed_at=completed_at,
            elapsed_seconds=elapsed_seconds,
            validation=validation,
            error_message=None,
        )
        result = {
            "backup_id": backup_id,
            "restore_tested": True,
            "restore_tested_at": completed_at,
            "elapsed_seconds": round(elapsed_seconds, 2),
            "validation": validation,
        }
        success_status = {
            **running_status,
            "status": "success",
            "completed_at": completed_at,
            "elapsed_seconds": round(elapsed_seconds, 2),
            "validation": validation,
            "detail": detail,
        }
        _atomic_write_json(RESTORE_TEST_STATUS_PATH, success_status)
        await _write_progress(
            redis,
            job_id,
            phase="complete",
            detail=detail,
            result=result,
        )
        return result
    except Exception as error:
        elapsed_seconds = time.monotonic() - started_monotonic
        completed_at = _iso_now()
        log_tail = ""
        try:
            if postgres_log.exists():
                log_tail = postgres_log.read_text(encoding="utf-8", errors="replace")[-2000:].strip()
        except OSError:
            pass
        detail = str(error)
        if log_tail:
            detail = f"{detail}\nPostgreSQL: {log_tail}"
        if backup is not None:
            try:
                _catalog_with_restore_result(
                    backup_id=backup_id,
                    status="failed",
                    completed_at=completed_at,
                    elapsed_seconds=elapsed_seconds,
                    error_message=detail,
                )
                _update_manifest(
                    backup_id=backup_id,
                    status="failed",
                    completed_at=completed_at,
                    elapsed_seconds=elapsed_seconds,
                    validation=validation,
                    error_message=detail,
                )
            except Exception:
                pass
        _atomic_write_json(RESTORE_TEST_STATUS_PATH, {
            **(running_status or _default_restore_status()),
            "status": "failed",
            "job_id": job_id,
            "backup_id": backup_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "elapsed_seconds": round(elapsed_seconds, 2),
            "validation": validation,
            "error_code": error.__class__.__name__,
            "error_message": detail,
            "detail": detail,
        })
        await _write_progress(
            redis,
            job_id,
            phase="failed",
            detail=detail,
        )
        raise
    finally:
        try:
            try:
                await _stop_restored_postgres(pgdata)
            finally:
                if workspace.exists():
                    shutil.rmtree(workspace)
        finally:
            await release_backup(redis, job_id)
