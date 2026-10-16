"""Public API for legacy snapshots and immutable incremental FEN backups."""
import asyncio
import gzip
import json
import logging
import os
from pathlib import Path

from chessism_api.database.ask_db import (
    refresh_database_summary_fen_counts, refresh_game_analysis_summary,
    refresh_scored_position_summary, refresh_scored_rating_summary,
)
from .backup_coordination import ensure_backup_reservation, release_backup
from .backup_files import atomic_write_json as _atomic_write_json, sha256 as _sha256
from .database_backups import record_application_usage, require_storage, storage_snapshot
from .fen_backup import archive, catalog, jobs
from .fen_backup.database import restore_batch as _restore_batch

BACKUP_FORMAT = archive.FORMAT
BACKUP_VERSION = 2  # Payload compatibility; the new restore manifest is v3.
BACKUP_DIR = Path(os.getenv('FEN_ANALYSIS_BACKUP_DIR', '/backup-volume/chessism/fen-analysis'))
BACKUP_DISPLAY_DIR = os.getenv('FEN_ANALYSIS_BACKUP_DISPLAY_DIR', '/main-monitor-db-backups/chessism/fen-analysis')


def _require_external_backup_volume(*, for_write=False):
    if for_write:
        try:
            require_storage(include_app_usage=True)
        except RuntimeError as error:
            raise OSError(str(error)) from error
    else:
        storage = storage_snapshot(include_app_usage=False)
        if not storage.get('available'):
            raise OSError(str(storage.get('error') or 'External backup storage is unavailable.'))


def _backup_path(filename):
    return archive.safe_path(BACKUP_DIR, filename)


def _metadata_path(path):
    return path.with_name(path.name + '.meta.json')


def _backup_records(path):
    with gzip.open(path, 'rt', encoding='utf-8') as source:
        header = json.loads(source.readline(archive.MAX_RECORD_BYTES + 1))
    return header, archive.records(path)


def _latest_incremental_backup():
    for backup in catalog.listing(BACKUP_DIR):
        if backup.get('snapshot_cutoff'):
            return _backup_path(backup['filename']), backup, catalog.timestamp(backup['snapshot_cutoff'])
    return None


def list_fen_analysis_backups():
    _require_external_backup_volume()
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return [{**{key: value for key, value in item.items() if key != 'files'},
             'parts': item.get('parts', 1)} for item in catalog.listing(BACKUP_DIR)]


async def create_fen_analysis_backup(ctx):
    _require_external_backup_volume(for_write=True)
    result = await jobs.create(ctx, BACKUP_DIR)
    try:
        await asyncio.to_thread(record_application_usage)
    except Exception:
        logging.getLogger(__name__).warning('Backup is durable, but storage-usage cache refresh failed', exc_info=True)
    return result


async def _refresh_summaries():
    await refresh_database_summary_fen_counts()
    await refresh_game_analysis_summary()
    await refresh_scored_position_summary()
    await refresh_scored_rating_summary()


async def restore_fen_analysis_backup(ctx, filename):
    _require_external_backup_volume(for_write=True)
    return await jobs.restore(ctx, BACKUP_DIR, filename, _refresh_summaries)


async def _run(ctx, operation, *args):
    job_id = str(ctx.get('job_id') or 'fen-analysis-backup')
    redis = ctx.get('redis')
    try:
        await ensure_backup_reservation(redis, job_id)
        return await operation(ctx, *args)
    finally:
        await release_backup(redis, job_id)


async def run_fen_analysis_backup_job(ctx, **kwargs):
    return await _run(ctx, create_fen_analysis_backup)


async def run_fen_analysis_restore_job(ctx, filename, **kwargs):
    return await _run(ctx, restore_fen_analysis_backup, filename)
