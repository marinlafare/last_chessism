"""Immutable base + ordered deltas; no old compressed payload is rewritten."""
from datetime import datetime, timezone

from sqlalchemy import text
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction
from chessism_api.operations.matrix_constructor.artifact_files import file_operation
from . import archive, catalog, database
from .progress import Progress


async def create(ctx, root):
    root.mkdir(parents=True, exist_ok=True)
    async with Progress(ctx, 'fen_analysis_backup') as progress:
        await progress.change(phase='checking_base', detail='Checking the last published backup.')
        backups = await file_operation(catalog.listing, root)
        previous = catalog.load(root, backups[0]['filename']) if backups else None
        epoch = catalog.restore_epoch(root)
        if previous and previous.get('restore_epoch') != epoch:
            previous = None
        files = catalog.source_files(root, previous) if previous else []
        # Adopting a legacy snapshot verifies it once. Subsequent incremental
        # updates reuse immutable descriptors; restore verifies EVERY part.
        if previous and previous.get('version') != 3:
            expected = files[0]
            actual = await file_operation(archive.sha256, archive.safe_path(root, expected['file']))
            if actual != expected['sha256']:
                raise ValueError('Legacy backup checksum does not match; original retained')
        fence = await database.export_fence()
        lower = catalog.timestamp(previous['snapshot_cutoff']) if previous and previous.get('snapshot_cutoff') else None
        if previous and lower is None:
            # An old v1 snapshot without a cutoff cannot be an incremental base.
            previous, files = None, []
        if lower is not None:
            lower = min(lower, fence)
        writer = archive.SegmentWriter(root)
        try:
            await progress.change(phase='exporting', detail='Exporting changed FENs in bounded batches.' if previous else 'Exporting the initial FEN snapshot in bounded batches.')
            async with read_transaction(AsyncDBSession) as session:
                snapshot_at = await session.scalar(text('SELECT clock_timestamp()'))
                async for batch in database.export_batches(session, lower):
                    await file_operation(writer.append, batch)
                    await progress.change(processed=writer.count, bytes=writer.bytes,
                                          detail=f'Exported {writer.count:,} result versions; unchanged backup files are retained.')
            await file_operation(writer.close_part)
            await progress.change(phase='verifying', total=writer.count, processed=writer.count,
                                  bytes=writer.bytes, detail='Verifying checksums of the new backup parts.')
            for part in writer.files:
                actual = await file_operation(archive.sha256, archive.safe_path(root, part['file']))
                if actual != part['sha256']:
                    raise ValueError('New backup part failed checksum verification')
            files = files + writer.files
            now = datetime.now(timezone.utc)
            filename = 'fen-analysis-' + now.strftime('%Y%m%dT%H%M%S_%fZ') + '.manifest.json'
            result = {'format': archive.SET_FORMAT, 'version': 3, 'format_version': 3,
                      'filename': filename, 'created_at': previous.get('created_at') if previous else snapshot_at.isoformat(),
                      'updated_at': now.isoformat(), 'snapshot_at': snapshot_at.isoformat(),
                      'snapshot_cutoff': fence.isoformat(), 'restore_epoch': epoch,
                      'records': sum(item['records'] for item in files), 'record_label': 'result versions',
                      'bytes': sum(item['bytes'] for item in files), 'files': files,
                      'exported_records': writer.count, 'new_bytes': writer.bytes,
                      'mode': 'incremental' if previous else 'snapshot',
                      'previous': previous['filename'] if previous else None}
            await progress.change(phase='publishing', detail='Publishing the durable backup manifest.')
            await file_operation(archive.publish, root, filename, result)
            # Progress/API responses do not need an unbounded list of filenames.
            summary = {key: value for key, value in result.items() if key != 'files'}
            summary['parts'] = len(files)
            await progress.change(phase='complete', result=summary,
                detail=f'Backup complete: {writer.count:,} result versions exported; {len(writer.files)} new parts. Existing backup data was not rewritten.')
            return summary
        finally:
            await file_operation(writer.abort)


def verify_part(root, part):
    path = archive.safe_path(root, part['file'])
    if path.stat().st_size != part['bytes'] or archive.sha256(path) != part['sha256']:
        raise ValueError(f'Backup checksum failed: {part["file"]}')
    count = 0
    for record in archive.records(path):
        database.restore_values(record)
        count += 1
    if count != part['records']:
        raise ValueError(f'Backup record count failed: {part["file"]}')


def take(iterator, size):
    from itertools import islice
    return list(islice(iterator, size))


async def restore(ctx, root, filename, refresh):
    async with Progress(ctx, 'fen_analysis_restore') as progress:
        backup = catalog.load(root, filename)
        files = catalog.source_files(root, backup)
        total = sum(item['records'] for item in files)
        await progress.change(phase='verifying', total=total,
                              detail='Verifying every backup part before changing the database.')
        for index, part in enumerate(files):
            await file_operation(verify_part, root, part)
            await progress.change(detail=f'Verified backup part {index + 1}/{len(files)}.')
        # Even an interrupted restore can put old analysis timestamps back in
        # the DB. Its next backup must not skip those restored values.
        await file_operation(catalog.invalidate_after_restore, root)
        restored = missing = processed = 0
        await progress.change(phase='restoring', detail='Restoring result versions in order; newest values win.')
        for part in files:
            iterator = archive.records(archive.safe_path(root, part['file']))
            try:
                while batch := await file_operation(take, iterator, database.PAGE_SIZE):
                    found, absent = await database.restore_batch(batch)
                    restored += found
                    missing += absent
                    processed += len(batch)
                    await progress.change(processed=processed, detail=f'Applied {restored:,} result versions; {missing:,} FENs not regenerated yet.')
            finally:
                iterator.close()
        await progress.change(phase='refreshing_summaries', detail='Refreshing database summaries after restoration.')
        await refresh()
        result = {'filename': filename, 'records': processed, 'restored': restored, 'missing': missing}
        await progress.change(phase='complete', result=result, detail=f'Restore complete: {restored:,} result versions applied.')
        return result
