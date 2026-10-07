"""Reserve useful FENs and reuse the normal transactional cloud importer.

The paused job is an admission fence, not work for the production controller.
Its run stays in the dedicated 'benchmark' phase until verified cleanup. This
module never submits cloud work, runs migrations, or prints DB credentials.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import os

from dotenv import dotenv_values
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine

from benchmark_platforms.run import require, save
from cloud_job.launch import ROOT, expected_contract
from cloud_job.run_host import host_database_url
from stockfish_batch.checkpoints import digest, encode, parse_input


@asynccontextmanager
async def database(*, statement_timeout_ms=120000):
    require(type(statement_timeout_ms) is int and 1 <= statement_timeout_ms <= 900000,
            'Database statement timeout must be bounded at 15 minutes')
    values = dotenv_values(ROOT.parent / '.env')
    url = host_database_url(values.get('DATABASE_URL'))
    os.environ['DATABASE_URL'] = url
    from chessism_api.database.engine import AsyncDBSession
    engine = create_async_engine(url, pool_pre_ping=True,
        connect_args={'ssl': False, 'server_settings': {'statement_timeout': str(statement_timeout_ms), 'lock_timeout': '10000'}})
    AsyncDBSession.configure(bind=engine)
    try:
        yield AsyncDBSession
    finally:
        await engine.dispose()


async def reserve(plan, config):
    async with database() as sessions:
        from chessism_api.database.ask_db import get_fens_for_analysis
        from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim
        from chessism_api.operations.cloud_analysis.admission import require_available
        from sqlalchemy import insert
        session, fens = await get_fens_for_analysis(config.max_positions, raise_errors=True)
        require(session is not None, 'No unclaimed unscored FENs')
        try:
            await require_available(session)
            require(len(fens) == config.max_positions, 'Not enough available unscored FENs')
            rows = [{'id': digest(fen.encode()), 'fen': fen} for fen in fens]
            raw = b''.join(encode(row) for row in rows)
            parse_input(raw, config.max_positions)
            contract = expected_contract(raw, rows, config, plan['worker_metadata'])
            job = CloudAnalysisJob(id=plan['db_job_id'], status='paused', target=len(rows), imported=0,
                selection={'mode': 'all', 'total_fens': len(rows), 'nodes': 100000,
                           'execution_mode': 'benchmark_vm200_v1', 'benchmark_id': plan['id']},
                error='Dedicated 200k sizing/recovery test managed by benchmark_vm16.large; monitor from the terminal. Do not use Resume saved job.')
            run = CloudAnalysisRun(id=plan['db_run_id'], job_id=job.id, status='benchmark',
                positions=rows, contract=contract, receipts={}, launch={'benchmark_id': plan['id'], 'jobs': []})
            session.add(job)
            await session.flush()
            session.add(run)
            await session.flush()
            for start in range(0, len(fens), 1000):
                await session.execute(insert(CloudFenClaim),
                    [{'fen': fen, 'run_id': run.id} for fen in fens[start:start+1000]])
            await session.commit()
            return raw, rows, contract
        finally:
            await session.close()


async def check(plan, *, submitted=False):
    async with database() as sessions, sessions() as session:
        from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim
        from chessism_api.operations.cloud_analysis.admission import require_available
        await require_available(session, own_job_id=plan['db_job_id'])
        job = await session.get(CloudAnalysisJob, plan['db_job_id'])
        run = await session.get(CloudAnalysisRun, plan['db_run_id'])
        require(job is not None and run is not None and run.job_id == job.id
                and job.selection.get('benchmark_id') == plan['id']
                and run.launch.get('benchmark_id') == plan['id']
                and run.contract == plan['contract'] and run.status == 'benchmark', 'Database reservation mismatch')
        require(job.status == 'paused', 'Benchmark admission fence changed; inspect before proceeding')
        if not submitted:
            require(job.imported == 0, 'Unexpected existing imports')
            count = await session.scalar(select(func.count()).select_from(CloudFenClaim).where(CloudFenClaim.run_id == run.id))
            require(count == plan['count'], 'Missing benchmark FEN claims')
        return {'imported': job.imported, 'run_id': run.id}


async def record_submission(plan, job):
    async with database() as sessions, sessions() as session, session.begin():
        from chessism_api.database.models import CloudAnalysisRun
        run = await session.get(CloudAnalysisRun, plan['db_run_id'], with_for_update=True)
        require(run is not None and run.launch.get('benchmark_id') == plan['id'], 'Foreign database run')
        run.launch = {**run.launch, 'jobs': [plan['job']], 'uid': job['uid'], 'spec': plan['specification']}
        run.cloud_state = job['status']['state']


async def import_results(directory, plan):
    require((directory / 'report.json').is_file(), 'Validate complete results before importing')
    await check(plan, submitted=True)
    async with database():
        from chessism_api.operations.cloud_analysis.importer import import_batch
        for index in range(plan['count'] // 500):
            raw = (directory / f'download/batches/{index:06d}.json').read_bytes()
            added = await import_batch(plan['db_run_id'], raw, index)
            if index % 20 == 0:
                print(json.dumps({'event': 'import_batch', 'index': index, 'added': added}), flush=True)
    return await finalize_import(directory, plan)


async def finalize_import(directory, plan):
    """Resume only derived views/receipt after all scores are already committed.

    High-frequency global FENs can touch almost the entire games catalog. Keep
    the ordinary import's two-minute timeout, but give these final full-catalog
    statements a separate, bounded fifteen-minute ceiling. This never reimports
    scores, increments counters, releases additional claims or submits compute.
    """
    report = json.loads((directory / 'report.json').read_bytes())
    require(report.get('all_result_checksums_and_legal_pvs_verified') is True
            and report.get('id') == plan['id'] and report.get('positions') == plan['count'], 'Invalid report')
    await check(plan, submitted=True)
    async with database(statement_timeout_ms=900000) as sessions:
        from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim
        from chessism_api.operations.cloud_analysis.importer import (
            refresh_projections, refresh_global_projections, validate_manifest,
        )
        async with sessions() as session:
            run = await session.get(CloudAnalysisRun, plan['db_run_id'])
            job = await session.get(CloudAnalysisJob, plan['db_job_id'])
            manifest = json.loads((directory / 'download/manifest.json').read_bytes())
            validate_manifest(manifest, run.positions, run.contract, run.receipts)
            remaining = await session.scalar(select(func.count()).select_from(CloudFenClaim).where(CloudFenClaim.run_id == run.id))
            require(job.imported == plan['count'] and remaining == 0, 'Incomplete import; cleanup prohibited')
            positions = run.positions
        print(json.dumps({'event': 'refreshing_game_player_summaries', 'statement_timeout_seconds': 900}), flush=True)
        await refresh_projections(positions)
        print(json.dumps({'event': 'refreshing_global_summaries'}), flush=True)
        await refresh_global_projections()
        receipt = {'benchmark_id': plan['id'], 'db_job_id': plan['db_job_id'], 'db_run_id': plan['db_run_id'],
                   'imported': plan['count'], 'manifest_sha256': digest((directory / 'download/manifest.json').read_bytes()),
                   'database_receipts_verified': True, 'remaining_claims': 0, 'cleanup_ready': True}
        save(directory, 'import-receipt.json', receipt)
        async with sessions() as session, session.begin():
            run = await session.get(CloudAnalysisRun, plan['db_run_id'], with_for_update=True)
            job = await session.get(CloudAnalysisJob, plan['db_job_id'], with_for_update=True)
            run.cloud_state = 'SUCCEEDED'
            run.launch = {**run.launch, 'import_receipt': receipt,
                          'performance': json.loads((directory / 'download/performance.json').read_bytes())}
            job.error = 'All 200000 benchmark results imported and verified; awaiting scoped cloud cleanup.'
        print(json.dumps(receipt), flush=True)
        return receipt


def execute(action, *args, **kwargs):
    return asyncio.run(action(*args, **kwargs))


async def verify_import(directory, plan, *, cleanup=None):
    """Recheck durable receipts and actual scores before deletion; finalize afterwards."""
    from chessism_api.database.models import CloudAnalysisJob, CloudAnalysisRun, CloudFenClaim, Fen
    from chessism_api.operations.cloud_analysis.importer import validate_manifest
    receipt = json.loads((directory / 'import-receipt.json').read_bytes())
    manifest_raw = (directory / 'download/manifest.json').read_bytes()
    require(receipt.get('cleanup_ready') is True and receipt.get('database_receipts_verified') is True
            and receipt.get('benchmark_id') == plan['id'] and receipt.get('db_run_id') == plan['db_run_id']
            and receipt.get('db_job_id') == plan['db_job_id'] and receipt.get('imported') == plan['count']
            and receipt.get('remaining_claims') == 0 and receipt.get('manifest_sha256') == digest(manifest_raw),
            'Import receipt mismatch; cleanup prohibited')
    async with database() as sessions, sessions() as session, session.begin():
        job = await session.get(CloudAnalysisJob, plan['db_job_id'], with_for_update=True)
        run = await session.get(CloudAnalysisRun, plan['db_run_id'], with_for_update=True)
        require(job is not None and run is not None and run.job_id == job.id
                and job.status in ('paused', 'complete') and run.status in ('benchmark', 'complete')
                and job.selection.get('benchmark_id') == plan['id']
                and run.launch.get('benchmark_id') == plan['id']
                and run.launch.get('import_receipt') == receipt and run.contract == plan['contract']
                and job.imported == plan['count'], 'Database import identity mismatch')
        validate_manifest(json.loads(manifest_raw), run.positions, run.contract, run.receipts)
        remaining = await session.scalar(select(func.count()).select_from(CloudFenClaim).where(CloudFenClaim.run_id == run.id))
        require(remaining == 0, 'Reserved FENs remain; cleanup prohibited')
        scored = 0
        for offset in range(0, len(run.positions), 1000):
            scored += await session.scalar(select(func.count()).select_from(Fen).where(
                Fen.fen.in_([r['fen'] for r in run.positions[offset:offset + 1000]]), Fen.score.is_not(None)))
        require(scored == plan['count'], 'Imported scores are missing; cleanup prohibited')
        if cleanup is not None:
            require(cleanup.get('complete') is True and not cleanup.get('remaining_jobs')
                    and not cleanup.get('remaining_compute') and not cleanup.get('remaining_object_versions')
                    and not cleanup.get('remaining_images'), 'Cloud cleanup is not verified')
            run.status = job.status = 'complete'
            run.error = job.error = None
            run.launch = {**run.launch, 'cleanup_verification': cleanup}
        return {'imported': job.imported, 'scored_fens_verified': scored,
                'remaining_claims': remaining, 'job_status': job.status}


async def import_receipt_ready(plan, receipt):
    """Wait for the commit after the local receipt write, without loading FEN data."""
    from chessism_api.database.models import CloudAnalysisRun
    from chessism_api.database.cloud_codec import read_projection
    async with database() as sessions, sessions() as session:
        ref = await session.scalar(select(CloudAnalysisRun._launch_ref).where(
            CloudAnalysisRun.id == plan['db_run_id'], CloudAnalysisRun.job_id == plan['db_job_id']))
        connection = await session.connection()
        saved = (await connection.run_sync(read_projection, ref, {'import_receipt'})).get('import_receipt') if ref else None
        require(saved is None or saved == receipt, 'Database/local import receipts disagree')
        return saved == receipt
