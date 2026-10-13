"""Repair one reviewed index in the retained PRIVATE v3 database and retest it.

Reuse accepted evidence only if application sources and the entire schema still
match. No full restore, row conversion, ingestion rerun or production access.
"""
import argparse
import asyncio
from contextlib import suppress
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import sys
import traceback
from uuid import uuid4

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'

from scripts.compact_db_indexes import drop_reviewed_impact_index,create_integer_impact_index,read_index,validate_index,uses_impact_index,INDEX_NAME
from scripts.compact_db_queries import SETTINGS,digest


def checked_source(status, report):
    workspace=Path(status.get('workspace',''))
    issues=[q['name'] for q in report.get('queries',[]) if q.get('error') or q.get('matches') is False or q.get('slowdown')]
    if (status.get('state')!='needs_review' or status.get('phase')!='compact_acceptance_complete'
            or status.get('mode')!='isolated_compact_acceptance' or status.get('live_database_changed') is not False
            or not status.get('private_postgres_stopped') or report.get('performance_review')!=['scored/impact']
            or issues!=['scored/impact'] or len(report.get('queries',[]))!=19
            or len(report.get('checks',[]))!=17 or not all(c.get('passed') for c in report['checks'])
            or workspace.parent!=Path('/restore-test') or not workspace.name.startswith('compact-db-')
            or workspace.is_symlink() or workspace.resolve()!=workspace
            or not re.fullmatch(r'compact_acceptance_[0-9a-f]{10}',report.get('small_database',''))):
        raise ValueError('Need the accepted stopped candidate with only the impact-query issue')
    return workspace


def unaffected_contract(contract):
    result=deepcopy(contract)
    result['indexes']=[row for row in result['indexes'] if not (
        row['schemaname']=='public' and row['tablename']=='fen' and row['indexname']==INDEX_NAME)]
    return result


async def run(args):
    import asyncpg
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.database.relational_storage import describe_contract,validate_backup_contract
    from chessism_api.database.backup_contract import contract_differences
    from scripts.compact_db_preflight import require_private
    from scripts.compact_db_application_checks import factory_for,LAYOUT
    from scripts.fen_migration_progress import ProgressReporter
    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Live data/socket must not be mounted')
    source=json.loads((args.acceptance/'status.json').read_text())
    previous=json.loads((args.acceptance/'acceptance.json').read_text())
    workspace=checked_source(source,previous)
    root=Path(__file__).resolve().parents[1]
    hashes=json.loads((args.acceptance/'source-files.json').read_text())
    changed=[name for name,expected in hashes.items() if name not in {
        'scripts/compact_db_migration.py','scripts/watch_compact_db.py'}
        and hashlib.sha256((root/name).read_bytes()).hexdigest()!=expected]
    if changed:
        raise RuntimeError('Earlier evidence invalidated by source changes: '+', '.join(changed))
    if args.output.exists():
        raise RuntimeError('Choose a fresh report directory')
    args.output.mkdir(parents=True,mode=0o755)
    baseline=json.loads((args.acceptance/'baseline.json').read_text())
    old_contract=json.loads((args.acceptance/'candidate-contract.json').read_text())
    reference=next(q for q in baseline['queries'] if q['name']=='scored/impact')
    case=next(q for q in previous['queries'] if q['name']=='scored/impact')
    reporter=ProgressReporter(args.output/'status.json',source['backup_id'],{})
    reporter.status.update(mode='isolated_compact_index_retune',live_database_changed=False,
        ready_for_live_migration=False,workspace=str(workspace),checks_completed=17,queries_completed=18,queries_total=19)
    report=deepcopy(previous)
    report.update(inherited_from=str(args.acceptance),ready_for_live_preflight=False,
        evidence_reuse='Application source hashes and complete pre-repair schema verified; only one index is changed.')
    def save():
        atomic_write_json(args.output/'acceptance.json',report)
    async def passed(name,**details):
        report['checks'].append({'name':name,'passed':True,**details})
        save()
        await reporter.publish('acceptance_check_complete',current_check=name,checks_completed=len(report['checks']))
    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    async def contract_for(database):
        engine,_=factory_for(socket,database,readonly=True)
        try:
            async with engine.connect() as connection:
                return await describe_contract(connection,LAYOUT)
        finally:
            await engine.dispose()
    async def repair(connection):
        await require_private(connection,pgdata)
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='30min'; SET LOCAL lock_timeout='5s'; SET LOCAL maintenance_work_mem='256MB'")
            old=await drop_reviewed_impact_index(connection)
            if old is None:
                raise RuntimeError('Expected the reviewed old impact index')
            new=await create_integer_impact_index(connection)
        await connection.execute('ANALYZE public.fen')
        return {'before':old,'after':new}
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    conn=small=restored=pulse=None
    failure=None
    work=workspace/('impact-retune-'+uuid4().hex[:10])
    with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
        if not acquired:
            raise RuntimeError('Restore workspace is busy')
        try:
            if (pgdata/'postmaster.pid').exists() or not (pgdata/'PG_VERSION').is_file():
                raise RuntimeError('Expected the private database to be retained and stopped')
            if shutil.disk_usage(workspace).free<5_000_000_000:
                raise RuntimeError('Need 5 GB free headroom for index replacement')
            work.mkdir(mode=0o700)
            pulse=asyncio.create_task(heartbeat())
            await reporter.publish('starting_retained_candidate')
            await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(work/'postgres.log'),
                '-w','-t','300','-o',restore._postgres_start_options(socket),'start')
            conn=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=restore.RESTORE_DATABASE)
            await require_private(conn,pgdata)
            before=await contract_for(restore.RESTORE_DATABASE)
            if contract_differences(old_contract,before):
                raise RuntimeError('Candidate schema changed since combined acceptance')
            async with conn.transaction(readonly=True):
                await conn.execute(SETTINGS)
                old_plan=json.loads(await conn.fetchval('EXPLAIN (FORMAT JSON) '+case['sql']))[0]
            atomic_write_json(args.output/'before-plan.json',old_plan)
            await reporter.publish('rebuilding_integer_impact_index')
            report['index_repair']=await repair(conn)
            after=await contract_for(restore.RESTORE_DATABASE)
            if contract_differences(unaffected_contract(before),unaffected_contract(after)):
                raise RuntimeError('Unexpected schema change outside the impact index')
            atomic_write_json(args.output/'candidate-contract.json',after)
            await passed('only_reviewed_impact_index_changed')
            await reporter.publish('measuring_affected_queries',current_query='scored/impact')
            async with conn.transaction(readonly=True):
                await conn.execute(SETTINGS)
                rows=json.loads(await conn.fetchval("SELECT COALESCE(json_agg(q),'[]'::json) FROM ("+case['sql']+') q'))
                plans=[json.loads(await conn.fetchval('EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON) '+case['sql']))[0] for _ in range(3)]
            median=statistics.median(p['Execution Time'] for p in plans)
            match=digest(rows)==reference['hash']
            uses_index=all(uses_impact_index(p['Plan']) for p in plans)
            slow=median>reference['median_ms']*1.2 and median-reference['median_ms']>5
            result={'name':'scored/impact','sql':case['sql'],'matches':match,'slowdown':slow,
                'reference_ms':reference['median_ms'],'candidate_ms':median,'hash':digest(rows),
                'rows':len(rows),'plans':plans,'uses_impact_index':uses_index}
            report['queries']=[result if q['name']=='scored/impact' else q for q in report['queries']]
            reporter.status['queries_completed']=19
            save()
            if not match or not uses_index:
                raise RuntimeError('Impact result mismatch or planner still not using corrected index')
            await passed('impact_results_match_and_natural_plan_uses_integer_index',median_ms=median)
            # Revalidate only the changed index's dump/restore contract using the
            # already-accepted small fixture. Never rebuild its ingestion inputs.
            await reporter.publish('checking_index_backup_compatibility')
            small_name=previous['small_database']
            small=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=small_name)
            if contract_differences(before,await contract_for(small_name)):
                raise RuntimeError('Retained small fixture schema changed')
            await repair(small)
            if contract_differences(after,await contract_for(small_name)):
                raise RuntimeError('Small fixture index does not match the full candidate')
            archive=work/'index-fixture.dump'
            pg_args=['-h',str(socket),'-p','55432','-U',restore.RESTORE_USER]
            await restore._run_command_capture('pg_dump',*pg_args,'-d',small_name,'-Fc','--compress=6','-f',str(archive))
            restore_name='compact_index_restore_'+uuid4().hex[:10]
            await conn.execute(f'CREATE DATABASE {restore_name}')
            await restore._run_command_capture('pg_restore',*pg_args,'-d',restore_name,'--exit-on-error',str(archive))
            restore_engine,_=factory_for(socket,restore_name,readonly=True)
            try:
                await validate_backup_contract(restore_engine.url,after)
            finally:
                await restore_engine.dispose()
            restored=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=restore_name)
            validate_index(await read_index(restored),integer=True)
            query='SELECT fen,score,n_games,n_games*abs(score) AS impact FROM public.fen WHERE score IS NOT NULL ORDER BY impact DESC,n_games DESC,fen'
            if await small.fetch(query)!=await restored.fetch(query):
                raise RuntimeError('Small restored fixture changed impact values/order')
            await passed('corrected_index_survives_compressed_backup_restore',compressed_bytes=archive.stat().st_size)
            report.update(performance_review=['scored/impact'] if slow else [],ready_for_live_preflight=not slow,
                restored_database=restore_name,ready_for_live_migration=False)
            save()
            fingerprints={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
                for folder in ('chessism_api','scripts') for p in sorted((root/folder).rglob('*.py'))}
            atomic_write_json(args.output/'source-files.json',fingerprints)
            atomic_write_json(args.output/'baseline.json',baseline)
            await reporter.finish('needs_review' if slow else 'succeeded','compact_index_retune_complete',
                impact_ms=median,reference_ms=reference['median_ms'],slowdown_flags=int(slow),
                result_mismatches=0,query_errors=0,ready_for_live_preflight=not slow,
                detail='Affected checks finished; previous acceptance evidence retained. Live database unchanged.')
        except BaseException as error:
            failure=error
            report['error']={'type':type(error).__name__,'detail':str(error),
                'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(error.__traceback__)]}
            save()
            await reporter.finish('failed','compact_index_retune_failed',detail=str(error))
        finally:
            if pulse:
                pulse.cancel()
                with suppress(asyncio.CancelledError):
                    await pulse
            for connection in (restored,small,conn):
                if connection:
                    await connection.close()
            await restore._stop_restored_postgres(pgdata)
            reporter.status['private_postgres_stopped']=not (pgdata/'postmaster.pid').exists()
            reporter.flush()
    if failure:
        raise RuntimeError('Index retest failed; see saved report') from failure


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--acceptance',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(run(parser.parse_args()))
