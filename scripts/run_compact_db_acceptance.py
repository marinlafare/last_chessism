"""One combined acceptance pass; reuse the retained full-size private copy.

Full-size data is read-only. Application writes and compressed dump/restore use
two small databases created from its exact schema. No production release,
Google client, queue consumer, production backup write or full corpus restore.
"""
import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import time
import traceback
from uuid import uuid4

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['DATABASE_URL']='postgresql+asyncpg://chessism_user@127.0.0.1:1/chessism_db'

from scripts.compact_db_queries import SETTINGS,cases,digest


def checked_workspace(status):
    path=Path(status.get('workspace',''))
    if (status.get('state')!='needs_review' or status.get('phase')!='private_candidate_complete'
            or status.get('mode')!='isolated_compact_candidate' or status.get('live_database_changed') is not False
            or not status.get('private_postgres_stopped') or path.parent!=Path('/restore-test')
            or not path.name.startswith('compact-db-') or path.is_symlink() or path.resolve()!=path):
        raise ValueError('A completed, stopped, isolated compact candidate is required')
    return path


async def run(args):
    import asyncpg
    from sqlalchemy import text
    from chessism_api.operations import database_restore_tests as restore
    from chessism_api.operations.restore_workspace import workspace_lock
    from chessism_api.operations.backup_files import atomic_write_json
    from chessism_api.database.relational_storage import describe_contract,validate_backup_contract
    from chessism_api.database.backup_contract import contract_differences
    from scripts.compact_db_preflight import require_private
    from scripts.compact_db_application_checks import application_checks,factory_for,LAYOUT
    from scripts.fen_migration_progress import ProgressReporter
    if Path('/var/lib/postgresql/data/PG_VERSION').exists() or Path('/var/run/postgresql/.s.PGSQL.5432').exists():
        raise RuntimeError('Live data/socket must not be mounted')
    source=json.loads((args.candidate/'status.json').read_text())
    workspace=checked_workspace(source)
    baseline=json.loads(args.baseline.read_text())
    if baseline.get('state')!='succeeded' or baseline.get('live_database_changed') is not False or not baseline.get('readonly'):
        raise RuntimeError('A successful read-only v2 baseline is required')
    if args.output.exists():
        raise RuntimeError('Choose a new report directory')
    args.output.mkdir(parents=True,mode=0o755)
    reporter=ProgressReporter(args.output/'status.json',source['backup_id'],{})
    reporter.status.update(mode='isolated_compact_acceptance',live_database_changed=False,ready_for_live_migration=False,
        workspace=str(workspace),checks_completed=0,queries_completed=0,queries_total=len(baseline['queries']))
    results={'checks':[],'queries':[],'cloud_calls':0,'full_database_restores':0,
        'comparison_limitations':baseline['comparison_limitations'],
        'backup_scope':'Exact full schema plus bounded application fixture; full-size post-migration restore test still required.'}
    atomic_write_json(args.output/'baseline.json',baseline)
    # Fingerprint only reviewed executable inputs; do not copy credentials or data.
    root=Path(__file__).resolve().parents[1]
    fingerprints={str(p.relative_to(root)):__import__('hashlib').sha256(p.read_bytes()).hexdigest()
        for folder in ('chessism_api','scripts') for p in sorted((root/folder).rglob('*.py'))}
    atomic_write_json(args.output/'source-files.json',fingerprints)
    def save():
        atomic_write_json(args.output/'acceptance.json',results)
    async def passed(name,**detail):
        results['checks'].append({'name':name,'passed':True,**detail})
        save()
        await reporter.publish('acceptance_check_complete',current_check=name,checks_completed=len(results['checks']))
    async def heartbeat():
        while True:
            reporter.flush()
            await asyncio.sleep(5)
    pgdata,socket=workspace/'pgdata',workspace/'socket'
    conn=probe=restored=pulse=None
    engine=None
    token=uuid4().hex[:10]
    small_name='compact_acceptance_'+token
    restored_name='compact_restored_'+token
    work=workspace/('acceptance-'+token)
    report_error=None
    with workspace_lock(restore.RESTORE_TEST_ROOT) as acquired:
        if not acquired:
            raise RuntimeError('Restore workspace is busy')
        try:
            if (pgdata/'postmaster.pid').exists() or not (pgdata/'PG_VERSION').is_file():
                raise RuntimeError('Expected the retained candidate to be stopped')
            if shutil.disk_usage(workspace).free<10_000_000_000:
                raise RuntimeError('Need 10 GB free headroom for acceptance')
            work.mkdir(mode=0o700)
            pulse=asyncio.create_task(heartbeat())
            await reporter.publish('starting_retained_candidate')
            await restore._run_command_capture(restore.PG_CTL,'-D',str(pgdata),'-l',str(work/'postgres.log'),
                '-w','-t','300','-o',restore._postgres_start_options(socket),'start')
            conn=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=restore.RESTORE_DATABASE)
            await require_private(conn,pgdata)
            engine,factory=factory_for(socket,restore.RESTORE_DATABASE,readonly=True)
            async with engine.connect() as connection:
                contract=await describe_contract(connection,LAYOUT)
            atomic_write_json(args.output/'candidate-contract.json',contract)
            old=json.loads((restore.MANIFEST_DIRECTORY/(source['backup_id']+'.json')).read_text())['relational_storage']
            # Explicitly verify the three categories the user asked to leave alone.
            for table in ('game','player'):
                for section,field in (('columns','table_name'),('constraints','table'),('indexes','tablename')):
                    assert [r for r in old[section] if r.get(field)==table]==[r for r in contract[section] if r.get(field)==table],(table,section)
            before=[r for r in old['constraints'] if r['table']=='fen_continuation']
            after=[r for r in contract['constraints'] if r['table']=='fen_continuation']
            assert before==after,'Continuation identity constraints changed'
            await passed('full_schema_contract_and_unchanged_requested_tables')
            await reporter.publish('checking_full_size_row_counts')
            proofs=json.loads((args.candidate/'proofs.json').read_text())
            async with conn.transaction(readonly=True):
                await conn.execute("SET LOCAL statement_timeout='5min'")
                for table,key in (('fen','fen_integer_values'),('fen_continuation','continuation_integer_values'),
                                  ('game_fen_association','association_sides')):
                    count=await conn.fetchval(f'SELECT count(*) FROM public.{table}')
                    assert count==proofs[key]['rows'],(table,count,proofs[key]['rows'])
                    await passed('preserved_row_count/'+table,rows=count)
            references={row['name']:row for row in baseline['queries']}
            for case in cases(baseline['inputs'],compact=True):
                await reporter.publish('measuring_affected_queries',current_query=case['name'])
                item={'name':case['name'],'sql':case['sql']}
                try:
                    async with conn.transaction(readonly=True):
                        await conn.execute(SETTINGS)
                        raw=await conn.fetchval("SELECT COALESCE(json_agg(q),'[]'::json) FROM ("+case['sql']+') q')
                        rows=json.loads(raw)
                        plans=[json.loads(await conn.fetchval('EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON) '+case['sql']))[0] for _ in range(3)]
                    reference=references[case['name']]
                    median=statistics.median(p['Execution Time'] for p in plans)
                    matches=digest(rows)==reference['hash']
                    slower=matches and median>reference['median_ms']*1.2 and median-reference['median_ms']>5
                    item.update(matches=matches,slowdown=slower,reference_ms=reference['median_ms'],candidate_ms=median,
                        hash=digest(rows),rows=len(rows),plans=plans)
                except Exception as error:
                    item.update(error_type=type(error).__name__,error=str(error))
                results['queries'].append(item)
                reporter.status['queries_completed']=len(results['queries'])
                save()
            # API functions, not just SQL samples. Factories are read-only and
            # injected; no runtime startup migration or external service runs.
            await reporter.publish('checking_application_reads')
            from unittest.mock import patch
            from chessism_api.database import ask_db
            with patch.object(ask_db,'AsyncDBSession',factory):
                for player in baseline['inputs']:
                    page=await ask_db.get_player_games_page(player['player_name'])
                    if not isinstance(page,dict) or not page:
                        raise AssertionError('Player game API returned no data')
                    await passed('player_games_api/'+player['player_name'])
            await engine.dispose();engine=None
            await reporter.publish('cloning_schema_only_for_application_tests')
            schema=work/'schema.dump'
            args_pg=['-h',str(socket),'-p','55432','-U',restore.RESTORE_USER]
            await restore._run_command_capture('pg_dump',*args_pg,'-d',restore.RESTORE_DATABASE,
                '--schema-only','-Fc','-f',str(schema))
            await conn.execute(f'CREATE DATABASE {small_name}')
            await restore._run_command_capture('pg_restore',*args_pg,'-d',small_name,'--exit-on-error',str(schema))
            probe=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=small_name)
            await reporter.publish('testing_real_ingestion_and_result_import')
            small_contract=await application_checks(probe,socket,small_name,passed)
            assert not contract_differences(contract,small_contract),'Schema-only clone changed storage contract'
            await reporter.publish('testing_small_compressed_backup_and_restore')
            archive=work/'application-fixture.dump'
            await restore._run_command_capture('pg_dump',*args_pg,'-d',small_name,'-Fc','--compress=6','-f',str(archive))
            await conn.execute(f'CREATE DATABASE {restored_name}')
            await restore._run_command_capture('pg_restore',*args_pg,'-d',restored_name,'--exit-on-error',str(archive))
            restored=await asyncpg.connect(host=str(socket),port=55432,user=restore.RESTORE_USER,database=restored_name)
            restore_engine,_=factory_for(socket,restored_name,readonly=True)
            try:
                await validate_backup_contract(restore_engine.url,small_contract)
            finally:
                await restore_engine.dispose()
            # Full-value comparison of EVERY table in the bounded fixture,
            # including typed cloud control documents and import receipts.
            from chessism_api.database.relational_storage import identifier
            tables=await probe.fetch("SELECT schemaname,tablename FROM pg_tables WHERE schemaname IN ('public','chessism_archive') ORDER BY 1,2")
            for table in tables:
                name=identifier(table['schemaname'])+'.'+identifier(table['tablename'])
                query=f"SELECT row_to_json(t)::text AS row FROM {name} t ORDER BY row_to_json(t)::text COLLATE \"C\""
                assert await probe.fetch(query)==await restored.fetch(query),name
            await passed('compressed_fixture_restore_exact_schema_and_all_table_values',tables=len(tables),compressed_bytes=archive.stat().st_size)
            issues=[q['name'] for q in results['queries'] if q.get('error') or not q.get('matches') or q.get('slowdown')]
            results.update(performance_review=issues,ready_for_live_preflight=not issues,ready_for_live_migration=False,
                small_database=small_name,restored_database=restored_name)
            save()
            state='needs_review' if issues else 'succeeded'
            await reporter.finish(state,'compact_acceptance_complete',slowdown_flags=sum(bool(q.get('slowdown')) for q in results['queries']),
                result_mismatches=sum(q.get('matches') is False for q in results['queries']),
                query_errors=sum('error' in q for q in results['queries']),ready_for_live_preflight=not issues,
                detail='Combined acceptance finished. Live migration still requires current rollback protection and approval.')
        except BaseException as error:
            report_error=error
            results['error']={'type':type(error).__name__,'detail':str(error),
                'frames':[{'file':Path(frame.filename).name,'line':frame.lineno,'function':frame.name} for frame in traceback.extract_tb(error.__traceback__)]}
            save()
            await reporter.finish('failed','compact_acceptance_failed',error_type=type(error).__name__,detail=str(error))
        finally:
            if pulse:
                pulse.cancel()
                with suppress(asyncio.CancelledError):
                    await pulse
            if engine:
                await engine.dispose()
            for connection in (restored,probe,conn):
                if connection:
                    await connection.close()
            await restore._stop_restored_postgres(pgdata)
            reporter.status['private_postgres_stopped']=not (pgdata/'postmaster.pid').exists()
            reporter.flush()
    if report_error:
        raise RuntimeError('Combined acceptance failed; see the saved failure report') from report_error


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate',type=Path,required=True)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    asyncio.run(run(parser.parse_args()))
