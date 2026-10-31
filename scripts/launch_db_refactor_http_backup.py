"""Host supervisor: isolated HTTP/ARQ or corpus-summary acceptance.

Only this small supervisor uses Docker/live Redis. The actual test container
has network=none, no Docker socket and no production data/backup mounts. A
throwaway Redis joins its loopback namespace for HTTP tests only. Corpus tests
start no queue services. No live services are restarted.
"""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid

APP_IMAGE='sha256:a3c0962fa5a3e631be743ae9a1782ab7c6bb332dd67dd34814e8be0d54977f57'
REDIS_IMAGE='sha256:2a51817f79255c8b69f86a974459c2e0359aff81417d80158f2b9e541e6f4b33'
LOCK='chessism:backup:active'
ROOT=Path(__file__).resolve().parents[1]
REPORTS=ROOT/'.local-migrations/db-refactor'


def name_argument(value):
    if not re.fullmatch(r'(http-backup|corpus-finalization|migration-sequence)-[0-9]{3}',value):
        raise argparse.ArgumentTypeError('Use http-backup-NNN, corpus-finalization-NNN or migration-sequence-NNN')
    return value


def command(*args,timeout=60,check=True):
    result=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
    if check and result.returncode:
        # Commands contain no credentials, but Docker diagnostics could contain
        # host paths; retain them only in the supervisor log.
        raise RuntimeError(f'{args[0]} command failed: {result.stderr[-1500:]}')
    return result.stdout.strip()


def runner_command(name):
    name_argument(name)
    container='chessism-refactor-'+name
    corpus = name.startswith('corpus-finalization-')
    sequence = name.startswith('migration-sequence-')
    script = ('run_db_refactor_migration_sequence.py' if sequence else
              'run_db_refactor_corpus.py' if corpus else 'run_db_refactor_http_backup.py')
    inputs = (['--baseline','/reports/baseline-001','--acceptance','/reports/corpus-finalization-001']
              if sequence else ['--release','/reports/canonical-release-001',
                  *(['--acceptance','/reports/http-backup-002'] if corpus else [])])
    return ['docker','run','-d','--name',container,'--network','none','--cpus','2','--memory','4g',
        '--shm-size','256m','--user','postgres','--workdir','/candidate','--entrypoint','python',
        '-e','PYTHONDONTWRITEBYTECODE=1',
        '--mount',f'type=bind,src={ROOT},dst=/candidate,readonly',
        '--mount',f'type=bind,src={REPORTS},dst=/reports',
        '--mount','type=volume,src=last_chessism_database_restore_test,dst=/restore-test',
        APP_IMAGE,'/candidate/scripts/'+script,*inputs,'--output','/reports/'+name]


def supervise(name,logdir):
    owner='db-refactor-http-'+uuid.uuid4().hex
    runner=redis=None
    reserved=False
    result={'state':'starting','report':str(REPORTS/name),'reservation_owner':owner}
    def save(**values):
        result.update(values)
        (logdir/'status.json').write_text(json.dumps(result,indent=2))
    try:
        if (REPORTS/name).exists():
            raise RuntimeError('Report already exists; use a new name')
        # Refuse name collisions before acquiring or changing anything.
        for container in ('chessism-refactor-'+name,'chessism-refactor-'+name+'-redis'):
            found=command('docker','ps','-a','--filter','name=^/'+container+'$','--format','{{.ID}}')
            if found: raise RuntimeError('Test container name already exists')
        if command('docker','exec','redis','redis-cli','--raw','SET',LOCK,owner,'NX','EX','21600')!='OK':
            raise RuntimeError('Backup lane is busy; no test services started')
        reserved=True
        runner=command(*runner_command(name))
        # Redis uses only the test's loopback network; persistence is disabled
        # and /data is tmpfs. No live Redis data is copied into it.
        if name.startswith('http-backup-'):
            redis=command('docker','run','-d','--name','chessism-refactor-'+name+'-redis',
                '--network','container:'+runner,'--read-only','--tmpfs','/data:rw,nosuid,size=64m',
                '--memory','128m','--cpus','0.25',REDIS_IMAGE,'redis-server','--bind','127.0.0.1',
                '--save','','--appendonly','no','--maxmemory','64mb','--maxmemory-policy','noeviction')
        save(state='running',runner_id=runner,redis_id=redis)
        code=command('docker','wait',runner,timeout=4*3600+600)
        save(state='succeeded' if code=='0' else 'failed',runner_exit_code=code)
        if code!='0':
            print(command('docker','logs','--tail','50',runner,check=False),flush=True)
    except BaseException as error:
        save(state='failed',error_type=type(error).__name__,detail=str(error))
        if runner:
            command('docker','stop','--time','90',runner,timeout=120,check=False)
        raise
    finally:
        if redis:
            command('docker','stop','--time','10',redis,timeout=30,check=False)
            command('docker','rm','-v',redis,check=False)
        if reserved:
            released=command('docker','exec','redis','redis-cli','--raw','EVAL',
                'if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end',
                '1',LOCK,owner,check=False)
            save(backup_reservation_released=released=='1')
        save(supervisor_finished=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name',type=name_argument,required=True)
    parser.add_argument('--supervise-logdir',type=Path,help=argparse.SUPPRESS)
    args=parser.parse_args()
    if args.supervise_logdir:
        def interrupted(*_args): raise KeyboardInterrupt()
        signal.signal(signal.SIGTERM,interrupted)
        supervise(args.name,args.supervise_logdir)
        return
    if (REPORTS/args.name).exists():
        parser.error('Use a new report name')
    logdir=Path(tempfile.mkdtemp(prefix='chessism-http-rehearsal-'))
    with (logdir/'supervisor.log').open('ab') as log:
        child=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--name',args.name,
            '--supervise-logdir',str(logdir)],stdout=log,stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,start_new_session=True)
    print(json.dumps({'supervisor_pid':child.pid,'supervisor':str(logdir),
                      'report':str(REPORTS/args.name)},indent=2))


if __name__=='__main__': main()
