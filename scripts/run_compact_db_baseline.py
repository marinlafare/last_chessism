"""Bounded, read-only v2 measurements. No backup, schema change or service restart.

The only live connection is psql inside the existing Compose db service.
Every connection is explicitly READ ONLY with statement and lock timeouts.
"""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.compact_db_queries import DISCOVER, SETTINGS, cases, decode_stream, digest


def read(sql):
    result=subprocess.run(['docker','compose','exec','-T','db','psql','-X','-qAt','-v','ON_ERROR_STOP=1',
        '-U','chessism_user','-d','chessism_db'],input='BEGIN READ ONLY; '+SETTINGS+sql+'; ROLLBACK;',
        capture_output=True,text=True,timeout=35,check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip()[-1500:])
    return decode_stream(result.stdout)


def run(args):
    if args.output.exists():
        raise RuntimeError('Choose a fresh baseline report directory')
    args.output.mkdir(parents=True)
    report={'state':'running','live_database_changed':False,'readonly':True,'queries':[],
        'comparison_limitations':['Live baseline and isolated candidate have different resource limits/cache state.',
            'Timing comparisons are valid only where result hashes match.']}
    def save():
        target=args.output/'baseline.json'
        temp=args.output/'baseline.json.tmp'
        temp.write_text(json.dumps(report,indent=2)+'\n')
        temp.replace(target)
    save()
    started=time.monotonic()
    try:
        version=read("SELECT json_build_object('version',version(),'database_bytes',pg_database_size(current_database()),'is_compact',EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='moves' AND column_name='id')=false)")[0]
        if version['is_compact']:
            raise RuntimeError('Baseline requires the unchanged v2 schema')
        report['source']=version
        inputs=read('SELECT COALESCE(json_agg(q),\'[]\'::json) FROM ('+DISCOVER+') q')[0]
        if not inputs or any(row['link'] is None for row in inputs):
            raise RuntimeError('Missing baseline player/game inputs')
        report['inputs']=inputs
        workload=cases(inputs,compact=False)
        report['queries_total']=len(workload)
        for case in workload:
            item={'name':case['name'],'sql':case['sql']}
            try:
                rows=read("SELECT COALESCE(json_agg(q),'[]'::json) FROM ("+case['sql']+') q')[0]
                plans=[read('EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON) '+case['sql'])[0][0] for _ in range(3)]
                item.update(hash=digest(rows),rows=len(rows),median_ms=statistics.median(p['Execution Time'] for p in plans),plans=plans)
            except (RuntimeError,subprocess.TimeoutExpired) as error:
                item.update(error=str(error),error_type=type(error).__name__)
            report['queries'].append(item)
            report.update(queries_completed=len(report['queries']),elapsed_seconds=round(time.monotonic()-started,1))
            save()
            print(case['name']+': '+('ERROR' if 'error' in item else f"{item['median_ms']:.1f} ms"),flush=True)
        report['state']='needs_review' if any('error' in q for q in report['queries']) else 'succeeded'
    except BaseException as error:
        report.update(state='failed',error_type=type(error).__name__,detail=str(error))
        raise
    finally:
        save()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    run(parser.parse_args())
