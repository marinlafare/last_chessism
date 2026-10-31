import copy
import sys
from time import perf_counter
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4
from datetime import datetime,timezone
import numpy as np
from chessism_api.operations.cuda_summaries import behavior_compute as b,ending_compute as e
from chessism_api.operations.player_analytics_summary import summarize_day,combine_partitions
from chessism_api.operations.player_timezone import resolve_player_timezone


def fixture():
    profiles=[{'player_name':'alice','timezone':'America/New_York'}, {'player_name':'bob','timezone':'UTC'}]
    rows=[]
    for p in profiles:
        for i in range(80):
            rows.append(dict(player_name=p['player_name'],mode=['bullet','blitz','rapid','daily',None][i%5],
                link=i+1,color='black' if i%2 else 'white',played_at=None if i%9==0 else
                datetime.fromisoformat(['2025-03-09T06:59:00+00:00','2025-03-09T07:01:00+00:00',
                    '2025-03-10T01:30:00+00:00'][i%3]),time_elapsed=1.125*i,
                result=[None,1,.5,0][i%4],rating=None if i%11==0 else 800+i,
                eco='https://www.chess.com/openings/Test-Opening' if i%3 else 'no_eco',opponent_name='enemy'+str(i%7),
                own_reason='timeout' if i%2 else 'resigned',opponent_reason='resigned',opponent_result=1 if i%4==3 else 0,
                final_exists=i%11!=0,final_tablebase=i%13==0,final_score=[None,0,200,201,-200,-201,9998][i%7],
                total_positions=5,analyzed_positions=3,unscored_positions=2,fully_analyzed=False))
    return profiles,rows


def normalize(value):
    if isinstance(value,list): return sorted([normalize(v) for v in value],key=repr)
    if isinstance(value,dict): return {k:normalize(v) for k,v in value.items()}
    return value


def check(xp,require_cuda):
    profiles,rows=fixture();raw,dictionaries=b.encode(rows,profiles)
    parts=b.partitions(xp,raw,dictionaries,profiles,require_cuda=require_cuda)
    entries=[(*key,payload) for key,payload in parts.items()]
    outputs=b.combine(xp,entries,profiles,require_cuda=require_cuda)
    for profile in profiles:
        player=profile['player_name']
        expected=combine_partitions([summarize_day([r for r in rows if r['player_name']==player],resolve_player_timezone(profile))])
        assert normalize(outputs[player])==normalize(expected),(outputs[player],expected)
    assert normalize(b.combine(xp,entries,profiles,require_cuda=require_cuda,by_day=True))==normalize(parts)
    extra=e.encode(rows);endings=e.partitions(xp,raw,extra,dictionaries,profiles,require_cuda=require_cuda)
    reduced=e.combine(xp,[(*k,v) for k,v in endings.items()],profiles,require_cuda=require_cuda)
    for profile in profiles:
        expected=e.blank()
        for row in (r for r in rows if r['player_name']==profile['player_name']):
            expected['coverage']['total_games']+=1
            for k in ('total_positions','analyzed_positions','unscored_positions'):expected['coverage'][k]+=row[k]
            if row['mode'] not in b.MODES:continue
            kind=None
            if row['result']==0 and row['opponent_result']==1:
                kind='lost_on_time' if row['own_reason']=='timeout' else 'lost_by_resignation'
            if row['result']==1 and row['opponent_result']==0:kind='won_by_resignation'
            if not kind:continue
            bucket=expected['by_mode'][row['mode']][kind];bucket['eligible_games']+=1
            score=row['final_score']
            if not row['final_exists']:bucket['missing_final_position']+=1
            elif row['final_tablebase']:bucket['tablebase_games']+=1
            elif score is None:bucket['missing_final_score']+=1
            elif abs(score)>=9000:bucket['mate_games']+=1
            else:
                score=score if row['color']=='white' else -score
                bucket['assessable_games']+=1
                bucket['ahead_games' if score>200 else 'behind_games' if score < -200 else 'balanced_games']+=1
        assert reduced[profile['player_name']]==expected
    assert b.partitions(xp,raw[:0],dictionaries,profiles,require_cuda=require_cuda)=={}
    assert e.partitions(xp,raw[:0],extra[:0],dictionaries,profiles,require_cuda=require_cuda)=={}


class BehaviorTests(unittest.TestCase):
    def test_checkpoints_merge_and_verify(self):
        from chessism_api.operations.cuda_summaries import metadata_files as f,input_snapshot as files
        profiles,rows=fixture();run=str(uuid4())
        for i,row in enumerate(rows):row.update(cursor=i+1,take_behavior=True,take_endings=True)
        with TemporaryDirectory() as directory,patch.object(files,'ROOT',Path(directory)),patch.object(files,'FREE_FLOOR',0):
            one=f.save(run,rows[:40],profiles);two=f.save(run,rows[40:],profiles)
            raw,extra,mask,dictionaries,loaded=f.load(run,[one,two])
            self.assertEqual(len(raw),len(rows));self.assertEqual(loaded,profiles)
            self.assertTrue(mask.all())
            actual=b.combine(np,[(*k,v) for k,v in b.partitions(np,raw,dictionaries,profiles,require_cuda=False).items()],profiles,require_cuda=False)
            original,d=b.encode(rows,profiles)
            expected=b.combine(np,[(*k,v) for k,v in b.partitions(np,original,d,profiles,require_cuda=False).items()],profiles,require_cuda=False)
            self.assertEqual(normalize(actual),normalize(expected))
            with self.assertRaises(ValueError):f.read(run,{**one,'name':'../bad.npz'})
            with (files.folder(run)/one['name']).open('ab') as stream:stream.write(b'bad')
            with self.assertRaises(ValueError):f.read(run,one)
    def test_scalar_parity(self):check(np,False)
    def test_group_keys_do_not_overflow_or_collide(self):
        keys=np.array([[2**63-1,-2**63],[0,0],[2**63-1,-2**63],[-2**63,2**63-1]],dtype=np.int64)
        groups,values=b.aggregate(np,keys,[np.array([3,4,5,6],dtype=np.uint64)])
        self.assertEqual({tuple(k):int(v) for k,v in zip(groups,values[0])},
            {(2**63-1,-2**63):8,(0,0):4,(-2**63,2**63-1):6})
    def test_cuda_required(self):
        profiles,rows=fixture();raw,d=b.encode(rows,profiles)
        with self.assertRaises(RuntimeError):b.partitions(np,raw,d,profiles)


if __name__=='__main__':
    if '--cuda' in sys.argv:
        from chessism_api.operations.research_algorithms.cuda_runtime import cuda_backend
        xp,device=cuda_backend(0);assert device['name']=='NVIDIA GeForce RTX 4060'
        check(xp,True);print('PASS RTX 4060 metadata: exact CPU parity, DST, nulls, modes, ratings, catalogs, endings, partial-game coverage')
        if '--large' in sys.argv:
            xp.get_default_memory_pool().set_limit(size=2*1024**3)
            started=perf_counter()
            profiles,rows=fixture();raw,d=b.encode(rows,profiles);factor=8192
            large=np.tile(raw,factor);extra=np.tile(e.encode(rows),factor)
            before=b.combine(xp,[(*k,v) for k,v in b.partitions(xp,raw,d,profiles).items()],profiles)
            after=b.combine(xp,[(*k,v) for k,v in b.partitions(xp,large,d,profiles).items()],profiles)
            for player in before:
                for mode,hours in before[player]['hours']['modes'].items():
                    assert after[player]['hours']['modes'][mode]=={k:v*factor for k,v in hours.items()}
            e.partitions(xp,large,extra,d,profiles)
            print('PASS single mixed-player metadata GPU batch:',len(large),'rows;',round(perf_counter()-started,3),'seconds')
    else:unittest.main()
