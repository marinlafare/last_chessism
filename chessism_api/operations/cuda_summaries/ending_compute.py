"""CUDA final-position outcome counts, including partially analyzed games."""
from datetime import date
import numpy as np
from .score_compute import check_device
from .behavior_compute import aggregate, MODES

VERSION='unexpected_endings_cuda_v1'
ENDINGS=('lost_on_time','won_on_time','lost_by_resignation','won_by_resignation')
COUNTS=('eligible_games','assessable_games','ahead_games','balanced_games','behind_games',
        'missing_final_position','missing_final_score','mate_games','tablebase_games')
COVERAGE=('total_games','analyzed_games','total_positions','analyzed_positions','unscored_positions')
DTYPE=np.dtype([(k,'<i8') for k in ('own_reason','opponent_reason','opponent_result','exists','tablebase',
    'total_positions','analyzed_positions','unscored_positions','fully_analyzed')]+[('score','<f8')])


def encode(rows):
    output=np.zeros(len(rows),DTYPE)
    def reason(value): return {'timeout':1,'resigned':2,'resignation':2}.get(str(value or '').strip().lower(),0)
    for i,row in enumerate(rows):
        output[i]=tuple([reason(row.get('own_reason')),reason(row.get('opponent_reason')),
            {1:1,.5:2,0:3}.get(row.get('opponent_result'),0),bool(row.get('final_exists')),
            bool(row.get('final_tablebase')),*[row.get(k) or 0 for k in ('total_positions','analyzed_positions','unscored_positions')],
            bool(row.get('fully_analyzed')),np.nan if row.get('final_score') is None else row['final_score']])
    return output


def blank():
    return {'by_mode':{m:{e:dict.fromkeys(COUNTS,0) for e in ENDINGS} for m in MODES},
            'coverage':dict.fromkeys(COVERAGE,0)}


def partitions(xp,raw,extra,dictionaries,profiles,*,require_cuda=True):
    check_device(xp,require_cuda)
    if not len(raw): return {}
    a={k:xp.asarray(raw[k]) for k in ('owner','utc_day','mode','result','black')}
    e={k:xp.asarray(extra[k]) for k in extra.dtype.names}
    result={}
    def part(owner,day):
        return result.setdefault((profiles[int(owner)]['player_name'],date.fromordinal(int(day)).isoformat()),blank())
    base=xp.stack((a['owner'],a['utc_day']),axis=1)
    keys,sums=aggregate(xp,base,[xp.ones(len(raw),dtype=xp.uint64),
        (e['fully_analyzed'] & (e['total_positions']>0)).astype(xp.uint64),
        e['total_positions'],e['analyzed_positions'],e['unscored_positions']])
    for (owner,day),*values in zip(keys,*sums):part(owner,day)['coverage']=dict(zip(COVERAGE,map(int,values)))
    lost=(a['result']==3)&(e['opponent_result']==1)
    won=(a['result']==1)&(e['opponent_result']==3)
    ending=xp.where(lost & (e['own_reason']==1),0,xp.where(won & (e['opponent_reason']==1),1,
        xp.where(lost & (e['own_reason']==2),2,xp.where(won & (e['opponent_reason']==2),3,-1))))
    chosen=(ending>=0)&xp.isin(a['mode'],xp.asarray([i for i,m in enumerate(dictionaries['mode']) if m in MODES],dtype=xp.int64))
    # Tablebase takes precedence even when its numerical score is missing.
    missing_pos=e['exists']==0;tb=~missing_pos & (e['tablebase']!=0)
    missing_score=~missing_pos & ~tb & xp.isnan(e['score'])
    mate=~missing_pos & ~tb & ~missing_score & (xp.abs(e['score'])>=9000)
    cp=~missing_pos & ~tb & ~missing_score & ~mate
    perspective=xp.where(a['black']!=0,-e['score'],e['score'])
    columns=[xp.ones(len(raw),dtype=xp.uint64),cp,cp & (perspective>200),
        cp & (perspective>=-200)&(perspective<=200),cp & (perspective < -200),missing_pos,missing_score,mate,tb]
    keys,sums=aggregate(xp,xp.stack((a['owner'],a['utc_day'],a['mode'],ending),axis=1)[chosen],
        [c[chosen].astype(xp.uint64) for c in columns])
    for (owner,day,m,end),*values in zip(keys,*sums):
        part(owner,day)['by_mode'][dictionaries['mode'][int(m)]][ENDINGS[int(end)]]=dict(zip(COUNTS,map(int,values)))
    return result


def combine(xp,entries,profiles,*,require_cuda=True,by_day=False):
    check_device(xp,require_cuda)
    owners={p['player_name']:i for i,p in enumerate(profiles)};days=sorted({d for _,d,_ in entries});day_ids={d:i for i,d in enumerate(days)}
    result={(p,d if by_day else None):blank() for p,d,_ in entries}
    identities=[];ending_rows=[];coverage_rows=[]
    for player,day,payload in entries:
        owner=owners[player];d=day_ids[day] if by_day else 0
        coverage_rows.append([owner,d,*[payload['coverage'][k] for k in COVERAGE]])
        for m,mode in enumerate(MODES):
            for e,end in enumerate(ENDINGS):
                identities.append([owner,d,m,e]);ending_rows.append([payload['by_mode'][mode][end][k] for k in COUNTS])
    if not entries:return result
    keys,sums=aggregate(xp,xp.asarray(identities,dtype=xp.int64),
        [xp.asarray([r[i] for r in ending_rows],dtype=xp.uint64) for i in range(len(COUNTS))])
    for (o,d,m,e),*values in zip(keys,*sums):
        result[(profiles[int(o)]['player_name'],days[int(d)] if by_day else None)]['by_mode'][MODES[int(m)]][ENDINGS[int(e)]]=dict(zip(COUNTS,map(int,values)))
    array=xp.asarray(coverage_rows,dtype=xp.int64)
    keys,sums=aggregate(xp,array[:,:2],[array[:,i+2] for i in range(len(COVERAGE))])
    for (o,d),*values in zip(keys,*sums):
        result[(profiles[int(o)]['player_name'],days[int(d)] if by_day else None)]['coverage']=dict(zip(COVERAGE,map(int,values)))
    return result if by_day else {p:v for (p,_),v in result.items()}
