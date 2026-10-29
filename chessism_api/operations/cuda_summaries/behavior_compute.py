"""CUDA game-metadata reductions. Host work encodes identities/timezones only."""
from datetime import datetime, date, timezone, timedelta
import re
import numpy as np
from .score_compute import check_device, host
from ..player_timezone import resolve_player_timezone

VERSION='behavior_metadata_cuda_v1'
MODES=('bullet','blitz','rapid')
NAT=-9223372036854775808
NAR=-2147483648
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)
INTEGER=('owner','utc_day','local_day','weekday','hour','stamp','link','black','mode','rating','result','opening','opponent')
DTYPE=np.dtype([(k,'<i8') for k in INTEGER]+[('seconds','<f8')])
COUNT_FIELDS=('games','wins','draws','losses')


def utc(stamp):
    return EPOCH+timedelta(microseconds=int(stamp))


def encode(rows, profiles):
    """Encoding is not statistical calculation. Keep original null/date rules."""
    owners={p['player_name']:i for i,p in enumerate(profiles)}
    zones=[resolve_player_timezone(p) for p in profiles]
    dictionaries={k:[] for k in ('mode','opening','opponent')}; indexes={k:{} for k in dictionaries}
    def code(k,value):
        if value not in indexes[k]:
            indexes[k][value]=len(dictionaries[k]);dictionaries[k].append(value)
        return indexes[k][value]
    values=np.zeros(len(rows),DTYPE)
    for i,row in enumerate(rows):
        owner=owners[row['player_name']];stamp=row['played_at']
        if isinstance(stamp,str): stamp=datetime.fromisoformat(stamp)
        local=zones[owner].local_datetime(stamp)
        opening=re.sub(r'^https?://(www\.)?chess\.com/openings/','',str(row['eco'] or '').strip(),flags=re.I)
        if opening=='no_eco': opening=''
        delta=stamp-EPOCH if stamp else None
        stamp_us=(delta.days*86400+delta.seconds)*1000000+delta.microseconds if delta else (0 if stamp else NAT)
        values[i]=tuple([owner,stamp.date().toordinal() if stamp else 1,local.date().toordinal() if local else 1,
            local.isoweekday() if local else 0,local.hour if local else -1,stamp_us,int(row['link']),int(row['color']=='black'),
            code('mode',row['mode']),NAR if row['rating'] is None else row['rating'],
            {1:1,.5:2,0:3}.get(row['result'],0),code('opening',opening),code('opponent',row['opponent_name'] or ''),
            float(row['time_elapsed'] or 0)])
    return values,dictionaries


def aggregate(xp,keys,values):
    """Exact integer CUDA atomics for counts; float64 only for elapsed seconds."""
    if not len(keys): return np.empty((0,keys.shape[1]),np.int64),[np.empty(0) for _ in values]
    # CuPy's axis-aware unique uses a Python comparison loop in the deployed
    # version. Sort whole columns on the device, then identify group boundaries
    # in parallel instead. Do not pack keys into an overflow-prone scalar.
    order=xp.lexsort(keys.T[::-1].copy())
    ordered=keys[order]
    starts=xp.r_[xp.array([True]),xp.any(ordered[1:]!=ordered[:-1],axis=1)]
    unique=ordered[starts]
    inverse=xp.empty(len(keys),dtype=xp.int64)
    inverse[order]=xp.cumsum(starts,dtype=xp.int64)-1
    output=[]
    for column in values:
        target=xp.zeros(len(unique),dtype=xp.float64 if column.dtype.kind=='f' else xp.uint64)
        xp.add.at(target,inverse,column.astype(target.dtype));output.append(host(xp,target))
    return host(xp,unique),output


def last(xp,keys,stamp,link,black):
    if not len(keys): return np.empty(0,np.int64)
    # Keys dominate; within a group choose the latest timestamp/link/black tie.
    order=xp.lexsort(xp.stack([black,link,stamp,*[keys[:,i] for i in range(keys.shape[1]-1,-1,-1)]]))
    ordered=keys[order]
    ends=xp.r_[xp.any(ordered[:-1]!=ordered[1:],axis=1),xp.array([True])]
    return host(xp,order[ends])


def partitions(xp,raw,dictionaries,profiles,*,require_cuda=True):
    check_device(xp,require_cuda)
    if not len(raw): return {}
    a={k:xp.asarray(raw[k]) for k in raw.dtype.names}
    result={}
    def part(owner,day):
        return result.setdefault((profiles[int(owner)]['player_name'],date.fromordinal(int(day)).isoformat()),
            {'hours':[],'latest':None,'activity':[],'ratings':[],'distribution':[],'catalogs':[]})
    def mode(i): return dictionaries['mode'][int(i)]
    def keys(*fields): return xp.stack([a[f] for f in fields],axis=1)
    def counts(mask):
        return [xp.ones(int(xp.count_nonzero(mask)),dtype=xp.uint64),
                *((a['result'][mask]==n).astype(xp.uint64) for n in (1,2,3))]
    all_rows=xp.ones(len(raw),dtype=xp.bool_)
    groups,sums=aggregate(xp,keys('owner','utc_day','mode'),[counts(all_rows)[0],a['seconds']])
    for (owner,day,m),games,seconds in zip(groups,*sums):
        part(owner,day)['hours'].append({'mode':mode(m),'games':int(games),'seconds':float(seconds)})
    valid=a['stamp']!=NAT
    chosen=last(xp,keys('owner','utc_day')[valid],a['stamp'][valid],a['link'][valid],a['black'][valid])
    rows=raw[np.flatnonzero(host(xp,valid))[chosen]]
    for row in rows: part(row['owner'],row['utc_day'])['latest']=utc(row['stamp']).isoformat()
    groups,sums=aggregate(xp,keys('owner','utc_day','mode','local_day','weekday','hour')[valid],counts(valid))
    for identity,*values in zip(groups,*sums):
        owner,day,m,local,weekday,hour=identity
        part(owner,day)['activity'].append({'mode':mode(m),'date':date.fromordinal(int(local)).isoformat(),
            'weekday':int(weekday),'hour':int(hour),**dict(zip(COUNT_FIELDS,map(int,values)))})
    supported=xp.isin(a['mode'],xp.asarray([i for i,m in enumerate(dictionaries['mode']) if m in MODES],dtype=xp.int64))
    selected=valid & supported
    chosen=last(xp,keys('owner','utc_day','mode','local_day')[selected],a['stamp'][selected],a['link'][selected],a['black'][selected])
    for row in raw[np.flatnonzero(host(xp,selected))[chosen]]:
        stamp=utc(row['stamp']);owner=int(row['owner'])
        part(owner,row['utc_day'])['ratings'].append({'mode':mode(row['mode']),
            'date':date.fromordinal(int(row['local_day'])).isoformat(),
            'rating':None if row['rating']==NAR else int(row['rating']),'played_at':stamp.isoformat(),
            'played_at_local':resolve_player_timezone(profiles[owner]).local_datetime(stamp).isoformat(),
            'link':int(row['link']),'color':'black' if row['black'] else 'white'})
    groups,sums=aggregate(xp,keys('owner','utc_day','mode','rating')[supported],[counts(supported)[0]])
    for (owner,day,m,rating),games in zip(groups,*sums):
        part(owner,day)['distribution'].append({'mode':mode(m),'rating':None if rating==NAR else int(rating),'games':int(games)})
    for kind,field in (('opponents','opponent'),('openings','opening')):
        groups,sums=aggregate(xp,keys('owner','utc_day','mode','black',field)[supported],counts(supported))
        for (owner,day,m,black,entry),*values in zip(groups,*sums):
            part(owner,day)['catalogs'].append({'kind':kind,'mode':mode(m),'color':'black' if black else 'white',
                'key':dictionaries[field][int(entry)],**dict(zip(COUNT_FIELDS,map(int,values)))})
    return result


def combine(xp,entries,profiles,*,require_cuda=True,by_day=False):
    """Merge compact day partitions on CUDA, across all captured players."""
    check_device(xp,require_cuda)
    owners={p['player_name']:i for i,p in enumerate(profiles)}
    lookup=[];ids={}
    def code(value):
        if value not in ids: ids[value]=len(lookup);lookup.append(value)
        return ids[value]
    result={}
    def empty():
        return {'hours':{'modes':{},'latest_played_at':None},'activity':{'rows':[]},'activity_daily':{'rows':[]},
            'ratings':{'rows':[]},'distribution':{'counts':[]},'opponents':{'rows':[]},'openings':{'rows':[]}}
    for player,day,_ in entries: result.setdefault((player,day if by_day else None),empty())
    def identity(player,day): return (owners[player],code(day) if by_day else 0)
    def output(key): return result[(profiles[int(key[0])]['player_name'],lookup[int(key[1])] if by_day else None)]
    specifications=[('hours',('mode',),('games','seconds')),('activity',('mode','weekday','hour'),COUNT_FIELDS),
        ('activity_daily',('mode','date','weekday','hour'),COUNT_FIELDS),
        ('distribution',('mode','rating'),('games',)),('catalogs',('kind','mode','color','key'),COUNT_FIELDS)]
    for section,fields,values in specifications:
        packed=[];numbers=[]
        for player,day,payload in entries:
            for row in payload['activity' if section=='activity_daily' else section]:
                packed.append([*identity(player,day),*[code(row[f]) for f in fields]])
                numbers.append([row[f] for f in values])
        if not packed: continue
        array=xp.asarray(packed,dtype=xp.int64)
        sums=[xp.asarray([row[i] for row in numbers],dtype=xp.float64 if f=='seconds' else xp.uint64) for i,f in enumerate(values)]
        keys,aggregated=aggregate(xp,array,sums)
        for key,*v in zip(keys,*aggregated):
            target=output(key);row=dict(zip(fields,(lookup[int(i)] for i in key[2:])))
            row.update({f:float(n) if f=='seconds' else int(n) for f,n in zip(values,v)})
            if section=='hours': target['hours']['modes'][row.pop('mode')]=row
            elif section=='catalogs': target[row.pop('kind')]['rows'].append(row)
            else: target[section]['counts' if section=='distribution' else 'rows'].append(row)
    for section in ('latest','ratings'):
        packed=[];rows=[];stamps=[];links=[];colors=[]
        for player,day,payload in entries:
            candidates=([{'played_at':payload['latest'],'link':0,'color':'white'}] if payload['latest'] else []) if section=='latest' else payload['ratings']
            for row in candidates:
                stamp=datetime.fromisoformat(row['played_at']);delta=stamp-EPOCH
                packed.append([*identity(player,day),*([code(row['mode']),code(row['date'])] if section=='ratings' else [])])
                rows.append(row);stamps.append((delta.days*86400+delta.seconds)*1000000+delta.microseconds)
                links.append(row['link']);colors.append(int(row['color']=='black'))
        if packed:
            selected=last(xp,xp.asarray(packed,dtype=xp.int64),xp.asarray(stamps),xp.asarray(links),xp.asarray(colors))
            for index in selected:
                target=output(packed[index]);row=rows[index]
                if section=='latest': target['hours']['latest_played_at']=row['played_at']
                else: target['ratings']['rows'].append(row)
    if by_day:
        # Preserve the partition contract used by incremental invalidation.
        return {key:{'hours':[{'mode':m,**r} for m,r in v['hours']['modes'].items()],
            'latest':v['hours']['latest_played_at'],'activity':v['activity_daily']['rows'],
            'ratings':v['ratings']['rows'],'distribution':v['distribution']['counts'],
            'catalogs':[{'kind':kind,**row} for kind in ('opponents','openings') for row in v[kind]['rows']]}
            for key,v in result.items()}
    for (player,_),payload in result.items():
        payload['ratings']['rows'].sort(key=lambda r:(r['mode'],r['date']))
    return {player:payload for (player,_),payload in result.items()}
