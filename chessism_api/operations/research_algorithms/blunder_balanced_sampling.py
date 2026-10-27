"""Outcome-blind balanced sampling v1; caps count BOTH players, in either color."""
from collections import Counter, defaultdict, deque
from datetime import datetime
import hashlib
import json
import re
import uuid

from .blunder_v2_model import MODES
from .blunder_v2_source import held_player

VERSION = 'player_balanced_v1'
POOL_PER_CELL = 512
MAX_SEEN_GAMES = 2_000_000
MAX_REPLAY_ROWS = 6_000_000


def plan(references):
    if not references or len(references)>32:
        raise ValueError('Balanced sampling needs 1–32 completed v2 cohort references.')
    normalized = []
    for ref in references:
        if set(ref) != {'id','sha256','training_before','test_from'} or not re.fullmatch('[0-9a-f]{64}',ref['sha256']):
            raise ValueError('Invalid sampling lineage.')
        uuid.UUID(ref['id'])
        for key in ('training_before','test_from'):
            if datetime.fromisoformat(ref[key]).tzinfo is None:
                raise ValueError('Sampling dates need a timezone.')
        normalized.append(dict(ref))
    normalized.sort(key=lambda r:r['id'])
    first=min(datetime.fromisoformat(r['training_before']) for r in normalized)
    last=max(datetime.fromisoformat(r['test_from']) for r in normalized)
    if first>=last:
        raise ValueError('Invalid chronological boundaries.')
    return {'version':VERSION,'seed':20261029,'player_share_limit':.1,
        'pool_per_cell':POOL_PER_CELL,'training_before':first.isoformat(),'test_from':last.isoformat(),
        'split_weights':[.6,.2,.2,.1], 'gap_bands':['0-200','201-400','over-400'],
        'references':normalized,'excluded_overlap_policy':'verified_prior_v2_games_all_partitions'}


def names(game):
    return {game[k].strip().lower() for k in ('white_name','black_name')}


def partition(game, policy):
    held=any(held_player(name) for name in names(game))
    stamp=game['played_at']
    phase=0 if stamp<datetime.fromisoformat(policy['training_before']) else 1 if stamp<datetime.fromisoformat(policy['test_from']) else 2
    return (3 if phase==2 else -1) if held else phase


def quotas(games_per_group, split):
    total=int(games_per_group*(.6,.2,.2,.1)[split])
    return [total//3+(i<total%3) for i in range(3)]


def concentration(metadata):
    report=[]
    for split in range(4):
        for mode in MODES:
            group=[g for g in metadata if g['split']==split and g['mode']==mode]
            counts=Counter(n for g in group for n in names(g))
            largest=max(counts.values(),default=0)
            report.append({'split':split,'mode':mode,'games':len(group),'unique_players':len(counts),
                'largest_player_games':largest,'largest_player_share':largest/len(group) if group else None,
                'status':'passed' if group and largest*10<=len(group) else 'insufficient_data' if not group else 'failed'})
    return report


def choose(candidates, games_per_group, policy, check=lambda:None):
    """Round-robin cells, deterministic order, shrinking actual-count caps.

    Never backfill a sparse cell from another gap/rating or relax player caps.
    Selecting by identities/dates/ratings only; no result/evaluation inspection.
    """
    groups=defaultdict(list)
    seen=set()
    for original in candidates:
        if original['link'] in seen:
            raise ValueError('Duplicate candidate game identity.')
        seen.add(original['link'])
        game=dict(original)
        game['split']=partition(game,policy)
        if game['split']>=0:
            groups[game['split'],game['mode']].append(game)
    selected, cells = [], []
    for split in range(4):
        for mode in MODES:
            check()
            bucket=defaultdict(list)
            for game in groups[split,mode]:
                bucket[game['rating_band'],game['gap_band']].append(game)
            for values in bucket.values():
                values.sort(key=lambda g:(g['pool_rank'],g['sample_order'],g['link']))
            limits=quotas(games_per_group,split)
            cap=sum(limits)*9//10
            for attempt in range(64):
                check()
                queues={key:deque(values) for key,values in bucket.items()}
                counts=Counter(); filled=Counter(); kept=[]
                active=True
                while active:
                    active=False
                    for key in sorted(queues):
                        if filled[key]>=limits[key[1]]:
                            continue
                        while queues[key]:
                            game=queues[key].popleft()
                            players=names(game)
                            if any(counts[n]>=cap for n in players):
                                continue
                            kept.append(game); counts.update(players); filled[key]+=1
                            active=True
                            break
                new_cap=len(kept)//10
                if not kept or max(counts.values(),default=0)<=new_cap:
                    break
                cap=new_cap
            else:
                raise ValueError('Player cap did not converge; sampling stopped without relaxing it.')
            selected.extend(kept)
            for rating in range(9):
                for gap in range(3):
                    cells.append({'split':split,'mode':mode,'rating_band':rating,'gap_band':gap,
                        'target':limits[gap],'candidate_games':len(bucket[rating,gap]),'selected_games':filled[rating,gap]})
    selected.sort(key=lambda g:(g['played_at'],g['link']))
    return selected, {'policy':policy,'candidate_games':len(candidates),'selected_games':len(selected),
        'concentration':concentration(selected),'cells':cells,
        'selected_ids_sha256':hashlib.sha256(json.dumps([g['link'] for g in selected]).encode()).hexdigest(),
        'limitations':['Balanced cohort metrics are not population-weighted estimates.',
            'Sparse cells remain sparse; the 10% cap is never relaxed.',
            'Candidate pools are bounded; full cells may have additional eligible games outside the pool.']}
