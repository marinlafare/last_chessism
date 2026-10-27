"""Read-only balanced cohort, verified exclusions, durable identity manifest."""
import hashlib
import json
from datetime import datetime

from sqlalchemy import text

from ..matrix_constructor.artifact_files import file_operation
from . import blunder_balanced_sampling as sampling
from . import blunder_v2_source as source


async def reference_plan(session):
    rows=(await session.execute(text("""SELECT id,result FROM algorithm_run
        WHERE status='complete' AND config->>'operation'='smooth_rating_blunder_v2'
        ORDER BY created_at DESC,id LIMIT 101"""))).mappings().all()
    if len(rows)>100:
        raise ValueError('Too many historical cohorts for this bounded lineage scan.')
    references=[]; identities=set()
    for row in rows:
        result=row['result'] or {}
        if result.get('format')!='chessism_rating_blunder_candidate_v2':
            continue
        frozen=result['source']
        if frozen['sha256'] in identities:
            continue
        identities.add(frozen['sha256'])
        references.append({'id':str(row['id']),'sha256':frozen['sha256'],
            'training_before':frozen['split']['training_before'],'test_from':frozen['split']['test_from']})
        # Balanced manifests include all earlier exclusions, transitively.
        if frozen.get('seen_game_ids') is not None:
            break
    return sampling.plan(references)


def candidate_sql(session):
    return source.eligible_sql(session,owners=True)+''', staged AS (
        SELECT *,CASE WHEN played_at<CAST(:training_before AS timestamptz) THEN 0
            WHEN played_at<CAST(:test_from AS timestamptz) THEN 1 ELSE 2 END AS phase,
            CASE WHEN ABS(white_elo-black_elo)<=200 THEN 0 WHEN ABS(white_elo-black_elo)<=400 THEN 1 ELSE 2 END AS gap_band,
            CASE WHEN white_complete AND black_complete THEN LEAST(LOWER(BTRIM(white_name)),LOWER(BTRIM(black_name)))
                 WHEN white_complete THEN LOWER(BTRIM(white_name)) ELSE LOWER(BTRIM(black_name)) END AS anchor,
            hashtextextended(link::text,:seed) AS balanced_order
        FROM eligible WHERE NOT(link=ANY(CAST(:excluded AS bigint[])))
    ), owners AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY phase,mode,rating_band,gap_band,anchor
            ORDER BY balanced_order,link) AS owner_rank FROM staged
    ), pool AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY phase,mode,rating_band,gap_band
            ORDER BY owner_rank,balanced_order,link) AS pool_rank FROM owners
    ) SELECT * FROM pool WHERE pool_rank<=:pool_per_cell ORDER BY phase,mode,rating_band,gap_band,pool_rank'''


async def exclusions(session, policy, reporter):
    excluded=set(); verified=[]; replayed=0
    for ref in policy['references']:
        await reporter.check()
        row=(await session.execute(text('SELECT status,config,result FROM algorithm_run WHERE id=:id'),{'id':ref['id']})).mappings().one_or_none()
        if row is None or row['status']!='complete' or not row['result']:
            raise ValueError('Referenced cohort is unavailable; no historical overlap assumptions are made.')
        result=row['result']; frozen=result['source']; config=row['config']
        actual={'id':ref['id'],'sha256':frozen['sha256'],
            'training_before':frozen['split']['training_before'],'test_from':frozen['split']['test_from']}
        if actual!=ref or result.get('format')!='chessism_rating_blunder_candidate_v2':
            raise ValueError('Referenced cohort changed; create a fresh request.')
        if frozen.get('seen_game_ids') is not None:
            ids=frozen['seen_game_ids']
            if hashlib.sha256(json.dumps(ids).encode()).hexdigest()!=frozen.get('seen_game_ids_sha256'):
                raise ValueError('Historical identity manifest is corrupt.')
            method='saved_identity_manifest'
        else:
            replayed+=frozen['positions']
            if replayed>sampling.MAX_REPLAY_ROWS:
                raise ValueError('Historical verification exceeds its six-million-row budget.')
            metadata=[dict(r) for r in (await session.execute(text(source.selection_sql(session)),config)).mappings()]
            source.partitions(metadata)
            if sum(g['total_positions'] for g in metadata)>source.MAX_ROWS:
                raise ValueError('Historical reconstruction exceeds its source-row limit.')
            _,_,digest=await source.read_positions(session,metadata,reporter,phase='verifying_previous_cohort')
            if digest!=ref['sha256']:
                raise ValueError('Historical source changed; exact prior game identities cannot be verified. No candidate was fitted.')
            ids=[g['link'] for g in metadata]; method='full_frozen_source_hash_match'
        if any(type(i) is not int or i<=0 for i in ids):
            raise ValueError('Invalid historical game identity.')
        excluded.update(ids)
        if len(excluded)>sampling.MAX_SEEN_GAMES:
            raise ValueError('Historical exclusion manifest exceeds its bounded capacity.')
        verified.append({**ref,'method':method,'games':len(ids)})
    from .blunder_strength_source import exploration_ids
    excluded.update(await exploration_ids(session))
    from .blunder_full_source import additional_roles
    training, validation = await additional_roles(session)
    excluded.update(training | validation)
    return excluded,verified


async def select_balanced(session, config, reporter):
    policy=config['sampling']
    if config.get('refinement') and policy != await reference_plan(session):
        raise ValueError('Research history changed while this refinement waited in the queue. Submit a fresh request so all previously evaluated games are excluded.')
    excluded,verified=await exclusions(session,policy,reporter)
    await reporter.progress('selecting_balanced_games',detail='Outcome-blind player caps and rating/gap quotas; verified prior games excluded.')
    query=candidate_sql(session)
    params={**policy,'excluded':sorted(excluded),
        'training_before':datetime.fromisoformat(policy['training_before']),
        'test_from':datetime.fromisoformat(policy['test_from'])}
    pool=[dict(r) for r in (await session.execute(text(query),params)).mappings()]
    if len(pool)>3*3*9*3*sampling.POOL_PER_CELL:
        raise ValueError('Candidate metadata pool exceeds its bound.')
    metadata,audit=await file_operation(sampling.choose,pool,config['games_per_group'],policy)
    selected={g['link'] for g in metadata}
    if selected & excluded:
        raise ValueError('Previous-cohort leakage detected.')
    seen=sorted(excluded | selected)
    if len(seen)>sampling.MAX_SEEN_GAMES:
        raise ValueError('Identity manifest exceeds its capacity.')
    audit.update(verified_references=verified,excluded_prior_games=len(excluded),prior_game_overlap=0,
        candidate_pool_sql_sha256=hashlib.sha256(query.encode()).hexdigest())
    split={'training_before':policy['training_before'],'test_from':policy['test_from'],
        'held_out_players':'Same deterministic player holdout; held players excluded from training and validation.'}
    return metadata,split,{'sampling':sampling.VERSION,'sampling_audit':audit,
        'game_ids_by_split':{str(s):[g['link'] for g in metadata if g['split']==s] for s in range(4)},
        'seen_game_ids':seen,'seen_game_ids_sha256':hashlib.sha256(json.dumps(seen).encode()).hexdigest()},query


async def retain_cp_balance(session, metadata, config, audit, reporter):
    """Enforce caps on games actually used for calibration, not mate-only games.

    CP availability is a source-coverage condition, not an outcome criterion.
    All mate transitions within retained games keep their explicit rules.
    """
    counts,_,_=await source.read_positions(session,metadata,reporter,phase='checking_balanced_cp_coverage')
    eligible=[g for g,n in zip(metadata,counts) if n>0]
    kept,checked=await file_operation(sampling.choose,eligible,config['games_per_group'],config['sampling'])
    summary=audit['sampling_audit']
    summary.update(pre_cp_selected_games=len(metadata),no_cp_games_excluded=len(metadata)-len(eligible),
        cp_rebalance_removed=len(eligible)-len(kept),selected_games=len(kept),
        concentration=checked['concentration'],selected_ids_sha256=checked['selected_ids_sha256'])
    filled={(c['split'],c['mode'],c['rating_band'],c['gap_band']):c['selected_games'] for c in checked['cells']}
    for cell in summary['cells']:
        cell['selected_games']=filled[cell['split'],cell['mode'],cell['rating_band'],cell['gap_band']]
    summary['limitations'].append('Games with no Stockfish CP observation are excluded, then caps are rechecked. Mate transitions in retained games remain assessed explicitly.')
    audit['game_ids_by_split']={str(s):[g['link'] for g in kept if g['split']==s] for s in range(4)}
    if '_reuse_context' in audit:
        from .blunder_development_reuse import audit_selection
        roles, seen = audit.pop('_reuse_context')
        summary.update(audit_selection(kept, roles, seen))
    # seen_game_ids deliberately also retains inspected-but-excluded identities.
    return kept
