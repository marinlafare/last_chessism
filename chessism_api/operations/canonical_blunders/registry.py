"""Explicit provisional adoption; frozen inference contract, not retraining."""
import hashlib
import json
import math

from sqlalchemy import text
from chessism_api.database.relational_storage import storage_for
from .schema import table, installed
from .compute import implementation_hash

ACK = 'adopt_latest_rating_aware_despite_rapid_and_unrun_quality_gates'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def freeze(report, acknowledgement):
    if acknowledgement != ACK or report.get('format') != 'chessism_blunder_full_development_v1' or report.get('production_changed') is not False:
        raise ValueError('Explicit provisional approval and completed full-development evidence required.')
    gates = report['gates']
    for gate in ('development_prediction_numerical','development_bullet_decision_numerical','player_balance','historical_partition_safety','current_source_revision'):
        if gates.get(gate) != 'passed': raise ValueError(f'Required implementation gate failed: {gate}')
    fits = [{'mode': f['mode'], 'a': f['a'], 'b': f['b']} for f in report['fits']]
    if [f['mode'] for f in fits] != ['bullet', 'blitz', 'rapid'] or any(not math.isfinite(f[k]) for f in fits for k in ('a','b')):
        raise ValueError('All finite frozen mode parameters are required.')
    config = {'version': 1, 'formula': 'expected_points_sigmoid_rating_v2', 'k0': .00368208,
        'threshold': .15, 'cp_clip': 1000, 'initial_white_cp': 15, 'mate_rules': 'existing_700cp_v1',
        'backend': 'cuda_cupy', 'device': 'NVIDIA GeForce RTX 4060', 'fits': fits,
        'ratings': [1, 4000], 'invalid_ratings': 'unassessed_no_fallback',
        'clock_aware': False, 'complexity_aware': False, 'precision': 'float64'}
    config['kernel_sha256']=implementation_hash()
    if report['config']['k0'] != config['k0'] or report['config']['threshold'] != config['threshold']:
        raise ValueError('Source coefficient or threshold changed.')
    evidence = {'source_model_id': report['model_id'], 'report_sha256': digest(report), 'gates': gates,
        'calibration_pp': {f['mode']: f['ece']*100 for f in report['fits']},
        'quality_approved': False, 'acknowledgement': acknowledgement,
        'limitations': ['Rapid did not pass development calibration/prediction selection.',
            'Independent final/population/coverage gates have not passed for this model.',
            'Low-rated bullet labels intentionally differ substantially from the old rule.',
            'Adopted by user decision, not a claim of validated chess-move quality.']}
    return digest(config), config, evidence


async def adopt(session, run_id, account_id, acknowledgement):
    layout = storage_for(session); t = lambda name: table(layout, name)
    if not await session.scalar(text("SELECT 1 FROM account WHERE id=:id AND is_active AND role='admin'"), {'id':account_id}):
        raise ValueError('An active administrator is required for explicit adoption.')
    row = (await session.execute(text('SELECT status,result FROM algorithm_run WHERE id=:id'), {'id':run_id})).mappings().one()
    if row['status'] != 'complete': raise ValueError('Source run is not complete.')
    model_id, config, evidence = freeze(row['result'], acknowledgement)
    await session.execute(text(f'SELECT id FROM {t("control")} WHERE id FOR UPDATE'))
    await session.execute(text(f'''INSERT INTO {t('model')}(id,source_run,config,evidence,approved_by,approval)
        VALUES(:id,:run,CAST(:config AS jsonb),CAST(:evidence AS jsonb),:account,'provisional_user_approved') ON CONFLICT DO NOTHING'''),
        {'id':model_id,'run':run_id,'config':json.dumps(config),'evidence':json.dumps(evidence),'account':str(account_id)})
    await select_target(session, model_id)
    return model_id


async def select_target(session, model_id):
    """Also the explicit rollback path: choose a retained version and rebuild it."""
    layout = storage_for(session); t = lambda name: table(layout, name)
    if not await session.scalar(text(f'SELECT 1 FROM {t("model")} WHERE id=:id'), {'id':model_id}):
        raise ValueError('Unknown immutable model version.')
    await session.execute(text(f'''UPDATE {t('control')} SET target_model=:id,status='building',error=NULL,
        progress='{{}}'::jsonb,updated_at=now() WHERE id'''), {'id':model_id})
    await session.execute(text(f'''INSERT INTO {t('dirty')}(game_link)
        SELECT g.link FROM {layout.table('game')} g JOIN {layout.table('game_analysis_coverage')} c ON c.link=g.link
        WHERE c.is_fully_analyzed AND c.total_positions>0 AND EXISTS(SELECT 1 FROM {layout.table('player')} p
            WHERE p.id IN(g.white_player_id,g.black_player_id) AND p.joined>0 AND p.deleted_at IS NULL)
        UNION SELECT game_link FROM {t('game')} WHERE model_id=:id
        ORDER BY 1 ON CONFLICT(game_link) DO UPDATE SET revision=EXCLUDED.revision'''), {'id':model_id})


async def status(session):
    layout = storage_for(session)
    if not await installed(session, layout): return {'installed': False}
    t = lambda name: table(layout,name)
    row = dict((await session.execute(text(f'''SELECT c.*,m.source_run,m.config,m.evidence,m.approval,
        (SELECT COUNT(*) FROM (SELECT game_link FROM {t('dirty')}
            UNION SELECT game_link FROM {layout.table('fen_summary_refresh')}) pending) AS pending_games,
        (SELECT COUNT(*) FROM {layout.table('fen_summary_refresh')}) AS pending_input_refresh_games,
        (SELECT COUNT(*) FROM {layout.table('fen_summary_refresh')} WHERE error IS NOT NULL) AS input_refresh_errors
        FROM {t('control')} c LEFT JOIN {t('model')} m ON m.id=c.target_model'''))).mappings().one())
    row['installed'] = True
    from ..cuda_summaries.runs import status as manual_status
    row['manual_summaries'] = await manual_status(session)
    return row
