"""Outcome-blind sampler invariants and guarded historical replay; offline only."""
from datetime import datetime, timezone
import copy
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from chessism_api.operations.research_algorithms import blunder_balanced_sampling as b
from chessism_api.operations.research_algorithms import blunder_balanced_source as s
from chessism_api.operations.research_algorithms import blunder_v2_job as job

REF={'id':'00000000-0000-4000-8000-000000000001','sha256':'a'*64,
     'training_before':'2021-01-01T00:00:00+00:00','test_from':'2022-01-01T00:00:00+00:00'}


def candidates():
    regular=[f'p{i}' for i in range(1000) if not b.held_player(f'p{i}')]
    held=[f'p{i}' for i in range(1000) if b.held_player(f'p{i}')]
    rows=[]
    for split in range(4):
        for mode in ('bullet','blitz','rapid'):
            for rating in range(9):
                for gap in range(3):
                    for i in range(20):
                        link=len(rows)+1
                        rows.append({'link':link,'mode':mode,'rating_band':rating,'gap_band':gap,
                            'played_at':datetime((2020,2021,2022,2022)[split],6,1,tzinfo=timezone.utc),
                            'white_name':(held if split==3 else regular)[i], 'black_name':regular[30+i],
                            'pool_rank':i+1,'sample_order':link,'white_result':i%3/2})
    return rows


class BalanceTests(unittest.TestCase):
    def test_caps_both_colors_quotas_and_determinism(self):
        policy=b.plan([REF]); pool=candidates()
        # A dominant black player appears first and in multiple rating/gap cells.
        donor=pool[0]
        for i in range(1000):
            pool.append({**donor,'link':100000+i,'pool_rank':0,'sample_order':-i,
                         'rating_band':i%9,'gap_band':i%3,'white_name':pool[i%100]['white_name']})
        selected,audit=b.choose(pool,1000,policy)
        self.assertTrue(selected)
        self.assertTrue(all(r['status']=='passed' for r in audit['concentration']))
        self.assertTrue(all(c['selected_games']<=c['target'] for c in audit['cells']))
        self.assertEqual(len(selected),len({g['link'] for g in selected}))
        second,_=b.choose(list(reversed(pool)),1000,policy)
        self.assertEqual(selected,second)
        json.dumps(audit,allow_nan=False)

    def test_outcomes_do_not_choose_games(self):
        pool=candidates(); policy=b.plan([REF])
        first,_=b.choose(pool,1000,policy)
        for g in pool: g['white_result']=1-g['white_result']; g['score']=99999
        second,_=b.choose(pool,1000,policy)
        self.assertEqual([g['link'] for g in first],[g['link'] for g in second])

    def test_sparse_groups_not_backfilled_or_cap_relaxed(self):
        pool=candidates()[:5]
        selected,audit=b.choose(pool,1000,b.plan([REF]))
        self.assertEqual(selected,[])
        self.assertTrue(all(c['selected_games']==0 for c in audit['cells']))

    def test_cutoffs_held_players_and_invalid_lineage(self):
        policy=b.plan([REF]); pool=candidates()
        first=pool[0]
        self.assertEqual(b.partition({**first,'played_at':datetime.fromisoformat(REF['training_before'])},policy),1)
        self.assertEqual(b.partition({**first,'played_at':datetime.fromisoformat(REF['test_from'])},policy),2)
        held=next(g['white_name'] for g in pool if b.held_player(g['white_name']))
        self.assertEqual(b.partition({**first,'white_name':held},policy),-1)
        self.assertEqual(b.partition({**first,'black_name':held,'played_at':datetime(2023,1,1,tzinfo=timezone.utc)},policy),3)
        with self.assertRaises(ValueError): b.plan([])
        with self.assertRaises(ValueError): b.plan([{**REF,'sha256':'bad'}])
        broken=copy.deepcopy(policy); broken['player_share_limit']=.5
        with self.assertRaisesRegex(ValueError,'Unsupported'): job.configuration(1000,broken)
        with self.assertRaisesRegex(RuntimeError,'cancel'):
            b.choose(pool,1000,policy,lambda:(_ for _ in ()).throw(RuntimeError('cancel')))


class HistoricalTests(unittest.IsolatedAsyncioTestCase):
    async def test_cp_filter_rebalances_actual_calibration_games(self):
        import numpy as np
        pool=candidates(); policy=b.plan([REF])
        meta,summary=b.choose(pool,1000,policy)
        counts=np.array([0 if g['black_name']==meta[0]['black_name'] else 1 for g in meta])
        audit={'sampling_audit':summary,'seen_game_ids':[g['link'] for g in meta]}
        with patch.object(s.source,'read_positions',AsyncMock(return_value=(counts,len(meta),'a'*64))):
            kept=await s.retain_cp_balance(None,meta,job.configuration(1000,policy),audit,AsyncMock())
        self.assertTrue(kept)
        self.assertTrue(all(r['status']=='passed' for r in audit['sampling_audit']['concentration']))
        self.assertEqual(audit['sampling_audit']['no_cp_games_excluded'],int(np.count_nonzero(counts==0)))
        self.assertEqual({i for ids in audit['game_ids_by_split'].values() for i in ids},{g['link'] for g in kept})
        self.assertGreaterEqual(len(audit['seen_game_ids']),len(kept))

    async def test_hash_mismatch_stops_instead_of_claiming_fresh_holdout(self):
        previous={'status':'complete','config':job.configuration(100),'result':{
            'format':'chessism_rating_blunder_candidate_v2','source':{
                'sha256':REF['sha256'],'positions':1,'split':{k:REF[k] for k in ('training_before','test_from')}}}}
        first=MagicMock(); first.mappings.return_value.one_or_none.return_value=previous
        second=MagicMock(); second.mappings.return_value=[{'link':1,'total_positions':1}]
        session=AsyncMock(); session.execute.side_effect=[first,second]
        with patch.object(s.source,'selection_sql',return_value='SELECT 1'),patch.object(s.source,'partitions'),patch.object(s.source,'read_positions',AsyncMock(return_value=([],1,'b'*64))):
            with self.assertRaisesRegex(ValueError,'Historical source changed'):
                await s.exclusions(session,b.plan([REF]),AsyncMock())

    async def test_manifest_union_and_corruption(self):
        import hashlib
        ids=[1,2,3]
        frozen={'sha256':REF['sha256'],'split':{k:REF[k] for k in ('training_before','test_from')},
                'seen_game_ids':ids,'seen_game_ids_sha256':hashlib.sha256(json.dumps(ids).encode()).hexdigest()}
        previous={'status':'complete','config':{},'result':{'format':'chessism_rating_blunder_candidate_v2','source':frozen}}
        result=MagicMock(); result.mappings.return_value.one_or_none.return_value=previous
        session=AsyncMock(); session.execute.return_value=result
        found,verified=await s.exclusions(session,b.plan([REF]),AsyncMock())
        self.assertEqual(found,set(ids)); self.assertEqual(verified[0]['method'],'saved_identity_manifest')
        frozen['seen_game_ids'].append(4)
        with self.assertRaisesRegex(ValueError,'corrupt'): await s.exclusions(session,b.plan([REF]),AsyncMock())


if __name__=='__main__': unittest.main()
