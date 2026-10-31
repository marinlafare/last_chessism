import json
import math
import sqlite3
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

from chessism_api.operations.salience_comparison import compute,artifacts,readers


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.id=str(uuid4())
        self.patch=patch.dict('os.environ',{'SALIENCE_COMPARISON_DIR':self.temp.name})
        self.patch.start()
        self.file=Path(self.temp.name)/f'{self.id}.sqlite'
        self.db=sqlite3.connect(self.file)
        compute.initialize(self.db)

    def tearDown(self):
        self.db.close();self.patch.stop();self.temp.cleanup()

    def add(self,game,white,fens,plies=None,mode='blitz'):
        self.db.execute('''INSERT INTO games(game_link,is_white,played_at,mode,opponent,result,metadata,moves)
            VALUES(?,?,?,?,?,?,?,?)''',(game,white,f'2026-10-{game:02d}',mode,'opponent',1,'{}','[]'))
        counts={}
        for ply,fen in zip(plies or range(1,len(fens)+1),fens):
            counts[fen]=counts.get(fen,0)+1
            self.db.execute('INSERT INTO positions VALUES(?,?,?,?)',(game,ply,fen,counts[fen]))

    def test_color_scope_repetition_window_and_parity(self):
        self.add(1,1,[10,11,10,12],[1,2,3,31])
        self.add(2,1,[10,12,13],[1,2,3])
        self.add(3,0,[10,11],[1,2])
        # Hand reference: F(10)=2, F(11)=1, F(12)=2; second 10 gets half weight.
        weights=[.25+.75*min(p/16,1) for p in (1,2,3,31)]
        expected=sum(w*s for w,s in zip(weights,(.5,1,.25,.5)))/sum(weights)
        self.db.execute('UPDATE games SET official=? WHERE game_link=1',(expected,))
        compute.compute(self.db)
        row=self.db.execute('SELECT current,equal,opening FROM games WHERE game_link=1').fetchone()
        self.assertAlmostEqual(row[0],expected)
        self.assertAlmostEqual(row[1],(.5+1+.25+.5)/4)
        self.assertAlmostEqual(row[2],(.5+1+.25)/3)
        # Position 12 in game 1 is outside the window; game 2 opening frequency is ONE.
        self.assertAlmostEqual(self.db.execute('SELECT opening FROM games WHERE game_link=2').fetchone()[0],(.5+1+1)/3)
        self.assertEqual(self.db.execute('SELECT current,equal,opening FROM games WHERE game_link=3').fetchone(),(1,1,1))
        rows=compute.occurrence_details(self.db,1)
        for variant,value in zip(compute.VARIANTS,row):
            self.assertAlmostEqual(math.fsum(r[f'{variant}_contribution'] for r in rows),value)
        self.assertFalse(rows[-1]['in_window'])

    def test_readers_sort_pagination_and_annotations_do_not_change_snapshot(self):
        self.add(1,1,[1,2]);self.add(2,1,[1,3],mode='bullet')
        compute.compute(self.db)
        for key,value in {'id':self.id,'player':'lafareto','created_at':'2026-10-29'}.items():compute.metadata(self.db,key,value)
        self.db.commit()
        before=self.file.read_bytes()
        page=readers.games(self.id,'date','desc',limit=1)
        self.assertEqual(page['games'][0]['game_link'],2)
        self.assertEqual(page['total'],2)
        self.assertEqual(readers.games(self.id,mode='blitz')['total'],1)
        artifacts.save_review(self.id,1,'repetitive','Same opening','1')
        self.assertEqual(artifacts.annotations(self.id)[1]['note'],'Same opening')
        self.assertEqual(len(artifacts.listing()['comparisons']),1)
        self.assertEqual(self.file.read_bytes(),before)
        for sort in ('current;DROP TABLE games','metadata'):
            with self.assertRaises(ValueError):readers.games(self.id,sort)
        with self.assertRaises(ValueError):artifacts.connect('../outside')
        with self.assertRaises(FileNotFoundError):artifacts.save_review(self.id,99,'uncertain','','1')

    def test_baseline_disagreement_fails_closed(self):
        self.add(1,1,[1,2])
        self.db.execute('UPDATE games SET official=.1')
        with self.assertRaisesRegex(ValueError,'disagrees'):compute.compute(self.db)

    def test_frozen_moves_replay_without_live_data(self):
        self.add(1,1,[1,2])
        meta={'initial_fen':'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'}
        self.db.execute('UPDATE games SET metadata=?,moves=?',(json.dumps(meta),json.dumps([[1,'e4','e5']])))
        compute.compute(self.db)
        data=readers.game_detail(self.id,1)
        self.assertEqual([m['san'] for m in data['moves']],['e4','e5'])
        self.assertEqual(data['moves'][1]['ply'],2)
        self.assertEqual(data['moves'][1]['uci'],'e7e5')
        self.assertEqual(len(data['comparison']['positions']),2)

    def test_api_requires_superuser_and_validates_writes(self):
        # Existing test helper supplies dummy auth configuration only.
        from tests import test_appearance
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from types import SimpleNamespace
        from chessism_api.auth import get_current_account
        from chessism_api.routers.salience_comparison import router
        self.add(1,1,[1,2]);compute.compute(self.db)
        for key,value in {'id':self.id,'player':'lafareto','created_at':'2026-10-29'}.items():compute.metadata(self.db,key,value)
        self.db.commit()
        app=FastAPI();app.include_router(router,prefix='/comparison')
        account=SimpleNamespace(id=1,role='user')
        app.dependency_overrides[get_current_account]=lambda:account
        with TestClient(app) as client:
            self.assertEqual(client.get('/comparison').status_code,403)
            self.assertEqual(client.post(f'/comparison/{self.id}/games/1/review',json={'judgement':'uncertain'}).status_code,403)
            account.role='admin'
            self.assertEqual(client.get('/comparison').status_code,200)
            self.assertEqual(client.get(f'/comparison/{self.id}/games?limit=10000').status_code,422)
            self.assertEqual(client.get(f'/comparison/{self.id}/games?sort=bad').status_code,422)
            self.assertEqual(client.post(f'/comparison/{self.id}/games/1/review',json={'judgement':'wrong'}).status_code,422)
            self.assertEqual(client.post(f'/comparison/{self.id}/games/1/review',json={'judgement':'distinctive','note':'x'*2001}).status_code,422)
            self.assertEqual(client.post(f'/comparison/{self.id}/games/1/review',json={'judgement':'distinctive','note':'A new line'}).status_code,200)
            self.assertEqual(client.get(f'/comparison/{self.id}/games').json()['games'][0]['review']['note'],'A new line')

    def test_independent_appearance_controls(self):
        from tests import test_appearance
        from chessism_api.operations.appearance import REGISTRY,validate_overrides
        targets=REGISTRY['scopes']['salience_comparison']['targets']
        self.assertIn('gameReview.panel',targets)
        self.assertIsNot(targets['gameReview.panel'],REGISTRY['scopes']['home']['targets']['gameReview.panel'])
        for key in ('current','equal','opening'):
            for prop in ('fill','barHoverColor','barHoverBorder'):
                validate_overrides([dict(scope='salience_comparison',target=f'variant.{key}.bar',property=prop,value='#123456')])
            paint=targets[f'variant.{key}.bar']['chartInteraction'][0]
            self.assertEqual(paint['marks'],['&'])
            self.assertEqual(paint['hover'],['&:hover'])


if __name__=='__main__':unittest.main()
