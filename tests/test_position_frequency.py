import tempfile
import unittest
import uuid
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import numpy as np
from chessism_api.operations.cuda_summaries import frequency_compute as compute, frequency_files as files
from chessism_api.operations.cuda_summaries import input_snapshot
from chessism_api.operations import player_frequency_position_page as page
from chessism_api.database.relational_storage import RelationalStorage


class FrequencyTests(unittest.TestCase):
    def test_cross_mode_color_player_and_singletons(self):
        raw=np.array([[1,1,10,1],[1,0,10,2],[1,1,20,1],[1,1,20,1],
                      [2,1,10,1],[1,1,30,1]],dtype=np.int64)
        actual=compute.calculate(np,raw,require_cuda=False)
        self.assertEqual({tuple(r) for r in actual}, {(1,1,10,1,1),(1,0,10,2,1),(1,1,20,1,2)})
        with self.assertRaises(RuntimeError): compute.calculate(np,raw)

    def test_large_ids_and_reference_counts(self):
        rng=np.random.default_rng(24)
        raw=np.column_stack((rng.integers(1,12,20000),rng.integers(0,2,20000),
                             rng.integers(1,800,20000)+2**55,rng.integers(1,4,20000)))
        counts=Counter(map(tuple,raw)); totals=Counter((r[0],r[2]) for r in raw)
        expected={(*key,count) for key,count in counts.items() if totals[key[0],key[2]]>1}
        self.assertEqual({tuple(r) for r in compute.calculate(np,raw,require_cuda=False)},expected)

    def test_checkpoint_merge_before_count_and_corruption(self):
        with tempfile.TemporaryDirectory() as root,patch.object(input_snapshot,'ROOT',Path(root)),patch.object(input_snapshot,'FREE_FLOOR',0):
            run=uuid.uuid4(); raw=np.array([[1,1,10,1],[1,0,10,2],[1,1,30,1]],dtype=np.int64)
            chunks=files.save(run,raw[:1],[1,1])+files.save(run,raw[1:],[2,2])
            shards=files.choose(chunks,0,SimpleNamespace(device_bytes=10**7,host_bytes=10**7))
            result=compute.calculate(np,files.load(run,chunks,shards),require_cuda=False)
            self.assertEqual(len(result),2)
            bad={**chunks[0],'sha256':'wrong'}
            with self.assertRaises(ValueError): files.read(run,bad)

    def test_ranked_sql_never_scans_game_occurrences(self):
        sql=str(page.ranked_sql(RelationalStorage('core','archive','reads'),
            dict(lower=-150,upper=-50,lower_inclusive=True,upper_inclusive=False)))
        self.assertNotIn('game_fen_association',sql)
        self.assertNotIn('game_player',sql)
        self.assertIn('SUM(n.appearances)>1',sql)
        self.assertIn('appearances DESC,fen_id,is_white DESC',sql)
        self.assertIn('ELSE -f.score',sql)

    def test_cursor_validates_filter_snapshot_and_exact_ids(self):
        cursor=page.cursor_value('filter','revision',dict(appearances=8,fen_id=str(2**55),player_color='black'))
        self.assertEqual(page.decode_cursor(cursor,'filter','revision'),(8,2**55,False))
        with self.assertRaises(page.FrequencySnapshotChanged):page.decode_cursor(cursor,'other','revision')
        with self.assertRaises(page.FrequencySnapshotChanged):page.decode_cursor(cursor,'filter','new')
        with self.assertRaises(ValueError):page.decode_cursor('bad','filter','revision')


if __name__=='__main__':unittest.main()
