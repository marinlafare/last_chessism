import asyncio
import struct
import unittest
import numpy as np
from chessism_api.operations.canonical_blunders import compute,binary_source as binary


def wire(records):
    rows=np.zeros(len(records),dtype=binary.WIRE)
    for key,n in (('fields',4),('len_link',8),('len_ply',4),('len_score',8),('len_kind',2)):rows[key]=n
    for i,(link,ply,score,kind) in enumerate(records):
        rows['link'][i]=link; rows['ply'][i]=ply
        rows['score'][i]=np.nan if score is None else score; rows['kind'][i]=-1 if score is None else kind
    return binary.HEADER+rows.tobytes()+b'\xff\xff'


class BinarySourceTests(unittest.TestCase):
    def test_bitwise_reference_and_mixed_game_order(self):
        rng=np.random.default_rng(21); games=[]; records=[]; expected=[]
        for i in range(80):
            n=int(rng.integers(1,500)); link=1000-i
            g={'game_link':link,'total_positions':n}; games.append(g)
            r=[(j+1,float(rng.uniform(-11000,11000)),int(rng.integers(-1,3))) for j in range(n)]
            r[0]=(1,None,-1)
            if n>3:r[1:4]=[(2,-0.,0),(3,np.nan,-1),(4,9998.,1)]
            records.extend((link,*x) for x in r); expected.append(compute.pack(g,r,i+13))
        raw=wire(sorted(records,key=lambda x:(x[0],x[1])))
        self.assertEqual(binary.pack(raw,games,13).tobytes(),np.concatenate(expected).tobytes())

    def test_corrupt_wire_and_coverage_rejected(self):
        games=[{'game_link':10,'total_positions':1}]; valid=wire([(10,1,15,0)])
        for bad in (valid[:-1],b'x'+valid[1:],valid[:-2],valid+b'\x00',
                valid[:19]+struct.pack('>h',3)+valid[21:]):
            with self.assertRaises(ValueError):binary.pack(bad,games,0)
        for rows in ([(11,1,15,0)],[(10,2,15,0)],[(10,1,float('inf'),0)],[(10,1,0,4)]):
            with self.assertRaises(ValueError):binary.pack(wire(rows),games,0)

    def test_missing_duplicate_unordered_plies_and_extra_games(self):
        games=[{'game_link':1,'total_positions':2},{'game_link':2,'total_positions':1}]
        for r in ([(1,1,0,0),(1,1,0,0),(2,1,0,0)],
                  [(1,2,0,0),(1,1,0,0),(2,1,0,0)],
                  [(2,1,0,0),(1,1,0,0),(1,2,0,0)],
                  [(1,1,0,0),(2,1,0,0),(2,2,0,0)]):
            with self.assertRaises(ValueError):binary.pack(wire(r),games,0)

    def test_bounded_blocks_and_transfer(self):
        games=[{'total_positions':32768} for _ in range(5)]
        self.assertEqual([len(b) for b in binary.blocks(games)],[2,2,1])
        with self.assertRaises(ValueError):list(binary.blocks([{'total_positions':32769}]))
        sink=binary.Sink(1)
        with self.assertRaises(ValueError):asyncio.run(sink.write(b'x'*(sink.limit+1)))


if __name__=='__main__':unittest.main()
