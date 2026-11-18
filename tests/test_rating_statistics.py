"""Independent small histogram oracle for the shared weighted partition calculation."""
from itertools import combinations
import unittest

from chessism_api.rating_statistics import weighted_jenks_breaks


class WeightedRatingTests(unittest.TestCase):
    def test_partitions_minimize_within_class_squared_error(self):
        values=[400,700,800,1200,1700,2200]
        weights=[1,5,2,7,2,1]
        def cost(groups):
            total=0
            for group in groups:
                samples=[values[i] for i in group for _ in range(weights[i])]
                mean=sum(samples)/len(samples)
                total+=sum((v-mean)**2 for v in samples)
            return total
        for count in (1,2,3,4):
            possible=[]
            for cuts in combinations(range(1,len(values)),count-1):
                edges=(0,*cuts,len(values))
                possible.append(cost([range(a,b) for a,b in zip(edges,edges[1:])]))
            actual=weighted_jenks_breaks(values,weights,count)
            self.assertEqual(len(actual),count)
            self.assertAlmostEqual(cost([[i for i,v in enumerate(values) if low<=v<=high]
                                         for low,high in actual]),min(possible))

    def test_empty_invalid_and_fewer_values_than_classes(self):
        self.assertEqual(weighted_jenks_breaks([],[]),[])
        self.assertEqual(weighted_jenks_breaks([1],[1,2]),[])
        self.assertEqual(weighted_jenks_breaks([500],[20]),[(500,500)]*3)
        self.assertEqual(weighted_jenks_breaks([500,600],[1,2]),[(500,500),(600,600),(600,600)])

    def test_equal_cost_partition_order_is_stable(self):
        self.assertEqual(weighted_jenks_breaks([1000,1400,1600,1800,2000,2200],[1]*6),
                         [(1000,1000),(1400,1600),(1800,2200)])
