import os
import unittest

os.environ.setdefault('DATABASE_URL','postgresql+asyncpg://test:test@localhost/test')
from chessism_api.operations.mate_distance import mate_distance
from chessism_api.operations.player_stockfish_summary import combine, empty_modes
from chessism_api.operations.player_game_explorer import _evaluation


class StockfishSummaryTests(unittest.TestCase):
    def test_known_encoding_both_sides_and_terminal_position(self):
        import chess.engine
        for distance in (0,1,2,4,6,50):
            for score in (chess.engine.Mate(distance), -chess.engine.Mate(distance)):
                encoded=score.score(mate_score=10000)
                self.assertEqual(mate_distance(encoded,'stockfish'),distance)
                self.assertEqual(mate_distance(encoded,'stockfish_backup'),distance)
                self.assertEqual(_evaluation(encoded,'stockfish')['mate_moves'],distance)

    def test_unknown_distance_is_not_guessed(self):
        for value,source in ((9998,None),(9998,'tablebase'),(150,'stockfish'),
                             (12000,'stockfish'),(float('inf'),'stockfish'),(9998.5,'stockfish')):
            self.assertIsNone(mate_distance(value,source))

    def test_partitions_sum_mates_and_preserve_unknown(self):
        first,second=empty_modes(),empty_modes()
        first['blitz'].update(mates=[{'moves':2,'side':'for','positions':3}],mate_positions=3)
        second['blitz'].update(mates=[{'moves':2,'side':'for','positions':4},{'moves':None,'side':'against','positions':1}],mate_positions=5)
        actual=combine([first,second])['blitz']
        self.assertEqual(actual['mate_positions'],8)
        self.assertIn({'moves':2,'side':'for','positions':7},actual['mates'])
        self.assertIn({'moves':None,'side':'against','positions':1},actual['mates'])
