import unittest

from chessism_api.operations.coefficient_research import (
    LICHESS_COEFFICIENT,
    RATING_BINS,
    expected_result,
    fit_coefficient,
    normalized_config,
    weighted_error,
)


class CoefficientResearchTests(unittest.TestCase):
    def test_rating_bins_cover_below_1200_and_above_2600(self):
        self.assertEqual(RATING_BINS[0]["key"], "under-1200")
        self.assertIsNone(RATING_BINS[0]["lower"])
        self.assertEqual(RATING_BINS[0]["upper"], 1199)
        self.assertEqual(RATING_BINS[-1]["key"], "2600-plus")
        self.assertEqual(RATING_BINS[-1]["lower"], 2600)
        self.assertIsNone(RATING_BINS[-1]["upper"])

    def test_expected_result_is_symmetric(self):
        positive = expected_result(300, LICHESS_COEFFICIENT)
        negative = expected_result(-300, LICHESS_COEFFICIENT)
        self.assertAlmostEqual(positive, -negative)
        self.assertEqual(expected_result(0, LICHESS_COEFFICIENT), 0)

    def test_fit_recovers_a_synthetic_coefficient(self):
        target = 0.0027
        observations = []
        for cp in range(-900, 901, 100):
            observations.append({
                "cp": cp,
                "outcome": expected_result(cp, target),
                "weight": 20.0,
            })
        fitted = fit_coefficient(observations)
        self.assertIsNotNone(fitted)
        self.assertAlmostEqual(fitted, target, places=6)
        self.assertLess(
            weighted_error(observations, fitted),
            weighted_error(observations, LICHESS_COEFFICIENT),
        )

    def test_config_normalization_keeps_supported_modes(self):
        config = normalized_config({
            "modes": ["rapid", "classical"],
            "max_rating_gap": 5000,
            "opening_moves_excluded": -2,
        })
        self.assertEqual(config["modes"], ["rapid"])
        self.assertEqual(config["max_rating_gap"], 1000)
        self.assertEqual(config["opening_moves_excluded"], 0)


if __name__ == "__main__":
    unittest.main()
