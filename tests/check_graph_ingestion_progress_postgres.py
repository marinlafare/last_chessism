"""Canonical mixed-event progress regression; explicit disposable socket only."""
if __name__ == '__main__':
    import unittest
    from tests.check_canonical_admin_graph_postgres import CanonicalAdminGraphTests
    suite=unittest.TestSuite(CanonicalAdminGraphTests(name) for name in (
        'test_backfill_unique_self_play_games_and_progress',
        'test_mixed_progress_keeps_duplicates_offsets_ownership_and_retry_state'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
