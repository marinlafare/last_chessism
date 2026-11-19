"""Canonical replacements for the retired function-upgrade installer checks.

Use CHESSISM_CANONICAL_TEST_SOCKET on the disposable server. Installation and
repeated initialization are covered by check_canonical_schema_postgres; source
and rollback guarantees are exercised against the initializer's actual triggers.
"""
if __name__ == '__main__':
    import unittest
    from tests.check_canonical_admin_graph_postgres import CanonicalAdminGraphTests
    suite=unittest.TestSuite(CanonicalAdminGraphTests(name) for name in (
        'test_counter_updates_do_not_wait_for_cloud_claim_or_outbox_locks',
        'test_score_chunking_and_cloud_attribution_precede_claim_deletion',
        'test_outbox_and_map_revision_roll_back_with_source'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
