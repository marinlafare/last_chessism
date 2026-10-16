from copy import deepcopy
import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

from chessism_api.database.backup_contract import canonical_contract, contract_differences, normalize_definition

BEFORE = "CHECK (((status)::text = ANY ((ARRAY['queued'::character varying, 'running'::character varying])::text[])))"
AFTER = "CHECK (((status)::text = ANY (ARRAY[('queued'::character varying)::text, ('running'::character varying)::text])))"


class BackupContractTests(unittest.TestCase):
    def contract(self):
        return {"version": "relational_v2", "columns": [{"column_name": "status", "data_type": "character varying"}],
                "constraints": [{"schema": "public", "table": "example", "kind": "c", "validated": True, "definition": BEFORE}],
                "indexes": [{"indexname": "partial", "indexdef": "CREATE INDEX partial ON example (status) WHERE " + BEFORE[6:-1]}],
                "views": [{"table_name": "view", "view_definition": "SELECT status FROM example"}]}

    def test_only_equivalent_array_cast_is_accepted(self):
        self.assertEqual(normalize_definition(BEFORE), AFTER)
        self.assertEqual(normalize_definition(AFTER), AFTER)
        source = self.contract()
        restored = canonical_contract(source)
        self.assertEqual(contract_differences(source, restored), [])
        self.assertEqual(source["constraints"][0]["definition"], BEFORE)

    def test_preserves_embedded_quotes(self):
        before = "(ARRAY['can''t'::character varying, 'ok'::character varying])::text[]"
        self.assertEqual(normalize_definition(before), "ARRAY[('can''t'::character varying)::text, ('ok'::character varying)::text]")

    def test_other_casts_and_expressions_are_never_normalized(self):
        for value in ("(ARRAY['long'::character varying(2)])::text[]", "(ARRAY[status::character varying])::text[]",
                      "(ARRAY['x'::text])::text[]", "(ARRAY[E'x\\n'::character varying])::text[]",
                      "(ARRAY['x'::character varying, NULL::character varying])::text[]"):
            self.assertEqual(normalize_definition(value), value)

    def test_never_rewrites_quoted_sql_looking_strings_or_identifiers(self):
        sql = "(ARRAY['x'::character varying])::text[]"
        for value in ("'" + sql.replace("'", "''") + "'", '"' + sql + '"'):
            self.assertEqual(normalize_definition(value), value)

    def test_actual_schema_changes_still_fail(self):
        source = self.contract()
        for section, change in (
            ("constraints", lambda c: c["constraints"][0].update(definition=AFTER.replace("'running'", "'cancelled'"))),
            ("constraints", lambda c: c["constraints"][0].update(validated=False)),
            ("constraints", lambda c: c["constraints"].clear()),
            ("constraints", lambda c: c["constraints"].append(deepcopy(c["constraints"][0]))),
            ("indexes", lambda c: c["indexes"][0].update(indexdef=c["indexes"][0]["indexdef"].replace("(status)", "(other)"))),
            ("indexes", lambda c: c["indexes"].clear()),
            ("columns", lambda c: c["columns"][0].update(data_type="integer")),
            ("views", lambda c: c["views"][0].update(view_definition="SELECT other FROM example")),
        ):
            with self.subTest(section=section, change=change):
                changed = deepcopy(source)
                change(changed)
                self.assertEqual(contract_differences(source, changed), [section])


class RestoreProgressTests(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_phase_is_persisted_without_log_noise(self):
        from chessism_api.operations import database_restore_tests as r
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status.json"
            r._atomic_write_json(path, {"job_id": "test", "status": "failed", "phase": "restoring",
                                        "postgres_log_tail": "checkpoint log"})
            with patch.object(r, "RESTORE_TEST_STATUS_PATH", path):
                await r._write_progress(None, "test", phase="failed", detail="schema mismatch")
            saved = r._read_json(path, {})
            self.assertEqual(saved["phase"], "failed")
            self.assertEqual(saved["detail"], "schema mismatch")
            self.assertEqual(saved["postgres_log_tail"], "checkpoint log")

    async def test_progress_cannot_replace_another_jobs_status(self):
        from chessism_api.operations import database_restore_tests as r
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status.json"
            before = {"job_id": "other", "phase": "restoring"}
            r._atomic_write_json(path, before)
            with patch.object(r, "RESTORE_TEST_STATUS_PATH", path):
                await r._write_progress(None, "test", phase="failed", detail="irrelevant")
            self.assertEqual(r._read_json(path, {}), before)


if __name__ == "__main__":
    unittest.main()
