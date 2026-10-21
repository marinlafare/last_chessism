"""Exercise reporting SQL in an isolated in-memory database, never Google/live DB."""
import os
import unittest
from unittest.mock import patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
from sqlalchemy import JSON, MetaData, create_engine, event, update
from sqlalchemy.dialects.postgresql import ARRAY
from chessism_api.database import models  # Registers column tables.
from chessism_api.database import cloud_codec as codec, cloud_columns as columns


class ReportingTests(unittest.TestCase):
    def setUp(self):
        # SQLite JSON stores arrays only in this disposable test adapter. The
        # production schema remains native PostgreSQL columns and arrays.
        metadata = MetaData()
        roots = columns.ROOTS.to_metadata(metadata)
        tables = {name: table.to_metadata(metadata) for name, table in columns.TABLES.items()}
        for table in metadata.tables.values():
            for column in table.c:
                if isinstance(column.type, ARRAY):
                    column.type = JSON()
        self.engine = create_engine('sqlite://')
        metadata.create_all(self.engine)
        self.connection = self.engine.connect()
        self.addCleanup(self.engine.dispose)
        self.addCleanup(self.connection.close)
        self.enterContext(patch.object(columns, 'ROOTS', roots))
        self.enterContext(patch.object(columns, 'TABLES', tables))
        self.statements = []
        event.listen(self.engine, 'before_cursor_execute',
                     lambda conn, cursor, statement, parameters, context, many: self.statements.append(statement))

    def save(self, ident, kind, value):
        codec.write_document(self.connection, ident, kind, value)

    def test_selection_never_fetches_game_links_and_queries_do_not_grow_with_jobs(self):
        for index in range(50):
            self.save(str(index), 'selection', {'backend': 'batch_spot', 'n_vms': 6,
                                               'player_name': None, 'game_links': list(range(5000))})
        self.statements.clear()
        result = codec.read_projections(self.connection, map(str, range(50)),
                                        set(columns.SCHEMAS['selection']) - {'game_links'})
        self.assertEqual(len(self.statements), 2)
        self.assertNotIn('game_links', self.statements[1])
        self.assertEqual(len(result), 50)
        self.assertEqual(result['49'], {'backend': 'batch_spot', 'n_vms': 6, 'player_name': None})

    def test_nested_projection_excludes_other_collections_of_the_same_type(self):
        performance = {'position_count': 2, 'metrics': {'fen_per_second': 1.5},
                       'workers': [{'worker': 0, 'positions': 2}],
                       'vms': [{'index': 0, 'workers': [{'worker': 0, 'positions': 2}]}]}
        value = {'performance': performance, 'task_performance': [performance],
                 'run_executions': [{'name': 'execution', 'uid': 'uid'}],
                 'vm_statuses': [{'index': 0, 'state': 'SUCCEEDED'}],
                 'tasks': [{'config': {'workers': 100}}]}
        self.save('one', 'launch', value)
        self.save('two', 'launch', {'performance': None, 'run_executions': []})
        self.statements.clear()
        names = {'performance', 'run_executions', 'vm_statuses'}
        result = codec.read_projections(self.connection, ['one', 'two', 'one'], names)
        self.assertEqual(result['one'], {key: value[key] for key in names})
        self.assertEqual(result['two'], {'performance': None, 'run_executions': []})
        self.assertEqual(len(self.statements), 6)  # roots, launch, worker, vm, identity, status
        self.assertTrue(all(' LIKE ' in query for query in self.statements[2:]))

    def test_empty_missing_null_and_unknown_fields(self):
        self.assertEqual(codec.read_projections(self.connection, [], {'backend'}), {})
        self.assertEqual(self.statements, [])
        with self.assertRaisesRegex(ValueError, 'Missing durable'):
            codec.read_projections(self.connection, ['missing'], {'backend'})
        self.save('null', 'launch', None)
        self.save('empty', 'launch', {})
        self.assertEqual(codec.read_projections(self.connection, ['null', 'empty'], {'unknown'}),
                         {'null': None, 'empty': {}})

    def test_reporting_is_not_a_replacement_for_checksum_verified_recovery(self):
        self.save('job', 'selection', {'backend': 'batch_spot', 'game_links': [1, 2]})
        table = columns.TABLES['selection']
        self.connection.execute(update(table).where(table.c.document_id == 'job').values(game_links=[3]))
        self.assertEqual(codec.read_projection(self.connection, 'job', {'backend'}), {'backend': 'batch_spot'})
        with self.assertRaisesRegex(ValueError, 'checksum'):
            codec.read_document(self.connection, 'job')


if __name__ == '__main__':
    unittest.main()
