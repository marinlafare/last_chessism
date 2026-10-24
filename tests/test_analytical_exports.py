"""Default-deny syntax and typed analytical contract tests, no database needed."""
import unittest
from types import SimpleNamespace
from pydantic import ValidationError
from fastapi import HTTPException
from chessism_api.operations.exports.sql_policy import validate_sql
from chessism_api.operations.exports.analytical_spec import SQLExportSpec, MoveMatrixSpec
from chessism_api.operations.exports.auth import authorize_spec
from chessism_api.operations.exports.analytical_reader import AnalyticalError, output_schema


class PolicyTests(unittest.TestCase):
    def test_backup_restores_only_known_role_identities_without_login_or_passwords(self):
        from chessism_api.operations.exports.backup_roles import restore_role_sql, SUPPORT_ROLES
        self.assertEqual(restore_role_sql({}),'')
        contract={'version':1,'names':list(SUPPORT_ROLES),'restore_login':False,'credentials_included':False}
        sql=restore_role_sql({'database_support_roles':contract})
        self.assertEqual(sql.count('CREATE ROLE'),2);self.assertNotIn('PASSWORD',sql)
        self.assertEqual(sql.count('NOLOGIN'),2)
        for changes in ({'names':['unsafe; DROP DATABASE postgres']},{'restore_login':True},{'credentials_included':True}):
            with self.assertRaises(RuntimeError):restore_role_sql({'database_support_roles':dict(contract,**changes)})

    def test_ctes_join_group_window_and_parameter_casts(self):
        query='''WITH chosen AS (SELECT game_id FROM exports_data_v1.games WHERE white_player=$1 ORDER BY game_id LIMIT $2),
            totals AS (SELECT m.game_id,count(*) AS n FROM exports_data_v1.moves m JOIN chosen c USING(game_id) GROUP BY m.game_id)
            SELECT game_id,n,row_number() OVER(ORDER BY n DESC,game_id) AS rank FROM totals ORDER BY rank'''
        result=validate_sql(query,['string','int64'])
        self.assertEqual(result.relations,frozenset(('games','moves')))
        self.assertIn('pg_catalog.count',result.sql)
        self.assertIn('CAST($1 AS pg_catalog.text)',result.sql)
        self.assertNotIn('hikaru',result.sql)

    def test_subqueries_case_null_zero_and_aggregates(self):
        for sql in (
            'SELECT CASE WHEN score_cp IS NULL THEN NULL ELSE score_cp END AS cp FROM exports_data_v1.positions',
            'SELECT COALESCE(score_cp,0) AS cp FROM exports_data_v1.positions WHERE score_cp IS DISTINCT FROM 0',
            'SELECT g.game_id FROM exports_data_v1.games g WHERE EXISTS(SELECT 1 FROM exports_data_v1.moves m WHERE m.game_id=g.game_id)',
            'SELECT avg(white_rating)::float8 AS avg,count(*) AS n FROM exports_data_v1.games GROUP BY mode HAVING count(*)>2',
            "SELECT game_id FROM exports_data_v1.games WHERE mode IN ('bullet','blitz') ORDER BY game_id",
            'SELECT MAX(score_cp) FILTER(WHERE move_number=1) AS move_1 FROM exports_data_v1.moves',
            'SELECT 1 AS n UNION ALL SELECT 2 AS n'):
            with self.subTest(sql=sql):self.assertTrue(validate_sql(sql).sql)

    def test_blocked_nested_writes_privileges_functions_operators_and_casts(self):
        attacks=[
            'SELECT 1; SELECT 2', 'UPDATE public.game SET n_moves=0 RETURNING link',
            'WITH x AS (DELETE FROM public.game RETURNING link) SELECT * FROM x',
            'SELECT * INTO TEMP hacked FROM exports_data_v1.games',
            'SELECT * FROM exports_data_v1.games FOR UPDATE',
            'SELECT (SELECT game_id FROM exports_data_v1.games FOR SHARE LIMIT 1)',
            "SELECT set_config('role','chessism_user',false)", 'SELECT pg_sleep(5)',
            "SELECT pg_read_file('/etc/passwd')", "SELECT nextval('x')", 'SELECT pg_advisory_lock(1)',
            'SELECT * FROM public.export_client', 'SELECT * FROM pg_catalog.pg_authid',
            'SELECT * FROM information_schema.tables', 'SELECT * FROM games',
            'SELECT public.lower(player_name) FROM exports_data_v1.players',
            'SELECT 1 OPERATOR(public.+) 2', "SELECT 'public.export_client'::regclass",
            "SELECT 'x'::public.some_type", 'SELECT current_user',
            'SELECT * FROM generate_series(1,100000000)', 'SELECT ARRAY(SELECT game_id FROM exports_data_v1.games)',
            "SELECT dblink('x','select 1')", 'COPY (SELECT 1) TO PROGRAM \'sh\'', 'SET ROLE chessism_user',
            'WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT 1 FROM a) SELECT * FROM a',
            'WITH a AS (SELECT 1) SELECT * FROM public.a',
            'SELECT * FROM exports_data_v1.games ORDER BY game_id USING OPERATOR(public.<)',
            'SELECT * FROM exports_data_v1.games TABLESAMPLE SYSTEM(1)',
        ]
        for sql in attacks:
            with self.subTest(sql=sql),self.assertRaises(ValueError):validate_sql(sql)

    def test_parameters_cannot_be_sql_and_scope_is_server_enforced(self):
        spec=SQLExportSpec(sql='SELECT player_name FROM exports_data_v1.players WHERE player_name=$1',
                           parameters=[{'type':'string','value':"x'; DELETE FROM game; --"}])
        self.assertNotIn('DELETE',spec.plan().sql)
        denied=SimpleNamespace(allow_analytics=False,allowed_sources=['players'],max_rows=100000)
        with self.assertRaises(HTTPException):authorize_spec(denied,spec)
        denied.allow_analytics=True;authorize_spec(denied,spec)
        denied.allowed_sources=['games']
        with self.assertRaises(HTTPException):authorize_spec(denied,spec)
        for query,params in [('SELECT $2',[]),('SELECT 1',[{'type':'int64','value':2}]),
                             ('SELECT $1',[{'type':'int64','value':1.2}])]:
            with self.assertRaises(ValidationError):SQLExportSpec(sql=query,parameters=params)

    def test_matrix_column_bounds_and_derived_result_limit(self):
        spec=MoveMatrixSpec(player='hikaru',max_games=1000,players='both')
        self.assertEqual(spec.max_games,1000);self.assertEqual(spec.limit,2000)
        self.assertEqual(spec.model_copy(update={'players':'requested'}).limit,1000)
        for changes in ({'first_move':5,'last_move':2},{'last_move':126},{'last_move':'all'},{'max_games':True},
                        {'players':'both','max_result_rows':1000},{'exclude_null_columns':True},
                        {'exclude_null_columns':1},{'max_result_rows':'2000'}):
            with self.assertRaises(ValidationError):MoveMatrixSpec(player='hikaru',**changes)
        compatible=MoveMatrixSpec(player='hikaru',max_games=1000,players='both',max_result_rows=100000,exclude_null_columns=False)
        self.assertEqual(compatible.model_dump(),spec.model_dump())
        self.assertNotIn('max_result_rows',spec.model_dump())
        self.assertNotIn('exclude_null_columns',spec.model_dump())
        client=SimpleNamespace(allow_analytics=True,allowed_sources=['games','moves','players'],max_rows=1000)
        with self.assertRaises(HTTPException):authorize_spec(client,spec)
        authorize_spec(client,spec.model_copy(update={'players':'requested'}))

    def test_stored_matrix_requests_retain_old_bounds_and_column_selection(self):
        from chessism_api.operations.exports.analytical_spec import parse_request, StoredLegacyMoveMatrixSpec
        document={'mode':'move_matrix','player':'hikaru','players':'both','max_games':1000,
                  'max_result_rows':1000,'exclude_null_columns':True}
        old=parse_request(document)
        self.assertIsInstance(old,StoredLegacyMoveMatrixSpec)
        self.assertEqual(old.limit,1000);self.assertTrue(old.exclude_null_columns)
        self.assertEqual(old.model_dump()['max_result_rows'],1000)
        new=parse_request(MoveMatrixSpec(player='hikaru',players='both',max_games=1000).model_dump())
        self.assertNotIsInstance(new,StoredLegacyMoveMatrixSpec)
        self.assertEqual(new.limit,2000);self.assertFalse(new.exclude_null_columns)

    def test_result_schema_rejects_ambiguous_names_and_unsafe_types(self):
        def attribute(name,oid):return SimpleNamespace(name=name,type=SimpleNamespace(oid=oid,name='numeric'))
        self.assertEqual(str(output_schema([attribute('id',20)]).field('id').type),'int64')
        for fields in ([attribute('n',1700)],[attribute('x',23),attribute('x',23)],[]*0):
            with self.assertRaises(AnalyticalError):output_schema(fields)
