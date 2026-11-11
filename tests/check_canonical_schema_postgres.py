"""Required real PostgreSQL checks. Explicit private socket; never silently skipped.

Run with: python -B -m unittest tests.check_canonical_schema_postgres -v
Each test owns a uniquely named empty database in the disposable server.
"""
import asyncio
import sys
import unittest
from unittest.mock import patch

import asyncpg

from chessism_api.database.postgresql import canonical
from tests.canonical_schema_fixture import seed, FENS, START, PLAYED, GAME_SQL
from tests.canonical_postgres_case import CanonicalDatabaseTestCase

# This is the existing selection contract, exercised against the new physical schema.
SELECT_PENDING = '''SELECT f.fen FROM public.fen f
 WHERE f.score IS NULL AND NOT EXISTS(SELECT 1 FROM public.cloud_fen_claim c WHERE c.fen=f.fen)
 ORDER BY f.n_games DESC LIMIT $1 FOR UPDATE OF f SKIP LOCKED'''
RECHECK = '''SELECT f.fen FROM unnest($1::text[]) wanted(fen) JOIN public.fen f USING(fen)
 WHERE f.score IS NULL EXCEPT SELECT fen FROM public.cloud_fen_claim WHERE fen=ANY($1::text[])'''


class CanonicalPostgresTests(CanonicalDatabaseTestCase):
    async def populated(self):
        await canonical.initialize(self.db)
        async with self.db.transaction():
            return await seed(self.db)

    async def test_empty_initialization_and_exact_repeat_preserve_data(self):
        self.assertTrue(await canonical.initialize(self.db))
        self.assertEqual(await canonical.validate(self.db),2)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM public.account'),0)
        self.assertEqual(await self.db.fetch(SELECT_PENDING,10),[])
        await seed(self.db)
        stamp = await self.db.fetchval('SELECT initialized_at FROM canonical_schema_version')
        self.assertFalse(await canonical.initialize(self.db))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),5)
        self.assertEqual(await self.db.fetchval('SELECT initialized_at FROM canonical_schema_version'),stamp)

    async def test_concurrent_initializers_serialize(self):
        other = await self.connect()
        self.assertEqual(sorted(await asyncio.gather(canonical.initialize(self.db),canonical.initialize(other))),[False,True])
        await canonical.validate(self.db)

    async def test_initializer_refuses_existing_objects_without_changing_them(self):
        await self.db.execute('CREATE TABLE preserve(value text); INSERT INTO preserve VALUES(\'untouched\')')
        with self.assertRaises(canonical.SchemaMismatch):
            await canonical.initialize(self.db)
        self.assertEqual(await self.db.fetchval('SELECT value FROM preserve'),'untouched')
        self.assertIsNone(await self.db.fetchval("SELECT to_regclass('public.fen')"))

    async def test_failed_ddl_rolls_back_every_object_and_marker(self):
        original = canonical.schema_sql()
        with patch.object(canonical,'schema_sql',return_value=original+'\nSELECT definitely_missing_function();'):
            with self.assertRaises(asyncpg.UndefinedFunctionError):
                await canonical.initialize(self.db)
        self.assertIsNone(await self.db.fetchval("SELECT to_regclass('public.game')"))
        self.assertIsNone(await self.db.fetchval("SELECT to_regtype('public.account_role')"))
        self.assertTrue(await canonical.initialize(self.db))

    async def test_version_and_artifact_mismatch_are_rejected_without_repair(self):
        await canonical.initialize(self.db)
        await self.db.execute('UPDATE canonical_schema_version SET version=99')
        for fn in (canonical.validate,canonical.initialize):
            with self.assertRaises(canonical.SchemaMismatch):
                await fn(self.db)
        self.assertEqual(await self.db.fetchval('SELECT version FROM canonical_schema_version'),99)
        await self.db.execute('UPDATE canonical_schema_version SET version=2,ddl_sha256=\'wrong\'')
        with self.assertRaises(canonical.SchemaMismatch):
            await canonical.validate(self.db)

    async def test_catalog_drift_rejected_even_when_version_is_unchanged(self):
        await canonical.initialize(self.db)
        await self.db.execute('ALTER TABLE months DROP CONSTRAINT months_pkey')
        with self.assertRaisesRegex(canonical.SchemaMismatch,'drift'):
            await canonical.validate(self.db)

    async def test_validation_works_in_read_only_transaction(self):
        await canonical.initialize(self.db)
        async with self.db.transaction(readonly=True):
            self.assertEqual(await canonical.validate(self.db),2)

    async def test_accepted_version_one_is_not_silently_reinterpreted(self):
        await canonical.initialize(self.db)
        await self.db.execute('UPDATE canonical_schema_version SET version=1')
        for operation in (canonical.validate, canonical.initialize):
            with self.assertRaises(canonical.SchemaMismatch):
                await operation(self.db)
        self.assertEqual(await self.db.fetchval('SELECT version FROM canonical_schema_version'),1)

    async def test_developer_command_initializes_repeats_and_validates_explicit_target(self):
        args = [sys.executable,'-B','-m','scripts.init_canonical_db','--socket',self.socket,
                '--database',self.name,'--user','test']
        for suffix,expected in [([],b'initialized.'),([],b'already matches'),(['--validate-only'],b'validated')]:
            process = await asyncio.create_subprocess_exec(*args,*suffix,stdout=asyncio.subprocess.PIPE,
                                                           stderr=asyncio.subprocess.PIPE)
            out,err = await asyncio.wait_for(process.communicate(),30)
            self.assertEqual(process.returncode,0,err.decode())
            self.assertIn(expected,out)

    async def test_standard_variants_custom_setups_zero_moves_and_self_play(self):
        ids = await self.populated()
        actual = await self.db.fetch('SELECT link,is_standard,n_moves FROM game ORDER BY link')
        self.assertEqual([tuple(r) for r in actual],[(101,True,2),(102,False,2),(103,False,2),(104,True,0),(105,True,2)])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_player WHERE game_link=105 AND player_id=$1',ids['white']),2)
        rows = await self.db.fetch('SELECT link FROM game WHERE is_standard AND n_moves>0 AND NOT fens_done ORDER BY link')
        self.assertEqual([r['link'] for r in rows],[101,105])
        await self.db.execute("UPDATE game SET initial_setup=$1 WHERE link=101",'  '+START.replace(' ','  ')+' 0 1  ')
        self.assertTrue(await self.db.fetchval('SELECT is_standard FROM game WHERE link=101'))

    async def test_one_identity_prevents_duplicates_across_classifications(self):
        ids = await self.populated()
        for link in (101,102,104):
            with self.subTest(link=link),self.assertRaises(asyncpg.UniqueViolationError):
                await self.db.execute(GAME_SQL,link,ids['white'],ids['black'],PLAYED,2,'chess',None)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game'),5)

    async def test_player_game_move_opening_and_month_constraints(self):
        ids = await self.populated()
        cases = ["INSERT INTO player(player_name) VALUES('synthetic_white')",
                 "INSERT INTO months VALUES(999999,2026,1,0)",
                 f"INSERT INTO months VALUES({ids['white']},2026,1,0)",
                 f"INSERT INTO months VALUES({ids['white']},2026,13,0)",
                 "UPDATE game SET white_player_id=999999 WHERE link=101",
                 "INSERT INTO game_opening VALUES(101,1,999999)",
                 "INSERT INTO moves VALUES(999999,1,'e4','e5',0,0,600,600)"]
        for sql in cases:
            with self.subTest(sql=sql),self.assertRaises(asyncpg.IntegrityConstraintViolationError):
                await self.db.execute(sql)
        self.assertEqual(await self.db.fetchval('SELECT n_games FROM months WHERE month=2'),0)
        await self.db.execute("INSERT INTO moves VALUES(102,1,'e4','e5',0,0,600,600)")
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute("INSERT INTO moves VALUES(102,1,'d4','d5',0,0,600,600)")

    async def test_fen_identity_references_and_occurrences(self):
        ids = await self.populated()
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO fen(fen) VALUES($1)',FENS[0])
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.db.execute('INSERT INTO fen(fen) VALUES($1)',FENS[0]+' 0 1')
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute('INSERT INTO game_fen_association VALUES(101,999999,7,true)')
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,0,true)',ids['fens'][1])
        # The same position can recur at a different move and in another game.
        await self.db.execute('INSERT INTO game_fen_association VALUES(101,$1,8,true)',ids['fens'][0])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM game_fen_association WHERE fen_id=$1',ids['fens'][0]),3)

    async def test_variants_cannot_gain_fen_references_or_extraction_claims(self):
        ids = await self.populated()
        for link in (102,103,104):
            with self.subTest(link=link),self.assertRaises(asyncpg.CheckViolationError):
                await self.db.execute('INSERT INTO game_fen_association VALUES($1,$2,0,true)',link,ids['fens'][0])
            with self.assertRaises(asyncpg.CheckViolationError):
                await self.db.execute('UPDATE game SET fens_processing=true WHERE link=$1',link)
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.db.execute("UPDATE game SET rules='chess960' WHERE link=101")
        self.assertTrue(await self.db.fetchval('SELECT is_standard FROM game WHERE link=101'))

    async def test_integer_scores_wdl_mate_and_tablebase(self):
        ids = await self.populated()
        await self.db.execute("""UPDATE fen SET score=0,analysis_source='stockfish',analyzed_at=now(),
            wdl_win=200,wdl_draw=600,wdl_loss=200 WHERE fen_id=$1""",ids['fens'][0])
        await self.db.execute("UPDATE fen SET score=9999,analysis_source='stockfish',analyzed_at=now() WHERE fen_id=$1",ids['fens'][3])
        await self.db.execute("UPDATE fen SET score=0,analysis_source='tablebase',tablebase_wdl=0,tablebase_dtz=0,analyzed_at=now() WHERE fen_id=$1",ids['fens'][4])
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen WHERE score IS NOT NULL'),3)
        for change in ["score=5", "score=5,analyzed_at=now()", "wdl_win=1", "wdl_win=1,wdl_draw=1,wdl_loss=1",
                       "score=5,analysis_source='stockfish',analyzed_at=now(),tablebase_wdl=2",
                       "score=5,analysis_source='tablebase',analyzed_at=now()"]:
            with self.subTest(change=change),self.assertRaises(asyncpg.CheckViolationError):
                await self.db.execute('UPDATE fen SET '+change+' WHERE fen_id=$1',ids['fens'][1])
        await self.db.execute("UPDATE fen SET analysis_source='tablebase_unavailable' WHERE fen_id=$1",ids['fens'][1])
        self.assertIn(FENS[1],[r['fen'] for r in await self.db.fetch(SELECT_PENDING,10)])

    async def test_continuation_keys_and_parent_deletion(self):
        ids = await self.populated()
        fen_id = ids['fens'][4]
        await self.db.execute("INSERT INTO fen_continuation VALUES($1,2,'e3e4',0)",fen_id)
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute("INSERT INTO fen_continuation VALUES($1,2,'e3d3',100)",fen_id)
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute("INSERT INTO fen_continuation VALUES(999999,1,'e3d3',0)")
        await self.db.execute('DELETE FROM fen WHERE fen_id=$1',fen_id)
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM fen_continuation'),0)

    async def test_unscored_timestamp_requires_explicit_unavailable_source(self):
        ids = await self.populated()
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.db.execute('UPDATE fen SET analyzed_at=now() WHERE fen_id=$1',ids['fens'][0])
        self.assertIsNone(await self.db.fetchval('SELECT analyzed_at FROM fen WHERE fen_id=$1',ids['fens'][0]))
        await self.db.execute("""UPDATE fen SET analysis_source='tablebase_unavailable',
            analyzed_at=now() WHERE fen_id=$1""",ids['fens'][0])
        self.assertIn(FENS[0],[row['fen'] for row in await self.db.fetch(SELECT_PENDING,10)])

    async def test_bundle_and_accounting_rollback_is_atomic(self):
        await canonical.initialize(self.db)
        with self.assertRaises(asyncpg.CheckViolationError):
            async with self.db.transaction():
                await seed(self.db)
                await self.db.execute('UPDATE months SET n_games=-1')
        for table in ('player','game','months','fen','cloud_analysis_job','account'):
            self.assertEqual(await self.db.fetchval('SELECT count(*) FROM '+table),0,table)

    async def test_accounts_sessions_and_revocation_fields(self):
        await self.populated()
        from chessism_api.auth import hash_secret, verify_secret, hash_session_token
        password = hash_secret('synthetic-password-only')
        await self.db.execute("UPDATE account SET password_hash=$1 WHERE id='account-a'", password)
        stored = await self.db.fetchval("SELECT password_hash FROM account WHERE id='account-a'")
        self.assertTrue(verify_secret('synthetic-password-only', stored))
        self.assertFalse(verify_secret('incorrect', stored))
        self.assertNotEqual(hash_session_token('synthetic-token'), 'synthetic-token')
        await self.db.execute("""INSERT INTO auth_session(id,account_id,token_hash,expires_at)
            VALUES('session-a','account-a',repeat('a',64),now()+interval '1 hour')""")
        for sql in ["INSERT INTO auth_session(id,account_id,token_hash,expires_at) VALUES('bad','absent','x',now())",
                    "INSERT INTO auth_session(id,account_id,token_hash,expires_at) VALUES('dup','account-b',repeat('a',64),now())",
                    "UPDATE account SET email='a@example.invalid' WHERE id='account-b'"]:
            with self.subTest(sql=sql),self.assertRaises(asyncpg.IntegrityConstraintViolationError):
                await self.db.execute(sql)
        await self.db.execute("UPDATE auth_session SET revoked_at=now() WHERE id='session-a'")
        self.assertIsNotNone(await self.db.fetchval("SELECT revoked_at FROM auth_session WHERE id='session-a'"))
        self.assertFalse(await self.db.fetchval("SELECT is_active FROM account WHERE id='account-b'"))
        await self.db.execute("DELETE FROM account WHERE id='account-a'")
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM auth_session'),0)

    async def test_ingestion_stages_belong_to_run_and_rollback_with_accounting(self):
        ids = await self.populated()
        await self.db.execute("INSERT INTO ingestion_pipeline_run(run_id,player_name,trigger,status,started_at) VALUES('i','synthetic_white','manual','running',now())")
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute("INSERT INTO ingestion_pipeline_stage_timing(run_id,stage,status,started_at) VALUES('missing','parsing','running',now())")
        with self.assertRaises(RuntimeError):
            async with self.db.transaction():
                await self.db.execute("INSERT INTO ingestion_pipeline_stage_timing(run_id,stage,status,started_at) VALUES('i','parsing','complete',now())")
                await self.db.execute('UPDATE months SET n_games=6 WHERE player_id=$1 AND month=1',ids['white'])
                raise RuntimeError('interrupt bundle')
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM ingestion_pipeline_stage_timing'),0)
        self.assertEqual(await self.db.fetchval('SELECT n_games FROM months WHERE month=1'),2)

    async def test_cloud_claims_are_unique_and_refer_to_existing_runs_and_fens(self):
        await self.populated()
        await self.db.execute("INSERT INTO cloud_fen_claim VALUES($1,'run-a')",FENS[0])
        for fen,run in [(FENS[0],'run-b'),(FENS[1],'absent'),('missing fen','run-a')]:
            with self.subTest(fen=fen,run=run),self.assertRaises(asyncpg.IntegrityConstraintViolationError):
                await self.db.execute('INSERT INTO cloud_fen_claim VALUES($1,$2)',fen,run)
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await self.db.execute("DELETE FROM cloud_analysis_run WHERE id='run-a'")

    async def test_preserved_support_tables_have_all_existing_columns_and_nullability(self):
        await canonical.initialize(self.db)
        from chessism_api.database.postgresql.models import Base
        names = {'account','auth_session','ingestion_pipeline_run','ingestion_pipeline_stage_timing',
                 'appearance_preset','appearance_setting','appearance_override','analysis_time'}
        for table in Base.metadata.tables.values():
            if not (table.name.startswith('cloud_') or table.name in names):
                continue
            actual = await self.db.fetch('''SELECT column_name,is_nullable FROM information_schema.columns
                WHERE table_schema='public' AND table_name=$1''',table.name)
            self.assertEqual({r['column_name']:r['is_nullable']=='YES' for r in actual},
                             {c.name:c.nullable for c in table.columns},table.name)

    async def test_existing_cloud_codec_round_trips_packed_receipts_and_cancellation_plans(self):
        await canonical.initialize(self.db)
        from chessism_api.database.postgresql.cloud_codec import encode_document, decode_document
        values = {
            'receipts': {'batch/0': {'sha256':'abc','records':[{'id':'fen-1','object':'result-1','sha256':'def'}]}},
            'cleanup': {'version':1,'run_id':'run-a','bucket':'synthetic','objects':[],
                        'jobs':[{'id':'job-a','region':'test-region','state':'running','uid':'uid-a','images':[],'prefixes':[]}]},
            'selection': {'mode':'games','player_name':None,'game_links':[101,105],'cancel_requested':False},
        }
        for kind,value in values.items():
            root, rows = encode_document('document-'+kind,kind,value)
            async with self.db.transaction():
                for table, records in [('cloud_control_document',[root])]+[('cloud_control_'+k,v) for k,v in rows.items()]:
                    for row in records:
                        columns = ','.join('"'+name+'"' for name in row)
                        binds = ','.join('$'+str(i+1) for i in range(len(row)))
                        await self.db.execute(f'INSERT INTO public.{table} ({columns}) VALUES ({binds})',*row.values())
            saved_root = dict(await self.db.fetchrow('SELECT * FROM cloud_control_document WHERE id=$1',root['id']))
            saved_rows = {k:[dict(r) for r in await self.db.fetch('SELECT * FROM public.cloud_control_'+k+' WHERE document_id=$1',root['id'])] for k in rows}
            self.assertEqual(decode_document(saved_root,saved_rows),value)

    async def test_cloud_receipt_and_evaluation_commit_or_rollback_together(self):
        await self.populated()
        receipt = """INSERT INTO cloud_result_batch(run_id,task_index,batch_index,object_name,sha256,receipt_sha256,position_count,committed_at)
            VALUES('run-a',0,0,'synthetic-result',repeat('a',64),repeat('b',64),1,now())"""
        with self.assertRaises(RuntimeError):
            async with self.db.transaction():
                await self.db.execute("UPDATE fen SET score=15,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1",FENS[0])
                await self.db.execute(receipt)
                raise RuntimeError('interrupt before commit')
        self.assertIsNone(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[0]))
        self.assertEqual(await self.db.fetchval('SELECT count(*) FROM cloud_result_batch'),0)
        async with self.db.transaction():
            await self.db.execute("UPDATE fen SET score=15,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1",FENS[0])
            await self.db.execute(receipt)
        with self.assertRaises(asyncpg.UniqueViolationError):
            await self.db.execute(receipt)
        self.assertEqual(await self.db.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[0]),15)

    async def test_cancellation_intent_and_typed_receipt_document_survive_new_connection(self):
        await self.populated()
        await self.db.execute("""INSERT INTO cloud_job_cancellation(job_id,phase,cursor) VALUES('job-a','stopping',0);
            INSERT INTO cloud_control_document VALUES('synthetic-doc','receipts',false,repeat('c',64),ARRAY['cloud_control_receipts']);
            INSERT INTO cloud_control_receipts(document_id,path,present_fields,null_fields,integer_fields)
            VALUES('synthetic-doc','',ARRAY[]::text[],ARRAY[]::text[],ARRAY[]::text[]);""")
        other = await self.connect()
        self.assertEqual(await other.fetchval("SELECT phase FROM cloud_job_cancellation WHERE job_id='job-a'"),'stopping')
        self.assertEqual(await other.fetchval('SELECT count(*) FROM cloud_control_receipts'),1)
        with self.assertRaises(asyncpg.ForeignKeyViolationError):
            await other.execute("DELETE FROM cloud_analysis_job WHERE id='job-a'")
        await other.execute("DELETE FROM cloud_control_document WHERE id='synthetic-doc'")
        self.assertEqual(await other.fetchval('SELECT count(*) FROM cloud_control_receipts'),0)

    async def test_concurrent_selectors_skip_locks_and_exclude_claimed_and_scored(self):
        await self.populated()
        await self.db.execute("INSERT INTO cloud_fen_claim VALUES($1,'run-a')",FENS[0])
        await self.db.execute("UPDATE fen SET score=0,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1",FENS[4])
        other = await self.connect()
        async with self.db.transaction():
            first = await self.db.fetch(SELECT_PENDING,1)
            self.assertEqual([r['fen'] for r in first],[FENS[1]])
            async with other.transaction():
                second = await other.fetch(SELECT_PENDING,10)
                self.assertEqual([r['fen'] for r in second],[FENS[2],FENS[3]])
        self.assertEqual([r['fen'] for r in await other.fetch(SELECT_PENDING,10)],[FENS[1],FENS[2],FENS[3]])

    async def test_fresh_snapshot_recheck_observes_claim_committed_during_lock_wait(self):
        await self.populated()
        other = await self.connect()
        transaction = other.transaction()
        await transaction.start()
        await other.execute('SELECT fen FROM fen WHERE fen=$1 FOR UPDATE',FENS[0])
        async with self.db.transaction():
            selection = asyncio.create_task(self.db.fetch('SELECT fen FROM fen WHERE fen=$1 FOR UPDATE',FENS[0]))
            # Observe the actual lock wait, not an arbitrary timing assumption.
            for _ in range(100):
                waiting = await other.fetchval("SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1",self.db.get_server_pid())
                if waiting: break
                await asyncio.sleep(.01)
            else:
                self.fail('Selector did not reach row-lock wait')
            await other.execute("INSERT INTO cloud_fen_claim VALUES($1,'run-a')",FENS[0])
            await transaction.commit()
            self.assertEqual([r['fen'] for r in await selection],[FENS[0]])
            self.assertEqual(await self.db.fetch(RECHECK,[FENS[0]]),[])

    async def test_failed_transaction_and_cancellation_release_locks_preserving_commits(self):
        await self.populated()
        await self.db.execute("UPDATE fen SET score=10,analysis_source='stockfish',analyzed_at=now() WHERE fen=$1",FENS[0])
        selected = asyncio.Event()
        async def worker():
            async with self.db.transaction():
                await self.db.fetch(SELECT_PENDING,1)
                selected.set()
                await asyncio.Future()
        task = asyncio.create_task(worker())
        await selected.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        other = await self.connect()
        async with other.transaction():
            self.assertEqual([r['fen'] for r in await other.fetch(SELECT_PENDING,1)],[FENS[1]])
        self.assertEqual(await other.fetchval('SELECT score FROM fen WHERE fen=$1',FENS[0]),10)
        with self.assertRaises(asyncpg.UndefinedColumnError):
            await self.db.fetch('SELECT missing_column FROM fen')
        self.assertEqual(await other.fetchval('SELECT count(*) FROM cloud_fen_claim'),0)


if __name__ == '__main__':
    unittest.main()
