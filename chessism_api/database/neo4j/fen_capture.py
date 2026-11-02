"""FEN outbox SQL, also usable for a function-only upgrade during ingestion."""


def function_sql(layout, *, cloud_tracking):
    # Transition tables have no indexes. Joining OLD/NEW after empty ingestion
    # updates can cache a nested-loop plan and turn a counter update quadratic.
    # EXCEPT compares only evaluation fields, with null-safe set semantics.
    publish = "PERFORM graph_sync.enqueue('score', affected);"
    if cloud_tracking:
        # Claims still belong to this transaction; only changed evaluations need
        # the indexed FEN lookup and cloud attribution (never counter-only rows).
        publish = f"""
          INSERT INTO graph_sync.event(kind,ids,source_job_id)
          SELECT 'score',array_agg(fen_id ORDER BY fen_id),job_id FROM (
            SELECT n.fen_id,r.job_id,
              (row_number() OVER(PARTITION BY r.job_id ORDER BY n.fen_id)-1)/1000 AS chunk
            FROM unnest(affected) changed(fen_id)
            JOIN {layout.table('fen')} n USING(fen_id)
            LEFT JOIN {layout.table('cloud_fen_claim')} c ON c.fen=n.fen
            LEFT JOIN {layout.table('cloud_analysis_run')} r ON r.id=c.run_id
          ) grouped GROUP BY job_id,chunk ORDER BY job_id NULLS LAST,chunk;
        """
    return f"""CREATE OR REPLACE FUNCTION graph_sync.capture_fen_update() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE affected bigint[]; BEGIN
          IF NOT EXISTS(SELECT 1 FROM graph_sync.control WHERE id AND enabled) THEN RETURN NULL; END IF;
          SELECT array_agg(fen_id) INTO affected FROM (
            SELECT fen_id,score,analysis_source,tablebase_wdl,tablebase_dtz FROM new_rows
            EXCEPT
            SELECT fen_id,score,analysis_source,tablebase_wdl,tablebase_dtz FROM old_rows
          ) changed;
          IF affected IS NULL THEN RETURN NULL; END IF;
          {publish}
          RETURN NULL;
        END $$;"""


async def upgrade(session, layout):
    """Replace only the installed function; caller owns the transaction.

    No trigger/table DDL, worker restart, queue reset, or graph reinstallation.
    Existing statements may finish using their old function execution plan.
    """
    from sqlalchemy import text
    from chessism_api.database.postgresql.relational_storage import driver_connection

    if layout is None or not layout.compact:
        raise ValueError('Requires the verified relational_v3 storage profile')
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL statement_timeout='10s'"))
    await session.execute(text('SELECT pg_advisory_xact_lock(731946299)'))
    function = await session.scalar(text('''SELECT p.oid FROM pg_trigger t
        JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE t.tgrelid=to_regclass(:table) AND t.tgname='chessism_capture_fen_update'
          AND n.nspname='graph_sync' AND p.proname='capture_fen_update'
          AND t.tgoldtable='old_rows' AND t.tgnewtable='new_rows' '''),
        {'table': layout.table('fen')})
    if function is None:
        raise RuntimeError('Expected installed FEN capture trigger was not found')
    cloud_tracking = bool(await session.scalar(text('SELECT to_regclass(:table) IS NOT NULL'),
                                               {'table': layout.table('cloud_fen_claim')}))
    conn = await driver_connection(session)
    await conn.execute(function_sql(layout, cloud_tracking=cloud_tracking))
    return {'function_oid': function, 'cloud_tracking': cloud_tracking, 'implementation': 'evaluation_set_difference_v1'}


async def main():
    import json
    import os
    from chessism_api.database.postgresql.runtime import bind_runtime_engine, configured_storage
    from chessism_api.database.postgresql.engine import AsyncDBSession

    engine = await bind_runtime_engine(os.environ['DATABASE_URL'])
    try:
        async with AsyncDBSession() as session, session.begin():
            result = await upgrade(session, configured_storage())
        print(json.dumps({'committed': True, **result}))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
