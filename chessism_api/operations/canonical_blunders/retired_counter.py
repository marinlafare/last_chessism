"""Metadata-only repair for the unused physical CPU blunder counter.

Canonical readers must already use CUDA assessments. Never fill unknown counts
with zero, rewrite historical rows, enqueue calculations or change the model.
"""
from sqlalchemy import text
from chessism_api.database.relational_storage import identifier
from .schema import table


async def _uses_column(connection, view, source, column):
    return bool(await connection.scalar(text('''SELECT EXISTS(
        SELECT 1 FROM pg_rewrite r JOIN pg_depend d
          ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid
          AND d.refclassid='pg_class'::regclass
        JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
        WHERE r.ev_class=to_regclass(:view) AND r.rulename='_RETURN'
          AND d.refobjid=to_regclass(:source) AND a.attname=:column
          AND NOT a.attisdropped)'''), {'view':view,'source':source,'column':column}))


async def repair(connection, layout):
    if not layout:
        raise ValueError('Retired-counter repair requires normalized CUDA storage')
    target = layout.table('game_player_engine_summary')
    engine = table(layout, 'engine')
    assessment = table(layout, 'assessment')
    reads = f'{identifier(layout.reads)}.{identifier("game_player_engine_summary")}'
    await connection.execute(text("SET LOCAL lock_timeout='2s'"))
    await connection.execute(text("SET LOCAL statement_timeout='15s'"))
    # The brief lock also freezes the dependent view definitions while their
    # column dependencies are checked. No table scan/rewrite is required.
    await connection.execute(text(f'LOCK TABLE ONLY {target} IN ACCESS EXCLUSIVE MODE'))
    column = (await connection.execute(text('''SELECT a.attnotnull,
        pg_get_expr(d.adbin,d.adrelid) AS default_value
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=to_regclass(:target) AND a.attname='blunder_count'
          AND a.atttypid='int4'::regtype AND NOT a.attisdropped AND c.relkind='r'
        '''), {'target':target})).mappings().one_or_none()
    if column is None:
        raise ValueError('Unexpected physical blunder counter contract')
    if (await _uses_column(connection, engine, target, 'blunder_count')
            or await _uses_column(connection, reads, target, 'blunder_count')
            or not await _uses_column(connection, reads, engine, 'blunder_count')
            or not await _uses_column(connection, engine, assessment, 'white_count')
            or not await _uses_column(connection, engine, assessment, 'black_count')):
        raise ValueError('CUDA-only blunder read contract must be installed first')
    changed = column['attnotnull'] or column['default_value'] is not None
    if changed:
        await connection.execute(text(f'''ALTER TABLE ONLY {target}
            ALTER COLUMN blunder_count DROP NOT NULL,
            ALTER COLUMN blunder_count DROP DEFAULT'''))
    return {'changed':bool(changed),'legacy_counter_nullable':True,
            'legacy_counter_default':None,'blunder_backend':'cuda_only',
            'results_recalculated':0,'historical_rows_deleted':0}
