"""Read a bounded typed matrix into worker RAM, with a consistent source snapshot."""

import hashlib
import json
import time

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.queries import matrix_sql
from .builder_frames import Frame
from .config import BATCH_ROWS, source_config
from .storage import ensure_capacity


async def extract(config, reporter):
    ensure_capacity(config)
    started = time.monotonic()
    schema = config['schemas']['input']
    arrays = {key: np.empty(config['max_rows'], dtype=float if kind == 'number' else object) for key, kind in schema.items()}
    row_keys = np.empty(config['max_rows'], dtype=object)
    dictionaries = {key: {} for key, kind in schema.items() if kind != 'number'}
    digest = hashlib.sha256()
    count = 0
    async with AsyncDBSession() as session:
        await session.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        await session.execute(text('SET LOCAL statement_timeout = 60000'))
        await session.execute(text('SET LOCAL lock_timeout = 2000'))
        snapshot = (await session.execute(text('SELECT transaction_timestamp()::text AS taken_at, pg_current_snapshot()::text AS snapshot'))).mappings().one()
        _, query, params = matrix_sql(source_config(config))
        await reporter.progress('extracting', detail='Reading the selected typed columns into bounded temporary memory.')
        stream = await session.stream(text(query), params, execution_options={'yield_per': BATCH_ROWS})
        try:
            async for records in stream.mappings().partitions(BATCH_ROWS):
                await reporter.check()
                for record in records:
                    row_key = str(record['row_key'])
                    row_keys[count] = row_key
                    encoded = [row_key]
                    for key, kind in schema.items():
                        value = record[key]
                        if kind == 'number':
                            if isinstance(value, int) and abs(value) > 2**53:
                                raise ValueError(f'{key}: integer exceeds exact float64 precision.')
                            value = float(value) if value is not None else np.nan
                            value = value if np.isfinite(value) else np.nan
                        elif value is not None:
                            value = str(value)
                            if len(value) > 256:
                                raise ValueError(f'{key}: text values longer than 256 characters are not supported.')
                            dictionary = dictionaries[key]
                            if value not in dictionary:
                                if len(dictionary) >= 10000:
                                    raise ValueError(f'{key}: more than 10,000 categories; narrow the matrix scope.')
                                dictionary[value] = value
                            value = dictionary[value]
                        arrays[key][count] = value
                        encoded.append(None if kind == 'number' and not np.isfinite(value) else value)
                    digest.update((json.dumps(encoded, ensure_ascii=False) + '\n').encode())
                    count += 1
                await reporter.progress('extracting', count, detail=f'Read {count:,} rows (cap {config["max_rows"]:,}).')
        finally:
            await stream.close()
    if not count:
        raise ValueError('The matrix currently matches no source rows.')
    frame = Frame({key: value[:count] for key, value in arrays.items()}, schema, row_keys[:count])
    source = {'taken_at': snapshot['taken_at'], 'snapshot': snapshot['snapshot'], 'input_sha256': digest.hexdigest(),
              'row_limit_reached': count == config['max_rows'], 'selection': 'ordered prefix, not random population sampling',
              'inputs_retained': False}
    return frame, source, time.monotonic() - started
