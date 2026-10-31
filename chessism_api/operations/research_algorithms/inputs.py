"""Read one consistent source snapshot into temporary, disk-backed arrays."""

import gzip
import hashlib
import json
import shutil
import time

import numpy as np
from sqlalchemy import text

from chessism_api.database.engine import AsyncDBSession
from ..matrix_constructor.queries import matrix_sql
from .config import BATCH_ROWS, source_config
from .numerics import ColumnStats
from . import storage


async def materialize(config, folder, reporter):
    storage.ensure_capacity(config)
    columns = config["columns"]
    stats = ColumnStats(len(columns))
    digest = hashlib.sha256()
    count = 0
    started = time.monotonic()
    values = np.lib.format.open_memmap(folder / "values.npy", mode="w+", dtype="float64", shape=(config["max_rows"], len(columns)))
    try:
        async with AsyncDBSession() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            await session.execute(text("SET LOCAL statement_timeout = 60000"))
            await session.execute(text("SET LOCAL lock_timeout = 2000"))
            snapshot = (await session.execute(text("SELECT transaction_timestamp()::text AS taken_at, pg_current_snapshot()::text AS snapshot"))).mappings().one()
            _, query, params = matrix_sql(source_config(config))
            await reporter.progress("extracting", detail="Reading a consistent database snapshot; total rows are not counted in advance.")
            stream = await session.stream(text(query), params, execution_options={"yield_per": BATCH_ROWS})
            try:
                with gzip.open(folder / "row_keys.jsonl.gz", "wt", encoding="utf-8", compresslevel=1) as keys:
                    async for records in stream.mappings().partitions(BATCH_ROWS):
                        await reporter.check()
                        if shutil.disk_usage(folder).free < storage.FREE_FLOOR + 16 * 1024 * 1024:
                            raise ValueError("Temporary matrix construction reached the protected disk reserve.")
                        block = []
                        for record in records:
                            row = []
                            for key in columns:
                                value = record[key]
                                if isinstance(value, int) and abs(value) > 2 ** 53:
                                    raise ValueError(f"{key} contains integers too large for exact float64 representation.")
                                row.append(float(value) if value is not None else np.nan)
                            block.append(row)
                            encoded = json.dumps(str(record["row_key"]), ensure_ascii=False) + "\n"
                            keys.write(encoded)
                            digest.update(encoded.encode("utf-8"))
                        block = np.asarray(block, dtype=np.float64)
                        block[~np.isfinite(block)] = np.nan
                        values[count:count + len(block)] = block
                        stats.add(block)
                        digest.update(block.tobytes())
                        count += len(block)
                        await reporter.progress("extracting", count, detail=f"Read {count:,} rows; configured limit {config['max_rows']:,}.")
            finally:
                await stream.close()
        values.flush()
    finally:
        values._mmap.close()
    if not count:
        raise ValueError("The matrix instructions currently match no source rows.")
    return {"rows": count, "means": stats.mean, "non_missing": stats.count,
            "source": {"taken_at": snapshot["taken_at"], "snapshot": snapshot["snapshot"],
                       "input_sha256": digest.hexdigest(), "row_limit_reached": count == config["max_rows"],
                       "selection": "ordered prefix, not a random sample", "inputs_retained": False},
            "seconds": time.monotonic() - started}
