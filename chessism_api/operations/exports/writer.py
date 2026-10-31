"""Bounded Arrow batches with an explicit schema, not NumPy matrix encoding."""
from datetime import date, datetime, timezone
import os

from . import config, files
from .catalog import SOURCES


def arrow_schema(spec):
    import pyarrow as pa
    types = {'int16': pa.int16(), 'int32': pa.int32(), 'int64': pa.int64(),
             'float64': pa.float64(), 'bool': pa.bool_(), 'string': pa.string(),
             'timestamp[us, UTC]': pa.timestamp('us', tz='UTC')}
    return pa.schema([pa.field(key, types[SOURCES[spec.source].columns[key].dtype], nullable=True)
                      for key in spec.select])


class ParquetOutput:
    def __init__(self, path, spec=None, *, schema=None):
        import pyarrow.parquet as pq
        self.path = path
        self.schema = arrow_schema(spec) if schema is None else schema
        self.writer = pq.ParquetWriter(path, self.schema, compression='zstd', use_dictionary=True,
                                       version='2.6', store_schema=True)
        os.chmod(path, 0o600)

    def write(self, rows):
        import pyarrow as pa
        # Schema is fixed before reading even an empty/all-null first batch.
        table = pa.Table.from_pylist([dict(row) for row in rows], schema=self.schema)
        self._write_table(table)

    def write_batch(self, batch):
        import pyarrow as pa
        self._write_table(pa.Table.from_batches([batch]).cast(self.schema, safe=True))

    def _write_table(self, table):
        self.writer.write_table(table, row_group_size=config.BATCH_ROWS)
        if self.path.stat().st_size > config.MAX_BYTES:
            raise ValueError('Export exceeds the configured file-size limit.')
        files.ensure_space()

    def close(self):
        if self.writer is not None:
            self.writer.close(); self.writer = None


def preview_value(value):
    # JSON previews must not round 64-bit identifiers in JavaScript clients.
    if isinstance(value, int) and abs(value) > 2**53-1:
        return str(value)
    if isinstance(value, datetime):
        return (value.astimezone(timezone.utc) if value.tzinfo is not None else value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not __import__('math').isfinite(value):
        return str(value)
    return value
