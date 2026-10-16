"""Lossless wire-object adapter over explicitly declared PostgreSQL columns."""
from collections import defaultdict
import hashlib
import json
import math
from urllib.parse import quote

from sqlalchemy import delete, insert, select, event, inspect
from sqlalchemy.orm import Session, object_session
from sqlalchemy.orm.attributes import flag_dirty

from . import cloud_columns as columns


def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def wrap(kind, value):
    if kind in ('positions', 'receipts', 'blocker'):
        return {'items': value}
    if kind in ('uid', 'count', 'severity'):
        return {'value': value}
    return value


def unwrap(kind, value):
    if kind in ('positions', 'receipts', 'blocker'):
        return value['items']
    if kind in ('uid', 'count', 'severity'):
        return value['value']
    return value


def scalar(value, kind, path):
    if value is None:
        return
    if kind is float:
        valid = type(value) in (int, float) and math.isfinite(value)
    else:
        valid = type(value) is kind
    if not valid:
        raise ValueError(f'Cloud column {path} requires {kind.__name__}, got {type(value).__name__}')


def encode_document(ident, kind, value):
    rows = defaultdict(list)

    def encode_node(kind, value, path, parent=None, collection=None, key=None, ordinal=0, packed=False):
        row = dict(document_id=ident, path=path, parent_path=parent, collection=collection,
                   item_key=key, ordinal=ordinal, present_fields=[], null_fields=[], integer_fields=[])
        spec = columns.SCHEMAS[kind]
        if packed:
            if len(value) > 500 or any(set(v) != set(spec) for v in value):
                raise ValueError('Invalid packed cloud identity batch')
            for field, field_kind in spec.items():
                for v in value:
                    scalar(v[field], field_kind, field)
                row[columns.column_name(field)] = [v[field] for v in value]
                row['present_fields'].append(field)
        else:
            def flatten(data, fields, prefix=''):
                if not isinstance(data, dict) or set(data) - set(fields):
                    unknown = set(data) - set(fields) if isinstance(data, dict) else type(data).__name__
                    raise ValueError(f'Unsupported cloud {kind} fields at {prefix}: {unknown}')
                for field, item in data.items():
                    name, ftype = prefix + field, fields[field]
                    row['present_fields'].append(name)
                    if item is None:
                        row['null_fields'].append(name)
                        continue
                    if isinstance(ftype, dict):
                        flatten(item, ftype, name + '.')
                    elif isinstance(ftype, columns.Children):
                        if not isinstance(item, dict if ftype.mapping else list):
                            raise ValueError(f'Invalid cloud collection {name}')
                        children = list(item.items()) if ftype.mapping else list(enumerate(item))
                        if ftype.packed:
                            children = [(n, item[n:n + 500]) for n in range(0, len(item), 500)]
                        for index, (child_key, child) in enumerate(children):
                            child_path = path + '/' + name + '/' + quote(str(child_key), safe='')
                            encode_node(ftype.schema, child if ftype.packed else wrap(ftype.schema, child),
                                        child_path, path, name, str(child_key), index, ftype.packed)
                    elif isinstance(ftype, list):
                        if not isinstance(item, list):
                            raise ValueError(f'Invalid cloud array {name}')
                        for v in item:
                            scalar(v, ftype[0], name)
                        row[columns.column_name(name)] = item
                    else:
                        scalar(item, ftype, name)
                        if ftype is float and type(item) is int:
                            row['integer_fields'].append(name)
                        row[columns.column_name(name)] = item
            flatten(value, spec)
        rows[kind].append(row)

    if value is not None:
        encode_node(kind, wrap(kind, value), '$')
    return {'id': ident, 'schema_name': kind, 'is_null': value is None, 'sha256': checksum(value),
            'table_names': sorted(rows)}, rows


def decode_document(root, rows, *, verify=True):
    if root['is_null']:
        result = None
    else:
        by_path = {(kind, r['path']): r for kind, group in rows.items() for r in group}
        children = defaultdict(list)
        for kind, group in rows.items():
            for r in group:
                children[kind, r['parent_path'], r['collection']].append(r)

        def decode_node(kind, row, packed=False):
            if packed:
                keys = list(columns.SCHEMAS[kind])
                values = [row[columns.column_name(k)] for k in keys]
                if len({len(v) for v in values}) != 1:
                    raise ValueError('Corrupt packed cloud identity columns')
                return [dict(zip(keys, items)) for items in zip(*values)]
            present, nulls = set(row['present_fields']), set(row['null_fields'])
            integers = set(row['integer_fields'])
            def expand(spec, prefix=''):
                data = {}
                for field, ftype in spec.items():
                    name = prefix + field
                    if name not in present:
                        continue
                    if name in nulls:
                        value = None
                    elif isinstance(ftype, dict):
                        value = expand(ftype, name + '.')
                    elif isinstance(ftype, columns.Children):
                        group = sorted(children[ftype.schema, row['path'], name], key=lambda r: r['ordinal'])
                        if ftype.mapping:
                            value = {r['item_key']: unwrap(ftype.schema, decode_node(ftype.schema, r)) for r in group}
                        elif ftype.packed:
                            value = [v for r in group for v in decode_node(ftype.schema, r, packed=True)]
                        else:
                            value = [unwrap(ftype.schema, decode_node(ftype.schema, r)) for r in group]
                    else:
                        value = row[columns.column_name(name)]
                        if name in integers and value is not None:
                            value = int(value)
                    data[field] = value
                return data
            return expand(columns.SCHEMAS[kind])
        result = unwrap(root['schema_name'], decode_node(root['schema_name'], by_path[root['schema_name'], '$']))
    if verify and checksum(result) != root['sha256']:
        raise ValueError('Cloud column snapshot checksum mismatch; recovery/cleanup withheld')
    return result


def reachable(kind):
    found = {kind}
    def visit(spec):
        for ftype in spec.values():
            if isinstance(ftype, dict):
                visit(ftype)
            elif isinstance(ftype, columns.Children) and ftype.schema not in found:
                found.add(ftype.schema)
                visit(columns.SCHEMAS[ftype.schema])
    visit(columns.SCHEMAS[kind])
    return found


def write_document(connection, ident, kind, value):
    root, groups = encode_document(ident, kind, value)
    # Validate a complete round trip BEFORE touching persistent control state.
    decode_document(root, groups)
    connection.execute(delete(columns.ROOTS).where(columns.ROOTS.c.id == ident))
    connection.execute(insert(columns.ROOTS), root)
    for name, rows in groups.items():
        table = columns.TABLES[name]
        keys = {key for row in rows for key in row}
        for offset in range(0, len(rows), 500):
            connection.execute(insert(table), [{k: r.get(k) for k in keys} for r in rows[offset:offset + 500]])


def read_document(connection, ident):
    return read_documents(connection, [ident])[ident]


def read_documents(connection, identifiers):
    roots = connection.execute(select(columns.ROOTS).where(columns.ROOTS.c.id.in_(identifiers))).mappings().all()
    if len(roots) != len(set(identifiers)):
        raise ValueError('Missing durable cloud control record')
    kinds = {kind for root in roots for kind in root['table_names']}
    grouped = defaultdict(lambda: defaultdict(list))
    for kind in kinds:
        table = columns.TABLES[kind]
        for row in connection.execute(select(table).where(table.c.document_id.in_(identifiers))).mappings():
            grouped[row['document_id']][kind].append(row)
    return {root['id']: decode_document(root, grouped[root['id']]) for root in roots}


def read_projection(connection, ident, names):
    """Reporting only: do not fetch FEN identities or unrelated control payloads.

    A projection is NOT a verified recovery/cleanup document.
    """
    root = connection.execute(select(columns.ROOTS).where(columns.ROOTS.c.id == ident)).mappings().one()
    kind = root['schema_name']
    table = columns.TABLES[kind]
    row = dict(connection.execute(select(table).where(table.c.document_id == ident, table.c.path == '$')).mappings().one())
    row['present_fields'] = [p for p in row['present_fields'] if p.split('.')[0] in names]
    needed = set()
    def visit(spec):
        for typ in spec.values():
            if isinstance(typ, dict): visit(typ)
            elif isinstance(typ, columns.Children):
                needed.add(typ.schema)
                visit(columns.SCHEMAS[typ.schema])
    visit({k: v for k, v in columns.SCHEMAS[kind].items() if k in names})
    rows = {kind: [row]}
    for child in needed:
        t = columns.TABLES[child]
        rows[child] = connection.execute(select(t).where(t.c.document_id == ident)).mappings().all()
    return decode_document(root, rows, verify=False)


class Document:
    """Python compatibility attribute backed exclusively by typed SQL columns."""
    def __init__(self, name, kind, default=None):
        self.name, self.kind, self.default = name, kind, default

    def __get__(self, obj, owner=None):
        if obj is None:
            return self
        cache = obj.__dict__.setdefault('_cloud_documents', {})
        if self.name not in cache:
            ref = getattr(obj, '_' + self.name + '_ref', None)
            if ref:
                session = object_session(obj)
                if session is None:
                    raise RuntimeError('Cloud control document must be loaded before detaching')
                cache[self.name] = read_document(session.connection(), ref)
            else:
                cache[self.name] = self.default() if callable(self.default) else self.default
        return cache[self.name]

    def __set__(self, obj, value):
        obj.__dict__.setdefault('_cloud_documents', {})[self.name] = value
        obj.__dict__.setdefault('_cloud_document_dirty', set()).add(self.name)
        if inspect(obj).persistent:
            flag_dirty(obj)


class Receipts(Document):
    def __get__(self, obj, owner=None):
        value = super().__get__(obj, owner)
        if obj is None:
            return self
        if obj.__dict__.get('_batch_receipts_loaded') or not inspect(obj).persistent:
            return value
        from .models import CloudResultBatch
        connection = object_session(obj).connection()
        pairs = connection.execute(select(CloudResultBatch.object_name, CloudResultBatch.detail_record_id).where(
            CloudResultBatch.run_id == obj.id, CloudResultBatch.detail_record_id.is_not(None))).all()
        docs = read_documents(connection, [ref for _, ref in pairs]) if pairs else {}
        value.update({name: docs[ref] for name, ref in pairs})
        obj.__dict__['_batch_receipts_loaded'] = True
        return value


def install(models):
    for model in models:
        @event.listens_for(model, 'load')
        def load(obj, context):
            # ORM load events run inside SQLAlchemy's async greenlet. No lazy
            # database I/O is attempted later in detached controller code.
            refs = {name: getattr(obj, '_' + name + '_ref') for name in obj.CLOUD_DOCUMENTS}
            found = {name: ref for name, ref in refs.items() if ref}
            decoded = read_documents(context.session.connection(), list(found.values())) if found else {}
            obj.__dict__['_cloud_documents'] = {name: decoded[ref] for name, ref in found.items()}
            if 'receipts' in refs:
                getattr(obj, 'receipts')

    @event.listens_for(Session, 'before_flush')
    def flush(session, context, instances):
        for obj in list(session.new) + list(session.dirty):
            if not isinstance(obj, models):
                continue
            dirty = obj.__dict__.get('_cloud_document_dirty', set())
            if obj in session.new:
                dirty = set(obj.CLOUD_DOCUMENTS)
            if 'positions' in dirty and not getattr(obj, 'details_pruned', False):
                obj.position_count = len(obj.positions)
            if 'receipts' in dirty and obj in session.new and not getattr(obj, 'details_pruned', False):
                obj.imported_count = sum(len(r['records']) for r in obj.receipts.values())
            for name in dirty:
                descriptor = getattr(type(obj), name)
                ident = f'{obj.id}:{name}'
                write_document(session.connection(), ident, descriptor.kind, getattr(obj, name))
                setattr(obj, '_' + name + '_ref', ident)
            obj.__dict__['_cloud_document_dirty'] = set()
