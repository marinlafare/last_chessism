"""Validate and expand the small, bundled Lichess catalogue before DB writes."""
import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path

import chess
import chess.pgn

DATA = Path(__file__).with_name('data')
VERSION = 'lichess_opening_moves_v1'
FILES = tuple(f'{letter}.tsv' for letter in 'abcde')


def position_key(board):
    # Clocks are irrelevant; turn, castling and *legal* en passant are not.
    return ' '.join(board.fen(en_passant='legal').split()[:4])


@dataclass(frozen=True)
class Catalog:
    revision: str
    lines: int
    records: list[tuple]


def build_catalog(lines, revision):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('Opening catalogue needs an immutable source revision')
    transitions, names = {}, {}
    count = 0
    for row in lines:
        if not re.fullmatch('[A-E][0-9]{2}', row['eco']) or not row['name'].strip():
            raise ValueError('Invalid opening identity')
        game = chess.pgn.read_game(io.StringIO(row['pgn']))
        if game is None or game.errors or game.headers.get('SetUp') or game.headers.get('Variant'):
            raise ValueError(f"Invalid standard-chess opening: {row['name']}")
        board = game.board()
        moves = list(game.mainline_moves())
        if not 1 <= len(moves) <= 256:
            raise ValueError('Unexpected opening length')
        for move in moves:
            before = position_key(board)
            board.push(move)
            key, after = (before, move.uci()), position_key(board)
            if transitions.get(key, after) != after:
                raise ValueError('Conflicting opening transition')
            transitions[key] = after
        names.setdefault(position_key(board), set()).add((row['eco'], row['name']))
        count += 1
    if not count:
        raise ValueError('Empty opening catalogue')
    records = []
    for (before, uci), after in sorted(transitions.items()):
        # Label only a recognized endpoint. An early prefix shared by hundreds
        # of lines must not be arbitrarily named after a later variation.
        labels = [{'eco': eco, 'name': name} for eco, name in sorted(names.get(after, set()))]
        records.append((before, uci, after, json.dumps(labels, ensure_ascii=False), revision, VERSION))
    return Catalog(revision, count, records)


def load_catalog():
    manifest = json.loads((DATA / 'manifest.json').read_text())
    if manifest['source'] != 'https://github.com/lichess-org/chess-openings' or manifest['license'] != 'CC0-1.0':
        raise ValueError('Unexpected opening catalogue source')
    if set(manifest['files']) != {*FILES, 'COPYING.txt'}:
        raise ValueError('Unexpected opening catalogue files')
    lines = []
    for name, expected in manifest['files'].items():
        data = (DATA / name).read_bytes()
        if len(data) != expected['bytes'] or hashlib.sha256(data).hexdigest() != expected['sha256']:
            raise ValueError(f'Opening catalogue checksum mismatch: {name}')
        if name in FILES:
            reader = csv.DictReader(io.StringIO(data.decode('utf-8')), delimiter='\t')
            if reader.fieldnames != ['eco', 'name', 'pgn']:
                raise ValueError('Unexpected opening catalogue columns')
            lines.extend(reader)
    return build_catalog(lines, manifest['revision'])
