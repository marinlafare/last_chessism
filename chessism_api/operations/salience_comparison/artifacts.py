"""Read-only snapshots and separate, explicitly written reviewer annotations."""
import json
import os
from pathlib import Path
import sqlite3
from uuid import UUID
from datetime import datetime, timezone


def root():
    return Path(os.getenv('SALIENCE_COMPARISON_DIR','/research-data/salience-comparisons'))


def path_for(identifier):
    if str(UUID(identifier)) != identifier:
        raise ValueError('Invalid comparison identifier')
    path = root()/f'{identifier}.sqlite'
    if not path.is_file() or path.is_symlink():
        raise FileNotFoundError('Comparison snapshot not found')
    return path


def connect(identifier):
    db = sqlite3.connect(path_for(identifier).as_uri()+'?mode=ro', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    return db


def overview(identifier):
    db = connect(identifier)
    try:
        return {r['key']:json.loads(r['value']) for r in db.execute('SELECT * FROM metadata')}
    finally:
        db.close()


def listing():
    if not root().exists():
        return {'comparisons':[]}
    reports = []
    for file in root().glob('*.sqlite'):
        try:
            report = overview(file.stem)
        except (ValueError,FileNotFoundError):
            continue
        reports.append(report)
    return {'comparisons':sorted(reports,key=lambda r:r['created_at'],reverse=True)}


def annotations(identifier):
    path_for(identifier)
    file = root()/f'{identifier}.reviews.sqlite'
    if not file.exists():
        return {}
    db = sqlite3.connect(file.as_uri()+'?mode=ro',uri=True,timeout=5)
    db.row_factory = sqlite3.Row
    try:
        return {r['game_link']:dict(r) for r in db.execute('SELECT * FROM reviews')}
    finally:
        db.close()


def save_review(identifier, game_id, judgement, note, reviewer):
    if judgement not in ('repetitive','distinctive','uncertain') or len(note)>2000:
        raise ValueError('Invalid review')
    db = connect(identifier)
    try:
        if not db.execute('SELECT 1 FROM games WHERE game_link=?',(game_id,)).fetchone():
            raise FileNotFoundError('Game not in this snapshot')
    finally:
        db.close()
    file = root()/f'{identifier}.reviews.sqlite'
    with sqlite3.connect(file, timeout=5) as db:
        db.execute('''CREATE TABLE IF NOT EXISTS reviews(game_link INTEGER PRIMARY KEY,
            judgement TEXT NOT NULL,note TEXT NOT NULL,reviewer TEXT NOT NULL,updated_at TEXT NOT NULL)''')
        db.execute('''INSERT INTO reviews VALUES(?,?,?,?,?) ON CONFLICT(game_link) DO UPDATE SET
            judgement=excluded.judgement,note=excluded.note,reviewer=excluded.reviewer,updated_at=excluded.updated_at''',
            (game_id,judgement,note,reviewer,datetime.now(timezone.utc).isoformat()))
    return {'saved':True}
