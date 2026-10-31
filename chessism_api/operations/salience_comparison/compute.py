"""CPU reference comparison over one saved input snapshot, not a live writer."""
import json
import math
import sqlite3

VERSION = 'salience-comparison-v1'
VARIANTS = ('current', 'equal', 'opening')
LABELS = ('Current formula', 'Whole game · equal weights', 'First 15 moves · equal weights')


def initialize(db):
    db.executescript('''
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE games (
            game_link INTEGER PRIMARY KEY, is_white INTEGER NOT NULL,
            played_at TEXT, mode TEXT, opponent TEXT, result REAL,
            metadata TEXT NOT NULL, moves TEXT, official REAL,
            positions INTEGER, current REAL, equal REAL, opening REAL,
            delta_equal REAL, delta_opening REAL, difference REAL);
        CREATE TABLE positions (
            game_link INTEGER NOT NULL, ply INTEGER NOT NULL, fen_id INTEGER NOT NULL,
            occurrence INTEGER NOT NULL, PRIMARY KEY(game_link,ply)) WITHOUT ROWID;
    ''')


def metadata(db, key, value):
    db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)', (key, json.dumps(value, allow_nan=False)))


def compute(db):
    # The scope is the player's entire extracted history, separately per color.
    # Window frequencies must count ONLY appearances within the same window.
    db.executescript('''
        CREATE TABLE frequency AS
            SELECT g.is_white,p.fen_id,count(DISTINCT p.game_link) AS games,
                count(DISTINCT CASE WHEN p.ply<=30 THEN p.game_link END) AS opening_games
            FROM positions p JOIN games g USING(game_link) GROUP BY g.is_white,p.fen_id;
        CREATE UNIQUE INDEX frequency_key ON frequency(is_white,fen_id);
        CREATE TEMP TABLE scores AS
            SELECT p.game_link,count(*) AS positions,
                sum((0.25+0.75*min(p.ply/16.0,1.0))/f.games/p.occurrence)
                    /sum(0.25+0.75*min(p.ply/16.0,1.0)) AS current,
                avg(1.0/f.games/p.occurrence) AS equal,
                avg(CASE WHEN p.ply<=30 THEN 1.0/f.opening_games/p.occurrence END) AS opening
            FROM positions p JOIN games g USING(game_link)
            JOIN frequency f ON f.is_white=g.is_white AND f.fen_id=p.fen_id GROUP BY p.game_link;
        CREATE UNIQUE INDEX scores_key ON scores(game_link);
        UPDATE games SET (positions,current,equal,opening)=
            (SELECT positions,current,equal,opening FROM scores WHERE scores.game_link=games.game_link);
        UPDATE games SET delta_equal=equal-current,delta_opening=opening-current,
            difference=max(abs(equal-current),abs(opening-current));
        CREATE INDEX games_date ON games(played_at DESC,game_link DESC);
        CREATE INDEX games_difference ON games(difference DESC,game_link DESC);
        CREATE INDEX games_current ON games(current,game_link);
        CREATE INDEX games_equal ON games(equal,game_link);
        CREATE INDEX games_opening ON games(opening,game_link);
    ''')
    if db.execute('SELECT count(*) FROM games WHERE current IS NULL OR opening IS NULL').fetchone()[0]:
        raise ValueError('Snapshot contains games without extracted positions')
    overview = []
    for name, label in zip(VARIANTS, LABELS):
        values = [r[0] for r in db.execute(f'SELECT {name} FROM games ORDER BY {name}')]
        if not values or any(not math.isfinite(v) or not 0 < v <= 1+1e-12 for v in values):
            raise ValueError('Invalid salience result')
        histogram = [0]*10
        for value in values:
            histogram[min(9, int(value*10))] += 1
        middle = len(values)//2
        overview.append(dict(id=name,label=label,games=len(values),minimum=values[0],
            maximum=values[-1],mean=math.fsum(values)/len(values),
            median=(values[middle]+values[~middle])/2,
            histogram=histogram,at_least_075=sum(v>=.75 for v in values)))
    metadata(db,'variants',overview)
    metadata(db,'short_games',db.execute('SELECT count(*) FROM games WHERE positions<30').fetchone()[0])
    parity = db.execute('SELECT count(official),max(abs(current-official)) FROM games').fetchone()
    metadata(db,'baseline_check',dict(compared_games=parity[0],max_absolute_difference=parity[1]))
    if parity[0] and parity[1] > 1e-9:
        raise ValueError(f'Current formula disagrees with saved baseline: {parity[1]}')
    db.commit()


def occurrence_details(db, game_id):
    rows = db.execute('''SELECT p.ply,p.occurrence,f.games,f.opening_games,
        g.current,g.equal,g.opening,g.positions FROM positions p JOIN games g USING(game_link)
        JOIN frequency f ON f.is_white=g.is_white AND f.fen_id=p.fen_id
        WHERE p.game_link=? ORDER BY p.ply''', (game_id,)).fetchall()
    denominator = math.fsum(.25+.75*min(r[0]/16,1) for r in rows)
    window_count = sum(r[0]<=30 for r in rows)
    return [dict(ply=ply,occurrence=k,games_with_position=frequency,
        opening_games_with_position=window_frequency,in_window=ply<=30,
        depth_weight=.25+.75*min(ply/16,1),
        current_contribution=(.25+.75*min(ply/16,1))/frequency/k/denominator,
        equal_contribution=1/frequency/k/count,
        opening_contribution=1/window_frequency/k/window_count if ply<=30 else 0)
        for ply,k,frequency,window_frequency,_,_,_,count in rows]
