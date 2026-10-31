"""One combined GPU stream: small DB blocks accumulate across player boundaries."""
import numpy as np

from . import blunder_cohort_compute as compute, blunder_fixed

TARGET_ROWS = 262_144
MAX_GAMES = 8_192


class SharedBatch:
    def __init__(self, xp, fits, check=lambda: None, *, target_rows=TARGET_ROWS, max_games=MAX_GAMES):
        if not 1 <= target_rows <= TARGET_ROWS or not 1 <= max_games <= MAX_GAMES:
            raise ValueError('Invalid shared GPU buffer bound.')
        self.xp, self.fits, self.check = xp, fits, check
        self.target_rows, self.max_games = target_rows, max_games
        self.parts, self.games, self.rows = [], [], 0

    def append(self, rows, games):
        if self.ready or rows.dtype != blunder_fixed.DTYPE or len(games) > 128 or len(rows) > 32768:
            raise ValueError('Flush a full buffer; incoming DB blocks must remain bounded.')
        if len(rows) and (rows['game'].min() < 0 or rows['game'].max() >= len(games)):
            raise ValueError('Source game index is outside its block.')
        rebased = rows.copy(); rebased['game'] += len(self.games)
        self.parts.append(rebased); self.games.extend(games); self.rows += len(rows)

    @property
    def ready(self):
        return self.rows >= self.target_rows or len(self.games) >= self.max_games

    def flush(self):
        if not self.games:
            return None
        self.check()
        result = compute.calculate(self.xp, np.concatenate(self.parts), self.games, self.fits, self.check)
        if result['status'] != 'passed':
            raise ValueError('Shared GPU batch reference mismatch: '+str(result['checks']))
        result['batch'] = {'rows': self.rows, 'games': len(self.games),
            'players': sorted({g['player'] for g in self.games})}
        self.parts, self.games, self.rows = [], [], 0
        return result
