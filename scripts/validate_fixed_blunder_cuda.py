"""Actual-GPU synthetic check; no source database or production changes."""
import json
from chessism_api.operations.research_algorithms.blunder_fixed import Comparison, BATCH_ROWS
from tests.fixed_blunder_fixtures import synthetic


def main():
    rows, games = synthetic()
    comparison = Comparison(games)
    for start in range(0,len(rows),BATCH_ROWS):
        comparison.consume(rows[start:start+BATCH_ROWS])
    report = comparison.report()
    assert report['status']=='passed', report
    assert report['rows']==len(rows) and report['unassessed_moves']==5
    # Explicit detection: saved stale counters cannot produce a passing report.
    games[0]['saved_blunders'] += 1
    assert comparison.report()['checks']['saved_games']==1
    assert comparison.report()['status']=='failed'
    damaged = rows[:1].copy()
    damaged['sql'] = 1-damaged['sql']
    comparison.consume(damaged)
    assert comparison.report()['checks']['sql_moves']==1
    assert report['mismatch_examples']==[], 'A saved report must not mutate when later checks run'
    print(json.dumps(report,allow_nan=False,indent=2))


if __name__=='__main__': main()
