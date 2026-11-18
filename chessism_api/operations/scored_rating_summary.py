"""Build and read the scored-game rating chart; no rebuilding during page reads."""
from chessism_api.database.postgresql import scored_rating_summary as storage
from chessism_api.database.postgresql.engine import AsyncDBSession
from chessism_api.database.postgresql.transactions import read_transaction, write_transaction
from chessism_api.rating_statistics import weighted_jenks_breaks


BUCKETS = ('equal', 'small', 'clear', 'decisive', 'mate')
GROUPS = (('bad', 'Bad'), ('medium', 'Medium'), ('great', 'Great'))


def rating_groups(totals):
    ratings = sorted(totals)
    ranges = weighted_jenks_breaks(ratings, [totals[r]['games'] for r in ratings], n_classes=3)
    groups = []
    for index, (key, label) in enumerate(GROUPS):
        low, high = ranges[index] if ranges else (0, 0)
        members = [totals[r] for r in ratings if low <= r <= high]
        groups.append(dict(key=key, label=label, min_rating=low, max_rating=high,
            games=sum(row['games'] for row in members),
            positions=sum(row['total_positions'] for row in members),
            buckets=[dict(key=name, label=name.title(), order=order,
                positions=sum(row[name+'_positions'] for row in members))
                for order, name in enumerate(BUCKETS, 1)]))
    return groups


async def refresh_scored_rating_summary(*, session_factory=None):
    """Serialize publishers before reading one source snapshot and committing it.

    At the default READ COMMITTED isolation, a waiter sees the source after the
    prior publisher commits. Stronger caller-provided isolation can instead raise
    serialization errors; those propagate without publishing partial results.
    """
    async with write_transaction(session_factory or AsyncDBSession) as session:
        await storage.lock_summary(session)
        totals, ratings = {}, []
        async for row in storage.source_games(session):
            rating = row['rating']
            counts = totals.setdefault(rating, dict(games=0, total_positions=0,
                **{name+'_positions': 0 for name in BUCKETS}))
            counts['games'] += 1
            for field in ('total_positions', *(name+'_positions' for name in BUCKETS)):
                counts[field] += row[field]
            ratings.append(dict(link=row['link'], x_index=len(ratings)+1, rating=rating))
        groups = rating_groups(totals)
        for point in ratings:
            # For fewer than three distinct ratings Jenks repeats its last range.
            # All matching groups count the game; chart points use the first one.
            point['group'] = next(group['key'] for group in groups
                if group['min_rating'] <= point['rating'] <= group['max_rating'])
        payload = dict(rating_basis='avg_elo', groups=groups, ratings=ratings)
        await storage.publish(session, payload)
    return payload


async def get_scored_advantage_by_rating(*, include_ratings=True, session_factory=None):
    async with read_transaction(session_factory or AsyncDBSession) as session:
        row = await storage.read_summary(session, include_ratings=include_ratings)
    payload = dict(rating_basis=row['rating_basis'], groups=row['groups_payload'] or rating_groups({}))
    if include_ratings:
        payload['ratings'] = row['ratings_payload']
    return payload
