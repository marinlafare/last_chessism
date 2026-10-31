"""Player-side selection, independent of the frozen blunder equation."""


def moving_owners(game):
    """Never request a player's blunders when that player made no moves.

    One position is White's first ply, not a Black move. Keep a participating
    opponent's valid side; do not mutate shared score-distribution metadata.
    """
    if game is None:
        return None
    plies = game.get('total_positions') or 0
    return {**game, 'white_owner': game['white_owner'] if plies > 0 else None,
            'black_owner': game['black_owner'] if plies > 1 else None}
