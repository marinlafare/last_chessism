"""Versioned built-in formulas. No database rows are created or overwritten."""

import json

from .definitions import definition_config


DEFAULT_FORMULAS = {
    'game-move-scores': {
        'name': 'Game move scores',
        'definition_kind': 'template',
        'row_type': 'move_scores',
        'feature_columns': ['player', 'game', 'result'],
        'label_columns': [],
    },
    'score-distribution': {
        'name': 'Score distribution',
        'definition_kind': 'template',
        'row_type': 'score_distribution',
        'summary_input_version': 2,
        'feature_columns': ['player', 'mode', 'player_score', 'score_kind', 'mate_moves'],
        'label_columns': [],
    },
    'blunder-fixed': {
        'name': 'Blunder moves — fixed coefficient',
        'definition_kind': 'template',
        'row_type': 'blunder_moves',
        'summary_input_version': 2,
        'feature_columns': ['player', 'game', 'mode', 'previous_player_score',
                            'previous_score_kind', 'player_score', 'score_kind', 'initial_prior'],
        'label_columns': [],
    },
    'blunder-moves': {
        'name': 'Blunder moves — rating-aware inputs',
        'definition_kind': 'template',
        'row_type': 'blunder_moves',
        'summary_input_version': 2,
        'feature_columns': ['player', 'game', 'mode',
                            'player_score', 'score_kind', 'analysis_source',
                            'previous_player_score', 'previous_score_kind',
                            'player_rating', 'opponent_rating', 'initial_prior'],
        'label_columns': [],
    },
}


def default_formula(key: str) -> dict:
    if key not in DEFAULT_FORMULAS:
        raise KeyError(key)
    config = definition_config(DEFAULT_FORMULAS[key])
    return {
        'id': f'default:{key}', 'builtin_key': key, 'builtin_version': config.get('summary_input_version', 1),
        'name': config['name'], 'row_type': config['row_type'], 'config': config,
        'storage_kind': 'definition', 'status': 'builtin', 'read_only': True,
        'feature_count': len(config['feature_columns']),
        'label_count': len(config['label_columns']),
        'dynamic_move_columns': bool((config.get('move_scores') or {}).get('all_moves')),
        'definition_bytes': len(json.dumps(config, ensure_ascii=False).encode('utf-8')),
        'created_at': None,
    }


def default_formulas() -> list[dict]:
    return [default_formula(key) for key in DEFAULT_FORMULAS]
