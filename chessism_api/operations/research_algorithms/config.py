"""Allowlisted CPU operation contracts, independent of execution and storage."""

from copy import deepcopy

from ..matrix_constructor.catalog import ROW_TYPES
from ..matrix_constructor.definitions import definition_config

VERSION = 1
MAX_ROWS = 1_000_000
MAX_COLUMNS = 12
SCATTER_LIMIT = 1000
BATCH_ROWS = 2048
QUEUE = "algorithms_queue"
ACTIVE = ("queued", "running")


def algorithm_catalog():
    from .builder_schema import AGGREGATIONS, BUILDER_MAX_ROWS, MAX_OUTPUTS, MAX_STEPS, OPERATIONS, SAMPLE_ROWS
    from .expressions import FUNCTIONS
    return {
        "operations": [{"key": "feature_relationships", "name": "Feature relationships",
                        "description": "Legacy template: summaries, Pearson correlations and scatter."},
                       {"key": "pipeline", "name": "Custom algorithm", "description": "Compose steps and formulas."}],
        "builder": {"max_rows": BUILDER_MAX_ROWS, "max_steps": MAX_STEPS, "max_outputs": MAX_OUTPUTS,
                    "sample_rows": SAMPLE_ROWS, "steps": OPERATIONS, "aggregations": AGGREGATIONS,
                    "functions": FUNCTIONS, "version": 2},
        "backends": ["numpy_cpu"], "max_rows": MAX_ROWS, "max_columns": MAX_COLUMNS,
        "scatter_limit": SCATTER_LIMIT, "implementation_version": VERSION,
        "missing": ["drop_rows", "column_mean"], "scaling": ["none", "standardize"],
        "retains_inputs": False,
    }


def normalize_algorithm(request: dict, matrix: dict) -> dict:
    if request.get('operation') == 'pipeline':
        from .builder_schema import normalize_builder
        return normalize_builder(request, matrix)
    recipe = definition_config(matrix)
    if request.get("operation", "feature_relationships") != "feature_relationships":
        raise ValueError("Unsupported algorithm.")
    columns = list(dict.fromkeys(request.get("columns") or []))
    if not 2 <= len(columns) <= MAX_COLUMNS:
        raise ValueError(f"Select 2–{MAX_COLUMNS} numerical columns.")
    available = set(recipe["feature_columns"] + recipe["label_columns"])
    catalog = ROW_TYPES[recipe["row_type"]].columns_by_key
    if any(key not in available or catalog[key].data_type != "number" for key in columns):
        raise ValueError("Only numerical columns already selected in the matrix may be used; category codes are not numerical features.")
    derive = bool(request.get("rating_difference", False))
    if derive and not {"rating", "opponent_rating"}.issubset(columns):
        raise ValueError("Rating difference requires both rating and opponent_rating.")
    output = columns + (["rating_difference"] if derive else [])
    x, y = request.get("x", output[0]), request.get("y", output[1])
    if x not in output or y not in output or x == y:
        raise ValueError("Choose two different scatter columns from the calculation.")
    missing = request.get("missing", "drop_rows")
    scaling = request.get("scaling", "none")
    if missing not in {"drop_rows", "column_mean"} or scaling not in {"none", "standardize"}:
        raise ValueError("Unsupported data preparation setting.")
    limit = int(request.get("max_rows", min(recipe["filters"]["max_rows"], 100_000)))
    if not 1 <= limit <= MAX_ROWS:
        raise ValueError(f"Algorithm runs are limited to {MAX_ROWS:,} source rows.")
    seed = int(request.get("seed", 42))
    if not 0 <= seed < 2 ** 32:
        raise ValueError("Seed must be an integer from 0 to 4294967295.")
    return {
        "operation": "feature_relationships", "implementation_version": VERSION, "backend": "numpy_cpu",
        "matrix": deepcopy(recipe), "columns": columns, "rating_difference": derive,
        "x": x, "y": y, "missing": missing, "scaling": scaling,
        "max_rows": min(limit, recipe["filters"]["max_rows"]), "seed": seed,
    }


def source_config(config):
    return {**config["matrix"], "feature_columns": config["columns"], "label_columns": [],
            "filters": {**config["matrix"]["filters"], "max_rows": config["max_rows"]}}
