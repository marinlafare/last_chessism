"""Cloud Run task index chooses one immutable, disjoint input/checkpoint scope."""
import asyncio
from dataclasses import replace
import os
import sys

from .config import parse_args
from .__main__ import execute


def task_config(config, environ):
    try:
        index = int(environ["CLOUD_RUN_TASK_INDEX"])
        count = int(environ["CLOUD_RUN_TASK_COUNT"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Missing/invalid Cloud Run task identity") from exc
    if not 0 <= index < count <= 400:
        raise ValueError("Invalid Cloud Run task index/count")
    if not config.input.endswith("/input.jsonl"):
        raise ValueError("Cloud Run root input must end with /input.jsonl")
    return replace(config, input=config.input.removesuffix("/input.jsonl") + f"/tasks/{index:06d}/input.jsonl",
                   output=config.output + f"/tasks/{index:06d}").validate()


if __name__ == "__main__":
    sys.exit(asyncio.run(execute(task_config(parse_args(), os.environ))))
