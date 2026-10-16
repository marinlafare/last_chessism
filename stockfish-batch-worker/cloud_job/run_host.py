"""Run the controller against this checkout's local Compose database.

Read only DATABASE_URL from the private Compose config; keep credentials out of
command arguments, unit files and output. This launcher does not run migrations.
"""
import argparse
import asyncio
import os
from pathlib import Path
import shutil
import sys

from dotenv import dotenv_values
from sqlalchemy.engine import make_url

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def host_database_url(value, *, host="127.0.0.1", port=5433):
    try:
        url = make_url(value or "")
    except Exception:
        raise ValueError("The Compose config must contain a valid DATABASE_URL") from None
    if url.drivername not in {"postgresql", "postgresql+asyncpg"}:
        raise ValueError("The controller requires PostgreSQL")
    if url.host not in {"db", "localhost", "127.0.0.1", "::1"}:
        raise ValueError("Refusing to remap a non-local database; use the controller module with an explicit environment instead")
    if host not in {"localhost", "127.0.0.1", "::1"} or not 1 <= port <= 65535:
        raise ValueError("Choose a loopback database host and a valid port")
    return url.set(drivername="postgresql+asyncpg", host=host, port=port).render_as_string(hide_password=False)


def gcloud_executable(value=None):
    candidate = value or shutil.which("gcloud")
    if not candidate:
        candidate = str(Path.home() / ".local/opt/google-cloud-sdk/bin/gcloud")
    resolved = shutil.which(candidate)
    if not resolved:
        raise ValueError("gcloud is unavailable; pass --gcloud /absolute/path/to/gcloud")
    return str(Path(resolved).resolve())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=REPOSITORY_ROOT / ".env")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=5433)
    parser.add_argument("--gcloud")
    parser.add_argument("--local-image", required=True)
    parser.add_argument("--poll-seconds", type=int, choices=range(5, 61), default=15, metavar="5..60")
    args = parser.parse_args()
    if not args.env_file.is_file():
        parser.error("Compose environment file not found; use --env-file")
    try:
        values = dotenv_values(args.env_file)
        os.environ["DATABASE_URL"] = host_database_url(values.get("DATABASE_URL"), host=args.db_host, port=args.db_port)
        args.gcloud = gcloud_executable(args.gcloud)
    except ValueError as exc:
        parser.error(str(exc))
    # Docker's credential helper also needs the SDK's bin directory on PATH.
    os.environ["PATH"] = str(Path(args.gcloud).parent) + os.pathsep + os.environ.get("PATH", "")
    sys.path.insert(0, str(REPOSITORY_ROOT))
    from chessism_api.operations.cloud_analysis.__main__ import serve
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
