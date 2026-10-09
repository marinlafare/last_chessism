"""Compose controller bootstrap: private temporary credentials, no host writes."""
import argparse
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import stat
import tempfile
import time

import httpx
from sqlalchemy.engine import make_url

HEARTBEAT_FILE = Path("/tmp/chessism-cloud-heartbeat")


def compose_connection_args(database_url):
    url = make_url(database_url)
    if url.drivername != "postgresql+asyncpg" or url.host != "db":
        raise ValueError("The Compose controller requires the internal db service")
    # The existing Compose PostgreSQL service has TLS off. Be explicit for this
    # local connection instead of probing root's optional client certificates
    # after dropping privileges. Never override explicitly requested TLS settings.
    return {} if "ssl" in url.query or "sslmode" in url.query else {"ssl": False}


def drop_privileges(source, socket):
    """Match the host config owner and socket group, without hard-coded host IDs."""
    owner, docker_socket = source.stat(), socket.stat()
    if not source.is_dir() or not stat.S_ISSOCK(docker_socket.st_mode):
        raise ValueError("Mount the host gcloud directory and Docker socket")
    if owner.st_uid == 0:
        raise ValueError("Use a non-root user's gcloud configuration")
    if os.geteuid() == 0:
        os.setgroups([docker_socket.st_gid])
        os.setgid(owner.st_gid)
        os.setuid(owner.st_uid)
    if os.geteuid() != owner.st_uid:
        raise ValueError("Controller must run as the gcloud configuration owner")


def prepare_credentials(source, runtime):
    """Snapshot login databases consistently; gcloud writes only to tmpfs copies."""
    if not (source / "credentials.db").is_file() or not (source / "configurations").is_dir():
        raise ValueError("Run gcloud auth login on the host, then restart cloud-controller")
    config = runtime / "gcloud"
    config.mkdir(mode=0o700)
    # Do not copy unrelated ADC keys, logs, or arbitrary symlink targets.
    for name in ("credentials.db", "access_tokens.db"):
        path = source / name
        if path.is_symlink():
            raise ValueError("Symlinked gcloud credentials are not supported")
        if path.exists():
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
                with closing(sqlite3.connect(config / name)) as writer:
                    reader.backup(writer)
    configs = source / "configurations"
    if configs.is_symlink() or any(path.is_symlink() for path in configs.rglob("*")):
        raise ValueError("Symlinked gcloud configurations are not supported")
    shutil.copytree(configs, config / "configurations")
    active = source / "active_config"
    if active.is_symlink():
        raise ValueError("Symlinked gcloud configuration is not supported")
    if active.exists():
        shutil.copyfile(active, config / "active_config")
    docker_config = runtime / "docker"
    docker_config.mkdir(mode=0o700)
    (docker_config / "config.json").write_text(json.dumps({
        "credHelpers": {"us-central1-docker.pkg.dev": "gcloud"},
    }))
    os.environ["CLOUDSDK_CONFIG"] = str(config)
    os.environ["DOCKER_CONFIG"] = str(docker_config)


async def wait_for_api(url, timeout=300):
    """API responds only after database initialization finishes; no migrations here."""
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=3, follow_redirects=False, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(2)
    raise RuntimeError("API did not become ready; check docker compose logs chessism-api")


async def run(args):
    from cloud_job.launch import inspect_worker
    from chessism_api.operations.cloud_analysis.__main__ import serve
    await wait_for_api(args.api_url)
    await asyncio.to_thread(inspect_worker, args.local_image)
    await serve(args)


async def run_until_stopped(args):
    task = asyncio.create_task(run(args))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)


def healthy():
    try:
        return 0 <= time.time() - HEARTBEAT_FILE.stat().st_mtime < 90
    except OSError:
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-health", action="store_true")
    parser.add_argument("--local-image", default=os.environ.get("CLOUD_WORKER_IMAGE", "chessism-stockfish-batch:fen-compat-v1"))
    parser.add_argument("--api-url", default="http://chessism-api:8000/")
    parser.add_argument("--poll-seconds", type=int, choices=range(5, 61), default=15)
    args = parser.parse_args()
    if args.check_health:
        raise SystemExit(0 if healthy() else 1)
    os.umask(0o077)
    args.gcloud, args.heartbeat_file = "gcloud", HEARTBEAT_FILE
    try:
        source = Path("/host-gcloud")
        drop_privileges(source, Path("/var/run/docker.sock"))
        HEARTBEAT_FILE.unlink(missing_ok=True)
        args.database_connect_args = compose_connection_args(os.environ.get("DATABASE_URL", ""))
        with tempfile.TemporaryDirectory(prefix="chessism-cloud-") as directory:
            prepare_credentials(source, Path(directory))
            asyncio.run(run_until_stopped(args))
    except Exception as exc:
        # Do not include SQL URLs, tokens, or subprocess output in startup logs.
        print(f"Cloud controller startup/runtime failure ({type(exc).__name__}). "
              "Check API/database readiness, gcloud login, Docker mounts and the worker image.", flush=True)
        raise SystemExit(1) from None
    finally:
        if os.geteuid() != 0:
            HEARTBEAT_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
