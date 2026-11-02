"""Single-purpose, network-isolated Docker controller for Neo4j backups.

Accepts only dump/test + a validated snapshot ID over a private volume. No
commands, container IDs, mount paths or database names are accepted from clients.
Docker access is privileged: never mount its socket into the API/backup worker.
"""
import os
import fcntl
from pathlib import Path
import re
import shutil
import signal
import time

from .backup import DATABASES, SAFE_ID, archive_directory, archive_files, verify_archives
from chessism_api.operations.backup_files import atomic_write_json, read_json, sha256

OWNER = "io.chessism.neo4j-backup"
TOKEN = re.compile(r"[a-f0-9]{32}\Z")
UID = 999  # PostgreSQL backup worker, not the Neo4j service user.


class Controller:
    def __init__(self, docker):
        self.docker = docker
        self.rpc = Path(os.environ.get("CHESSISM_NEO4J_BACKUP_RPC", "/neo4j-backup-rpc"))
        self.root = Path("/backup-volume/chessism/database")
        self.project = os.environ["CHESSISM_COMPOSE_PROJECT"]
        self.marker = os.environ["BACKUP_VOLUME_UUID"]
        self.free_floor = int(os.getenv("BACKUP_FREE_FLOOR_BYTES", "150000000000"))
        self.quota = int(os.getenv("DATABASE_BACKUP_QUOTA_BYTES", "200000000000"))
        self.host_backup_root = os.getenv("NEO4J_BACKUP_HOST_ROOT", "/main-monitor-db-backups")
        self.journal = self.rpc / "active.json"
        self.token = None
        self.baseline_bytes = 0
        self.output = None

    @property
    def labels(self):
        return {OWNER: self.project, OWNER + ".operation": self.token}

    def write(self, path, value):
        atomic_write_json(path, value)
        os.chown(path, UID, UID)

    def progress(self, detail):
        self.write(self.rpc / f"{self.token}.progress.json", {"detail": detail})

    def guard(self):
        (self.rpc / "heartbeat").touch()
        lease = self.rpc / f"{self.token}.lease"
        if not lease.is_file() or time.time() - lease.stat().st_mtime > 45:
            raise RuntimeError("Neo4j operation cancelled: backup worker lease expired")
        marker = Path("/backup-volume/.chessism-backup-volume")
        if marker.read_text().strip() != self.marker:
            raise RuntimeError("Backup volume identity mismatch")
        if shutil.disk_usage("/backup-volume").free < self.free_floor + 2_000_000_000:
            raise RuntimeError("Neo4j backup stopped at protected backup free-space floor")
        if shutil.disk_usage("/scratch-space").free < 15_000_000_000:
            raise RuntimeError("Neo4j restore stopped at local free-space floor")
        if self.output is not None:
            size = sum(p.stat().st_size for p in self.output.iterdir() if p.is_file())
            if self.baseline_bytes + size >= self.quota:
                raise RuntimeError("Neo4j backup reached the Chessism backup allocation")

    def live(self):
        found = self.docker.containers.list(all=True, filters={"label": [
            f"com.docker.compose.project={self.project}", "com.docker.compose.service=neo4j"]})
        if len(found) != 1:
            raise RuntimeError("Expected exactly one Neo4j service in this Compose project")
        target = found[0]
        target.reload()
        return target

    def wait_ready(self, container, *, check_lease=True):
        for _ in range(120):
            if check_lease:
                self.guard()
            else:
                (self.rpc / "heartbeat").touch()
            container.reload()
            if container.status in {"dead", "exited"}:
                raise RuntimeError("Neo4j failed to start; inspect its container logs")
            if not check_lease:
                if container.attrs.get("State", {}).get("Health", {}).get("Status") == "healthy":
                    return
                # The real service has a health check; isolated test fixtures may not.
                if container.attrs.get("Config", {}).get("Healthcheck"):
                    time.sleep(2)
                    continue
            result = container.exec_run(["cypher-shell", "--format", "plain", "-d", "neo4j", "RETURN 1"])
            if result.exit_code == 0:
                return
            # Live service requires authentication; HTTP health is the configured
            # readiness check. Disposable restore has authentication disabled.
            if container.attrs.get("State", {}).get("Health", {}).get("Status") == "healthy":
                return
            time.sleep(2)
        raise RuntimeError("Neo4j did not become ready within four minutes")

    def create(self, image, *, volumes, command=None, server=False):
        env = {"NEO4J_AUTH": "none", "NEO4J_server_memory_heap_initial__size": "256m",
               "NEO4J_server_memory_heap_max__size": "512m", "NEO4J_server_memory_pagecache_size": "256m",
               "NEO4J_dbms_usage__report_enabled": "false", "NEO4J_client_allow__telemetry": "false",
               "NEO4J_dbms_fleet__manager_enabled": "false", "NEO4J_server_fleet__discovery_enabled": "false",
               "JAVA_OPTS": "-Xms128m -Xmx512m"}
        options = {} if server else {"entrypoint": ["neo4j-admin"], "user": "0:0"}
        child = self.docker.containers.create(image, command=command, environment=env, volumes=volumes,
            network_mode="none", hostname="localhost", labels=self.labels, mem_limit="3g", nano_cpus=2_000_000_000,
            pids_limit=256, detach=True, **options)
        child.start()
        return child

    def admin(self, image, volumes, command):
        child = self.create(image, volumes=volumes, command=command)
        try:
            while True:
                self.guard()
                child.reload()
                if child.status == "exited":
                    if child.attrs["State"]["ExitCode"] != 0:
                        detail = child.logs(tail=12).decode(errors="replace")[-2000:]
                        raise RuntimeError("Neo4j admin operation failed: " + detail)
                    return
                time.sleep(2)
        finally:
            child.remove(force=True, v=True)

    def owned_cleanup(self):
        filters = {"label": [f"{OWNER}={self.project}", f"{OWNER}.operation={self.token}"]}
        for child in self.docker.containers.list(all=True, filters=filters):
            child.remove(force=True, v=True)
        for volume in self.docker.volumes.list(filters=filters):
            volume.remove()

    def recover(self):
        state = read_json(self.journal, {})
        if not state:
            return
        token = state.get("token", "")
        if not TOKEN.fullmatch(token):
            raise RuntimeError("Invalid Neo4j recovery journal; manual inspection required")
        self.token = token
        self.owned_cleanup()  # Release dump locks before restarting live Neo4j.
        if state.get("restart_live"):
            target = self.live()
            if target.id != state["container_id"]:
                raise RuntimeError("Neo4j container changed during backup; recovery refused")
            if target.status != "running":
                target.start()
            self.wait_ready(target, check_lease=False)
        self.journal.unlink()

    def dump(self, backup_id):
        target = self.live()
        if target.status != "running":
            raise RuntimeError("Neo4j must be running before requesting a backup")
        mounts = [m for m in target.attrs["Mounts"] if m["Destination"] == "/data" and m["Type"] == "volume"]
        if len(mounts) != 1:
            raise RuntimeError("Neo4j data must use its dedicated named volume")
        image = target.attrs["Image"]
        image_info = self.docker.images.get(image)
        digest = next(iter(image_info.attrs.get("RepoDigests", [])), None)
        if not digest:
            raise RuntimeError("Neo4j image has no immutable repository digest")
        output = archive_directory(self.root, backup_id)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.mkdir(mode=0o700)  # Exclusive: never overwrite a published archive.
        self.output = output
        self.progress("Pausing Neo4j for its offline graph and system database snapshots")
        self.write(self.journal, {"token": self.token, "restart_live": True, "container_id": target.id,
                                  "backup_id": backup_id})
        target.stop(timeout=60)
        target.reload()
        if target.attrs["State"]["ExitCode"] != 0:
            raise RuntimeError("Neo4j did not shut down cleanly; refusing the offline dump")
        # The fixed host mount matches the existing PostgreSQL backup destination.
        host_output = self.host_backup_root + "/chessism/database/neo4j/" + backup_id
        volumes = {mounts[0]["Name"]: {"bind": "/data", "mode": "rw"},
                   host_output: {"bind": "/backups", "mode": "rw"}}
        archives = {}
        started = time.time()
        for name, path in zip(DATABASES, archive_files(self.root, backup_id)):
            self.progress(f"Backing up Neo4j {name} database")
            self.admin(image, volumes, ["database", "dump", name, "--to-path=/backups"])
            with path.open("rb") as source:
                os.fsync(source.fileno())
            archives[name] = {"file": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}
            os.chmod(path, 0o600)
            os.chown(path, UID, UID)
        os.chown(output, UID, UID)
        os.chown(output.parent, UID, UID)
        for directory in (output, output.parent):
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        self.progress("Restarting Neo4j after its snapshots")
        self.recover()
        return {"format": "neo4j-admin-dump-v1", "image": digest, "archives": archives,
                "completed_at": time.time(), "dump_seconds": round(time.time() - started, 2),
                "authentication_records_included": True, "plaintext_passwords_included": False,
                "consistency": "Each database is consistent; PostgreSQL and Neo4j are separate snapshots."}

    def test(self, backup_id, metadata):
        verify_archives(self.root, backup_id, metadata)
        image = metadata.get("image", "")
        if not re.fullmatch(r"neo4j@sha256:[a-f0-9]{64}", image):
            raise RuntimeError("Restore requires the recorded official Neo4j image digest")
        # No silent upgrade and no implicit image download.
        self.docker.images.get(image)
        self.write(self.journal, {"token": self.token, "restart_live": False})
        volume = self.docker.volumes.create(name=f"chessism-neo4j-restore-{self.token}", labels=self.labels)
        host_output = self.host_backup_root + "/chessism/database/neo4j/" + backup_id
        volumes = {volume.name: {"bind": "/data", "mode": "rw"},
                   host_output: {"bind": "/backups", "mode": "ro"}}
        for name in DATABASES:
            self.progress(f"Restoring and checking disposable Neo4j {name} database")
            self.admin(image, volumes, ["database", "load", name, "--from-path=/backups"])
            self.admin(image, volumes, ["database", "check", name, "--threads=2",
                                       "--max-off-heap-memory=512m", "--report-path=/tmp"])
        self.progress("Starting the isolated Neo4j restore and verifying graph queries/schema")
        child = self.create(image, volumes={volume.name: {"bind": "/data", "mode": "rw"}}, server=True)
        self.wait_ready(child)
        def query(database, cypher):
            result = child.exec_run(["cypher-shell", "--format", "plain", "-d", database, cypher])
            if result.exit_code:
                raise RuntimeError("Restored Neo4j query/schema validation failed")
            return result.output.decode().strip()
        query("neo4j", "CALL db.awaitIndexes(120)")
        def count(cypher):
            return int(query("neo4j", cypher).splitlines()[-1])
        result = {"status": "success", "databases": list(DATABASES), "consistency_check": "passed",
                  "nodes": count("MATCH (n) RETURN count(n)"),
                  "relationships": count("MATCH ()-[r]->() RETURN count(r)"),
                  "indexes": count("SHOW INDEXES YIELD name RETURN count(name)"),
                  "constraints": count("SHOW CONSTRAINTS YIELD name RETURN count(name)")}
        query("system", "SHOW USERS YIELD user RETURN count(user)")
        self.progress("Removing the disposable Neo4j restore")
        self.recover()
        return result

    def handle(self, path):
        self.token = path.name.split(".")[0]
        if not TOKEN.fullmatch(self.token):
            return
        self.baseline_bytes, self.output = 0, None
        try:
            item = read_json(path, {})
            if not SAFE_ID.fullmatch(item.get("backup_id", "")) or item.get("operation") not in {"dump", "test"}:
                raise RuntimeError("Invalid Neo4j backup operation")
            self.baseline_bytes = max(0, int(item.get("baseline_bytes", 0)))
            self.guard()
            result = self.dump(item["backup_id"]) if item["operation"] == "dump" else self.test(item["backup_id"], item["metadata"])
            self.write(self.rpc / f"{self.token}.response.json", {"ok": True, "result": result})
        except Exception as error:
            # Recovery failure must remain durable and blocks all further work.
            self.recover()
            if self.output is not None:
                # This directory was exclusively created by this request and was
                # never published. Remove partial dumps, not any prior snapshot.
                shutil.rmtree(self.output)
            self.write(self.rpc / f"{self.token}.response.json", {"ok": False, "error": str(error)})
        finally:
            path.unlink(missing_ok=True)

    def recover_interrupted(self):
        interrupted = read_json(self.journal, {})
        self.recover()
        if interrupted:
            token = interrupted["token"]
            self.write(self.rpc / f"{token}.response.json", {"ok": False, "error": "Neo4j operation interrupted; service recovered. Retry the backup/test."})
            (self.rpc / f"{token}.request.json").unlink(missing_ok=True)
            if interrupted.get("backup_id"):
                path = archive_directory(self.root, interrupted["backup_id"])
                if path.exists():
                    shutil.rmtree(path)

    def run(self):
        self.rpc.mkdir(parents=True, exist_ok=True)
        os.chown(self.rpc, UID, UID)
        self.recover_interrupted()
        while True:
            (self.rpc / "heartbeat").touch()
            for path in sorted(self.rpc.glob("*.request.json")):
                self.handle(path)
            # Only terminal private mailbox artifacts, never backup archives.
            for path in self.rpc.iterdir():
                token = path.name.split(".")[0]
                if (TOKEN.fullmatch(token) and path.name.endswith((".progress.json", ".response.json", ".lease"))
                        and time.time() - path.stat().st_mtime > 86400
                        and not (self.rpc / f"{token}.request.json").exists()):
                    path.unlink()
            time.sleep(1)


def main():
    import docker
    controller = Controller(docker.from_env(timeout=90))
    def stop(_signum, _frame):
        raise SystemExit()
    signal.signal(signal.SIGTERM, stop)
    controller.rpc.mkdir(parents=True, exist_ok=True)
    with (controller.rpc / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            controller.run()
        finally:
            controller.recover_interrupted()


if __name__ == "__main__":
    main()
