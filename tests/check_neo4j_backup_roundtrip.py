"""Run inside the controller image with a disposable /backup-volume mount.

Creates a synthetic Neo4j service, a private helper and a network-none PostgreSQL
runner. Never reads live credentials, mounts live data, or selects live services.
"""
import json
import os
from pathlib import Path
import time
import uuid
import io
import tarfile

import docker

IMAGE = "neo4j@sha256:44ad2a2504fd7d27df1d1e976f2d481e4266a0814d3657f2d5df62124ce657f1"


def main():
    client = docker.from_env(timeout=120)
    token = uuid.uuid4().hex[:12]
    project = "chessism-backup-test-" + token
    labels = {"io.chessism.backup-test": token}
    host = os.environ["TEST_BACKUP_HOST_ROOT"]
    assert host.startswith("/tmp/chessism-neo4j-backup-test.")
    root = Path("/backup-volume")
    os.chmod(root, 0o777)
    (root / ".chessism-backup-volume").write_text("isolated-test-volume")
    containers, volumes = [], []
    def create_volume(suffix):
        volume = client.volumes.create(project + "-" + suffix, labels=labels)
        volumes.append(volume)
        return volume.name
    def run(image, **kwargs):
        child = client.containers.run(image, detach=True, network_mode="none", hostname="localhost", **kwargs)
        containers.append(child)
        return child
    try:
        data, rpc, scratch = create_volume("data"), create_volume("rpc"), create_volume("scratch")
        graph = run(IMAGE, labels={**labels, "com.docker.compose.project": project,
                    "com.docker.compose.service": "neo4j"},
                    environment={"NEO4J_AUTH": "none", "NEO4J_server_memory_heap_initial__size": "256m",
                                 "NEO4J_server_memory_heap_max__size": "512m", "NEO4J_server_memory_pagecache_size": "256m"},
                    volumes={data: {"bind": "/data", "mode": "rw"}}, mem_limit="2g", nano_cpus=2_000_000_000)
        for _ in range(90):
            graph.reload()
            if graph.status != "running":
                archive, _ = graph.get_archive("/logs/debug.log")
                with tarfile.open(fileobj=io.BytesIO(b"".join(archive))) as tar:
                    debug = tar.extractfile(tar.getmembers()[0]).read().decode()[-6000:]
                raise RuntimeError("Synthetic Neo4j stopped: " + str(graph.attrs["State"]) + graph.logs(tail=25).decode() + debug)
            probe = graph.exec_run(["cypher-shell", "--format", "plain", "RETURN 1"])
            if probe.exit_code == 0:
                break
            time.sleep(2)
        else:
            raise RuntimeError("Synthetic Neo4j did not start")
        for query in ["CREATE CONSTRAINT position_id IF NOT EXISTS FOR (p:Position) REQUIRE p.id IS UNIQUE",
                      "CREATE (a:Position {id:1, fen:'synthetic-start'}), (b:Position {id:2, fen:'synthetic-next'}), (a)-[:MOVE {san:'e4'}]->(b)"]:
            assert graph.exec_run(["cypher-shell", query]).exit_code == 0
        helper = run("chessism-neo4j-backup:latest", labels=labels,
            environment={"CHESSISM_COMPOSE_PROJECT": project, "BACKUP_VOLUME_UUID": "isolated-test-volume",
                         "BACKUP_FREE_FLOOR_BYTES": "0", "NEO4J_BACKUP_HOST_ROOT": host},
            volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "ro"},
                     host: {"bind": "/backup-volume", "mode": "rw"},
                     rpc: {"bind": "/neo4j-backup-rpc", "mode": "rw"},
                     scratch: {"bind": "/scratch-space", "mode": "rw"}})
        for _ in range(30):
            if helper.exec_run(["test", "-f", "/neo4j-backup-rpc/heartbeat"]).exit_code == 0:
                break
            time.sleep(1)
        runner = run("chessism-backup-worker:neo4j-backup-20261030", labels=labels,
            command=["python", "tests/check_snapshot_roundtrip.py"], working_dir="/workspace",
            environment={"PYTHONPATH": "/workspace", "PYTHONDONTWRITEBYTECODE": "1",
                         "CHESSISM_ROUNDTRIP_BACKUP_ROOT": "/backup-volume"},
            volumes={os.environ["TEST_SOURCE_ROOT"]: {"bind": "/workspace", "mode": "ro"},
                     host: {"bind": "/backup-volume", "mode": "rw"},
                     rpc: {"bind": "/neo4j-backup-rpc", "mode": "rw"}},
            mem_limit="2g", nano_cpus=2_000_000_000, shm_size="256m")
        started = time.monotonic()
        while time.monotonic() - started < 900:
            runner.reload()
            if runner.status == "exited":
                break
            time.sleep(2)
        print(runner.logs(tail=45).decode(), flush=True)
        if runner.attrs["State"].get("ExitCode") != 0 or runner.status != "exited":
            print(helper.logs(tail=30).decode(), flush=True)
            raise RuntimeError("Combined backup roundtrip failed")
        graph.reload()
        assert graph.status == "running", "Source graph must have been restarted"
        query = graph.exec_run(["cypher-shell", "--format", "plain", "MATCH (a)-[r]->(b) RETURN a.fen, r.san, b.fen"])
        assert query.exit_code == 0 and b"synthetic-start" in query.output and b"e4" in query.output
        assert not client.containers.list(all=True, filters={"label": "io.chessism.neo4j-backup=" + project})
        assert not client.volumes.list(filters={"label": "io.chessism.neo4j-backup=" + project})
        # Hard-kill the controller during an offline dump, then prove durable
        # startup recovery restarts the source and removes its partial output.
        crash_token = uuid.uuid4().hex
        crash_id = "snapshot-20261030T000000-" + uuid.uuid4().hex[:12]
        submit = ("from pathlib import Path; import json; r=Path('/neo4j-backup-rpc'); "
                  f"(r/'{crash_token}.lease').touch(); "
                  f"(r/'{crash_token}.request.json').write_text(json.dumps("
                  f"{{'operation':'dump','backup_id':'{crash_id}','baseline_bytes':0}}))")
        assert helper.exec_run(["python", "-c", submit]).exit_code == 0
        for _ in range(240):
            graph.reload()
            if graph.status == "exited":
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("Interrupted-dump test never reached the offline phase")
        helper.kill(signal="SIGKILL")
        helper.start()
        for _ in range(120):
            probe = helper.exec_run(["python", "-c",
                f"from pathlib import Path; import json; p=Path('/neo4j-backup-rpc/{crash_token}.response.json'); "
                "assert p.exists(); assert json.loads(p.read_text())['ok'] is False"])
            if probe.exit_code == 0:
                break
            time.sleep(2)
        else:
            raise RuntimeError("Controller did not recover the interrupted dump")
        graph.reload()
        assert graph.status == "running"
        assert not (root / "chessism/database/neo4j" / crash_id).exists()
        assert not client.containers.list(all=True, filters={"label": "io.chessism.neo4j-backup=" + project})
        print(json.dumps({"state": "passed", "source_preserved": True, "scratch_removed": True,
                          "hard_kill_recovery": "passed",
                          "elapsed_seconds": round(time.monotonic() - started, 2)}), flush=True)
    finally:
        for child in reversed(containers):
            child.remove(force=True, v=True)
        # These exact labeled objects belong exclusively to this disposable test.
        for child in client.containers.list(all=True, filters={"label": "io.chessism.neo4j-backup=" + project}):
            child.remove(force=True, v=True)
        for volume in client.volumes.list(filters={"label": "io.chessism.neo4j-backup=" + project}):
            volume.remove()
        for volume in reversed(volumes):
            volume.remove()


if __name__ == "__main__":
    main()
