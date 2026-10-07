"""Input validation, run identity, and verifiable immutable checkpoints."""
import hashlib
import io
import json
import math
import re
from dataclasses import dataclass

import chess

from . import __version__
from .storage import child, MAX_MANIFEST_BYTES, MAX_RESULT_BYTES


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def semantic_digest(rows, results):
    """Compare chess outcomes across modes, excluding hardware/wall-clock counters."""
    stable = ("multipv", "score", "pv", "wdl", "depth", "seldepth", "nodes", "tbhits")
    normalized = []
    for row in rows:
        result = results[row["id"]]["engine_result"]
        analysis = result["analysis"]
        if isinstance(analysis, list):
            analysis = [{key: line[key] for key in stable if key in line} for line in analysis]
        normalized.append({"id": row["id"], "fen": result["fen"], "analysis": analysis})
    return digest(encode(normalized))


def parse_input(raw, max_positions):
    rows = []
    ids = set()
    # Do not materialize a second, decoded copy of every input line at once.
    for number, line in enumerate(io.BytesIO(raw), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or set(row) != {"id", "fen"}:
            raise ValueError(f"Line {number}: expected exactly id and fen")
        if not isinstance(row["id"], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", row["id"]):
            raise ValueError(f"Line {number}: unsafe or invalid id")
        if row["id"] in ids:
            raise ValueError(f"Duplicate id: {row['id']}")
        fen = row["fen"]
        if not isinstance(fen, str) or len(fen) > 256 or len(fen.split()) not in (4, 6):
            raise ValueError(f"Line {number}: a four-field position key or six-field FEN is required")
        # Chessism stores four-field keys. Like the local service, chess.Board
        # supplies counters 0/1 for analysis. Preserve the original string in
        # rows/checkpoints/results so imports address the existing database key.
        board = chess.Board(fen)
        if not board.is_valid():
            raise ValueError(f"Line {number}: invalid chess position")
        rows.append(row)
        ids.add(row["id"])
        if len(rows) > max_positions:
            raise ValueError(f"Input exceeds max_positions={max_positions}; nothing was analyzed")
    if not rows:
        raise ValueError("Input contains no positions")
    return rows


def make_contract(raw, rows, config, engine_sha):
    body = {"schema_version": 1, "worker_version": __version__, "chess_version": chess.__version__,
            "input_sha256": digest(raw), "position_count": len(rows),
            "engine": {"name": "Stockfish 16.1", "binary_sha256": engine_sha},
            "settings": config.analysis_settings()}
    if config.batch_size > 1:
        body["checkpoint_format"] = {"kind": "input-batches-v1", "batch_size": config.batch_size}
    return {**body, "fingerprint": digest(encode(body))}


def validate_result(result, row, multipv):
    if not isinstance(result, dict) or result.get("fen") != row["fen"] or result.get("is_valid") is not True:
        raise ValueError("Result does not match input")
    board = chess.Board(row["fen"])
    analysis = result.get("analysis")
    if board.is_game_over():
        expected = (10000 if board.turn == chess.BLACK else -10000) if board.is_checkmate() else 0
        if analysis != {"pv": [], "score": expected}:
            raise ValueError("Invalid terminal result")
        return encode(result)
    count = min(multipv, board.legal_moves.count())
    if not isinstance(analysis, list) or len(analysis) != count:
        raise ValueError("Incomplete MultiPV analysis")
    first_moves = set()
    for index, line in enumerate(analysis, 1):
        if not isinstance(line, dict) or "error" in line or type(line.get("score")) is not int:
            raise ValueError("Missing or invalid score")
        if line.get("multipv") != index or type(line.get("nodes")) is not int or line["nodes"] < 1:
            raise ValueError("Missing MultiPV index or search nodes")
        wdl = line.get("wdl")
        if not isinstance(wdl, list) or len(wdl) != 3 or any(type(n) is not int or n < 0 for n in wdl) or sum(wdl) != 1000:
            raise ValueError("Invalid WDL")
        pv = line.get("pv")
        if not isinstance(pv, list) or not pv or pv[0] in first_moves:
            raise ValueError("Missing or duplicate principal variation")
        first_moves.add(pv[0])
        position = board.copy()
        for uci in pv:
            move = chess.Move.from_uci(uci)
            if move not in position.legal_moves:  # Also reject the UCI null move, 0000.
                raise ValueError("Illegal move in principal variation")
            position.push(move)
    return encode(result)  # Reject nonfinite floats and non-JSON values.


class Checkpoints:
    def __init__(self, storage, prefix, contract):
        self.storage, self.prefix, self.contract = storage, prefix, contract

    def exact(self, name, value):
        uri, raw = child(self.prefix, name), encode(value)
        if not self.storage.create(uri, raw):
            existing = self.storage.read(uri, MAX_MANIFEST_BYTES if name == "manifest.json" else MAX_RESULT_BYTES)
            if existing is None or json.loads(existing) != value:
                raise ValueError(f"Conflicting {name}; use a NEW output prefix, never overwrite it")

    def open(self):
        self.exact("contract.json", self.contract)

    def name(self, row):
        return f"positions/{row['id']}.json"

    def validate(self, value, row):
        if not isinstance(value, dict) or value.get("fingerprint") != self.contract["fingerprint"] or value.get("id") != row["id"]:
            raise ValueError("Checkpoint belongs to a different input/settings/engine")
        result = value.get("engine_result")
        if value.get("result_sha256") != digest(encode(result)):
            raise ValueError("Checkpoint checksum mismatch")
        elapsed = value.get("elapsed_ms")
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("Invalid checkpoint timing")
        validate_result(result, row, self.contract["settings"]["multipv"])
        return value

    def load(self, row):
        raw = self.storage.read(child(self.prefix, self.name(row)))
        return None if raw is None else self.validate(json.loads(raw), row)

    def _prepare_encoded(self, row, result, elapsed_ms):
        if type(elapsed_ms) not in (int, float) or not math.isfinite(elapsed_ms) or elapsed_ms < 0:
            raise ValueError("Invalid checkpoint timing")
        # New engine output needs one legal-PV walk and one result checksum.
        # Recovered/conflict-winning objects still go through validate().
        result_raw = validate_result(result, row, self.contract["settings"]["multipv"])
        value = {"id": row["id"], "fingerprint": self.contract["fingerprint"],
                 "engine_result": result, "result_sha256": digest(result_raw),
                 "elapsed_ms": round(elapsed_ms, 3)}
        raw = encode(value)
        if len(raw) > 256 * 1024:
            raise ValueError("Checkpoint exceeds the 256 KiB record limit")
        return value, raw

    def prepare(self, row, result, elapsed_ms):
        value, _ = self._prepare_encoded(row, result, elapsed_ms)
        return value

    def save(self, row, result, elapsed_ms):
        value, raw = self._prepare_encoded(row, result, elapsed_ms)
        if self.storage.create(child(self.prefix, self.name(row)), raw):
            return value
        # Another attempt may have committed first, or a retry followed a lost HTTP response.
        existing = self.load(row)
        if existing is None:
            raise ValueError("Checkpoint disappeared after create conflict")
        return existing

    def finish(self, rows, results):
        if len(results) != len(rows):
            raise ValueError("Refusing to mark incomplete results as complete")
        records = []
        for row in rows:
            value = self.validate(results[row["id"]], row)
            records.append({"id": row["id"], "object": self.name(row), "sha256": value["result_sha256"]})
        manifest = {"status": "complete", "fingerprint": self.contract["fingerprint"],
                    "position_count": len(rows), "records": records}
        self.exact("manifest.json", manifest)
        return manifest


@dataclass(frozen=True)
class _PreparedBatch:
    """Private in-process handoff; immutable bytes are the upload source of truth."""
    owner: object
    index: int
    raw: bytes


class BatchCheckpoints(Checkpoints):
    """Deterministic input groups: immutable uploads, bounded reads, safe resume races."""
    MAX_BYTES = 16 * 1024 * 1024

    def __init__(self, storage, prefix, contract, rows, batch_size):
        super().__init__(storage, prefix, contract)
        self.groups = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
        self.group_index = {row["id"]: i for i, group in enumerate(self.groups) for row in group}

    def name(self, row):
        return f"batches/{self.group_index[row['id']]:06d}.json"

    def validate_batch(self, value, index):
        group = self.groups[index]
        if not isinstance(value, dict) or value.get("fingerprint") != self.contract["fingerprint"]:
            raise ValueError("Batch belongs to a different contract")
        records = value.get("records")
        if not isinstance(records, list) or len(records) != len(group):
            raise ValueError("Incomplete batch")
        return {row["id"]: self.validate(record, row) for row, record in zip(group, records)}

    def load_batch(self, index):
        raw = self.storage.read(child(self.prefix, self.name(self.groups[index][0])), self.MAX_BYTES)
        return {} if raw is None else self.validate_batch(json.loads(raw), index)

    def save_batch(self, index, records):
        value = {"fingerprint": self.contract["fingerprint"],
                 "records": [records[row["id"]] for row in self.groups[index]]}
        validated = self.validate_batch(value, index)
        raw = encode(value)
        if len(raw) > self.MAX_BYTES:
            raise ValueError("Batch exceeds 16 MiB; use a smaller batch size")
        uri = child(self.prefix, self.name(self.groups[index][0]))
        if self.storage.create(uri, raw):
            return validated
        winner = self.load_batch(index)
        if not winner:
            raise ValueError("Batch disappeared after a create conflict")
        return winner

    def prepare_batch(self, index, items):
        """Validate new (result, elapsed_ms) pairs once, off the event loop."""
        group = self.groups[index]
        if set(items) != {row["id"] for row in group}:
            raise ValueError("Incomplete batch")
        parts = []
        total = 0
        for row in group:
            _, raw = self._prepare_encoded(row, *items[row["id"]])
            total += len(raw)
            if total > self.MAX_BYTES:
                raise ValueError("Batch exceeds 16 MiB; use a smaller batch size")
            parts.append(raw.rstrip(b"\n"))
        raw = (b'{"fingerprint":' + encode(self.contract["fingerprint"]).rstrip(b"\n")
               + b',"records":[' + b','.join(parts) + b']}\n')
        if len(raw) > self.MAX_BYTES:
            raise ValueError("Batch exceeds 16 MiB; use a smaller batch size")
        return _PreparedBatch(self, index, raw)

    def commit_prepared_batch(self, batch):
        if not isinstance(batch, _PreparedBatch) or batch.owner is not self:
            raise ValueError("Prepared batch belongs to a different checkpoint writer")
        index = batch.index
        uri = child(self.prefix, self.name(self.groups[index][0]))
        if self.storage.create(uri, batch.raw):
            # Decode immutable validated bytes, not mutable producer-owned dicts.
            return {value["id"]: value for value in json.loads(batch.raw)["records"]}
        winner = self.load_batch(index)  # Untrusted storage: full checksum + PV validation.
        if not winner:
            raise ValueError("Batch disappeared after a create conflict")
        return winner

    def reference(self, row, value):
        # Called only AFTER validating/loading or committing a whole batch.
        return {"id": row["id"], "object": self.name(row), "sha256": value["result_sha256"]}

    def finish_references(self, rows, references):
        if len(rows) != len(references):
            raise ValueError("Incomplete result references")
        records = [references[row["id"]] for row in rows]
        for row, record in zip(rows, records):
            if (set(record) != {"id", "object", "sha256"} or record["id"] != row["id"]
                    or record["object"] != self.name(row)
                    or not re.fullmatch(r"[a-f0-9]{64}", record["sha256"])):
                raise ValueError("Invalid committed result reference")
        manifest = {"status": "complete", "fingerprint": self.contract["fingerprint"],
                    "position_count": len(rows), "records": records}
        self.exact("manifest.json", manifest)
        return manifest
