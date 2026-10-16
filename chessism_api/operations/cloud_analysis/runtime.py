"""Controller bridge to standalone packages (never imported by the web API)."""
from pathlib import Path
import os
import sys
import tempfile

WORKER_ROOT = Path(__file__).resolve().parents[3] / "stockfish-batch-worker"
if not (WORKER_ROOT / "stockfish_batch").is_dir():
    raise RuntimeError("Run the cloud controller from a complete repository checkout")
sys.path.insert(0, str(WORKER_ROOT))

# PostgreSQL holds the authoritative cleanup plan. This writable local copy is
# recreated after container restarts; application code remains read-only.
CLEANUP_ROOT = Path(os.environ.get("CLOUD_CLEANUP_DIRECTORY",
                                  str(Path(tempfile.gettempdir()) / "chessism-cloud-cleanup")))
