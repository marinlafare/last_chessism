"""Bounded reads and immutable, atomic creates. Never overwrite or delete a GCS object."""
import os
from pathlib import Path
import tempfile
import time

from .config import gcs_parts
from .metrics import StorageStats

MAX_INPUT_BYTES = 96 * 1024 * 1024
MAX_MANIFEST_BYTES = 80 * 1024 * 1024
MAX_RESULT_BYTES = 2 * 1024 * 1024


class Storage:
    def __init__(self):
        self._client = None
        self.stats = StorageStats()

    def _blob(self, uri):
        from google.cloud import storage
        if self._client is None:
            self._client = storage.Client()  # Attached service account / ADC, never key files.
        bucket, key = gcs_parts(uri)
        return self._client.bucket(bucket).blob(key)

    def read(self, uri, limit=MAX_RESULT_BYTES):
        """Return None only for a missing object, not permission/network/corruption errors."""
        self.stats.add("read_calls")
        if uri.startswith("gs://"):
            from google.api_core.exceptions import NotFound
            from google.cloud.storage.retry import DEFAULT_RETRY
            blob = self._blob(uri)
            retry = DEFAULT_RETRY.with_timeout(20)
            try:
                self.stats.add("metadata_requests")
                blob.reload(timeout=10, retry=retry)
            except NotFound:
                return None
            if blob.size is None or blob.size > limit:
                raise ValueError(f"Object exceeds read limit: {uri}")
            # Pin the generation inspected above, so concurrent replacements cannot change input.
            self.stats.add("download_requests")
            data = blob.download_as_bytes(if_generation_match=blob.generation, timeout=10, retry=retry)
        else:
            try:
                with Path(uri).open("rb") as source:
                    data = source.read(limit + 1)
            except FileNotFoundError:
                return None
        if len(data) > limit:
            raise ValueError(f"Object exceeds read limit: {uri}")
        return data

    def create(self, uri, data):
        """Return True if newly created, False if another attempt already created it."""
        self.stats.add("create_calls")
        if uri.startswith("gs://"):
            from google.api_core.exceptions import PreconditionFailed
            from google.cloud.storage.retry import DEFAULT_RETRY
            started = time.monotonic()
            self.stats.add("upload_requests")
            self.stats.add("upload_bytes", len(data))
            try:
                self._blob(uri).upload_from_string(
                    data, content_type="application/json", if_generation_match=0,
                    timeout=10, retry=DEFAULT_RETRY.with_timeout(20), checksum="crc32c",
                )
            except PreconditionFailed:
                return False
            finally:
                self.stats.add("upload_seconds", time.monotonic() - started)
            return True
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        # The final filename is visible only after a complete fsynced write; link is create-only.
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".checkpoint-", delete=False) as temp:
            temporary = Path(temp.name)
            try:
                temp.write(data)
                temp.flush()
                os.fsync(temp.fileno())
                try:
                    os.link(temporary, path)
                except FileExistsError:
                    return False
                descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                return True
            finally:
                temporary.unlink(missing_ok=True)


def child(prefix, name):
    return prefix.rstrip("/") + "/" + name
