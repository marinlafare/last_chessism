"""Retry only immutable storage READS. Never replay a launch, write or delete."""
import asyncio
import threading
import time
from uuid import uuid4

from . import runtime
from cloud_job.launch import Client
from . import recovery_history

DELAYS = (5, 10, 20, 40)


def transient_read_error(error):
    from google.api_core import exceptions as google
    from requests import exceptions as requests
    # SDK RetryError wraps its final cause. Do not classify by error text.
    if isinstance(error, google.RetryError):
        return error.cause is not None and transient_read_error(error.cause)
    if isinstance(error, (google.TooManyRequests, google.InternalServerError,
                          google.BadGateway, google.ServiceUnavailable, google.GatewayTimeout,
                          requests.Timeout, TimeoutError, ConnectionResetError)):
        return True
    return isinstance(error, requests.ConnectionError) and not isinstance(error, requests.SSLError)


class RecoveringStorage:
    def __init__(self, storage, observe, stopping, *, delays=DELAYS):
        self.storage, self.observe, self.stopping, self.delays = storage, observe, stopping, delays

    def __getattr__(self, name):
        return getattr(self.storage, name)

    def read(self, uri, *args, **kwargs):
        episode, started, retries = uuid4().hex, time.monotonic(), 0
        # Only a basename/category is retained; never URLs, tokens or headers.
        operation = 'storage read · ' + ('result batch' if '/batches/' in uri else uri.rsplit('/', 1)[-1][:40])
        while True:
            if self.stopping.is_set():
                raise InterruptedError('Controller stopping; read recovery cancelled')
            try:
                result = self.storage.read(uri, *args, **kwargs)
            except Exception as error:
                if not transient_read_error(error):
                    raise
                exhausted = retries >= len(self.delays)
                self.observe(episode_id=episode, kind='exhausted' if exhausted else 'retry',
                    operation=operation, error_type=type(error).__name__,
                    message='Temporary storage read failure; retry budget exhausted.' if exhausted else 'Temporary storage read failure; retrying the same read.',
                    attempt=retries + 1, delay_seconds=None if exhausted else self.delays[retries],
                    elapsed_seconds=time.monotonic() - started)
                if exhausted:
                    raise
                delay = self.delays[retries]
                retries += 1
                if self.stopping.wait(delay):
                    raise InterruptedError('Controller stopping; read recovery cancelled')
            else:
                if retries:
                    self.observe(episode_id=episode, kind='recovered', operation=operation,
                        attempt=retries + 1, elapsed_seconds=time.monotonic() - started,
                        message='The same read succeeded automatically.')
                return result


class RecoveringClient(Client):
    """Controller-only adapter. Cloud workers retain their existing contract."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observer = None
        self.stopping = threading.Event()

    def observe_job(self, job_id):
        loop = asyncio.get_running_loop()
        def observe(**event):
            # Commit the retry event before waiting. Fail closed if history
            # cannot be saved; don't leave detached history writes behind.
            future = asyncio.run_coroutine_threadsafe(recovery_history.record(job_id, **event), loop)
            try:
                future.result(timeout=15)
            except BaseException:
                future.cancel()
                raise
        self.observer = observe

    def storage(self):
        storage = super().storage()
        return RecoveringStorage(storage, self.observer, self.stopping) if self.observer else storage
