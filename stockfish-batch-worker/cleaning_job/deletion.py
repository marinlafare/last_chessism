"""One bounded rolling deletion queue, shared by normal and cancelled workflows."""
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import threading

from .cloud import Cloud, CleanupError
from .batch_transport import BatchTransport


def notify(timings, plan, phase, removed=None):
    callback = getattr(timings, 'cleanup_progress', None)
    if callback:
        callback(plan['sha256'], phase, len(plan['objects']), removed)


def remove_objects(cloud, plan, objects, *, timings=None, concurrency=4, batch_size=100):
    if type(concurrency) is not int or not 1 <= concurrency <= 4:
        raise ValueError('Deletion concurrency must be 1–4 batches')
    if type(batch_size) is not int or not 1 <= batch_size <= 100:
        raise ValueError('Deletion batch size must be 1–100')
    identity = lambda obj: (obj['name'], obj['generation'])
    expected, current = {identity(o) for o in plan['objects']}, {identity(o) for o in objects}
    if (len(expected) != len(plan['objects']) or len(current) != len(objects)
            or not current <= expected):
        raise CleanupError('Deletion targets differ from the approved plan')
    removed = len(expected) - len(current)
    notify(timings, plan, 'objects', removed)
    abort, lock, acknowledged = threading.Event(), threading.Lock(), set()
    # Non-HTTP adapters (including offline fixtures) retain their exact-object
    # interface. Production Cloud/Client always use the multipart transport.
    transport = BatchTransport(cloud, abort) if isinstance(cloud, Cloud) else None
    width, size = (concurrency, batch_size) if transport else (8, 1)
    groups = iter(objects[offset:offset + size] for offset in range(0, len(objects), size))
    def acknowledge(items):
        nonlocal removed
        keys = {identity(o) for o in items}
        with lock:
            if not keys <= current or keys & acknowledged:
                raise CleanupError('Invalid or duplicate deletion acknowledgement')
            acknowledged.update(keys)
            removed += len(keys)
            notify(timings, plan, 'objects', removed)
    def remove(group):
        if transport:
            transport.remove(group, acknowledge)
        elif not abort.is_set():
            cloud.delete_object(group[0])
            acknowledge(group)
    try:
        with ThreadPoolExecutor(max_workers=width, thread_name_prefix='cloud-delete') as pool:
            pending = {pool.submit(remove, group) for _, group in zip(range(width), groups)}
            try:
                while pending:
                    finished, pending = wait(pending, return_when=FIRST_COMPLETED)
                    # Check every finished future before scheduling more work.
                    for future in finished:
                        future.result()
                    for _ in finished:
                        group = next(groups, None)
                        if group is not None:
                            pending.add(pool.submit(remove, group))
            except BaseException:
                abort.set()
                for future in pending:
                    future.cancel()
                raise  # Pool drains in-flight requests before sessions close.
    finally:
        if transport:
            transport.close()
    return {'removed': removed, 'total': len(expected),
            'http_requests': transport.requests if transport else len(objects),
            'retries': transport.retries if transport else 0}
