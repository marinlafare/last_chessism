"""Small read-only fan-out; never cache inventories across a mutation boundary."""
from concurrent.futures import ThreadPoolExecutor


def read_parallel(*readers):
    """Ordered results, at most four readers, and drain all readers on error.

    Do not pass deletes, publication or other mutations. A failed/incomplete
    inventory must propagate before the caller can authorize any write.
    """
    if not readers:
        return []
    with ThreadPoolExecutor(max_workers=min(4, len(readers)), thread_name_prefix='cloud-inventory') as pool:
        futures = [pool.submit(read) for read in readers]
        return [future.result() for future in futures]
