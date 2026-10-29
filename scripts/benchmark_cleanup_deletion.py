"""Explicit, bounded live test. Only creates/deletes its own tiny random-prefix objects."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import threading
import time
from uuid import uuid4

from cloud_job.launch import Client
from cloud_job.run_compose import prepare_credentials
from cleaning_job.cloud import BUCKET
from cleaning_job.cleanup import check_bucket, digest
from cleaning_job.deletion import remove_objects


def main(count):
    if not 100 <= count <= 600:
        raise ValueError('Benchmark limited to 100–600 tiny objects per configuration')
    cloud, local = Client(), threading.local()
    check_bucket(cloud)
    marker = uuid4().hex
    prefix = f'cleanup-benchmarks/{marker}/'
    names = {f'{prefix}{case}/{index:06d}.txt' for case in ('old-eight', 'batch-two', 'batch-four') for index in range(count)}
    clients, lock = [], threading.Lock()
    print(json.dumps({'benchmark_prefix': prefix, 'objects_per_case': count}), flush=True)
    def upload(name):
        if not hasattr(local, 'client'):
            local.client = cloud.storage()._client
            with lock: clients.append(local.client)
        blob = local.client.bucket(BUCKET).blob(name)
        blob.metadata = {'chessism-cleanup-benchmark': marker}
        blob.upload_from_string(b'chessism disposable cleanup benchmark\n', if_generation_match=0, timeout=20)
        return {'name': name, 'generation': str(blob.generation), 'size': str(blob.size)}
    def owned_inventory():
        current = cloud.objects(prefix)
        for item in current:
            if item['name'] not in names or item.get('metadata', {}).get('chessism-cleanup-benchmark') != marker:
                raise ValueError('Unexpected benchmark object; refusing cleanup')
        return [{'name': o['name'], 'generation': str(o['generation']), 'size': str(o['size'])} for o in current]
    try:
        for case, concurrency in [('old-eight', 0), ('batch-two', 2), ('batch-four', 4)]:
            with ThreadPoolExecutor(max_workers=8) as pool:
                objects = list(pool.map(upload, sorted(name for name in names if name.startswith(prefix+case+'/'))))
            print(json.dumps({'case': case, 'created': len(objects)}), flush=True)
            plan = {'objects': objects, 'prefixes': [prefix+case+'/']}
            plan['sha256'] = digest(plan)
            started = time.monotonic()
            if concurrency:
                result = remove_objects(cloud, plan, objects, concurrency=concurrency)
            else:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    for offset in range(0, len(objects), 8):
                        list(pool.map(cloud.delete_object, objects[offset:offset+8]))
                result = {'http_requests': len(objects), 'retries': None}
            seconds = time.monotonic() - started
            if cloud.objects(prefix+case+'/'):
                raise ValueError('Benchmark deletions incomplete')
            print(json.dumps({'case': case, 'files': count, 'seconds': round(seconds, 3),
                'files_per_second': round(count/seconds, 2), **result}), flush=True)
    finally:
        # Also covers a lost upload response: only objects bearing this unique
        # benchmark marker AND an exact planned name may be removed.
        leftovers = owned_inventory()
        if leftovers:
            plan = {'objects': leftovers, 'prefixes': [prefix]}
            plan['sha256'] = digest(plan)
            try:
                remove_objects(cloud, plan, leftovers)
            except Exception:
                # Benchmark-only fallback: never leave test files behind if the
                # transport under test fails. Revalidate exact names/markers and
                # pin each version. Production cleanup has no silent fallback.
                for item in owned_inventory():
                    cloud.delete_object(item)
        if owned_inventory():
            raise ValueError('Benchmark temporary objects remain')
        for client in clients: client.close()
        print(json.dumps({'benchmark_prefix': prefix, 'remaining_objects': 0}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', required=True)
    parser.add_argument('--count', type=int, default=512)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cleanup-benchmark-') as directory:
        prepare_credentials(Path('/host-gcloud'), Path(directory))
        main(args.count)
