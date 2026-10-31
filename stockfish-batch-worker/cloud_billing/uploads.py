"""Read-only, bounded input-object observations; never launch or modify a job."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import re
import threading
import time

from cleaning_job.cloud import BUCKET, Cloud


def read_uploaded_tasks(run_id, cloud):
    if not re.fullmatch(r'[a-f0-9]{32}', run_id):
        raise ValueError('Invalid run ID')
    prefix = f'inputs/ui-{run_id}/tasks/'
    pattern = re.compile(re.escape(prefix) + r'([0-9]{6})/input\.jsonl')
    indices, seen = set(), 0
    for page in cloud.pages('storage', f'b/{BUCKET}/o', prefix=prefix,
                           maxResults=1000, fields='items(name,size),nextPageToken'):
        for item in page.get('items', []):
            seen += 1
            if seen > 2400:
                raise ValueError('Unexpected input inventory size')
            match = pattern.fullmatch(item.get('name', ''))
            if match and int(item.get('size', 0)) > 0:
                index = int(match[1])
                if index >= 1200 or index in indices:
                    raise ValueError('Unexpected input task identity')
                indices.add(index)
    return {'run_id': run_id, 'state': 'ready', 'tasks': sorted(indices),
            'observed_at': datetime.now(timezone.utc).isoformat()}


class UploadCache:
    """UI reads return immediately; at most one Google listing runs at a time."""
    def __init__(self, cloud=None, executor=None, clock=time.monotonic):
        self.cloud = cloud or Cloud()
        self.executor = executor or ThreadPoolExecutor(max_workers=1, thread_name_prefix='upload-progress')
        self.clock, self.entries, self.lock = clock, {}, threading.Lock()

    def get(self, run_id):
        if not re.fullmatch(r'[a-f0-9]{32}', run_id):
            raise ValueError('Invalid run ID')
        with self.lock:
            now = self.clock()
            entry = self.entries.get(run_id)
            if entry is None:
                if len(self.entries) >= 8:
                    disposable = next((key for key, value in self.entries.items()
                                       if not value.get('future') or value['future'].done()), None)
                    if disposable is None:
                        return {'state': 'pending', 'run_id': run_id}
                    del self.entries[disposable]
                entry = self.entries[run_id] = {'next': 0, 'value': None}
            future = entry.get('future')
            if future and future.done():
                try:
                    entry['value'] = future.result()
                except Exception:
                    entry['value'] = {'state': 'unavailable', 'run_id': run_id}
                entry.update(future=None, next=now + 10, observed=now)
            if not entry.get('future') and now >= entry['next']:
                entry['future'] = self.executor.submit(read_uploaded_tasks, run_id, self.cloud)
            if entry['value'] and now - entry.get('observed', now) <= 45:
                return dict(entry['value'])
            return {'state': 'pending', 'run_id': run_id}
