"""Common receipt transformations for Cloud Run and legacy Batch fleets.

Keep the bounded-work-unit verifier separate: its durable proof is deliberately
different from these per-task manifests.
"""


def task_receipts(receipts, index):
    prefix = f"tasks/{index:06d}/"
    selected = {}
    for name, receipt in receipts.items():
        if not name.startswith(prefix):
            continue
        records = []
        for record in receipt['records']:
            if not record['object'].startswith(prefix):
                raise ValueError('Receipt refers to another task; cleanup withheld')
            records.append({**record, 'object': record['object'][len(prefix):]})
        selected[name[len(prefix):]] = {**receipt, 'records': records}
    return selected


def completion_manifest(contract, position_count, receipts):
    return {'status': 'complete', 'fingerprint': contract['fingerprint'],
            'position_count': position_count,
            'records': [record for name in sorted(receipts)
                        for record in receipts[name]['records']]}
