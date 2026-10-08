"""Explicit column schemas for cloud control state (no JSON or pickled columns).

The controller still speaks Google's dictionary-shaped API. These declarations
map that wire format to named, typed columns and child tables. Unknown fields
fail closed: a provider/schema change must be reviewed, never silently dropped.
Presence/null markers distinguish absent, empty and null fields for exact
recovery-plan checksums. They contain field names only, never encoded payloads.
"""
from dataclasses import dataclass
import hashlib
import re

from sqlalchemy import (Table, Column, String, Text, Integer, BigInteger, Float,
                        Boolean, ForeignKey, Index)
from sqlalchemy.dialects.postgresql import ARRAY


@dataclass(frozen=True)
class Children:
    schema: str
    mapping: bool = False
    packed: bool = False


SCHEMAS = {}


def schema(_kind, **fields):
    SCHEMAS[_kind] = fields
    return _kind


def fields(names, kind=str):
    return dict.fromkeys(names.split(), kind)


schema('selection', **fields('backend benchmark_id execution_mode mode plan_id player_name recovery_policy'),
       **fields('n_cpus n_vms nodes positions_per_run runs stall_timeout_seconds total_fens', int),
       cancel_requested=bool, game_links=[int])
schema('position', id=str, fen=str)
schema('record', id=str, object=str, sha256=str)
schema('receipt', sha256=str, records=Children('record', packed=True))
schema('positions', items=Children('position', packed=True))
schema('receipts', items=Children('receipt', mapping=True))
schema('config', **fields('engine input output upload_mode'),
       **fields('batch_size hash_mb max_positions memory_mib multipv nodes position_timeout run_timeout stall_timeout threads upload_queue_size workers', int),
       compact_results=bool)
schema('contract', schema_version=int, position_count=int,
       **fields('chess_version fingerprint input_sha256 worker_version'),
       engine={'binary_sha256': str, 'name': str},
       checkpoint_format={'batch_size': int, 'kind': str},
       settings={**fields('hash_mb mate_score multipv nodes threads', int),
                 **fields('new_game_per_position tablebases wdl', bool), 'score_perspective': str})
schema('identity', name=str, uid=str)
schema('uid', value=str)
schema('quota', limit=float, usage=float, needed=float)
schema('recovery', **fields('version application_failures preemptions session last_exit_code', int),
       **fields('current_job execution run_id spec_hash status'), jobs=[str], uids=Children('uid', mapping=True))
schema('manifest', status=str, fingerprint=str, position_count=int, records=Children('record', packed=True))
schema('worker', **fields('positions worker', int),
       **fields('analysis_seconds first_search_seconds last_search_seconds upload_wait_seconds', float))
SERIES = {**fields('histogram_bin_bytes max_bytes min_bytes p95_upper_bound_bytes samples', int), 'mean_bytes': float}
MEMORY = {**fields('cgroup_oom_kills_delta cgroup_peak_bytes host_total_bytes', int),
          'sampling_interval_seconds': float, 'scope_note': str,
          **{key: SERIES for key in ('worker', 'host_available', 'host_swap_used', 'host_used_excluding_reclaimable')}}
METRICS = {**fields('container_memory_peak_sampled_bytes resource_samples upload_queue_peak', int),
           **fields('engine_analysis_seconds_sum engine_initialization_seconds_sum engine_save_wait_seconds_sum fen_per_second startup_seconds vm_cpu_busy_percent vm_cpu_steal_percent worker_seconds', float),
           'semantic_sha256': str, 'memory': MEMORY,
           'result_pipeline': {**fields('batch_commit_seconds batch_prepare_seconds', float),
                               **fields('batch_saved_events pending_result_bytes_peak prepared_batch_queue_peak raw_batch_queue_peak', int)},
           'storage': fields('create_calls download_requests metadata_requests read_calls upload_bytes upload_requests upload_seconds', float)}
schema('vm_performance', **fields('analyzed_this_attempt index positions resumed', int), metrics=METRICS,
       workers=Children('worker'))
schema('performance', **fields('analyzed_this_attempt cpus_per_vm memory_gib_per_vm parallelism position_count resumed schema_version task_count vm_count', int),
       **fields('backend fingerprint machine_type note'), worker_seconds_sum=float, metrics=METRICS,
       workers=Children('worker'), vms=Children('vm_performance'))
# https://docs.cloud.google.com/batch/docs/reference/rest/v1/StatusEvent
schema('event', description=str, eventTime=str, type=str,
       taskState=str, taskExecution={'exitCode': int})
schema('count', value=str)
schema('instance', bootDisk=fields('image sizeGb type'), **fields('machineType provisioningModel taskPack'))
schema('task_group_status', counts=Children('count', mapping=True), instances=Children('instance'))
schema('batch_status', runDuration=str, state=str, statusEvents=Children('event'), taskGroups=Children('task_group_status', mapping=True))
schema('condition', **fields('lastTransitionTime message state type reason executionReason severity'))
schema('container', args=[str], command=[str], image=str, resources={'limits': fields('cpu memory')})
RUN_TEMPLATE = {'containers': Children('container'), 'executionEnvironment': str,
                'maxRetries': int, 'serviceAccount': str, 'timeout': str}
schema('execution', **fields('completionTime createTime creator etag generation job launchStage logUri name observedGeneration startTime uid updateTime deleteTime'),
       **fields('parallelism succeededCount failedCount cancelledCount retriedCount runningCount taskCount', int),
       reconciling=bool, conditions=Children('condition'), template=RUN_TEMPLATE)
schema('runnable', container={**fields('imageUri entrypoint options'), 'commands': [str]},
       timeout=str, ignoreExitStatus=bool, background=bool,
       script={'text': str})
schema('task_group', **fields('parallelism taskCount taskCountPerNode'),
       taskSpec={'computeResource': fields('cpuMilli memoryMib'), 'maxRetryCount': int,
                 'maxRunDuration': str, 'runnables': Children('runnable')})
schema('instance_policy', policy={'bootDisk': fields('image sizeGb type'), **fields('machineType provisioningModel')})
schema('network', network=str, subnetwork=str, noExternalIpAddress=bool)
LABELS = fields('analysis_profile app benchmark_id purpose run_id')
schema('spec', labels=LABELS, logsPolicy={'destination': str}, taskGroups=Children('task_group'),
       allocationPolicy={'instances': Children('instance_policy'), 'labels': LABELS,
                         'location': {'allowedLocations': [str]},
                         'network': {'networkInterfaces': Children('network')},
                         'serviceAccount': {'email': str}},
       template={'parallelism': int, 'taskCount': int, 'template': RUN_TEMPLATE})
schema('object', name=str, generation=str, size=str)
schema('image', uri=str, upload_time=str)
schema('cleanup_job', **fields('id region state uid'), images=[str], prefixes=[str])
schema('cleanup', **fields('backend bucket created_at job project run_id sha256 uid'), version=int,
       jobs=Children('cleanup_job'), objects=Children('object'), images=Children('image'),
       prefixes=[str], executions=Children('identity'), spec=SCHEMAS['spec'])
schema('severity', value=int)
schema('log_stream', entries=int, unrelated=bool, severities=Children('severity', mapping=True))
schema('log_plan', version=int, project=str, sha256=str, instances=[str],
       streams=Children('log_stream', mapping=True), retained=Children('log_stream', mapping=True))
LOG_REPORT = {'complete': bool, 'note': str,
              **{k: [str] for k in ('deleted', 'remaining', 'retained_shared', 'pending', 'retained_unrelated')}}
# Pending compute resources are structured records returned by owned_resources,
# not display strings. Keep their identity/scope in native child-table columns.
schema('compute_resource', kind=str, scope=str, name=str)
schema('blocker', items=[str])
CLEAN_REPORT = {'complete': bool, 'image_cleanup_included': bool, 'remaining_object_versions': int,
                'data_deletion_deferred': bool, 'image_deletion_deferred': bool,
                **fields('note plan phase'), 'logs': LOG_REPORT, 'log_cleanup': LOG_REPORT,
                **{k: [str] for k in ('remaining_jobs', 'remaining_images', 'image_delete_operations')},
                'remaining_compute': Children('compute_resource'),
                'image_blockers': Children('blocker', mapping=True)}
schema('task', **fields('index start count recovery_session', int), **fields('run_id status recovery_execution'),
       jobs=[str], prior_jobs=[str], prior_uids=Children('uid', mapping=True),
       config=SCHEMAS['config'], contract=SCHEMAS['contract'], spec=SCHEMAS['spec'],
       recovery_summary=SCHEMAS['recovery'], batch_status=SCHEMAS['batch_status'])
schema('vm_status', **fields('index positions preemptions application_failures', int), state=str)
schema('work_unit', index=int, vm_index=int, count=int, run_id=str,
       config=SCHEMAS['config'], contract=SCHEMAS['contract'],
       manifest_sha256=str, verified=bool, refreshed=bool, performance=SCHEMAS['performance'])
schema('launch', **fields('backend benchmark_id image image_id profile run_job run_operation run_uid uid recovery_execution'),
       **fields('n_cpus n_vms recovery_session workflow_version', int), multi_vm=bool,
       jobs=[str], prior_jobs=[str], prior_uids=Children('uid', mapping=True),
       config=SCHEMAS['config'], spec=SCHEMAS['spec'], recovery_summary=SCHEMAS['recovery'],
       run_execution=SCHEMAS['identity'], run_executions=Children('identity'),
       tasks=Children('task'), units=Children('work_unit'), quota=Children('quota', mapping=True),
       batch_status=SCHEMAS['batch_status'], execution_summary=SCHEMAS['execution'],
       manifest=SCHEMAS['manifest'], task_manifests=Children('manifest'),
       performance=SCHEMAS['performance'], task_performance=Children('performance'),
       vm_statuses=Children('vm_status'), cleanup_report=CLEAN_REPORT, cleanup_verification=CLEAN_REPORT,
       log_cleanup_plan=SCHEMAS['log_plan'], log_cleanup_report=LOG_REPORT,
       import_receipt={**fields('benchmark_id db_job_id db_run_id manifest_sha256'),
                       **fields('cleanup_ready database_receipts_verified', bool),
                       **fields('imported remaining_claims', int)})


def leaves(spec, prefix=''):
    for key, kind in spec.items():
        path = prefix + key
        if isinstance(kind, dict):
            yield from leaves(kind, path + '.')
        elif not isinstance(kind, Children):
            yield path, kind


def column_name(path):
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', path.replace('.', '__')).lower()
    return name if len(name) <= 60 else name[:48] + '_' + hashlib.sha256(path.encode()).hexdigest()[:10]


TABLES = {}
ROOTS = None


def register(metadata):
    global ROOTS
    ROOTS = Table('cloud_control_document', metadata,
        Column('id', String(96), primary_key=True), Column('schema_name', String(32), nullable=False),
        Column('is_null', Boolean, nullable=False), Column('sha256', String(64), nullable=False),
        Column('table_names', ARRAY(Text), nullable=False))
    sqltypes = {str: Text, int: BigInteger, float: Float, bool: Boolean}
    for name, spec in SCHEMAS.items():
        columns = [Column('document_id', String(96), ForeignKey(ROOTS.c.id, ondelete='CASCADE'), primary_key=True),
                   Column('path', Text, primary_key=True), Column('parent_path', Text),
                   Column('collection', Text), Column('item_key', Text), Column('ordinal', Integer),
                   Column('present_fields', ARRAY(Text), nullable=False),
                   Column('null_fields', ARRAY(Text), nullable=False),
                   Column('integer_fields', ARRAY(Text), nullable=False)]
        for path, kind in leaves(spec):
            base = kind[0] if isinstance(kind, list) else kind
            columns.append(Column(column_name(path), ARRAY(sqltypes[base]) if isinstance(kind, list) else sqltypes[base]))
        TABLES[name] = Table('cloud_control_' + name, metadata, *columns)
    # Large per-FEN identity lists are packed into <=500-element typed arrays,
    # rather than creating one permanent history row per FEN.
    for name in ('position', 'record'):
        table = TABLES[name]
        for path, kind in leaves(SCHEMAS[name]):
            table.c[column_name(path)].type = ARRAY(sqltypes[kind])
