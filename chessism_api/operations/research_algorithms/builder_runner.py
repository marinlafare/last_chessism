"""Execute a validated graph, freeing working frames after their last consumer."""

import time

import numpy as np

from ..matrix_constructor.artifact_files import file_operation
from .builder_frames import apply_step, missing, prepare, preview
from .builder_inputs import extract
from .builder_outputs import render_output

MEMORY_LIMIT = 256 * 1024 * 1024


def check_memory(frames):
    arrays = {}
    for frame in frames.values():
        for value in [*frame.columns.values(), frame.row_keys]:
            base = value
            while isinstance(base.base, np.ndarray):
                base = base.base
            arrays[id(base)] = base
    # Object storage includes an allowance for interned strings and row keys.
    size = sum(value.nbytes + (64 * value.size if value.dtype.kind == 'O' else 0) for value in arrays.values())
    if size > MEMORY_LIMIT:
        raise ValueError('Working frames reached the 256 MiB allowance. Reduce rows, columns or retained branches.')


async def execute(config, reporter):
    raw, source, extraction_seconds = await extract(config, reporter)
    input_rows = raw.rows
    started = time.monotonic()
    prepared, missing_counts, excluded, imputed = await file_operation(prepare, raw, config['missing'])
    del raw
    prepared_rows = prepared.rows
    frames = {'input': prepared}
    del prepared
    check_memory(frames)
    steps = config['steps']
    outputs = [None] * len(config['outputs'])
    traces = []

    async def capture(key):
        for index, output in enumerate(config['outputs']):
            if output['input'] == key:
                await reporter.check()
                outputs[index] = await file_operation(render_output, frames[key], output, config['seed'])

    await capture('input')
    for index, step in enumerate(steps):
        await reporter.check()
        await reporter.progress('step_' + step['id'], index, len(steps), f'Step {index + 1}/{len(steps)}: {step["name"]}')
        before = time.monotonic()
        incoming = frames[step['input']].rows
        try:
            frame = await file_operation(apply_step, frames[step['input']], step, config['schemas'][step['id']], config['invalid_values'])
            # Non-finite aggregate/transform results are explicit N/A, never JSON Infinity.
            for key, kind in frame.schema.items():
                if kind == 'number' and np.isinf(frame.columns[key]).any():
                    if config['invalid_values'] == 'error':
                        raise ValueError(f'{key} overflowed; narrow the numerical range.')
                    frame.columns[key] = np.where(np.isfinite(frame.columns[key]), frame.columns[key], np.nan)
            frames[step['id']] = frame
            check_memory(frames)
            traces.append({'id': step['id'], 'name': step['name'], 'op': step['op'], 'input_rows': incoming,
                           'output_rows': frame.rows, 'seconds': time.monotonic() - before,
                           'missing': {key: int(missing(value).sum()) for key, value in frame.columns.items()},
                           'preview': preview(frame)})
            await capture(step['id'])
            final_rows = frame.rows
            del frame
            needed = {later['input'] for later in steps[index + 1:]}
            for key in list(frames):
                if key not in needed:
                    del frames[key]
        except ValueError as error:
            raise ValueError(f'{step["name"]}: {error}') from error
    return {'format': 'algorithm_builder_v2', 'input_rows': input_rows, 'included_rows': prepared_rows,
            'final_rows': final_rows, 'excluded_rows': excluded, 'imputed_rows': imputed,
            'missing_by_column': missing_counts, 'steps': traces, 'outputs': outputs, 'source': source,
            'backend': 'numpy_cpu', 'precision': 'float64', 'seed': config['seed'], 'implementation_version': 2,
            'sample_run': bool(config.get('sample_run')), 'timings': {'extraction_seconds': extraction_seconds,
            'calculation_seconds': time.monotonic() - started},
            'warnings': ['Input selection is an ordered prefix, not a random population sample.',
                         'Missing numeric values are ignored by aggregates; count counts rows. Std uses population variance.',
                         'Correlations use rows complete across all selected correlation columns; constant correlations are N/A.',
                         'Mean imputation, if selected, changes numeric values; missing category keys remain missing.',
                         'Associations do not establish causation. Rerunning reads current database values.']}
