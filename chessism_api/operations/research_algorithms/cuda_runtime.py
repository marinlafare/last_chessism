"""Shared, explicit GPU selection. Research never silently falls back to CPU."""


def cuda_backend(device=0):
    try:
        import cupy as cp
        if not 0 <= device < cp.cuda.runtime.getDeviceCount():
            raise ValueError('Requested CUDA device is unavailable.')
        cp.cuda.Device(device).use()
        cp.get_default_memory_pool().set_limit(size=256 * 1024 * 1024)
        properties = cp.cuda.runtime.getDeviceProperties(device)
        name = properties['name']
        return cp, {'name': name.decode() if isinstance(name, bytes) else str(name),
                    'device': device, 'cupy': cp.__version__, 'cuda_runtime': cp.cuda.runtime.runtimeGetVersion(),
                    'driver': cp.cuda.runtime.driverGetVersion()}
    except Exception as error:
        raise RuntimeError(f'CUDA unavailable; no CPU fallback was used: {error}') from error
