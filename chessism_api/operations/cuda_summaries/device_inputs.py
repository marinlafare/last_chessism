"""One position upload reused by the three CUDA algorithms."""
import numpy as np
from .score_compute import check_device


class DeviceInputs:
    def __init__(self, xp, raw, games):
        self.xp = xp
        self.links = {g['game_link']:i for i,g in enumerate(games)}
        self.columns = {name: xp.asarray(raw[name]) for name in raw.dtype.names}
        self.rows = len(raw); self.bytes = raw.nbytes

    def select(self, games):
        xp = self.xp; chosen = [self.links[g['game_link']] for g in games]
        if chosen == list(range(len(self.links))):
            return self.columns
        remap = np.full(len(self.links),-1,dtype=np.int32); remap[chosen] = np.arange(len(chosen))
        ids = xp.asarray(remap)[self.columns['game']]; keep = ids >= 0
        return {name: ids[keep] if name == 'game' else values[keep] for name,values in self.columns.items()}


def upload(xp, raw, games, *, require_cuda=True):
    check_device(xp, require_cuda)
    return DeviceInputs(xp, raw, games)
