"""Independent daily salience histograms, calculated only by manual CUDA runs."""
import numpy as np
from .score_compute import check_device, host

VERSION = 'daily_salience_bands_v1'
BOUNDS = (.5, .6, .7, .8, .9)
LABELS = ('0–50', '>50–60', '>60–70', '>70–80', '>80–90', '>90–100')


def bands():
    return [{'id':i,'label':label,'lower':(0,50,60,70,80,90)[i],
        'upper':(50,60,70,80,90,100)[i],'lower_inclusive':i==0,'upper_inclusive':True}
        for i,label in enumerate(LABELS)]


def calculate(xp, rows, *, require_cuda=True):
    """Counts, not averages; all tied modes. Input values remain unrounded 0–1."""
    check_device(xp,require_cuda)
    if not rows:return []
    dates=sorted({r['day'] for r in rows});index={day:i for i,day in enumerate(dates)}
    days=xp.asarray([index[r['day']] for r in rows],dtype=xp.int64)
    modes=xp.asarray([r['mode_bit'] for r in rows],dtype=xp.int64)
    values=xp.asarray([r['salience'] if r['salience'] is not None else xp.nan for r in rows],dtype=xp.float64)
    valid=xp.isfinite(values) & (values>=0) & (values<=1)
    # Compare raw fractions: multiplying or rounding can move a boundary value
    # into the wrong band. 0.50 belongs to band 0; 0.50001 belongs to band 1.
    band=xp.searchsorted(xp.asarray(BOUNDS,dtype=xp.float64),values,side='left')
    result=[]
    for mask in range(1,8):
        selected=valid & ((modes & mask)!=0)
        if not int(xp.count_nonzero(selected)):continue
        counts=xp.bincount(days[selected]*6+band[selected],minlength=len(dates)*6).reshape(-1,6)
        games=counts.sum(axis=1)
        winners=(counts==counts.max(axis=1)[:,None]) & (games[:,None]>0)
        counts,games,winners=(host(xp,v) for v in (counts,games,winners))
        for i,day in enumerate(dates):
            if games[i]:
                result.append({'day':day,'mode_mask':mask,'counts':counts[i].tolist(),
                    'modal_bands':np.flatnonzero(winners[i]).tolist(),'games':int(games[i])})
    return result
