import { useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'

const number = value => Number.isFinite(value) ? value.toLocaleString('en-US') : '—'
const stateLabel = state => (state || 'Pending').replaceAll('_', ' ')
const tone = state => state === 'SUCCEEDED' ? 'complete'
  : ['FAILED', 'CANCELLED', 'STOPPED'].includes(state) ? 'error'
    : ['RUNNING', 'RECOVERING', 'RETRYING'].includes(state) ? 'running' : 'pending'

export default function CloudVmTiles({ run, cycleLabel }) {
  const [selected, setSelected] = useState(null)
  const vms = run.vm_statuses || []
  const vm = vms.find(item => item.index === selected)
  const detailId = `cloud-vm-details-${run.id}`
  return <section {...pageStyleTarget('cloud.job.vms')} className="cloud-job-part cloud-vms" aria-label="Batch VM progress">
    <div className="cloud-vm-grid">
      {vms.map(item => <button {...pageStyleTarget('cloud.vm.toggle')} type="button" key={item.index}
        id={`cloud-vm-${run.id}-${item.index}`} className="cloud-vm-tile" data-state={tone(item.state)}
        aria-label={`VM ${item.index + 1}: ${stateLabel(item.state)}`} aria-expanded={selected === item.index}
        aria-controls={detailId} title={`VM ${item.index + 1}: ${stateLabel(item.state)}`}
        onClick={() => setSelected(current => current === item.index ? null : item.index)}>
        <strong>VM {item.index + 1}</strong>
        <span className="cloud-vm-state"><span className="cloud-vm-state-dot" aria-hidden="true" />
          <small>{stateLabel(item.state)}</small>
        </span>
      </button>)}
    </div>
    {vm && <section {...pageStyleTarget('cloud.vm.details')} id={detailId} className="cloud-vm-details"
      aria-labelledby={`cloud-vm-${run.id}-${vm.index}`}>
      <header>
        <strong {...pageStyleTarget('title')}>VM {vm.index + 1}{cycleLabel ? ` · ${cycleLabel}` : ''}</strong>
        <button {...pageStyleTarget('cloud.vm.close')} type="button" aria-label="Close VM details" onClick={() => setSelected(null)}>×</button>
      </header>
      <dl>
        <div><dt {...pageStyleTarget('label')}>Status</dt><dd {...pageStyleTarget('value')}>{stateLabel(vm.state)}</dd></div>
        <div><dt {...pageStyleTarget('label')}>FENs</dt><dd {...pageStyleTarget('value')}>{number(vm.positions)}</dd></div>
        <div><dt {...pageStyleTarget('label')}>Google interruptions</dt><dd {...pageStyleTarget('value')}>{number(vm.preemptions)}</dd></div>
        <div><dt {...pageStyleTarget('label')}>Application failures</dt><dd {...pageStyleTarget('value')}>{number(vm.application_failures)} / 2</dd></div>
      </dl>
    </section>}
  </section>
}
