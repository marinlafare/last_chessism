import { useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'

const number = value => Number.isFinite(value) ? value.toLocaleString('en-US') : '—'
const stateLabel = state => (state || 'Pending').replaceAll('_', ' ')
const tone = state => state === 'SUCCEEDED' ? 'complete'
  : ['FAILED', 'CANCELLED', 'STOPPED'].includes(state) ? 'error'
    : ['RUNNING', 'RECOVERING', 'RETRYING'].includes(state) ? 'running' : 'pending'

export default function CloudVmTiles({ run, cycleLabel }) {
  const [selected, setSelected] = useState(null)
  const [focused, setFocused] = useState(null)
  const vms = run.vm_statuses || []
  const tabIndex = vms.some(item => item.index === focused) ? focused : vms[0]?.index
  const vm = vms.find(item => item.index === selected)
  const detailId = `cloud-vm-details-${run.id}`
  return <section {...pageStyleTarget('cloud.job.vms')} className="cloud-job-part cloud-vms" aria-label="Batch VM progress">
    <div {...pageStyleTarget('cloud.vm.list')} className="cloud-vm-grid">
      {vms.map(item => <button {...pageStyleTarget('cloud.vm.toggle')} type="button" key={item.index}
        id={`cloud-vm-${run.id}-${item.index}`} className="cloud-vm-tile" data-state={tone(item.state)}
        aria-label={`VM ${item.index + 1}: ${stateLabel(item.state)}`} aria-expanded={selected === item.index}
        aria-controls={detailId} title={`VM ${item.index + 1}: ${stateLabel(item.state)}`}
        tabIndex={item.index === tabIndex ? 0 : -1} onFocus={() => setFocused(item.index)}
        onKeyDown={event => {
          if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
          event.preventDefault()
          const index = vms.findIndex(vm => vm.index === item.index)
          const next = event.key === 'Home' ? 0 : event.key === 'End' ? vms.length - 1
            : (index + (event.key === 'ArrowLeft' ? -1 : 1) + vms.length) % vms.length
          document.getElementById(`cloud-vm-${run.id}-${vms[next].index}`)?.focus({ preventScroll: true })
        }}
        onClick={() => setSelected(current => current === item.index ? null : item.index)}>
        <strong>VM {item.index + 1}</strong>
        <span {...pageStyleTarget('cloud.vm.state')} className="cloud-vm-state"><span {...pageStyleTarget('cloud.vm.dot')} className="cloud-vm-state-dot" aria-hidden="true" />
          <small>{stateLabel(item.state)}</small>
        </span>
      </button>)}
    </div>
    {vm && <section {...pageStyleTarget('cloud.vm.details')} id={detailId} className="cloud-vm-details"
      aria-labelledby={`cloud-vm-${run.id}-${vm.index}`}>
      <header>
        <strong {...pageStyleTarget('cloud.vm.title')}>VM {vm.index + 1}{cycleLabel ? ` · ${cycleLabel}` : ''}</strong>
        <button {...pageStyleTarget('cloud.vm.close')} type="button" aria-label="Close VM details" onClick={() => setSelected(null)}>×</button>
      </header>
      <dl>
        <div><dt {...pageStyleTarget('cloud.vm.status.label')}>Status</dt><dd {...pageStyleTarget('cloud.vm.status.value')}>{stateLabel(vm.state)}</dd></div>
        <div><dt {...pageStyleTarget('cloud.vm.fens.label')}>FENs</dt><dd {...pageStyleTarget('cloud.vm.fens.value')}>{number(vm.positions)}</dd></div>
        <div><dt {...pageStyleTarget('cloud.vm.interruptions.label')}>Google interruptions</dt><dd {...pageStyleTarget('cloud.vm.interruptions.value')}>{number(vm.preemptions)}</dd></div>
        <div><dt {...pageStyleTarget('cloud.vm.failures.label')}>Application failures</dt><dd {...pageStyleTarget('cloud.vm.failures.value')}>{number(vm.application_failures)} / 2</dd></div>
      </dl>
    </section>}
  </section>
}
