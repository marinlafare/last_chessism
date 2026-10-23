import { pageStyleTarget } from '../../appearance/styleCore.js'
import { MAX_BATCH_VMS, MAX_CLOUD_RUN_CPUS } from './cloudAnalysisForm'

export default function CloudComputeField({ value, onChange, disabled, label, formId, resource = 'vm', variant = 'single' }) {
  const cpu = resource === 'cpu'
  return <div className="cloud-compute-settings">
    <label {...pageStyleTarget(`cloud.${cpu ? 'run' : 'batch'}.${variant}.compute.group`)} className="cloud-compute-field">
      <span {...pageStyleTarget(`cloud.${cpu ? 'run' : 'batch'}.${variant}.compute.label`)} className="cloud-compute-label">{cpu ? 'CPUs' : 'VM'}</span>
      <input {...pageStyleTarget(`cloud.${cpu ? 'run' : 'batch'}.${variant}.compute.input`)} className="text-input" aria-label={label}
        type="number" min="1" max={cpu ? MAX_CLOUD_RUN_CPUS : MAX_BATCH_VMS} step="1" required form={formId}
        disabled={disabled} value={value} onChange={event => onChange(event.target.value)} />
    </label>
  </div>
}
