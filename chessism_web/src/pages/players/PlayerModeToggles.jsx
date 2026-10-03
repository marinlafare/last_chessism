export const PLAYER_ANALYSIS_MODES = ['bullet', 'blitz', 'rapid']

export const PLAYER_MODE_COLORS = {
  bullet: '#3aa7ff',
  blitz: '#d3b66d',
  rapid: '#f08ac0',
  unknown: '#8da1b2',
}

export default function PlayerModeToggles({
  activeModes,
  availableModes,
  colored = false,
  label,
  onToggle,
}) {
  return (
    <div className={`behavior-mode-controls ${colored ? 'colored' : 'neutral'}`} aria-label={label}>
      {PLAYER_ANALYSIS_MODES.map((mode) => {
        const available = availableModes.has(mode)
        const active = available && activeModes.has(mode)
        return (
          <button
            type="button"
            className={active ? 'active' : ''}
            aria-pressed={active}
            disabled={!available}
            style={colored ? { '--mode-color': PLAYER_MODE_COLORS[mode] } : undefined}
            onClick={() => onToggle(mode)}
            key={mode}
          >
            {colored ? <i aria-hidden="true" /> : null}{mode}
          </button>
        )
      })}
    </div>
  )
}
