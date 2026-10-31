import './daily-year-navigation.css'

export default function DailyDateBands({ bands, activePeriod, top, bottom, onSelect, onFocusPeriod }) {
  return (
    <g className="daily-year-bands" aria-label={bands[0]?.kind === 'month' ? 'Choose a month' : 'Choose a year'}>
      {bands.map(band => (
        <rect
          key={band.key}
          className={`daily-year-band${band.kind === 'month' ? ' daily-month-band' : ''}${activePeriod === band.key ? ' highlighted' : ''}`}
          data-year={band.kind === 'year' ? band.key : undefined}
          data-month={band.kind === 'month' ? band.key : undefined}
          x={band.left}
          y={top}
          width={band.right - band.left}
          height={bottom - top}
          role="button"
          tabIndex={0}
          aria-label={`Show ${band.label}${band.kind === 'month' ? ` ${band.key.slice(0, 4)}` : ''} only`}
          onFocus={() => onFocusPeriod(band.key)}
          onBlur={() => onFocusPeriod(null)}
          onClick={() => onSelect(band.key)}
          onKeyDown={event => {
            if (event.key === 'Enter' || event.key === ' ') {
              event.preventDefault()
              onSelect(band.key)
            }
          }}
        />
      ))}
    </g>
  )
}
