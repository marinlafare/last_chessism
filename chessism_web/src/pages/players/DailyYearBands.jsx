import './daily-year-navigation.css'

export default function DailyYearBands({ bands, activeYear, top, bottom, onSelect, onFocusYear }) {
  return (
    <g className="daily-year-bands" aria-label="Choose a year">
      {bands.map(band => (
        <rect
          key={band.year}
          className={`daily-year-band${activeYear === band.year ? ' highlighted' : ''}`}
          data-year={band.year}
          x={band.left}
          y={top}
          width={band.right - band.left}
          height={bottom - top}
          role="button"
          tabIndex={0}
          aria-label={`Show ${band.year} only`}
          onFocus={() => onFocusYear(band.year)}
          onBlur={() => onFocusYear(null)}
          onClick={() => onSelect(band.year)}
          onKeyDown={event => {
            if (event.key === 'Enter' || event.key === ' ') {
              event.preventDefault()
              onSelect(band.year)
            }
          }}
        />
      ))}
    </g>
  )
}
