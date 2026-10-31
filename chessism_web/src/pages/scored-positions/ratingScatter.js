// Do not spread dataset-sized arrays into Math.min/Math.max: the browser's
// function-argument limit is much smaller than the number of scored games.
export function ratingScatterBounds(points) {
  let minRating = Infinity
  let maxRating = -Infinity
  let maxIndex = 1
  for (const point of points) {
    const rating = Number(point.rating || 0)
    const index = Number(point.x_index || 0)
    minRating = Math.min(minRating, rating)
    maxRating = Math.max(maxRating, rating)
    maxIndex = Math.max(maxIndex, index)
  }
  return { minRating: points.length ? minRating : 0,
    maxRating: points.length ? maxRating : 0, maxIndex }
}
export const RATING_GROUP_OPTIONS = [
  { key: 'bad', label: 'bad' },
  { key: 'medium', label: 'medium' },
  { key: 'great', label: 'great' },
]

export const RATING_GROUP_COLORS = { bad: '#f07167', medium: '#f4c057', great: '#3fd089' }
