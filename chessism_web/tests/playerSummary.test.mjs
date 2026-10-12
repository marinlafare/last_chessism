import assert from 'node:assert/strict'
import { formatPlayerCount } from '../src/pages/players/playerSummary.js'

for (const [value, expected] of [[0, '0'], [null, '0'], [undefined, '0'], [NaN, '0'], [Infinity, '0'],
  [999999, '999,999'], [1000000, '1.0 M'], [2563236, '2.6 M'], ['3800000', '3.8 M'],
  [10000000, '10.0 M'], [1234.5, '1,234.5']]) {
  assert.equal(formatPlayerCount(value), expected)
}
