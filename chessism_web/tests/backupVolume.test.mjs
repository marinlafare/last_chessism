import test from 'node:test'
import assert from 'node:assert/strict'
import { allocationDescription, backupVolumeLayout, formatBytes } from '../src/pages/backups/backupVolume.js'

const snapshot = {
  total_bytes: 1000, used_bytes: 300, free_bytes: 650, free_floor_bytes: 150,
  allocations: [
    { id: 'chessism', label: 'Chessism', is_application: true, reserved_bytes: 200, used_bytes: 125 },
    { id: 'other', label: 'Other app', reserved_bytes: null, used_bytes: 100 },
    { id: 'reserved', label: 'Reserved app', reserved_bytes: 50, used_bytes: 25 },
  ],
}
test('whole disk width includes allocations, actual usage, filesystem blocks, reserve and unallocated space', () => {
  const result = backupVolumeLayout(snapshot), byId = Object.fromEntries(result.rows.map(row => [row.id, row]))
  assert.equal(result.rows.reduce((sum, row) => sum + row.size, 0), 1000)
  assert.equal(byId.chessism.percent, 20)
  assert.equal(byId.chessism.fillPercent, 62.5)
  assert.equal(byId.other.size, 100)
  assert.equal(byId.other.fillPercent, 100)
  assert.equal(byId.reserved.fillPercent, 50)
  assert.equal(byId.__system.size, 100) // 50 unassigned used + 50 unavailable to the application
  assert.equal(byId.__reserve.size, 150)
  assert.equal(byId.__free.size, 400)
  assert.equal(result.overcommitted, 0)
  assert.deepEqual(result.rows.map(row => row.id), ['chessism', '__free', 'other', 'reserved', '__system', '__reserve'])
})
test('no measurements means unknown, not an empty disk', () => {
  assert.equal(backupVolumeLayout(null), null)
  assert.equal(backupVolumeLayout({ ...snapshot, allocations: undefined }), null)
  assert.equal(backupVolumeLayout({ ...snapshot, total_bytes: 0 }), null)
  assert.equal(formatBytes(null), 'Unknown')
  assert.equal(formatBytes(NaN), 'Unknown')
  assert.equal(formatBytes(-1), 'Unknown')
  assert.equal(formatBytes(200000000000), '200.00 GB')
})
test('over-allocation is explicit and the stored amount is not clipped to quota', () => {
  const result = backupVolumeLayout({ ...snapshot, allocations: [{ ...snapshot.allocations[0], used_bytes: 250 }] })
  assert.equal(result.rows[0].size, 250)
  assert.match(allocationDescription(result.rows[0]), /over allocation/)
  const over = backupVolumeLayout({ ...snapshot, allocations: [{ ...snapshot.allocations[0], reserved_bytes: 1200 }] })
  assert.ok(over.overcommitted > 0)
  assert.ok(!over.rows.some(row => row.id === '__free'))
})
test('reserve cannot be drawn as free when the physical disk is full', () => {
  const full = backupVolumeLayout({ ...snapshot, used_bytes: 1000, free_bytes: 0 })
  assert.equal(full.reserveShortfall, 150)
  assert.ok(!full.rows.some(row => row.id === '__reserve'))
})
test('tiny allocations remain proportional and zero-byte apps remain in the key', () => {
  const layout = backupVolumeLayout({ ...snapshot, allocations: [
    ...snapshot.allocations, { id: 'tiny', used_bytes: 1 }, { id: 'empty', used_bytes: 0 },
  ] })
  assert.equal(layout.rows.find(row => row.id === 'tiny').percent, 0.1)
  assert.equal(layout.rows.find(row => row.id === 'empty').size, 0)
  assert.match(allocationDescription(layout.rows.find(row => row.id === 'other')), /no published reservation/)
})
