import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { openHealthOutbox } from '../chatgpt/health-outbox.mjs'

test('collection failure and recovery survive disconnection/restart in their original order without private error text', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-health-outbox-'))
  let outbox, clock = 100_000
  t.after(async () => { await outbox?.close(); await rm(dir, { recursive: true, force: true }) })
  outbox = await openHealthOutbox({ dataDir: dir, now: () => clock })
  const scope = { sourceKey: 'source', mainId: 'main', lifecycleId: 'run' }
  outbox.push({ ...scope, roundId: 'round-failed', outcome: 'failed', settledAt: clock, error: { code: 'EACCES', message: 'PRIVATE_URL_TOKEN_BODY' } })
  clock++
  outbox.push({ ...scope, roundId: 'round-failed-again', outcome: 'failed', settledAt: clock, error: { code: 'EACCES', message: 'PRIVATE_URL_TOKEN_BODY' } })
  assert.equal(outbox.diagnostics().pending, 1, 'continuous identical failures coalesce before delivery')
  clock++
  outbox.push({ ...scope, roundId: 'round-recovered', outcome: 'succeeded', settledAt: clock })
  const disconnected = await outbox.flush(async () => { throw new Error('PRIVATE_URL_TOKEN_BODY') })
  assert.equal(disconnected.sent, 0)
  assert.equal(outbox.diagnostics().pending, 2)
  await outbox.close()
  outbox = await openHealthOutbox({ dataDir: dir, now: () => clock })
  const delivered = []
  assert.equal((await outbox.flush(async value => delivered.push(value))).sent, 2)
  assert.deepEqual(delivered.map(value => [value.roundId, value.outcome, value.settledAt]), [['round-failed', 'failed', 100_000], ['round-recovered', 'succeeded', 100_002]])
  assert.equal(delivered[0].error.code, 'EACCES')
  assert.equal(JSON.stringify(delivered).includes('PRIVATE_URL_TOKEN_BODY'), false)
  assert.equal(outbox.diagnostics().pending, 0)
  await outbox.close()
  for (const suffix of ['', '-wal']) {
    const bytes = await readFile(join(dir, 'health-outbox.sqlite') + suffix).catch(error => error.code === 'ENOENT' ? Buffer.alloc(0) : Promise.reject(error))
    assert.equal(bytes.includes(Buffer.from('PRIVATE_URL_TOKEN_BODY')), false)
  }
})

test('outbox capacity and fixed 24-hour expiry are explicit and do not revive expired records', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-health-budget-'))
  let clock = 100_000
  const outbox = await openHealthOutbox({ dataDir: dir, now: () => clock })
  t.after(async () => { await outbox.close(); await rm(dir, { recursive: true, force: true }) })
  const event = n => ({ sourceKey: 'source', mainId: 'main-' + n, lifecycleId: 'run', roundId: 'round-' + n, outcome: 'failed', settledAt: 100_000, error: { code: 'EACCES' } })
  for (let n = 0; n < 256; n++) outbox.push(event(n))
  assert.throws(() => outbox.push(event(256)), error => error.code === 'monitoring_outbox_capacity')
  assert.equal(outbox.diagnostics().rejected, 1)
  assert.equal(outbox.diagnostics().pending, 256)
  clock += 86_400_000
  assert.equal(outbox.diagnostics().pending, 0)
  assert.equal(outbox.diagnostics().expired, 256)
  outbox.push(event(0))
  assert.equal(outbox.diagnostics().pending, 0)
})
