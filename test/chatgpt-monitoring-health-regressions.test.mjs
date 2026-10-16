import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

const mainChannel = id => 'main:' + createHash('sha256').update(JSON.stringify(id)).digest('hex')
async function fixture(t, options = {}) {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-health-regression-'))
  let clock = Date.now(), server, core, adapter
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  server = await startServer({ dataDir: join(dir, 'core'), now: () => clock, ...options })
  core = await connectCore({ dataDir: join(dir, 'core') })
  const sourceKey = await core.registerSource({ registrationId: 'chatgpt:health-regression', hostRunId: 'host', name: 'Own health fixture', policy: 'chatgpt-main/1' })
  await core.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true })
  const settle = (mainId, lifecycleId, outcome, roundId) => core.recordCollectionOutcome({ sourceKey, mainId, lifecycleId, channel: mainChannel(mainId), roundId, outcome, settledAt: ++clock, ...(outcome === 'failed' ? { error: { code: 'EIO', stage: 'collection' } } : {}) })
  return { dir, get core() { return core }, sourceKey, settle, now: () => clock, advance: (ms = 1) => clock += ms, async request(query) {
    const response = await fetch(server.url + query, { headers: { authorization: 'Bearer ' + server.token } }); return { status: response.status, body: await response.json() }
  }, async restart() {
    await core.close(); await server.close(); server = await startServer({ dataDir: join(dir, 'core'), now: () => clock, ...options }); core = await connectCore({ dataDir: join(dir, 'core') })
    const key = await core.registerSource({ registrationId: 'chatgpt:health-regression', hostRunId: 'host', name: 'Own health fixture', policy: 'chatgpt-main/1' }); assert.equal(key, sourceKey)
    await core.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true })
  }, async attach({ omitHealth = false } = {}) {
    const thread = { id: 'main', path: join(dir, 'missing.jsonl'), cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null }
    const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: clock - 10000, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread() { return { thread, anchorTurnId: null } }, async listThreads() { return { data: [], nextCursor: null } }, async close() {} }
    const connection = omitHealth ? { ...core, async failureReceipts(...args) { const { health, ...page } = await core.failureReceipts(...args); return page } } : core
    adapter = await startChatGPTAdapter({ core: connection, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'health-regression', token: 'a'.repeat(64), collectMaterials: true, pollMs: 0, now: () => clock })
    return adapter
  } }
}

for (const status of ['running', 'idle']) test(`the first confirmed collection failure in a ${status} execution remains claimable as a monitoring notice`, async t => {
  const f = await fixture(t)
  await f.core.ingest(f.sourceKey, { tasks: [{ id: 'main', lifecycleId: 'ended', title: 'Ended main', status }] })
  const first = await f.settle('main', 'ended', 'failed', 'first-idle-EIO')
  const claims = await f.core.claimNotifications(f.sourceKey)
  assert.equal(claims.length, 1, JSON.stringify(await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })))
  assert.equal(claims[0].kind, 'monitoring-failure')
  assert.equal(claims[0].mainId, 'main'); assert.equal(claims[0].lifecycleId, 'ended')
  assert.deepEqual(claims[0].receiptIds, [first.receipt.id])
  assert.equal((await f.core.failureReceipts({ sourceKey: f.sourceKey })).receipts[0].state, 'active')
})

for (const lifecycleId of [null, 'run']) for (const guard of ['chat-pause', 'global-pause', 'plugin-disabled', 'plugin-unknown', 'app-exit', 'app-unknown', 'host-replaced']) test(`a first health notice retains scope ${lifecycleId ?? 'unanchored'} and is suppressed by ${guard}`, async t => {
  const f = await fixture(t)
  await f.core.ingest(f.sourceKey, { tasks: [{ id: 'main', lifecycleId: lifecycleId ?? 'previous', title: 'Known main', status: 'idle' }] })
  const first = await f.settle('main', lifecycleId, 'failed', 'first-' + guard)
  if (guard === 'chat-pause') {
    const current = await f.core.conversationMonitoring({ registrationId: 'chatgpt:health-regression', taskId: 'main' })
    await f.core.setConversationMonitoring({ ...current, paused: true })
  } else if (guard === 'global-pause') await f.core.pause()
  else if (guard === 'host-replaced') {
    await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'host', appRunning: false, pluginEnabled: true })
    const replacement = await f.core.registerSource({ registrationId: 'chatgpt:health-regression', hostRunId: 'next-host', name: 'New host', policy: 'chatgpt-main/1' })
    await f.core.setNativeNotificationEligibility({ sourceKey: replacement, hostRunId: 'next-host', appRunning: true, pluginEnabled: true })
    assert.deepEqual(await f.core.claimNotifications(replacement), [], 'the old receipt is never rebound to the replacement host')
  } else await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'host', appRunning: guard.startsWith('app-') ? guard === 'app-exit' ? false : null : true, pluginEnabled: guard.startsWith('plugin-') ? guard === 'plugin-disabled' ? false : null : true })
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  const records = (await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records
  assert.equal(records.length, 1); assert.equal(records[0].status, 'cancelled')
  assert.equal(records[0].mainId, 'main'); assert.equal(records[0].lifecycleId, lifecycleId)
  assert.deepEqual(records[0].receiptIds, [first.receipt.id])
})

test('24-hour expiry of a failure and an old callback create no fresh notice or synthetic execution', async t => {
  const f = await fixture(t, { leaseMs: 172800000 }), first = await f.settle('main', null, 'failed', 'old')
  const settledAt = first.receipt.firstFailedAt
  f.advance(86400001)
  const page = await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId: 'main', lifecycleId: null })
  assert.deepEqual(page.receipts, []); assert.equal(page.health.state, 'unknown')
  const replay = await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId: 'main', lifecycleId: null, channel: mainChannel('main'), roundId: 'old', outcome: 'failed', settledAt, error: { code: 'EIO', stage: 'collection' } })
  assert.equal(replay.created, false); assert.equal(replay.receipt, null)
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  assert.deepEqual((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records, [])
  assert.equal((await f.core.snapshot()).tasks.length, 0)
})

test('an old core reply without a current health projection remains explicitly unknown rather than inferred from receipt history', async t => {
  const f = await fixture(t), adapter = await f.attach({ omitHealth: true })
  const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'a'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main' } } }) })
  assert.equal(response.status, 200); const packet = await response.json(), health = packet.result.structuredContent.health
  assert.equal(adapter.diagnostics().healthOutbox.pending, 0)
  assert.equal(health.state, 'unknown'); assert.equal(health.orderingGap, true)
  assert.equal((await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId: 'main', lifecycleId: null })).health.state, 'failed')
})

test('explicit failure health scopes preserve omitted, null and literal-null identities through authenticated GET', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.sourceKey, { tasks: [{ id: 'null', lifecycleId: 'null', status: 'idle', title: 'Literal identities' }] })
  const a = await f.settle(null, null, 'failed', 'source'), b = await f.settle('null', null, 'failed', 'main'), c = await f.settle('null', 'null', 'failed', 'lifecycle')
  const all = await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId: undefined, lifecycleId: undefined })
  assert.equal(all.receipts.length, 3); assert.equal(Object.hasOwn(all, 'health'), false, 'history without an exact scope has no ambiguous health aggregate')
  for (const [mainId, lifecycleId, id] of [[null, null, a.receipt.id], ['null', null, b.receipt.id], ['null', 'null', c.receipt.id]]) {
    const page = await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId, lifecycleId })
    assert.deepEqual(page.receipts.map(value => value.id), [id]); assert.equal(page.health.state, 'failed')
    assert.equal(page.health.failures[0].mainId, mainId); assert.equal(page.health.failures[0].lifecycleId, lifecycleId)
    const afterHistory = await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId, lifecycleId, afterSequence: 999, limit: 1 })
    assert.deepEqual(afterHistory.receipts, []); assert.deepEqual(afterHistory.health, page.health, 'the current index is independent of history cursor and page size')
  }
  assert.equal(Object.hasOwn(await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId: 'null' }), 'health'), false)
  for (const mainId of ['', true, 0]) await assert.rejects(f.core.failureReceipts({ sourceKey: f.sourceKey, mainId, lifecycleId: null }), /nonempty/)
  const prefix = '/v1/monitoring/failures?sourceKey=' + f.sourceKey
  const source = await f.request(prefix + '&mainId=&lifecycleId=')
  assert.equal(source.status, 200); assert.equal(source.body.health.failures[0].mainId, null)
  const literal = await f.request(prefix + '&mainId=null&lifecycleId=null')
  assert.equal(literal.status, 200); assert.equal(literal.body.health.failures[0].lifecycleId, 'null')
  assert.equal((await f.request(prefix + '&mainId=&lifecycleId=null')).status, 400, 'a lifecycle cannot lose its main identity')
})

test('a still-active scope remains failed after its historical receipt is pruned at the existing cap', async t => {
  const f = await fixture(t), first = await f.settle('main', null, 'failed', 'still-active'), maximum = (await f.core.diagnostics()).failureReceipts.maximumReceipts
  for (let index = 0; index <= maximum; index++) {
    const value = { sourceKey: f.sourceKey, mainId: 'main', lifecycleId: null, channel: 'directory', roundId: 'other-failure-' + index, outcome: 'failed', settledAt: f.advance(), error: { code: 'EPERM', stage: 'collection' } }
    await f.core.recordCollectionOutcome(value)
    await f.core.recordCollectionOutcome({ ...value, outcome: 'succeeded', roundId: 'other-recovery-' + index, settledAt: f.advance(), error: undefined })
  }
  const page = await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId: 'main', lifecycleId: null })
  assert.equal(page.historyGap, true); assert.equal(page.receipts.some(receipt => receipt.id === first.receipt.id), false)
  assert.equal(page.health.state, 'failed'); assert.equal(page.health.failures.length, 1)
  assert.equal(page.health.failures[0].id, first.receipt.id); assert.equal(page.health.failures[0].error.code, 'EIO')
  assert.equal(page.health.failures[0].firstFailedAt, null, 'expired history is not fabricated from the current-state index')
  assert.ok((await f.core.diagnostics()).failureReceipts.receipts <= maximum)
})

for (const claimed of [false, true]) test(`core reconnect preserves an unsubmitted health notice while claimed=${claimed} remains attempt-safe`, async t => {
  const f = await fixture(t), first = await f.settle('main', null, 'failed', 'pre-reconnect-failure')
  if (claimed) assert.equal((await f.core.claimNotifications(f.sourceKey)).length, 1)
  await f.restart()
  const claims = await f.core.claimNotifications(f.sourceKey)
  assert.equal(claims.length, claimed ? 0 : 1)
  const records = (await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records
  assert.equal(records.length, 1); assert.deepEqual(records[0].receiptIds, [first.receipt.id])
  assert.equal(records[0].status, claimed ? 'cancelled' : 'claimed')
})

for (const [recover, anchor] of [[true, true], [false, true], [true, false]]) test(`the first unsubmitted monitoring notice survives recovery=${recover} and anchor=${anchor}`, async t => {
  const f = await fixture(t), first = await f.settle('main', null, 'failed', 'first-pre-anchor-EIO')
  if (recover) await f.settle('main', null, 'succeeded', 'pre-anchor-recovery')
  if (anchor) await f.core.ingest(f.sourceKey, { tasks: [{ id: 'main', lifecycleId: 'new', title: 'New execution', status: 'running' }] })
  const claims = await f.core.claimNotifications(f.sourceKey)
  assert.equal(claims.length, 1, JSON.stringify(await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })))
  assert.equal(claims[0].kind, 'monitoring-failure'); assert.equal(claims[0].mainId, 'main'); assert.equal(claims[0].lifecycleId, null)
  assert.deepEqual(claims[0].receiptIds, [first.receipt.id])
  assert.equal(claims[0].evidenceThrough, first.receipt.firstFailedAt)
  assert.equal((await f.core.failureReceipts({ sourceKey: f.sourceKey })).receipts[0].state, recover ? 'recovered' : 'active')
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [], 'recovery does not create a second notice')
})

for (const recoveredCount of [99, 100]) test(`an acknowledged active collection failure is visible through production MCP after ${recoveredCount} recovered episodes`, async t => {
  const f = await fixture(t), adapter = await f.attach()
  for (let index = 0; index < recoveredCount; index++) { await f.settle('main', null, 'failed', 'old-failure-' + index); await f.settle('main', null, 'succeeded', 'old-recovery-' + index) }
  f.advance()
  const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'a'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main' } } }) })
  assert.equal(response.status, 200)
  const packet = await response.json(); assert.equal(packet.result.isError, undefined)
  const first = await f.core.failureReceipts({ sourceKey: f.sourceKey, mainId: 'main', lifecycleId: null })
  assert.equal(first.receipts.length, 100); assert.equal(first.hasMore, recoveredCount === 100)
  assert.equal(first.receipts.filter(receipt => receipt.state === 'recovered').length, recoveredCount)
  assert.equal(adapter.diagnostics().healthOutbox.pending, 0, 'the error was acknowledged, not inferred from a queued outbox item')
  const health = packet.result.structuredContent.health
  assert.equal(health.state, 'failed', JSON.stringify(health))
  assert.ok(health.failures.some(value => value.error.code === 'ENOENT'))
})
