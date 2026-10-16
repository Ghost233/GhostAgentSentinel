import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

test('real directory and record IO failures reach durable receipts and the authenticated panel without cross-channel recovery', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-collection-failure-')), token = 'e'.repeat(64), started = Date.now() - 10_000
  const line = (type, payload) => JSON.stringify({ type, timestamp: new Date(started + 2000).toISOString(), payload }) + '\n'
  const file = id => writeFile(join(dir, id + '.jsonl'), line('session_meta', { id }) + line('event_msg', { type: 'task_started', turn_id: 'run-' + id, turn_attribution: { turn_id: 'run-' + id, turn_trigger: 'composer' } }))
  await file('main-b')
  let directoryFailed = true, bFailed = false, enabled = true
  const thread = id => ({ id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: join(dir, id + '.jsonl'), cwd: dir })
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: started, profile: { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' } } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads() { if (directoryFailed) throw Object.assign(new Error('PRIVATE_DIRECTORY_BODY'), { code: 'EACCES' }); return { data: [thread('main-b')], nextCursor: null } },
    async readThread(id) { if (id === 'main-b' && bFailed) throw Object.assign(new Error('PRIVATE_METADATA_BODY'), { code: 'ECONNRESET' }); return { thread: thread(id), anchorTurnId: 'run-' + id } },
    async close() {},
  }
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') })
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'collection-install', token, collectMaterials: true, pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const read = async id => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: id } } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result.structuredContent
  }
  await adapter.refresh()
  const b = await read('main-b'), sourceKey = (await core.snapshot()).sources[0].key
  assert.equal(b.health.state, 'failed', 'a successful main read cannot recover a directory failure')
  const first = await core.failureReceipts({ sourceKey })
  assert.equal(first.receipts.length, 1)
  assert.equal(first.receipts[0].mainId, null)
  assert.equal(first.receipts[0].channel, 'directory')
  assert.equal(first.receipts[0].error.code, 'EACCES')
  directoryFailed = false
  await adapter.refresh()
  assert.equal((await read('main-b')).health.state, 'recovered')
  const missing = await read('main-a')
  assert.equal(missing.health.state, 'failed')
  assert.ok(missing.health.failures.some(failure => failure.error.code === 'ENOENT'))
  assert.notEqual((await read('main-b')).health.state, 'failed', 'the verified main identity keeps a first attach failure out of another chat')
  const unanchored = (await core.failureReceipts({ sourceKey })).receipts.find(receipt => receipt.mainId === 'main-a')
  assert.equal(unanchored.lifecycleId, null, 'the scope retains its verified main without inventing an execution')
  assert.equal(unanchored.state, 'active')
  await file('main-a')
  assert.equal((await read('main-a')).health.state, 'recovered')
  bFailed = true
  const failedB = await read('main-b')
  assert.equal(failedB.health.state, 'failed')
  assert.ok(failedB.health.failures.some(failure => failure.error.code === 'ECONNRESET'))
  assert.notEqual((await read('main-a')).health.state, 'failed', 'an already-verified main failure does not leak into another main')
  const all = await core.failureReceipts({ sourceKey })
  assert.equal(all.receipts.length, 3)
  assert.equal(all.receipts.find(receipt => receipt.mainId === 'main-b').lifecycleId, 'run-main-b')
  assert.equal(JSON.stringify(all).includes('PRIVATE_'), false)
  assert.equal(adapter.diagnostics().healthOutbox.pending, 0)
  const snapshot = core.snapshot
  let reached, release
  const healthRead = new Promise(resolve => { reached = resolve }), gate = new Promise(resolve => { release = resolve })
  core.snapshot = async () => { const value = await snapshot(); reached(); await gate; return value }
  const readingB = read('main-b')
  await healthRead
  enabled = false
  const readingA = read('main-a')
  let disconnected = false
  for (let n = 0; n < 50; n++) if ((await snapshot()).sources.every(source => source.status === 'disconnected')) { disconnected = true; break }
  release()
  const [lateB, stoppedA] = await Promise.all([readingB, readingA])
  assert.equal(disconnected, true)
  assert.notEqual(lateB.health.state, 'failed', 'late health snapshots cannot republish a fault after plugin eligibility is revoked')
  assert.notEqual(stoppedA.health.state, 'failed')
})
