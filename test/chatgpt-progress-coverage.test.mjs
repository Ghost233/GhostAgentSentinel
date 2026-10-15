import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const row = (type, payload, at) => JSON.stringify({ type, payload, timestamp: new Date(at).toISOString() }) + '\n'
const progressGaps = ['record-catching-up', 'record-partial-record', 'main-header-unverified', 'current-turn-not-observed']

async function setup(t, records) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-progress-')), path = join(dir, 'main.jsonl')
  const started = Date.now() - 2000
  const header = row('session_meta', { id: 'progress-main' }, started)
  const start = row('event_msg', { type: 'task_started', turn_id: 'progress-turn' }, started)
  await writeFile(path, records({ header, start, started }))
  const server = await startServer({ dataDir: join(dir, 'core') })
  const core = await connectCore({ dataDir: join(dir, 'core') })
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'progress-native:run', hostStartedAt: started - 10000, profile } },
    async readThread() { return { thread: { id: 'progress-main', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }, anchorTurnId: 'progress-turn' } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() {},
  }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'progress-installation', token: 'c'.repeat(64), pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const read = async () => (await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'progress-main' } } }) }).then(value => value.json())).result.structuredContent
  return { path, started, header, start, core, read }
}

test('an unfinished large baseline becomes complete in the same lifecycle without retaining a false historical gap', async t => {
  const f = await setup(t, ({ header, start, started }) => header + start + row('response_item', { text: 'x'.repeat(300000) }, started))
  const initial = await f.read()
  assert.equal(initial.observation.coverage, 'catching-up')
  assert.equal(initial.observation.status, 'unknown')
  let task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'progress-turn')
  assert.equal(task.status, 'unknown')
  assert.deepEqual(task.coverage, { complete: false, gaps: ['Task baseline is unconfirmed'] })
  assert.deepEqual(task.gaps, [])
  const caughtUp = await f.read()
  assert.equal(caughtUp.observation.coverage, 'caught-up')
  assert.equal(caughtUp.observation.phase, 'started')
  assert.equal(caughtUp.observation.status, 'running')
  task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'progress-turn')
  assert.deepEqual(task.coverage, { complete: true, gaps: [] })
  assert.deepEqual(task.gaps, [])
  assert.equal(task.currentRequest, '')
  assert.equal(task.analysis.state, 'observation-only')
  assert.equal(task.analysis.completedAt, null)
})

test('half records and not-yet-observed headers or turns recover without becoming historical capture losses', async t => {
  const cases = [
    { name: 'half record', initial: ({ header, start }) => header + start + '{"type":"response_item","payload":{"text":"pending', coverage: 'partial-record', gap: null, complete: () => '"}}\n' },
    { name: 'header not observed', initial: () => '', coverage: 'caught-up', gap: 'main-header-unverified', complete: f => f.header + f.start },
    { name: 'turn not observed', initial: ({ header }) => header, coverage: 'caught-up', gap: 'current-turn-not-observed', complete: f => f.start },
  ]
  for (const sample of cases) {
    await t.test(sample.name, async t => {
      const f = await setup(t, sample.initial)
      const initial = await f.read()
      assert.equal(initial.observation.coverage, sample.coverage)
      assert.equal(initial.observation.gap, sample.gap)
      assert.equal(initial.observation.status, 'unknown')
      const before = await f.core.snapshot()
      assert.equal(before.tasks.length, 1)
      assert.deepEqual(before.tasks[0].coverage, { complete: false, gaps: ['Task baseline is unconfirmed'] })
      assert.deepEqual(before.tasks[0].gaps, [])
      await appendFile(f.path, sample.complete(f))
      const complete = await f.read()
      assert.equal(complete.observation.coverage, 'caught-up')
      assert.equal(complete.observation.gap, null)
      assert.equal(complete.observation.status, 'running')
      const after = await f.core.snapshot(), current = after.tasks.find(task => task.lifecycleId === 'progress-turn')
      assert.deepEqual(current.coverage, { complete: true, gaps: [] })
      assert.equal(current.currentRequest, '')
      assert.ok(after.tasks.every(task => task.gaps.every(gap => !progressGaps.includes(gap))))
    })
  }
})

test('real historical capture losses survive recovered progress and empty gap descriptors', async t => {
  const f = await setup(t, ({ header, start, started }) => header + start + row('response_item', { text: 'x'.repeat(300000) }, started))
  await f.read()
  let snapshot = await f.core.snapshot()
  const sourceKey = snapshot.sources[0].key
  await f.core.ingest(sourceKey, { tasks: [{ id: 'progress-main', lifecycleId: 'progress-turn', sessionId: 'progress-main', title: 'Observed main', status: 'unknown', gaps: ['Fixed genuine capture loss'] }] })
  assert.equal((await f.read()).observation.coverage, 'caught-up')
  snapshot = await f.core.snapshot()
  let task = snapshot.tasks.find(task => task.lifecycleId === 'progress-turn')
  assert.equal(task.status, 'running')
  assert.deepEqual(task.coverage, { complete: false, gaps: ['Fixed genuine capture loss'] })
  await f.core.ingest(sourceKey, { tasks: [{ id: 'progress-main', lifecycleId: 'progress-turn', sessionId: 'progress-main', title: 'Observed main', status: 'running', gaps: [] }] })
  await f.read()
  task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'progress-turn')
  assert.deepEqual(task.coverage, { complete: false, gaps: ['Fixed genuine capture loss'] })
  assert.deepEqual(task.gaps, ['Fixed genuine capture loss'])
  assert.equal(task.currentRequest, '')
})
