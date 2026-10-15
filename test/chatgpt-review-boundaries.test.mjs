import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { createServer } from 'node:http'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const row = (type, payload, at) => JSON.stringify({ type, payload, timestamp: new Date(at).toISOString() }) + '\n'

async function setup(t, { records, hostStartedAt = Date.now() - 10000, proxyHook } = {}) {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-review-boundary-'))
  const path = join(dir, 'main.jsonl'), started = hostStartedAt + 2000
  await fs.writeFile(path, row('session_meta', { id: 'review-main' }, started) + (records?.({ hostStartedAt, started }) ?? row('event_msg', { type: 'task_started', turn_id: 'review-turn' }, started)))
  const server = await startServer({ dataDir: join(dir, 'core') })
  let proxy
  if (proxyHook) {
    proxy = createServer(async (req, res) => {
      const chunks = []; for await (const chunk of req) chunks.push(chunk)
      const remote = await fetch(server.url + req.url, { method: req.method, headers: { authorization: req.headers.authorization, 'content-type': 'application/json' }, body: chunks.length ? Buffer.concat(chunks) : undefined })
      const body = await remote.text()
      await proxyHook(req.url)
      res.writeHead(remote.status, { 'content-type': 'application/json' }).end(body)
    })
    await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve))
    const runtimePath = join(dir, 'core/runtime.json'), runtime = JSON.parse(await fs.readFile(runtimePath, 'utf8'))
    await fs.writeFile(runtimePath, JSON.stringify({ ...runtime, url: 'http://127.0.0.1:' + proxy.address().port }))
  }
  const core = await connectCore({ dataDir: join(dir, 'core') })
  const facts = { enabled: true, threads: new Map([['review-main', { thread: { id: 'review-main', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }, anchorTurnId: 'review-turn' }]]) }
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'review-native:run', hostStartedAt, profile } },
    async readThread(id) { return facts.threads.get(id) },
    async readEnablement() { return { user: facts.enabled ? 'enabled' : 'disabled', effective: 'enabled' } }, async close() {},
  }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'review-installation', token: 'b'.repeat(64), pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); if (proxy) { proxy.closeAllConnections(); await new Promise(resolve => proxy.close(resolve)) }; await fs.rm(dir, { recursive: true, force: true }) })
  const read = async (threadId = 'review-main') => (await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'b'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: threadId } } }) }).then(value => value.json())).result.structuredContent
  return { dir, path, core, native, facts, adapter, read }
}

test('a lifecycle start in an unfinished bounded baseline cannot turn the current task running', async t => {
  const f = await setup(t, { records: ({ started }) => row('event_msg', { type: 'task_started', turn_id: 'review-turn' }, started) + row('response_item', { text: 'x'.repeat(300000) }, started) })
  const incomplete = await f.read()
  assert.equal(incomplete.observation.phase, 'unknown')
  assert.equal(incomplete.observation.coverage, 'catching-up')
  assert.equal(incomplete.observation.status, 'unknown')
  assert.equal((await f.core.snapshot()).tasks[0].status, 'unknown')
  assert.ok(incomplete.observation.readBytes <= 262144)
  const caughtUp = await f.read()
  assert.equal(caughtUp.observation.coverage, 'caught-up')
  assert.equal(caughtUp.observation.phase, 'started')
  assert.equal(caughtUp.observation.status, 'running')
})

test('a snapshot returned after another chat confirms user-level disable cannot revive a live result', async t => {
  let hold = false, release, reached
  const held = new Promise(resolve => { reached = resolve }), released = new Promise(resolve => { release = resolve })
  const f = await setup(t, { proxyHook: async path => { if (path === '/v1/snapshot' && hold) { hold = false; reached(); await released } } })
  const peerPath = join(f.dir, 'peer.jsonl'), at = Date.now() - 2000
  await fs.writeFile(peerPath, row('session_meta', { id: 'review-peer' }, at) + row('event_msg', { type: 'task_started', turn_id: 'peer-turn' }, at))
  f.facts.threads.set('review-peer', { thread: { id: 'review-peer', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', path: peerPath, cwd: f.dir }, anchorTurnId: 'peer-turn' })
  assert.equal((await f.read()).observation.status, 'running')
  assert.equal((await f.read('review-peer')).observation.status, 'running')
  hold = true
  const oldRequest = f.read()
  try {
    await held
    f.facts.enabled = false
    const disabled = await f.read('review-peer')
    assert.equal(disabled.observation.gap, 'plugin-disabled')
    assert.equal((await f.core.snapshot()).sources[0].status, 'disconnected')
  } finally { release() }
  const late = await oldRequest
  assert.equal(late.observation.status, 'unknown')
  assert.equal(late.observation.phase, 'unknown')
  assert.equal(late.identity.threadId, null)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('an ambiguous host-start boundary closes its reader and does not reopen it for the same source', async t => {
  const f = await setup(t, { records: ({ hostStartedAt }) => row('event_msg', { type: 'task_started', turn_id: 'review-turn' }, hostStartedAt + 500) })
  const first = await f.read()
  assert.equal(first.observation.gap, 'host-start-time-overlap')
  assert.equal(first.observation.status, 'unknown')
  assert.equal(f.adapter.diagnostics().openReaders, 0)
  const recordPath = await fs.realpath(f.path), originalOpen = fs.open
  let recordOpens = 0
  t.mock.method(fs, 'open', async (...args) => { if (args[0] === recordPath) recordOpens++; return originalOpen(...args) })
  await fs.appendFile(f.path, row('response_item', { content: 'new material after the ambiguous start' }, Date.now()))
  const retry = await f.read()
  assert.equal(retry.observation.gap, 'host-start-time-overlap')
  assert.equal(retry.observation.status, 'unknown')
  assert.equal(recordOpens, 0)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
  await fs.appendFile(f.path, row('event_msg', { type: 'task_started', turn_id: 'trusted-next-turn' }, Date.now()))
  f.facts.threads.get('review-main').anchorTurnId = 'trusted-next-turn'
  const newExecution = await f.read()
  assert.equal(newExecution.observation.phase, 'started')
  assert.equal(newExecution.observation.status, 'running')
  assert.equal(recordOpens, 1)
})

test('an append observed after the bounded read keeps completion unknown until the new execution is caught up', async t => {
  const f = await setup(t, { records: ({ started }) => row('event_msg', { type: 'task_started', turn_id: 'review-turn' }, started) + row('response_item', { text: 'x'.repeat(261000) }, started) + row('event_msg', { type: 'task_complete', turn_id: 'review-turn' }, started + 1) })
  const originalStat = fs.stat
  let recordStats = 0
  t.mock.method(fs, 'stat', async (...args) => {
    if (args[0] === f.path && ++recordStats === 2) await fs.appendFile(f.path, row('event_msg', { type: 'task_started', turn_id: 'appended-next-turn' }, Date.now()))
    return originalStat(...args)
  })
  const changed = await f.read()
  assert.equal(changed.observation.coverage, 'catching-up')
  assert.equal(changed.observation.phase, 'unknown')
  assert.equal(changed.observation.status, 'unknown')
  assert.equal((await f.core.snapshot()).tasks[0].status, 'unknown')
  assert.ok(changed.observation.readBytes > 261000 && changed.observation.readBytes <= 262144)
  const caughtUp = await f.read()
  assert.equal(caughtUp.observation.coverage, 'caught-up')
  assert.equal(caughtUp.observation.phase, 'started')
  assert.equal(caughtUp.observation.status, 'running')
  assert.ok(caughtUp.observation.readBytes <= 262144)
})
