import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, appendFile, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const record = (type, payload, at = Date.now()) => JSON.stringify({ type, timestamp: new Date(at).toISOString(), payload }) + '\n'

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-chatgpt-'))
  const path = join(dir, 'main.jsonl'), started = Date.now() - 10_000
  await writeFile(path, record('session_meta', { id: 'main-a' }) + record('event_msg', { type: 'task_started', turn_id: 'turn-a' }, started + 2000))
  const facts = {
    host: { available: true, hostRunId: 'test-native:run', hostStartedAt: started, profile: structuredClone(profile) },
    thread: { id: 'main-a', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', forkedFromId: null, path, cwd: dir },
    enablement: { user: 'enabled', effective: 'enabled' },
  }
  const native = {
    async sampleHost() { return structuredClone(facts.host) },
    async readThread(id) { return id === facts.thread.id ? { thread: structuredClone(facts.thread), anchorTurnId: 'turn-a' } : null },
    async readEnablement() { return structuredClone(facts.enablement) },
    async close() {},
  }
  const server = await startServer({ dataDir: join(dir, 'core') })
  const core = await connectCore({ dataDir: join(dir, 'core') })
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'test-installation', token: 'a'.repeat(64), pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  let id = 0
  const call = async (meta, auth = true) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', ...(auth ? { authorization: 'Bearer ' + 'a'.repeat(64) } : {}) }, body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: meta } }) })
    return { status: response.status, data: await response.json() }
  }
  return { dir, path, facts, native, core, adapter, call }
}

test('current main chat exposes real lifecycle through the adapter, shared core HTTP and authenticated MCP without inventing analysis', async t => {
  const f = await fixture(t)
  assert.equal((await f.call({ thread_id: 'main-a' }, false)).status, 401)
  const response = await f.call({ thread_id: 'main-a', threadId: 'main-a' })
  assert.equal(response.status, 200)
  const view = response.data.result.structuredContent
  assert.equal(view.identity.threadId, 'main-a')
  assert.equal(view.observation.phase, 'started')
  assert.equal(view.observation.status, 'running')
  assert.equal(view.analysis.state, 'not-connected')
  assert.equal(view.signal, 'gray')
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.tasks.length, 1)
  assert.equal(snapshot.tasks[0].id, 'main-a')
  assert.equal(snapshot.tasks[0].lifecycleId, 'turn-a')
  assert.equal(snapshot.tasks[0].status, 'running')
  assert.deepEqual(snapshot.tasks[0].alerts, [])
  assert.equal(JSON.stringify(response.data).includes('a'.repeat(64)), false)
  await appendFile(f.path, record('event_msg', { type: 'task_complete', turn_id: 'turn-a' }))
  const complete = (await f.call({ threadId: 'main-a' })).data.result.structuredContent
  assert.equal(complete.observation.phase, 'completed')
  assert.equal(complete.observation.status, 'idle')
  assert.equal((await f.core.snapshot()).tasks[0].status, 'idle')
  const cursor = await readFile(join(f.dir, 'adapter', 'cursors', complete.identity.cursorKey + '.json'), 'utf8')
  assert.doesNotMatch(cursor, /task_complete|Bearer|content|text/)
})

test('missing, conflicting, foreign and subagent identities cannot reuse a prior main-chat result', async t => {
  const f = await fixture(t)
  assert.equal((await f.call({ thread_id: 'main-a' })).data.result.structuredContent.identity.threadId, 'main-a')
  for (const meta of [{}, { thread_id: 'main-a', threadId: 'unknown' }, { threadId: 4 }, { 'openai/session': 'main-a' }, { thread_id: 'unknown' }]) {
    const view = (await f.call(meta)).data.result.structuredContent
    assert.equal(view.identity.threadId, null)
    assert.equal(view.observation.phase, 'unknown')
    assert.equal(view.signal, 'gray')
  }
  f.facts.thread.parentThreadId = 'parent'
  const child = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(child.identity.threadId, null)
  assert.equal(child.observation.gap, 'main-identity-unverified')
  assert.equal((await f.core.snapshot()).tasks[0].status, 'unknown')
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('a source disabled while reading never publishes the late running observation and closes its reader', async t => {
  const f = await fixture(t)
  assert.equal((await f.call({ thread_id: 'main-a' })).data.result.structuredContent.observation.status, 'running')
  let checks = 0
  f.native.readEnablement = async cwd => ({ user: cwd === null || ++checks === 1 ? 'enabled' : 'disabled', effective: 'enabled' })
  const view = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(view.observation.gap, 'plugin-disabled')
  assert.equal(view.observation.status, 'unknown')
  assert.equal((await f.core.snapshot()).tasks[0].status, 'unknown')
  assert.equal(f.adapter.diagnostics().openReaders, 0)
  await assert.rejects(f.core.claimNotifications((await f.core.snapshot()).sources[0].key), /not registered or connected/)
})

test('bounded partial records catch up without retaining bodies, then a new adapter resumes only necessary bytes', async t => {
  const f = await fixture(t)
  await appendFile(f.path, record('response_item', { text: 'BODY-NOT-FOR-CURSOR'.repeat(40000) }))
  let view
  for (let attempt = 0; attempt < 10; attempt++) {
    view = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
    assert.ok(view.observation.readBytes <= 262144)
    if (view.observation.coverage === 'caught-up') break
  }
  assert.equal(view.observation.phase, 'started')
  assert.equal(view.observation.coverage, 'caught-up')
  assert.equal((await f.call({ thread_id: 'main-a' })).data.result.structuredContent.observation.readBytes, 0)
  await f.adapter.close()
  const restored = await startChatGPTAdapter({ core: f.core, native: { ...f.native, close: async () => {} }, sessionsRoot: f.dir, dataDir: join(f.dir, 'adapter'), installationId: 'test-installation', token: 'b'.repeat(64), pollMs: 0 })
  t.after(() => restored.close())
  const result = await restored.read({ thread_id: 'main-a' })
  assert.equal(result.observation.phase, 'started')
  assert.ok(result.observation.readBytes <= 64)
  assert.equal((await restored.read({ thread_id: 'main-a' })).observation.readBytes, 0)
  const checkpoint = await readFile(join(f.dir, 'adapter', 'cursors', result.identity.cursorKey + '.json'), 'utf8')
  assert.doesNotMatch(checkpoint, /BODY-NOT-FOR-CURSOR/)
  assert.ok(Buffer.byteLength(checkpoint) < 2048)
})

test('a truncated record closes observation instead of retaining running state', async t => {
  const f = await fixture(t)
  assert.equal((await f.call({ thread_id: 'main-a' })).data.result.structuredContent.observation.status, 'running')
  await writeFile(f.path, '')
  const view = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(view.observation.gap, 'record-file-truncated')
  assert.equal(view.observation.status, 'unknown')
  assert.equal(f.adapter.diagnostics().openReaders, 0)
  assert.equal((await f.core.snapshot()).tasks[0].status, 'unknown')
})

test('an execution-source change and unsupported host version never reuse old live state', async t => {
  const f = await fixture(t)
  assert.equal((await f.call({ thread_id: 'main-a' })).data.result.structuredContent.observation.status, 'running')
  f.facts.host.hostRunId = 'test-native:new-generation'
  const changed = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(changed.observation.gap, 'source-generation-changed')
  assert.equal(changed.observation.status, 'unknown')
  f.facts.host.profile.appBuild = 'unverified-build'
  const unknown = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(unknown.observation.gap, 'unsupported-host-version')
  assert.equal(unknown.observation.status, 'unknown')
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('switching between two verified main chats and reopening returns each own core observation', async t => {
  const f = await fixture(t)
  const secondPath = join(f.dir, 'second.jsonl')
  await writeFile(secondPath, record('session_meta', { id: 'main-b' }) + record('event_msg', { type: 'task_started', turn_id: 'turn-b' }, Date.now() - 2000) + record('event_msg', { type: 'task_complete', turn_id: 'turn-b' }))
  const first = structuredClone(f.facts.thread), second = { ...first, id: 'main-b', path: secondPath }
  f.native.readThread = async id => id === 'main-a' ? { thread: first, anchorTurnId: 'turn-a' } : id === 'main-b' ? { thread: second, anchorTurnId: 'turn-b' } : null
  const a = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  const b = (await f.call({ thread_id: 'main-b' })).data.result.structuredContent
  const reopened = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(a.identity.threadId, 'main-a', JSON.stringify(a))
  assert.equal(a.observation.status, 'running')
  assert.equal(b.identity.threadId, 'main-b', JSON.stringify(b))
  assert.equal(b.observation.status, 'idle')
  assert.equal(reopened.identity.threadId, 'main-a')
  assert.equal(reopened.observation.status, 'running')
  assert.equal((await f.core.snapshot()).sources.length, 1)
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => [task.id, task.lifecycleId, task.status]).sort(), [['main-a', 'turn-a', 'running'], ['main-b', 'turn-b', 'idle']])
})

test('a main header without an observed execution stays gray and never fabricates a request', async t => {
  const f = await fixture(t)
  await writeFile(f.path, record('session_meta', { id: 'main-a' }))
  const view = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(view.identity.threadId, 'main-a')
  assert.equal(view.observation.phase, 'unknown')
  assert.equal(view.observation.gap, 'current-turn-not-observed')
  assert.equal(view.signal, 'gray')
  const task = (await f.core.snapshot()).tasks[0]
  assert.equal(task.currentRequest, '')
  assert.equal(task.status, 'unknown')
  assert.deepEqual(task.alerts, [])
})

test('project qualification revokes one chat, whereas user-level disable revokes the shared App source', async t => {
  const f = await fixture(t)
  const secondPath = join(f.dir, 'peer.jsonl')
  await writeFile(secondPath, record('session_meta', { id: 'main-b' }) + record('event_msg', { type: 'task_started', turn_id: 'turn-b' }, Date.now() - 2000))
  const first = structuredClone(f.facts.thread), second = { ...first, id: 'main-b', path: secondPath, cwd: f.dir + '/project-b' }
  f.native.readThread = async id => id === 'main-a' ? { thread: first, anchorTurnId: 'turn-a' } : id === 'main-b' ? { thread: second, anchorTurnId: 'turn-b' } : null
  await f.call({ thread_id: 'main-a' }); await f.call({ thread_id: 'main-b' })
  f.native.readEnablement = async cwd => ({ user: 'enabled', effective: cwd === first.cwd ? 'disabled' : 'enabled' })
  assert.equal((await f.call({ thread_id: 'main-a' })).data.result.structuredContent.observation.gap, 'plugin-disabled')
  const projectSnapshot = await f.core.snapshot()
  assert.equal(projectSnapshot.sources.length, 1)
  assert.equal(projectSnapshot.sources[0].status, 'connected')
  assert.equal(projectSnapshot.tasks.find(task => task.id === 'main-a').status, 'unknown')
  assert.equal(projectSnapshot.tasks.find(task => task.id === 'main-b').status, 'running')
  assert.equal((await f.call({ thread_id: 'main-b' })).data.result.structuredContent.observation.status, 'running')
  f.native.readEnablement = async () => ({ user: 'disabled', effective: 'enabled' })
  await f.call({ thread_id: 'main-b' })
  const disabled = await f.core.snapshot()
  assert.equal(disabled.sources[0].status, 'disconnected')
  assert.ok(disabled.tasks.every(task => task.status === 'unknown'))
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('closing the adapter releases panel and reader resources without controlling the observed chat', async t => {
  const f = await fixture(t)
  await f.call({ thread_id: 'main-a' })
  await f.adapter.close()
  assert.deepEqual(f.adapter.diagnostics(), { closed: true, trackedThreads: 0, openReaders: 0, pendingOperations: 0, refreshTimers: 0, queuedBatches: 0, directoryEntries: 0, directoryPending: 0, directoryPages: 0, directoryCursor: 0, activeRequests: 0, transports: 0, sockets: 0 })
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.sources[0].status, 'disconnected')
  assert.equal(snapshot.tasks[0].status, 'unknown')
  assert.equal((await readFile(f.path, 'utf8')).split('\n').filter(Boolean).length, 2)
})

test('a new execution keeps the prior completed result historical and never borrows its running state', async t => {
  const f = await fixture(t)
  await f.call({ thread_id: 'main-a' })
  await appendFile(f.path, record('event_msg', { type: 'task_complete', turn_id: 'turn-a' }))
  await f.call({ thread_id: 'main-a' })
  await appendFile(f.path, record('event_msg', { type: 'task_started', turn_id: 'turn-next' }))
  const view = (await f.call({ thread_id: 'main-a' })).data.result.structuredContent
  assert.equal(view.observation.phase, 'started')
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => [task.lifecycleId, task.status]).sort(), [['turn-a', 'idle'], ['turn-next', 'running']])
})
