import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const record = (type, payload, at) => JSON.stringify({ type, payload, timestamp: new Date(at).toISOString() }) + '\n'

async function fixture(t, { pollMs = 0 } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-discovery-')), born = Date.now() - 10000
  const rows = new Map(), facts = { enabled: true, host: true, pages: 0, samples: 0, reads: [], configurations: [], projects: new Map(), pageFailure: false }
  async function add(id, extra = {}) {
    const path = join(dir, id + '.jsonl')
    await writeFile(path, record('session_meta', { id }, born + 2000) + record('event_msg', { type: 'task_started', turn_id: 'turn-' + id }, born + 2000))
    const thread = { id, path, cwd: join(dir, 'project-a'), originator: 'Codex Desktop', source: 'vscode', parentThreadId: null, forkedFromId: null, sessionId: 'shared-tree-id', updatedAt: Date.now() / 1000 | 0, ...extra }
    rows.set(id, thread); return thread
  }
  const native = {
    async sampleHost() { facts.samples++; return { available: facts.host, reason: 'execution-source-unavailable', hostRunId: 'discovery-native:run', hostStartedAt: born, profile } },
    async listThreads({ cursor, limit, archived }) { facts.pages++; assert.ok(limit <= 50); if (facts.pageFailure) throw new Error('controlled metadata failure'); const all = [...rows.values()].filter(thread => Boolean(thread.archived) === archived); return { data: cursor ? all.slice(1) : all.slice(0, 1), nextCursor: cursor || all.length <= 1 ? null : 'remaining' } },
    async readThread(id) { facts.reads.push(id); return rows.has(id) ? { thread: structuredClone(rows.get(id)), anchorTurnId: 'turn-' + id } : null },
    async readEnablement(cwd) { facts.configurations.push(cwd); return { user: facts.enabled ? 'enabled' : 'disabled', effective: facts.projects.get(cwd) ?? 'enabled' } },
    async close() {},
  }
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') })
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'discovery-installation', token: 'a'.repeat(64), pollMs })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const read = async id => (await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'a'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: id } } }) }).then(value => value.json())).result.structuredContent
  return { dir, rows, facts, native, core, adapter, add, read }
}

test('background directory pagination discovers existing and new independent local main chats without opening panels', async t => {
  const f = await fixture(t)
  await f.add('main-a'); await f.add('main-b')
  await f.adapter.refresh()
  let snapshot = await f.core.snapshot()
  assert.deepEqual(snapshot.tasks.map(task => task.id).sort(), ['main-a', 'main-b'])
  assert.equal(snapshot.sources.length, 1)
  assert.ok(snapshot.tasks.every(task => task.status === 'running' && task.currentRequest === ''))
  assert.equal(f.facts.samples, 2, 'one fresh before/after host pair must cover the collection batch')
  await f.add('main-new')
  await f.adapter.refresh()
  snapshot = await f.core.snapshot()
  assert.deepEqual(snapshot.tasks.map(task => task.id).sort(), ['main-a', 'main-b', 'main-new'])
  const view = await f.read('main-new')
  assert.equal(view.identity.threadId, 'main-new')
  assert.equal(view.discovery.counts.main, 3)
  assert.equal(view.discovery.directoryComplete, true)
  assert.equal(view.discovery.complete, false, 'cached directory completion does not prove all execution domains are discovered')
})

test('twenty objects share fresh host and project samples and directory heartbeats do not manufacture activity', async t => {
  const f = await fixture(t)
  for (let index = 0; index < 20; index++) await f.add('main-' + index, { cwd: join(f.dir, index % 2 ? 'project-a' : 'project-b') })
  await f.adapter.refresh()
  const first = await f.core.snapshot()
  assert.equal(first.tasks.length, 20)
  assert.equal(f.facts.samples, 2)
  assert.equal(f.facts.configurations.filter(cwd => cwd === null).length, 2)
  assert.equal(f.facts.configurations.filter(cwd => cwd === join(f.dir, 'project-a')).length, 2)
  assert.equal(f.facts.configurations.filter(cwd => cwd === join(f.dir, 'project-b')).length, 2)
  const activity = first.tasks.map(task => [task.id, task.lastActivity]).sort()
  await f.adapter.refresh()
  const second = await f.core.snapshot()
  assert.equal(f.facts.samples, 4)
  assert.deepEqual(second.tasks.map(task => [task.id, task.lastActivity]).sort(), activity)
  assert.equal((await f.core.diagnostics()).retained.events, 20)
})

test('directory exclusions remain visible without reading fork, child, foreign or unknown materials', async t => {
  const f = await fixture(t)
  await f.add('ordinary')
  await f.add('fork', { forkedFromId: 'ordinary' })
  await f.add('child', { parentThreadId: 'ordinary', source: { subAgent: { thread_spawn: {} } } })
  await f.add('remote', { cwd: 'ssh://test-host/project' })
  await f.add('cloud', { path: 'https://test-work/main.jsonl' })
  await f.add('unknown', { environments: [{ environmentId: 'opaque-environment-id' }] })
  await f.adapter.refresh()
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => task.id), ['ordinary'])
  assert.ok(f.facts.reads.every(id => id === 'ordinary'))
  const coverage = f.adapter.discovery()
  assert.deepEqual(coverage.counts, { main: 1, fork: 1, child: 1, remote: 0, cloud: 0, unknown: 3 })
  assert.ok(coverage.gaps.includes('fork-coverage-pending'))
  assert.ok(coverage.gaps.includes('unclassified-directory-entries'))
  assert.equal(coverage.complete, false)
})

test('project disable revokes only that project, whereas user disable stops every discovered chat', async t => {
  const f = await fixture(t), projectA = join(f.dir, 'project-a'), projectB = join(f.dir, 'project-b')
  await f.add('main-a', { cwd: projectA }); await f.add('main-b', { cwd: projectB })
  await f.adapter.refresh()
  f.facts.projects.set(projectA, 'disabled')
  await f.adapter.refresh()
  let snapshot = await f.core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.id === 'main-a').status, 'unknown')
  assert.equal(snapshot.tasks.find(task => task.id === 'main-b').status, 'running')
  assert.equal(snapshot.sources[0].status, 'connected')
  f.facts.enabled = false
  const reads = f.facts.reads.length, pages = f.facts.pages
  await f.adapter.refresh()
  snapshot = await f.core.snapshot()
  assert.ok(snapshot.tasks.every(task => task.status === 'unknown'))
  assert.equal(snapshot.sources[0].status, 'disconnected')
  assert.equal(f.facts.reads.length, reads)
  assert.equal(f.facts.pages, pages)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('a failed directory page reports incomplete coverage and later bounded refresh recovers new objects', async t => {
  const f = await fixture(t)
  await f.add('existing'); await f.adapter.refresh()
  await f.add('new-main')
  f.facts.pageFailure = true
  await f.adapter.refresh()
  let coverage = f.adapter.discovery()
  assert.equal(coverage.state, 'unavailable')
  assert.equal(coverage.directoryComplete, false)
  assert.ok(coverage.gaps.includes('directory-page-failed'))
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => task.id), ['existing'])
  f.facts.pageFailure = false
  await f.adapter.refresh()
  coverage = f.adapter.discovery()
  assert.equal(coverage.directoryComplete, true)
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => task.id).sort(), ['existing', 'new-main'])
})

test('a fresh after sample invalidates every staged record when the execution domain disappears', async t => {
  const f = await fixture(t)
  for (let index = 0; index < 16; index++) await f.add('main-' + index)
  await f.adapter.refresh()
  await appendFile(f.rows.get('main-0').path, record('event_msg', { type: 'task_complete', turn_id: 'turn-main-0' }, Date.now()))
  let checks = 0
  const before = f.native.sampleHost
  f.native.sampleHost = async () => { const sample = await before(); return ++checks === 1 ? sample : { ...sample, available: false } }
  await f.adapter.refresh()
  const snapshot = await f.core.snapshot()
  assert.equal(checks, 2)
  assert.equal(snapshot.sources[0].status, 'disconnected')
  assert.ok(snapshot.tasks.every(task => task.status === 'unknown'))
  assert.equal(snapshot.tasks.find(task => task.id === 'main-0').events.some(event => event.data?.nativeType === 'task_complete'), false)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('stable bounded scan cycles revisit the head and include archived main chats under active updates', async t => {
  const f = await fixture(t)
  for (let index = 1; index <= 8; index++) await f.add('main-' + index, { createdAt: index })
  await f.add('archived-main', { createdAt: 9, archived: true })
  f.native.listThreads = async ({ cursor, limit, archived }) => {
    f.facts.pages++; assert.ok(limit <= 50)
    const all = [...f.rows.values()].filter(thread => Boolean(thread.archived) === archived).sort((a, b) => b.createdAt - a.createdAt)
    const start = cursor ? all.findIndex(thread => 'after-' + thread.id === cursor) + 1 : 0
    const data = all.slice(start, start + 1)
    return { data, nextCursor: start + data.length < all.length ? 'after-' + data[0].id : null }
  }
  let pages = f.facts.pages
  await f.adapter.refresh()
  assert.ok(f.facts.pages - pages <= 4)
  assert.equal(f.adapter.discovery().directoryComplete, false)
  assert.ok(f.adapter.discovery().gaps.includes('directory-page-budget'))
  await f.add('new-head', { createdAt: 100 })
  for (const thread of f.rows.values()) thread.updatedAt = 1000 - thread.createdAt
  for (let cycle = 0; cycle < 4; cycle++) {
    pages = f.facts.pages; await f.adapter.refresh(); assert.ok(f.facts.pages - pages <= 4)
    if (f.adapter.discovery().directoryComplete) break
  }
  assert.equal(f.adapter.discovery().directoryComplete, true)
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => task.id).sort(), [...f.rows.keys()].sort())
  assert.equal(f.adapter.discovery().complete, false)
})

test('automatic polling finds new main chats without calls from the current panel', async t => {
  const f = await fixture(t, { pollMs: 15 })
  await f.add('automatic-main')
  const deadline = Date.now() + 2000
  while (!(await f.core.snapshot()).tasks.some(task => task.id === 'automatic-main')) { assert.ok(Date.now() < deadline); await delay(10) }
  await f.adapter.close()
  const diagnostics = f.adapter.diagnostics()
  for (const key of ['trackedThreads', 'openReaders', 'pendingOperations', 'refreshTimers', 'queuedBatches', 'directoryEntries', 'directoryPending', 'directoryCursor', 'activeRequests', 'transports', 'sockets']) assert.equal(diagnostics[key], 0, key)
})

test('closing during a late directory response seals the result and releases the discovery queue', async t => {
  const f = await fixture(t)
  const late = await f.add('late-main')
  let release, entered
  const waiting = new Promise(resolve => { entered = resolve })
  f.native.listThreads = async () => { entered(); return new Promise(resolve => { release = resolve }) }
  f.native.close = async () => { release?.({ data: [late], nextCursor: null }) }
  const refresh = f.adapter.refresh()
  await waiting
  await f.adapter.close(); await refresh
  assert.equal((await f.core.snapshot()).tasks.length, 0)
  const diagnostics = f.adapter.diagnostics()
  for (const key of ['trackedThreads', 'openReaders', 'pendingOperations', 'refreshTimers', 'queuedBatches', 'directoryEntries', 'directoryPending', 'directoryCursor']) assert.equal(diagnostics[key], 0, key)
})

test('unverified panel identities do not occupy the bounded observation registry', async t => {
  const f = await fixture(t)
  for (let index = 0; index < 4; index++) assert.equal((await f.read('unverified-' + index)).identity.threadId, null)
  assert.equal(f.adapter.diagnostics().trackedThreads, 0)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('the verified local environment literal remains eligible while opaque selected environments stay unclassified', async t => {
  const f = await fixture(t)
  await f.add('known-local', { environments: [{ environmentId: 'local', cwd: join(f.dir, 'project-a'), runtimeWorkspaceRoots: [] }] })
  await f.add('opaque-selected', { environments: [{ environmentId: 'opaque-selection', cwd: join(f.dir, 'project-a'), runtimeWorkspaceRoots: [] }] })
  await f.adapter.refresh()
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => task.id), ['known-local'])
  assert.equal(f.adapter.discovery().counts.main, 1)
  assert.equal(f.adapter.discovery().counts.unknown, 1)
  assert.equal(f.facts.reads.includes('opaque-selected'), false)
})

test('expired absent directory identities release cached observations and reader handles', async t => {
  let clock = Date.now()
  t.mock.method(Date, 'now', () => clock)
  const f = await fixture(t)
  await f.add('expired-main'); await f.adapter.refresh()
  assert.equal(f.adapter.diagnostics().directoryEntries, 1)
  f.rows.clear(); clock += 86400001
  await f.adapter.refresh()
  assert.equal(f.adapter.diagnostics().directoryEntries, 0)
  assert.equal(f.adapter.diagnostics().trackedThreads, 0)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('opaque selected environment IDs never classify a host from ssh, cloud or URL-shaped labels', async t => {
  const f = await fixture(t)
  await f.add('known-local')
  for (const [index, environmentId] of ['ssh://opaque', 'cloud://opaque', 'remote://opaque', 'https://opaque'].entries()) await f.add('opaque-' + index, { environments: [{ environmentId, cwd: join(f.dir, 'project-a') }] })
  await f.adapter.refresh()
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => task.id), ['known-local'])
  assert.deepEqual(f.adapter.discovery().counts, { main: 1, fork: 0, child: 0, remote: 0, cloud: 0, unknown: 4 })
  assert.ok(f.adapter.discovery().gaps.includes('unclassified-directory-entries'))
  assert.ok(f.facts.reads.every(id => id === 'known-local'))
})

test('unconfirmed panel visits during global qualification failures cannot exhaust the registry or erase confirmed chat history', async t => {
  for (const mode of ['host-unavailable', 'disabled', 'user-unknown', 'effective-unknown']) await t.test(mode, async t => {
    const f = await fixture(t)
    await f.add('retained-main')
    assert.equal((await f.read('retained-main')).identity.threadId, 'retained-main')
    const original = (await f.core.snapshot()).tasks.find(task => task.id === 'retained-main').events[0]
    const readEnablement = f.native.readEnablement
    if (mode === 'host-unavailable') f.facts.host = false
    else f.native.readEnablement = async () => mode === 'disabled' ? { user: 'disabled', effective: 'disabled' } : mode === 'user-unknown' ? { user: 'unknown', effective: 'unknown' } : { user: 'enabled', effective: 'unknown' }
    for (let index = 0; index < 256; index++) {
      const invalid = await f.adapter.read({ thread_id: 'unconfirmed-' + index })
      assert.notEqual(invalid.observation.gap, 'observation-capacity-reached')
    }
    assert.equal(f.adapter.diagnostics().trackedThreads, 1, 'only the previously confirmed observation may retain a registry entry')
    assert.equal(f.adapter.diagnostics().openReaders, 0)
    assert.equal((await f.core.snapshot()).tasks.length, 1)
    f.facts.host = true; f.native.readEnablement = readEnablement
    await f.add('new-main')
    const recovered = await f.read('new-main')
    assert.equal(recovered.identity.threadId, 'new-main')
    assert.equal(recovered.observation.status, 'running')
    assert.equal((await f.read('retained-main')).identity.threadId, 'retained-main')
    const retained = (await f.core.snapshot()).tasks.find(task => task.id === 'retained-main').events.find(event => event.id === original.id)
    assert.equal(retained.capturedAt, original.capturedAt)
    assert.equal(f.adapter.diagnostics().trackedThreads, 2)
    await f.adapter.close()
    for (const field of ['trackedThreads', 'openReaders', 'pendingOperations', 'queuedBatches', 'refreshTimers', 'directoryEntries', 'directoryPending']) assert.equal(f.adapter.diagnostics()[field], 0, field)
  })
})
