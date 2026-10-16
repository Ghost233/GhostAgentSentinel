import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { createNativeIO } from '../chatgpt/native-io.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')
const B = '00000000-0000-4000-8000-000000000001', A = '00000000-0000-4000-8000-000000000002', F = '00000000-0000-4000-8000-000000000003'
const app = '/Applications/ChatGPT.app/Contents/MacOS/ChatGPT', binary = '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'

async function fixture(t, options = {}) {
  let clock = Date.now(), projectEnabled = true, ownerTurn = 'original'
  t.mock.method(Date, 'now', () => clock)
  const hostStartedAt = clock - 20000, started = new Date(hostStartedAt).toUTCString().replace(',', '').replace(/ GMT$/, '')
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-storage-')), sessionsRoot = join(dir, 'sessions'), archive = join(dir, 'archived_sessions')
  await fs.mkdir(sessionsRoot); await fs.mkdir(archive)
  const ownerPath = join(sessionsRoot, options.wrongFilename ? 'rollout-date-' + B + '-' + A + '.jsonl' : 'rollout-date-' + B + '_' + A + '.jsonl'), forkPath = join(sessionsRoot, 'fork.jsonl')
  const header = id => ({ id, history_mode: 'paginated', cwd: '/p', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion })
  const row = (ordinal, type, payload, at = clock - 1000) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
  const start = run => ({ type: 'task_started', turn_id: run, turn_attribution: { turn_id: run, turn_trigger: 'composer' } })
  const user = (id, run, text) => ({ type: 'item_completed', thread_id: id, turn_id: run, item: { type: 'UserMessage', id: run + '-user', content: [{ type: 'text', text }] } })
  const originalHeader = header(options.headerId ?? B)
  if (options.headerSession) originalHeader.session_id = options.headerSession
  const prefix = row(0, 'session_meta', originalHeader) + row(1, 'event_msg', start('original'))
    + row(2, 'event_msg', user(B, 'original', 'Keep every change inside the requested project'))
    + row(3, 'event_msg', { type: 'item_completed', thread_id: B, turn_id: 'original', item: { type: 'CommandExecution', id: 'old-test', aggregated_output: 'Original tests passed', exit_code: 0 } })
    + row(4, 'event_msg', { type: 'task_complete', turn_id: 'original' })
  await fs.writeFile(ownerPath, prefix)
  await fs.writeFile(forkPath, row(5, 'session_meta', { ...header(F), forked_from_id: B, forked_from_ordinal_exclusive: 5, history_base: { thread_id: A, end_ordinal_exclusive: 5, end_byte_offset: Buffer.byteLength(prefix) } })
    + row(6, 'event_msg', start('fork')) + row(7, 'event_msg', user(F, 'fork', 'Inspect this independent fork request'))
    + row(8, 'event_msg', { type: 'item_completed', thread_id: F, turn_id: 'fork', item: { type: 'AgentMessage', id: 'fork-output', content: [{ type: 'Text', text: 'Inspecting only the fork request' }] } }))
  const metadata = id => ({ id: id === F ? F : B, sessionId: id === F ? F : B, path: id === F ? forkPath : ownerPath, cwd: '/p', parentThreadId: null, forkedFromId: id === F ? B : null, source: 'vscode', originator: 'Codex Desktop', cliVersion: 'historical-creation-version', historyMode: 'paginated' })
  const calls = [], socketPath = join(tmpdir(), 'ss-' + randomUUID().slice(0, 8) + '.sock'), unix = createServer(), ws = new WebSocketServer({ server: unix })
  ws.on('connection', connection => connection.on('message', bytes => {
    const message = JSON.parse(bytes); if (!message.id) return
    calls.push({ method: message.method, params: message.params })
    let result
    if (message.method === 'initialize') result = { serverInfo: { name: 'controlled-native-storage' } }
    if (message.method === 'thread/read') { const thread = metadata(message.params.threadId); options.metadata?.(thread, message.params.threadId); result = { thread } }
    if (message.method === 'thread/turns/list') result = { data: [{ id: message.params.threadId === F ? 'fork' : ownerTurn }], nextCursor: null }
    if (message.method === 'config/read') result = { config: { plugins: { 'sentinel@test': { enabled: projectEnabled } } }, layers: [{ name: { type: 'user' }, config: { plugins: { 'sentinel@test': { enabled: projectEnabled } } } }] }
    connection.send(JSON.stringify({ id: message.id, result }))
  }))
  await new Promise(resolve => unix.listen(socketPath, resolve))
  let server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }), core = await connectCore({ dataDir: join(dir, 'core') }), adapter, deliveryLost = false
  const open = async () => {
    const native = createNativeIO({ pluginId: 'sentinel@test', socketPath, profileSample: async () => supportedProfile, processSample: async () => [{ pid: 10, ppid: 1, command: app, started }, { pid: 11, ppid: 10, command: binary + ' app-server', started }] })
    const wrapped = { ...core }
    if (options.dropBefore || options.lostAck) wrapped.ingest = async (...args) => {
      if (options.dropBefore && !deliveryLost && args[1].tasks.some(task => task.id === B)) { deliveryLost = true; throw new Error('controlled delivery loss before HTTP') }
      const value = await core.ingest(...args)
      if (options.lostAck && !deliveryLost && args[1].events?.some(event => event.data?.forkReceipt)) { deliveryLost = true; throw new Error('controlled acknowledgement loss after HTTP') }
      return value
    }
    if (options.legacyCore) wrapped.health = async () => { const value = await core.health(); return { ...value, features: value.features.filter(feature => feature !== 'chatgpt-fork-storage/1') } }
    adapter = await startChatGPTAdapter({ core: wrapped, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'storage-install', token: 'a'.repeat(64), pollMs: 0, collectMaterials: true, now: () => clock })
  }
  await open()
  t.after(async () => { await adapter?.close(); await core.close(); await server.close(); for (const peer of ws.clients) peer.terminate(); await new Promise(resolve => ws.close(resolve)); await new Promise(resolve => unix.close(resolve)); await fs.rm(socketPath, { force: true }); await fs.rm(dir, { recursive: true, force: true }) })
  const read = async (id = F) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'a'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: id } } }) })
    assert.equal(response.status, 200); const value = await response.json(); assert.equal(value.result.isError, undefined); return value.result.structuredContent
  }
  const catchUp = async id => { let value; for (let n = 0; n < 40; n++) { value = await read(id); if (value.observation.coverage !== 'catching-up') break } return value }
  return { read, catchUp, calls, ownerPath, forkPath, dir, setProject(value) { projectEnabled = value }, advance(ms) { clock += ms }, get clock() { return clock }, get core() { return core }, async nextOwner() { ownerTurn = 'new-original'; await fs.appendFile(ownerPath, row(5, 'event_msg', start(ownerTurn)) + row(6, 'event_msg', user(B, ownerTurn, 'Inspect a genuinely new original request'))); return catchUp(B) }, async restart() { await adapter.close(); await core.close(); await server.close(); server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') }); await open() } }
}

test('native storage key reaches its separately qualified owner and preserves both identities through core HTTP and MCP', async t => {
  const f = await fixture(t), view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(view.forkProvenance.complete, true)
  const snapshot = await f.core.snapshot(), task = snapshot.tasks.find(task => task.id === F)
  assert.equal(task.currentRequest, 'Inspect this independent fork request')
  assert.deepEqual(task.instructionContext.gaps, [])
  assert.equal(task.instructionContext.userLimitRefs.length, 1)
  const boundary = task.events.find(event => event.data?.forkReceipt), inherited = task.events.find(event => event.data?.history && event.kind === 'input')
  assert.equal(boundary.data.sourceThreadId, A)
  assert.equal(boundary.data.sourceOwnerThreadId, B)
  assert.equal(inherited.data.sourceThreadId, B)
  assert.equal(inherited.authorizationEligible, false)
  assert.ok(inherited.data.nativeRecordId)
  assert.equal(task.events.find(event => event.kind === 'input' && !event.data.history).authorizationEligible, true)
  assert.ok(task.events.some(event => event.text === 'Original tests passed' && event.data.history && event.data.inherited))
  assert.equal(snapshot.tasks.every(task => task.id === F && !task.parentId), true)
  assert.ok(f.calls.some(call => call.method === 'thread/read' && call.params.threadId === A))
  assert.ok(f.calls.some(call => call.method === 'thread/read' && call.params.threadId === B))
})

test('a matching session, project or creation relation cannot replace the native storage-owner proof', async t => {
  const cases = {
    'storage-header-with-owner-session': { headerId: A, headerSession: B },
    'unrelated-native-header': { headerId: F },
    'unobserved-filename-separator': { wrongFilename: true },
    'different-storage-path': { metadata(thread, requested) { if (requested === A) thread.path = thread.path.replace('.jsonl', '-other.jsonl') } },
    'canonical-id-mismatch': { metadata(thread, requested) { if (requested === B) thread.id = A } },
    'canonical-child': { metadata(thread, requested) { if (requested === B) thread.parentThreadId = F } },
  }
  for (const [name, options] of Object.entries(cases)) await t.test(name, async t => {
    const f = await fixture(t, options), view = await f.catchUp(F), snapshot = await f.core.snapshot()
    assert.notEqual(view.forkProvenance?.complete, true)
    assert.equal(snapshot.tasks.flatMap(task => task.events).some(event => event.data?.history), false)
    assert.equal(snapshot.tasks.some(task => task.id === B || task.id === A), false)
  })
})

test('a nonfork original and its storage-key fork retain one native first capture while keeping their request scopes', async t => {
  const f = await fixture(t)
  await f.catchUp(B)
  const original = (await f.core.snapshot()).tasks.find(task => task.id === B), first = original.events.find(event => event.kind === 'input')
  assert.equal(original.currentRequest, 'Keep every change inside the requested project')
  assert.ok(first.data.nativeRecordId)
  f.advance(500)
  await f.catchUp(F)
  const snapshot = await f.core.snapshot(), fork = snapshot.tasks.find(task => task.id === F), inherited = snapshot.tasks.flatMap(task => task.events).find(event => event.taskId === F && event.kind === 'input' && event.data?.history)
  assert.ok(inherited, JSON.stringify(fork.instructionContext))
  assert.equal(inherited.data.nativeRecordId, first.data.nativeRecordId)
  assert.equal(inherited.capturedAt, first.capturedAt)
  assert.equal(inherited.text, first.text)
  assert.equal(inherited.evidence, first.evidence)
  assert.equal(inherited.authorizationEligible, false)
  assert.equal(original.events.find(event => event.id === first.id).authorizationEligible, true)
  assert.deepEqual(fork.instructionContext.gaps, [])
  assert.equal(fork.currentRequest, 'Inspect this independent fork request')
  assert.equal(fork.status, 'running')
  assert.equal(snapshot.tasks.find(task => task.id === B).status, 'idle')
})

test('native first captures survive both observer orders and core/adapter recovery, while a new record gets its own clock', async t => {
  for (const order of [[B, F], [F, B]]) await t.test(order[0] === B ? 'original-first' : 'fork-first', async t => {
    const f = await fixture(t)
    await f.catchUp(order[0])
    const first = (await f.core.snapshot()).tasks.flatMap(task => task.events).find(event => event.text === 'Keep every change inside the requested project')
    f.advance(500); await f.catchUp(order[1]); await f.restart()
    await f.catchUp(B); await f.catchUp(F)
    const snapshot = await f.core.snapshot(), copies = snapshot.tasks.flatMap(task => task.events).filter(event => event.data?.nativeRecordId === first.data.nativeRecordId)
    assert.equal(copies.length, 2)
    assert.ok(copies.every(event => event.capturedAt === first.capturedAt && event.text === first.text && event.evidence === first.evidence))
    assert.equal(copies.filter(event => event.authorizationEligible).length, 1)
    assert.deepEqual(snapshot.tasks.find(task => task.id === F).instructionContext.gaps, [])
    f.advance(500); await f.nextOwner()
    const next = (await f.core.snapshot()).tasks.find(task => task.id === B && task.lifecycleId === 'new-original').events.find(event => event.kind === 'input')
    assert.equal(next.capturedAt, f.clock)
    assert.notEqual(next.data.nativeRecordId, first.data.nativeRecordId)
    assert.equal(next.text, 'Inspect a genuinely new original request')
  })
})

test('an expired native record cannot acquire a fresh clock in the other observer', async t => {
  for (const order of [[B, F], [F, B]]) await t.test(order[0] === B ? 'original-before-expiry' : 'fork-before-expiry', async t => {
    const f = await fixture(t); await f.catchUp(order[0])
    const first = (await f.core.snapshot()).tasks.flatMap(task => task.events).find(event => event.text === 'Keep every change inside the requested project')
    f.advance(86400001)
    await f.catchUp(order[1])
    const snapshot = await f.core.snapshot()
    assert.equal(snapshot.tasks.flatMap(task => task.events).some(event => event.data?.nativeRecordId === first.data.nativeRecordId), false)
    assert.equal(snapshot.tasks.some(task => task.id === order[1] && task.instructionContext?.gaps?.length === 0 && task.currentRequest === first.text), false)
    assert.ok((await f.core.diagnostics()).retained.hierarchyEvidenceIdentities <= 10000)
  })
})

test('a core without storage-owner capability is stopped before material ingestion', async t => {
  const f = await fixture(t, { legacyCore: true }), view = await f.catchUp(F)
  assert.equal(view.observation.gap, 'core-capability-unavailable')
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  assert.equal(f.calls.some(call => call.method === 'thread/read'), false)
})

test('missing, wrong or malformed mapped owners cannot silently establish inherited context through HTTP', async t => {
  const f = await fixture(t); await f.catchUp(F)
  const snapshot = await f.core.snapshot(), current = snapshot.tasks[0]
  for (const value of [undefined, A, F, {}]) await t.test(value === undefined ? 'missing-owner' : typeof value === 'object' ? 'malformed-owner' : 'wrong-owner-' + value.slice(-1), async () => {
    const source = await f.core.registerSource({ registrationId: 'copied-' + JSON.stringify(value), hostRunId: 'storage-host', name: 'Copied proof', policy: 'chatgpt-main/1' })
    const batch = { tasks: [{ id: F, sessionId: F, lifecycleId: current.lifecycleId, title: 'Fork', status: 'running' }], events: structuredClone(current.events), hierarchyContext: structuredClone(snapshot.sources[0].hierarchyContext) }
    const boundary = batch.events.find(event => event.data?.forkReceipt)
    if (value === undefined) delete boundary.data.sourceOwnerThreadId
    else boundary.data.sourceOwnerThreadId = value
    if (typeof value === 'object') await assert.rejects(f.core.ingest(source, batch))
    else {
      await f.core.ingest(source, batch)
      const task = (await f.core.snapshot()).tasks.find(task => task.sourceKey === source)
      assert.equal(task.currentRequest, 'Inspect this independent fork request')
      assert.equal(task.instructionContext.userLimitRefs.length, 0)
      assert.ok(task.instructionContext.gaps.includes('fork_boundary_chain_missing'))
    }
  })
})

test('old material and fork checkpoints are refused without refreshing first captures', async t => {
  for (const id of [B, F]) await t.test(id === B ? 'old-material-origin' : 'old-fork-receipt', async t => {
    const f = await fixture(t); const view = await f.catchUp(id)
    const snapshot = await f.core.snapshot(), original = snapshot.tasks.flatMap(task => task.events).map(event => [event.data?.nativeRecordId, event.capturedAt])
    const cursorPath = join(f.dir, 'adapter/cursors', view.identity.cursorKey + '.json'), cursor = JSON.parse(await fs.readFile(cursorPath, 'utf8'))
    assert.ok(Buffer.byteLength(JSON.stringify(cursor)) <= 16384)
    if (id === B) delete cursor.reader.nativeOriginVersion
    else cursor.reader.receiptVersion = 1
    await fs.writeFile(cursorPath, JSON.stringify(cursor)); f.advance(500); await f.restart()
    const rejected = await f.read(id)
    assert.equal(rejected.observation.gap, 'cursor-invalid')
    const restored = (await f.core.snapshot()).tasks.flatMap(task => task.events)
    for (const [origin, at] of original) assert.ok(restored.some(event => event.data?.nativeRecordId === origin && event.capturedAt === at))
  })
})

test('a mapped prefix replays its same proof and first clocks after a lost HTTP acknowledgement', async t => {
  const f = await fixture(t, { lostAck: true }); await f.read(F)
  const first = (await f.core.snapshot()).tasks.flatMap(task => task.events).map(event => [event.data?.nativeRecordId, event.capturedAt])
  assert.ok(first.length > 0)
  f.advance(500); const view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.deepEqual(task.instructionContext.gaps, [])
  for (const [origin, at] of first) assert.ok(task.events.some(event => event.data?.nativeRecordId === origin && event.capturedAt === at))
})

test('an earlier native capture delivered after the fork copy keeps the earliest source time in both scopes', async t => {
  const f = await fixture(t, { dropBefore: true }), earliest = f.clock
  await f.read(B)
  f.advance(500); await f.catchUp(F)
  const firstAccepted = (await f.core.snapshot()).tasks.flatMap(task => task.events).find(event => event.text === 'Keep every change inside the requested project')
  assert.equal(firstAccepted.capturedAt, f.clock)
  await f.catchUp(B)
  const copies = (await f.core.snapshot()).tasks.flatMap(task => task.events).filter(event => event.data?.nativeRecordId === firstAccepted.data.nativeRecordId)
  assert.equal(copies.length, 2)
  assert.ok(copies.every(event => event.capturedAt === earliest))
})
