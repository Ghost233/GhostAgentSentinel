import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createNativeIO } from '../chatgpt/native-io.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')
const id = number => '00000000-0000-4000-8000-' + number.toString(16).padStart(12, '0')
const B = id(1), C = id(2), F = id(3), keys = [16, 17, 18, 19].map(id)
const app = '/Applications/ChatGPT.app/Contents/MacOS/ChatGPT', binary = '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'

async function fixture(t, mutation = null, { generations = keys.length, sharedOwner = true, original = true, lostAck = false, legacyLineage = false, cwdBytes = 8, historyPaddingBytes = 0, failBootstrap = null, bootstrapError = null, onBootstrap = null } = {}) {
  const records = Array.from({ length: generations }, (_, index) => id(16 + index)), owners = records.map((record, index) => sharedOwner || index === generations - 1 ? B : record), project = '/' + 'p'.repeat(cwdBytes - 1)
  let clock = Date.now(), changed = false, pumpBytes = 0, maximumPumpBytes = 0, maximumReaders = 0, metadataCalls = 0, maximumMetadataCalls = 0, ownerTurn = 'B-' + (generations - 1)
  let dir, socketPath, unix, ws, server, core, adapter, cleanupPromise
  const opened = new Set(), natives = new Set()
  const cleanup = () => cleanupPromise ??= (async () => {
    const failures = [], attempt = async operation => { try { await operation() } catch (error) { failures.push(error) } }
    await attempt(() => adapter?.close())
    for (const native of natives) await attempt(() => native.close())
    await attempt(() => core?.close())
    await attempt(() => server?.close())
    if (ws) {
      for (const peer of ws.clients) await attempt(() => peer.close())
      await attempt(() => new Promise((resolve, reject) => ws.close(error => error && error.code !== 'ERR_SERVER_NOT_RUNNING' ? reject(error) : resolve())))
    }
    if (unix?.listening) await attempt(() => new Promise((resolve, reject) => unix.close(error => error ? reject(error) : resolve())))
    if (socketPath) await attempt(() => fs.rm(socketPath, { force: true }))
    if (dir) await attempt(() => fs.rm(dir, { recursive: true, force: true }))
    await attempt(() => assert.equal(opened.size, 0))
    if (failures.length) throw new AggregateError(failures, 'lineage fixture cleanup failed')
  })()
  t.after(cleanup)
  try {
  t.mock.method(Date, 'now', () => clock)
  dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-lineage-'))
  const sessionsRoot = join(dir, 'sessions'), archive = join(dir, 'archived_sessions')
  await fs.mkdir(sessionsRoot); await fs.mkdir(archive)
  const row = (ordinal, type, payload) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(clock - 1000).toISOString() }) + '\n'
  const header = owner => ({ id: owner, cwd: project, originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, history_mode: 'paginated' })
  const start = run => ({ type: 'task_started', turn_id: run, turn_attribution: { turn_id: run, turn_trigger: 'composer' } })
  const user = (owner, run, text) => ({ type: 'item_completed', thread_id: owner, turn_id: run, item: { type: 'UserMessage', id: run + '-user', content: [{ type: 'text', text }] } })
  const paths = records.map((key, index) => join(sessionsRoot, 'rollout-' + owners[index] + (sharedOwner ? '_' + key : '') + '.jsonl')), originalPath = join(sessionsRoot, 'rollout-' + C + '.jsonl'), currentPath = join(sessionsRoot, 'fork.jsonl')
  let bytes = original ? row(0, 'session_meta', header(C)) + row(1, 'event_msg', start('C')) + row(2, 'event_msg', user(C, 'C', 'Keep the original project restriction')) + row(3, 'event_msg', { type: 'task_complete', turn_id: 'C', ...(historyPaddingBytes ? { ignored_padding: 'x'.repeat(historyPaddingBytes) } : {}) }) : ''
  if (original) await fs.writeFile(originalPath, bytes)
  let ordinal = original ? 4 : 0, parentKey = original ? C : null
  for (let index = 0; index < records.length; index++) {
    const run = 'B-' + index, inheritedBytes = Buffer.byteLength(bytes)
    bytes = row(ordinal, 'session_meta', { ...header(owners[index]), ...(original ? { forked_from_id: C } : {}), ...(parentKey ? { history_base: { thread_id: parentKey, end_ordinal_exclusive: ordinal, end_byte_offset: inheritedBytes } } : {}) })
      + row(ordinal + 1, 'event_msg', start(run)) + row(ordinal + 2, 'event_msg', user(owners[index], run, 'Retain native requirement ' + index)) + row(ordinal + 3, 'event_msg', { type: 'task_complete', turn_id: run })
    await fs.writeFile(paths[index], bytes); ordinal += 4; parentKey = sharedOwner ? records[index] : owners[index]
  }
  await fs.writeFile(currentPath, row(ordinal, 'session_meta', { ...header(F), forked_from_id: B, history_base: { thread_id: parentKey, end_ordinal_exclusive: ordinal, end_byte_offset: Buffer.byteLength(bytes) } })
    + row(ordinal + 1, 'event_msg', start('F')) + row(ordinal + 2, 'event_msg', user(F, 'F', 'Check this independent fork request'))
    + row(ordinal + 3, 'event_msg', { type: 'item_completed', thread_id: F, turn_id: 'F', item: { type: 'AgentMessage', id: 'F-output', content: [{ type: 'Text', text: 'New fork activity' }] } }))
  const sourcePaths = new Map(await Promise.all([...paths, ...(original ? [originalPath] : []), currentPath].map(async path => [await fs.realpath(path), path])))
  const originalOpen = fs.open
  t.mock.method(fs, 'open', async (path, ...args) => {
    const handle = await originalOpen(path, ...args)
    const sourcePath = sourcePaths.get(String(path))
    if (sourcePath) {
      opened.add(handle); maximumReaders = Math.max(maximumReaders, opened.size); const read = handle.read, close = handle.close
      handle.read = async function (...params) { const value = await read.apply(this, params); pumpBytes += value.bytesRead; if (sourcePath === paths.at(-1)) changed = true; return value }
      handle.close = async function (...params) { try { return await close.apply(this, params) } finally { opened.delete(handle) } }
    }
    return handle
  })
  const metadata = requested => {
    const index = sharedOwner ? records.indexOf(requested) : owners.indexOf(requested), owner = requested === F ? F : requested === C ? C : index >= 0 ? owners[index] : B
    const thread = { id: owner, path: owner === F ? currentPath : owner === C ? originalPath : index >= 0 ? paths[index] : paths.at(-1), cwd: project, originator: 'Codex Desktop', source: 'vscode', parentThreadId: null, forkedFromId: owner === F ? B : owner === C || !original ? null : C, historyMode: 'paginated' }
    if (mutation && (mutation.phase === 'cross-role' && requested === records[0] || mutation.phase === 'record-after' && requested === records.at(-1) && changed || mutation.phase === 'owner-after' && requested === B && changed)) mutation.change(thread)
    return thread
  }
  socketPath = join(tmpdir(), 'sl-' + randomUUID().slice(0, 8) + '.sock'); unix = createServer(); ws = new WebSocketServer({ server: unix })
  ws.on('connection', peer => peer.on('message', buffer => {
    const request = JSON.parse(buffer); if (!request.id) return
    let result
    if (request.method === 'initialize') result = { serverInfo: { name: 'controlled-lineage' } }
    if (request.method === 'thread/read') result = { thread: metadata(request.params.threadId) }
    if (request.method === 'thread/turns/list') result = { data: [{ id: request.params.threadId === F ? 'F' : request.params.threadId === C ? 'C' : ownerTurn }], nextCursor: null }
    if (request.method === 'config/read') result = { config: { plugins: { 'sentinel@test': { enabled: true } } }, layers: [{ name: { type: 'user' }, config: { plugins: { 'sentinel@test': { enabled: true } } } }] }
    peer.send(JSON.stringify({ id: request.id, result }))
  }))
  await new Promise((resolve, reject) => {
    unix.once('error', reject)
    unix.listen(socketPath, () => { unix.off('error', reject); resolve() })
  })
  const started = new Date(clock - 20000).toUTCString().replace(',', '').replace(/ GMT$/, '')
  server = await startServer({ dataDir: join(dir, 'core'), now: () => clock })
  let maxIngest = 0, deliveryLost = false
  onBootstrap?.({ stage: 'core-listening', dir, socketPath, unix, ws, server, opened })
  if (failBootstrap === 'connect') throw bootstrapError
  core = await connectCore({ dataDir: join(dir, 'core') })
  const openAdapter = async () => {
    const native = createNativeIO({ pluginId: 'sentinel@test', socketPath, profileSample: async () => supportedProfile, processSample: async () => [{ pid: 10, ppid: 1, command: app, started }, { pid: 11, ppid: 10, command: binary + ' app-server', started }] })
    natives.add(native)
    if (failBootstrap === 'adapter') { await native.readEnablement(null); onBootstrap?.({ stage: 'adapter-starting', dir, socketPath, unix, ws, server, core, native, opened }); throw bootstrapError }
    for (const method of ['readThread', 'readEnablement']) { const invoke = native[method]; native[method] = async (...args) => { metadataCalls++; return invoke(...args) } }
    const connection = { ...core, async ingest(sourceKey, batch) {
      maxIngest = Math.max(maxIngest, batch.events?.length ?? 0)
      const value = await core.ingest(sourceKey, batch)
      if (lostAck && !deliveryLost && batch.events?.some(event => event.data?.forkReceipt)) { deliveryLost = true; throw new Error('controlled lineage HTTP acknowledgement loss') }
      return value
    } }
    if (legacyLineage) connection.health = async () => { const health = await core.health(); return { ...health, features: health.features.filter(feature => feature !== 'chatgpt-fork-lineage/1') } }
    adapter = await startChatGPTAdapter({ core: connection, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'lineage-fixture', token: 'e'.repeat(64), collectMaterials: true, pollMs: 0, now: () => clock })
  }
  await openAdapter()
  const read = async (thread = F) => {
    pumpBytes = 0; metadataCalls = 0
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'e'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: thread } } }) })
    assert.equal(response.status, 200); const body = await response.json(); assert.equal(body.result.isError, undefined)
    maximumPumpBytes = Math.max(maximumPumpBytes, pumpBytes); assert.ok(pumpBytes <= 262144, 'source physical read budget')
    maximumMetadataCalls = Math.max(maximumMetadataCalls, metadataCalls)
    return body.result.structuredContent
  }
  return { get core() { return core }, get adapter() { return adapter }, dir, read, async catchUp(thread) { let view; for (let pump = 0; pump < 32; pump++) { view = await read(thread); if (['caught-up', 'retained-history'].includes(view.observation.coverage)) break } return view }, advance(ms = 500) { clock += ms }, get clock() { return clock }, get maxIngest() { return maxIngest }, get maximumPumpBytes() { return maximumPumpBytes }, get maximumReaders() { return maximumReaders }, get maximumMetadataCalls() { return maximumMetadataCalls }, get liveReaders() { return opened.size }, async restart() { await adapter.close(); await core.close(); await server.close(); assert.equal(opened.size, 0); server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') }); await openAdapter() }, async appendOwnerRequest() { ownerTurn = 'B-next'; const next = generations * 4 + Number(original) * 4; await fs.appendFile(paths.at(-1), row(next, 'event_msg', start(ownerTurn)) + row(next + 1, 'event_msg', user(B, ownerTurn, 'Inspect one new native owner request'))); return read(B) } }
  } catch (error) {
    try { await cleanup() } catch (cleanupError) { throw new AggregateError([error, cleanupError], 'lineage fixture bootstrap and cleanup failed', { cause: error }) }
    throw error
  }
}

test('lineage bootstrap failures preserve their error and release partially initialized resources before rejecting', async t => {
  for (const failBootstrap of ['connect', 'adapter']) await t.test(failBootstrap, async t => {
    const owned = {}, hooks = [], expected = Object.assign(new Error('controlled lineage bootstrap failure'), { code: 'EADDRNOTAVAIL' })
    let normalCoreClose = false, rescueUsed = false
    t.after(async () => {
      if (!normalCoreClose) {
        rescueUsed = true
        await owned.native?.close(); await owned.core?.close(); await owned.server?.close()
        if (owned.ws) { for (const peer of owned.ws.clients) peer.close(); await new Promise(resolve => owned.ws.close(resolve)) }
        if (owned.unix?.listening) await new Promise((resolve, reject) => owned.unix.close(error => error ? reject(error) : resolve()))
        if (owned.socketPath) await fs.rm(owned.socketPath, { force: true })
        if (owned.dir) await fs.rm(owned.dir, { recursive: true, force: true })
      }
      t.diagnostic(JSON.stringify({ controlledBootstrapFailure: failBootstrap, diagnosticRescueUsed: rescueUsed }))
    })
    const context = { mock: t.mock, after: hook => hooks.push(hook) }
    await assert.rejects(fixture(context, null, { generations: 1, failBootstrap, bootstrapError: expected, onBootstrap(resources) {
      Object.assign(owned, resources)
      if (resources.stage === 'core-listening') { const close = resources.server.close; resources.server.close = async () => { await close(); normalCoreClose = true } }
      assert.equal(resources.unix.listening, true)
    } }), error => error === expected)
    assert.equal(hooks.length, 1, 'cleanup must be registered before a resource starts')
    assert.equal(normalCoreClose, true, 'bootstrap rejection must wait for its owned core to close')
    assert.equal(owned.unix.listening, false); assert.equal(owned.ws.clients.size, 0); assert.equal(owned.opened.size, 0)
    if (owned.core) assert.deepEqual(owned.core.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
    if (owned.native) assert.deepEqual(owned.native.diagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0, connected: false })
    await assert.rejects(fs.stat(owned.dir), error => error.code === 'ENOENT')
    await assert.rejects(fs.stat(owned.socketPath), error => error.code === 'ENOENT')
    await hooks[0](); await hooks[0]()
  })
})

test('a shorter native lineage with identical record and canonical paths reaches the same public seam', async t => {
  const f = await fixture(t, null, { generations: 1 }), view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(view.forkProvenance.complete, true)
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(task.currentRequest, 'Check this independent fork request')
  assert.equal(task.instructionContext.userLimitRefs.length, 2)
  assert.deepEqual(task.instructionContext.gaps, [])
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maxIngest <= 16)
})

for (const [name, options] of [
  ['three physical generations with record-specific old paths', { generations: 2, original: false }],
  ['six physical generations with exact record and canonical paths', { generations: 4, sharedOwner: false }],
]) test(name + ' preserve complete and scoped native evidence', async t => {
  const f = await fixture(t, null, options), view = await f.catchUp(F), expectedUsers = options.generations + Number(options.original !== false)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(view.forkProvenance.complete, true)
  const latest = 'Retain native requirement ' + (options.generations - 1), first = (await f.core.snapshot()).tasks.find(task => task.id === F), inherited = first.events.find(event => event.text === latest)
  assert.equal(first.currentRequest, 'Check this independent fork request')
  assert.equal(first.instructionContext.userLimitRefs.length, expectedUsers)
  assert.deepEqual(first.instructionContext.gaps, [])
  assert.ok(inherited.data.nativeRecordId)
  assert.equal(inherited.authorizationEligible, false)
  assert.equal(first.events.filter(event => event.data?.forkReceipt).length, expectedUsers)
  if (options.sharedOwner !== false) assert.ok(first.events.some(event => event.data?.sourceThreadId === keys[0] && event.data.sourceOwnerThreadId === B))
  f.advance(); await f.catchUp(B)
  const snapshot = await f.core.snapshot(), owner = snapshot.tasks.find(task => task.id === B)
  assert.equal(owner.currentRequest, latest)
  const current = owner.events.find(event => event.text === inherited.text && !event.data.history)
  assert.equal(current.data.nativeRecordId, inherited.data.nativeRecordId)
  assert.equal(current.capturedAt, inherited.capturedAt)
  assert.equal(current.evidence, inherited.evidence)
  assert.equal(current.authorizationEligible, true)
  assert.equal(snapshot.tasks.find(task => task.id === F).currentRequest, 'Check this independent fork request')
  assert.equal(snapshot.tasks.some(task => keys.includes(task.id)), false)
  assert.ok(f.maxIngest <= 16)
  const cursor = JSON.parse(await fs.readFile(join(f.dir, 'adapter/cursors', view.identity.cursorKey + '.json'), 'utf8'))
  assert.ok(Buffer.byteLength(JSON.stringify(cursor)) <= 262144)
  assert.ok(f.maximumReaders <= 4, 'logical depth is independent of live file readers')
})

test('multi-generation native evidence survives both observer orders, adapter/core recovery and a genuinely new record', async t => {
  for (const order of [[B, F], [F, B]]) await t.test(order[0] === B ? 'owner-first' : 'fork-first', async t => {
    const f = await fixture(t, null, { generations: 4, sharedOwner: false })
    await f.catchUp(order[0])
    const first = (await f.core.snapshot()).tasks.flatMap(task => task.events).find(event => event.text === 'Retain native requirement 3')
    assert.ok(first)
    f.advance(); await f.catchUp(order[1]); await f.restart(); await f.catchUp(B); await f.catchUp(F)
    const snapshot = await f.core.snapshot(), copies = snapshot.tasks.flatMap(task => task.events).filter(event => event.data?.nativeRecordId === first.data.nativeRecordId)
    assert.equal(copies.length, 2)
    assert.ok(copies.every(event => event.text === first.text && event.capturedAt === first.capturedAt && event.evidence === first.evidence))
    assert.equal(copies.filter(event => event.authorizationEligible).length, 1)
    assert.equal(snapshot.tasks.find(task => task.id === F).currentRequest, 'Check this independent fork request')
    assert.equal(snapshot.tasks.find(task => task.id === B).currentRequest, 'Retain native requirement 3')
    f.advance(); await f.appendOwnerRequest(); await f.catchUp(B)
    const next = (await f.core.snapshot()).tasks.find(task => task.id === B && task.lifecycleId === 'B-next').events.find(event => event.kind === 'input')
    assert.equal(next.text, 'Inspect one new native owner request'); assert.equal(next.capturedAt, f.clock)
    assert.notEqual(next.data.nativeRecordId, first.data.nativeRecordId)
    assert.ok(f.maximumReaders <= 4); assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144)
  })
})

test('multi-generation expiry and old cursor recovery cannot refresh a native first capture', async t => {
  for (const oldCursor of [false, true]) await t.test(oldCursor ? 'rejected-old-cursor' : 'normal-checkpoint', async t => {
  const f = await fixture(t, null, { generations: 4, sharedOwner: false })
  const view = await f.catchUp(F), initial = await f.core.snapshot(), first = initial.tasks.flatMap(task => task.events).find(event => event.text === 'Retain native requirement 3')
  assert.ok(first)
  const path = join(f.dir, 'adapter/cursors', view.identity.cursorKey + '.json'), cursor = JSON.parse(await fs.readFile(path, 'utf8'))
  if (oldCursor) { cursor.reader.format = 1; cursor.reader.kind = 'fork-history/1'; await fs.writeFile(path, JSON.stringify(cursor)) }
  f.advance(); await f.restart()
  const resumed = await f.read(F)
  if (oldCursor) assert.equal(resumed.observation.gap, 'cursor-invalid')
  else assert.equal(resumed.forkProvenance.complete, true)
  f.advance(86400001); await f.catchUp(B)
  const after = await f.core.snapshot(), copies = after.tasks.flatMap(task => task.events).filter(event => event.data?.nativeRecordId === first.data.nativeRecordId)
  assert.equal(copies.length, 0, JSON.stringify({ first: { sourceKeys: initial.sources.map(source => source.key), capturedAt: first.capturedAt }, sourceKeys: after.sources.map(source => source.key), copies: copies.map(event => ({ capturedAt: event.capturedAt, taskId: event.taskId })) }))
  })
})

test('multi-generation proof replays without replacing its first capture after a lost HTTP acknowledgement', async t => {
  const f = await fixture(t, null, { generations: 4, sharedOwner: false, lostAck: true })
  for (let attempts = 0; attempts < 4; attempts++) { await f.read(F); if ((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.forkReceipt)) break }
  const original = (await f.core.snapshot()).tasks.flatMap(task => task.events).map(event => [event.data?.nativeRecordId, event.capturedAt, event.text, event.evidence])
  assert.ok(original.length)
  f.advance(); await f.restart(); const view = await f.catchUp(F)
  assert.equal(view.forkProvenance.complete, true)
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.deepEqual(task.instructionContext.gaps, [])
  for (const [record, at, text, evidence] of original) assert.ok(task.events.some(event => event.data?.nativeRecordId === record && event.capturedAt === at && event.text === text && event.evidence === evidence))
})

test('missing lineage capability stops all material reads before any native record is opened', async t => {
  const f = await fixture(t, null, { generations: 1, legacyLineage: true }), view = await f.read(F)
  assert.equal(view.observation.gap, 'core-capability-unavailable')
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  assert.equal(f.maximumReaders, 0); assert.equal(f.maximumPumpBytes, 0)
})

test('sixteen physical frames restore within one fixed fork cursor while file handles and per-pump work stay bounded', async t => {
  const f = await fixture(t, null, { generations: 14, sharedOwner: false, cwdBytes: 1024 }), view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
  const before = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(before.instructionContext.userLimitRefs.length, 15); assert.deepEqual(before.instructionContext.gaps, [])
  const serialized = await fs.readFile(join(f.dir, 'adapter/cursors', view.identity.cursorKey + '.json'), 'utf8'), cursor = JSON.parse(serialized)
  assert.ok(Buffer.byteLength(serialized) > 16384); assert.ok(Buffer.byteLength(serialized) <= 262144)
  assert.equal(cursor.reader.kind, 'fork-lineage/1'); assert.equal(cursor.reader.frames.length, 16)
  assert.doesNotMatch(serialized, /Retain native requirement|independent fork request/)
  const origins = before.events.map(event => [event.data?.nativeRecordId, event.capturedAt, event.text, event.evidence])
  await f.restart(); const restored = await f.catchUp(F)
  assert.equal(restored.forkProvenance.complete, true)
  const after = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.deepEqual(after.events.map(event => [event.data?.nativeRecordId, event.capturedAt, event.text, event.evidence]), origins)
  assert.ok(f.maximumReaders > 0 && f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maxIngest <= 16)
  assert.ok(f.maximumMetadataCalls <= 160, '144 wrapper calls plus at most 16 outer adapter qualification calls')
  t.diagnostic(JSON.stringify({ cursorBytes: Buffer.byteLength(serialized), maximumReaders: f.maximumReaders, maximumMetadataCalls: f.maximumMetadataCalls, maximumPumpBytes: f.maximumPumpBytes }))
})

test('an unfinished seventeenth frame is a visible gap and never becomes an ordinal-zero terminal', async t => {
  const f = await fixture(t, null, { generations: 15, sharedOwner: false }), view = await f.catchUp(F), snapshot = await f.core.snapshot()
  assert.notEqual(view.forkProvenance?.complete, true)
  assert.equal(snapshot.tasks.flatMap(task => task.events).some(event => event.data?.history), false)
  assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
})

test('an in-progress prefix checkpoint resumes its qualified candidate without losing ancestry or publishing buffered current material', async t => {
  const f = await fixture(t, null, { generations: 1, historyPaddingBytes: 300000 })
  await f.read(F)
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.kind === 'input'), false)
  const directory = join(f.dir, 'adapter/cursors'), files = await fs.readdir(directory)
  assert.equal(files.length, 1)
  const saved = JSON.parse(await fs.readFile(join(directory, files[0]), 'utf8'))
  assert.equal(saved.reader.kind, 'fork-lineage/1'); assert.equal(saved.reader.resolution.phase, 'prefix')
  assert.ok(saved.reader.resolution.probe.reader.offset > 0)
  assert.doesNotMatch(JSON.stringify(saved), /Retain native requirement|independent fork request|ignored_padding/)
  await f.restart(); const result = await f.catchUp(F)
  assert.equal(result.observation.coverage, 'caught-up', JSON.stringify({ observation: result.observation, health: result.health, history: result.history, diagnostics: f.adapter.diagnostics() }))
  assert.equal(result.forkProvenance.complete, true)
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(task.currentRequest, 'Check this independent fork request')
  assert.equal(task.instructionContext.userLimitRefs.length, 2); assert.deepEqual(task.instructionContext.gaps, [])
  assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
})

test('fixed fork quota and ordinary cursor default are independently enforced before restoration', async t => {
  for (const [thread, limit] of [[F, 262144], [B, 16384]]) await t.test(thread === F ? 'fork-256KiB' : 'ordinary-16KiB', async t => {
    const f = await fixture(t, null, { generations: 2, original: false }), view = await f.catchUp(thread), first = await f.core.snapshot()
    assert.ok(view.identity.cursorKey)
    const path = join(f.dir, 'adapter/cursors', view.identity.cursorKey + '.json'), cursor = JSON.parse(await fs.readFile(path, 'utf8'))
    cursor.reader.padding = 'x'.repeat(limit)
    await fs.writeFile(path, JSON.stringify(cursor)); await f.restart()
    const refused = await f.read(thread)
    assert.equal(refused.observation.gap, 'cursor-over-limit')
    const after = await f.core.snapshot()
    for (const event of first.tasks.flatMap(task => task.events)) assert.ok(after.tasks.flatMap(task => task.events).some(value => value.data?.nativeRecordId === event.data?.nativeRecordId && value.capturedAt === event.capturedAt))
  })
})

test('nonpath owner differences and independent record/owner changes never become a path-only exception', async t => {
  const changes = { id: thread => { thread.id = C }, cwd: thread => { thread.cwd = '/another-enabled-project' }, source: thread => { thread.source = 'cli' }, originator: thread => { thread.originator = 'Other Desktop' }, parentThreadId: thread => { thread.parentThreadId = C }, forkedFromId: thread => { thread.forkedFromId = F } }
  for (const phase of ['cross-role', 'record-after', 'owner-after']) for (const [field, change] of Object.entries(changes)) await t.test(phase + '/' + field, async t => {
    const f = await fixture(t, { phase, change }, { generations: 2, original: false }), view = await f.catchUp(F), snapshot = await f.core.snapshot()
    assert.notEqual(view.forkProvenance?.complete, true)
    assert.equal(snapshot.tasks.flatMap(task => task.events).some(event => event.data?.history), false)
    assert.equal(snapshot.tasks.some(task => keys.includes(task.id)), false)
  })
})

test('a record or canonical path changing after the read is refused even though the two roles may have different paths', async t => {
  for (const phase of ['record-after', 'owner-after']) await t.test(phase, async t => {
    const f = await fixture(t, { phase, change(thread) { thread.path = thread.path.replace('.jsonl', '-changed.jsonl') } }, { generations: 2, original: false }), view = await f.read(F)
    assert.notEqual(view.forkProvenance?.complete, true)
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.history), false)
  })
})
