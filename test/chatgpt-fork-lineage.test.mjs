import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import { basename, join } from 'node:path'
import { tmpdir } from 'node:os'
import { NativeJsonlSource } from '../chatgpt/native-jsonl-source.mjs'
import { zstdCompressSync } from 'node:zlib'
import { createNativeIO } from '../chatgpt/native-io.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')
const id = number => '00000000-0000-4000-8000-' + number.toString(16).padStart(12, '0')
const B = id(1), C = id(2), F = id(3), keys = [16, 17, 18, 19].map(id)
const app = '/Applications/ChatGPT.app/Contents/MacOS/ChatGPT', binary = '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'

async function fixture(t, mutation = null, { generations = keys.length, sharedOwner = true, original = true, lostAck = false, legacyLineage = false, cwdBytes = 8, historyPaddingBytes = 0, failBootstrap = null, bootstrapError = null, onBootstrap = null, nativeOriginVersion = 1, compressedOriginal = false, compressedGenerations = false, duplicateOriginal = null, originalProducer = null, compressionCapability = true, disabledProducer = false, archivedMains = false, compressedCurrent = false, oldCurrentStart = false, currentPaddingBytes = 0, currentTerminal = null, exactHistoryBytes = 0, currentTool = false, lostCurrentAck = false } = {}) {
  const records = Array.from({ length: generations }, (_, index) => id(16 + index)), owners = records.map((record, index) => sharedOwner || index === generations - 1 ? B : record), project = '/' + 'p'.repeat(cwdBytes - 1)
  let clock = Date.now(), changed = false, pumpBytes = 0, maximumPumpBytes = 0, maximumReaders = 0, metadataCalls = 0, maximumMetadataCalls = 0, ownerTurn = 'B-' + (generations - 1), growOnForkRead = false, originalGrewAfterRead = false
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
  const row = (ordinal, type, payload, at = clock - 1000) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
  const header = owner => ({ id: owner, cwd: project, originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, history_mode: 'paginated' })
  const start = run => ({ type: 'task_started', turn_id: run, turn_attribution: { turn_id: run, turn_trigger: 'composer' } })
  const user = (owner, run, text) => ({ type: 'item_completed', thread_id: owner, turn_id: run, item: { type: 'UserMessage', id: run + '-user', content: [{ type: 'text', text }] } })
  const currentRoot = archivedMains ? archive : sessionsRoot
  const paths = records.map((key, index) => join(currentRoot, 'rollout-' + owners[index] + (sharedOwner ? '_' + key : '') + '.jsonl' + (compressedGenerations ? '.zst' : '')))
  let currentPath = join(currentRoot, 'fork.jsonl' + (compressedCurrent ? '.zst' : ''))
  let originalPath = join(sessionsRoot, 'rollout-' + C + '.jsonl' + (compressedOriginal ? '.zst' : ''))
  const initialHistory = row(0, 'session_meta', header(C)) + row(1, 'event_msg', start('C')) + row(2, 'event_msg', user(C, 'C', 'Keep the original project restriction'))
  let bytes = original ? initialHistory + row(3, 'event_msg', { type: 'task_complete', turn_id: 'C', ...(historyPaddingBytes ? { ignored_padding: 'x'.repeat(historyPaddingBytes) } : {}) }) : ''
  if (exactHistoryBytes) {
    const ending = row(3, 'event_msg', { type: 'task_complete', turn_id: 'C', ignored_padding: '' }), padding = exactHistoryBytes - Buffer.byteLength(initialHistory + ending)
    assert.ok(padding >= 0)
    bytes = initialHistory + row(3, 'event_msg', { type: 'task_complete', turn_id: 'C', ignored_padding: 'x'.repeat(padding) })
    assert.equal(Buffer.byteLength(bytes), exactHistoryBytes)
  }
  if (disabledProducer) bytes = bytes.replace(project, '/disabled-producer')
  if (originalProducer) bytes = bytes.replace(supportedProfile.cliVersion, originalProducer)
  if (original) await fs.writeFile(originalPath, compressedOriginal ? zstdCompressSync(Buffer.from(bytes)) : bytes)
  if (duplicateOriginal) {
    let duplicate = duplicateOriginal === 'body' ? bytes.replace('Keep the original project restriction', 'Changed original project restriction!') : duplicateOriginal === 'header' ? bytes.replace(project, '/another') : bytes
    let encoded = zstdCompressSync(Buffer.from(duplicate)); if (duplicateOriginal === 'corrupt') encoded = encoded.subarray(0, encoded.length - 2)
    await fs.writeFile(originalPath + '.zst', encoded)
  }
  let ordinal = original ? 4 : 0, parentKey = original ? C : null
  for (let index = 0; index < records.length; index++) {
    const run = 'B-' + index, inheritedBytes = Buffer.byteLength(bytes)
    bytes = row(ordinal, 'session_meta', { ...header(owners[index]), ...(original ? { forked_from_id: C } : {}), ...(parentKey ? { history_base: { thread_id: parentKey, end_ordinal_exclusive: ordinal, end_byte_offset: inheritedBytes } } : {}) })
      + row(ordinal + 1, 'event_msg', start(run)) + row(ordinal + 2, 'event_msg', user(owners[index], run, 'Retain native requirement ' + index)) + row(ordinal + 3, 'event_msg', { type: 'task_complete', turn_id: run })
    await fs.writeFile(paths[index], compressedGenerations ? zstdCompressSync(Buffer.from(bytes)) : bytes); ordinal += 4; parentKey = sharedOwner ? records[index] : owners[index]
  }
  const currentRow = (ordinal, type, payload) => row(ordinal, type, payload, clock - (oldCurrentStart ? 30000 : 1000)), ending = currentTerminal ?? (archivedMains ? 'completed' : null)
  const forkBytes = currentRow(ordinal, 'session_meta', { ...header(F), forked_from_id: B, history_base: { thread_id: parentKey, end_ordinal_exclusive: ordinal, end_byte_offset: Buffer.byteLength(bytes) } })
    + currentRow(ordinal + 1, 'event_msg', start('F')) + currentRow(ordinal + 2, 'event_msg', user(F, 'F', 'Check this independent fork request'))
    + currentRow(ordinal + 3, 'event_msg', { type: 'item_completed', thread_id: F, turn_id: 'F', item: { type: 'AgentMessage', id: 'F-output', content: [{ type: 'Text', text: 'New fork activity' + 'p'.repeat(currentPaddingBytes) }] } })
    + (currentTool ? currentRow(ordinal + 4, 'event_msg', { type: 'item_completed', thread_id: F, turn_id: 'F', item: { type: 'CommandExecution', id: 'F-tool', aggregated_output: 'Current tool failed with exit 2', exit_code: 2 } }) : '')
    + (ending ? currentRow(ordinal + 4 + Number(currentTool), 'event_msg', { type: ending === 'completed' ? 'task_complete' : 'turn_aborted', turn_id: 'F' }) : '')
  await fs.writeFile(currentPath, compressedCurrent ? zstdCompressSync(Buffer.from(forkBytes)) : forkBytes)
  let nextCurrentOrdinal = ordinal + 4 + Number(currentTool) + Number(Boolean(ending))
  const sourcePaths = new Map(await Promise.all([...paths, ...(original ? [originalPath] : []), ...(duplicateOriginal ? [originalPath + '.zst'] : []), currentPath].map(async path => [await fs.realpath(path), path])))
  const originalOpen = fs.open
  t.mock.method(fs, 'open', async (path, ...args) => {
    const handle = await originalOpen(path, ...args)
    const sourcePath = sourcePaths.get(String(path))
    if (sourcePath) {
      opened.add(handle); maximumReaders = Math.max(maximumReaders, opened.size); const read = handle.read, close = handle.close
      handle.read = async function (...params) { const value = await read.apply(this, params); pumpBytes += value.bytesRead; if (sourcePath === paths.at(-1)) changed = true; if (growOnForkRead && sourcePath === currentPath && params[3] >= Buffer.byteLength(forkBytes)) { growOnForkRead = false; await growOriginal(false); originalGrewAfterRead = true } return value }
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
    if (request.method === 'thread/list') result = { data: request.params.archived === archivedMains ? [metadata(B), metadata(F)] : [], nextCursor: null }
    if (request.method === 'thread/turns/list') result = { data: [{ id: request.params.threadId === F ? 'F' : request.params.threadId === C ? 'C' : ownerTurn }], nextCursor: null }
    if (request.method === 'config/read') result = { config: { plugins: { 'sentinel@test': { enabled: true } } }, layers: [{ name: { type: 'user' }, config: { plugins: { 'sentinel@test': { enabled: true } } } }] }
    if (request.method === 'config/read' && disabledProducer && request.params.cwd === '/disabled-producer') result = { config: { plugins: { 'sentinel@test': { enabled: false } } }, layers: [{ name: { type: 'user' }, config: { plugins: { 'sentinel@test': { enabled: true } } } }, { name: { type: 'project' }, config: { plugins: { 'sentinel@test': { enabled: false } } } }] }
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
      if (!deliveryLost && batch.events?.some(event => lostCurrentAck ? event.data?.forkHistoryPending : lostAck && event.data?.forkReceipt)) { deliveryLost = true; throw new Error('controlled lineage HTTP acknowledgement loss') }
      return value
    } }
    if (!compressionCapability) connection.health = async () => { const health = await core.health(); return { ...health, features: health.features.filter(feature => feature !== 'chatgpt-fork-compression/1') } }
    if (legacyLineage) connection.health = async () => { const health = await core.health(); return { ...health, features: health.features.filter(feature => feature !== 'chatgpt-fork-lineage/1') } }
    adapter = await startChatGPTAdapter({ core: connection, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'lineage-fixture', token: 'e'.repeat(64), collectMaterials: true, nativeOriginVersion, pollMs: 0, now: () => clock })
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
  const archiveMains = async () => {
    for (const [index, prior] of paths.entries()) { const next = join(archive, basename(prior)); await fs.rename(prior, next); sourcePaths.set(await fs.realpath(next), next); paths[index] = next }
    const prior = currentPath; currentPath = join(archive, basename(prior)); await fs.rename(prior, currentPath); sourcePaths.set(await fs.realpath(currentPath), currentPath)
  }
  const growOriginal = async rewrite => { const bytes = await fs.readFile(originalPath, 'utf8'); await fs.writeFile(originalPath, (rewrite ? bytes.replace('Keep the original project restriction', 'Swap the original project restriction') : bytes) + row(4, 'event_msg', { type: 'task_complete', turn_id: 'C' })) }
  const appendForkOutput = () => fs.appendFile(currentPath, row(ordinal + 4, 'event_msg', { type: 'item_completed', thread_id: F, turn_id: 'F', item: { type: 'AgentMessage', id: 'F-later', content: [{ type: 'Text', text: 'New tail after ancestor growth' }] } }))
  return { async appendCurrentActivity() { const number = nextCurrentOrdinal++; await fs.appendFile(currentPath, row(number, 'event_msg', { type: 'item_completed', thread_id: F, turn_id: 'F', item: { type: 'AgentMessage', id: 'F-ongoing-' + number, content: [{ type: 'Text', text: 'Fresh activity ' + number }] } })) }, async replaceCurrent() { const bytes = await fs.readFile(currentPath); const next = currentPath + '.replacement'; await fs.writeFile(next, bytes); await fs.rename(next, currentPath) }, close: cleanup, get native() { return [...natives].at(-1) }, get core() { return core }, get adapter() { return adapter }, dir, read, archiveMains, growOriginal, appendForkOutput, armOriginalGrowth() { growOnForkRead = true }, get originalGrewAfterRead() { return originalGrewAfterRead }, async catchUp(thread) { let view; for (let pump = 0; pump < 32; pump++) { view = await read(thread); if (['caught-up', 'retained-history'].includes(view.observation.coverage) && (!view.forkProvenance || view.forkProvenance.complete)) break } return view }, advance(ms = 500) { clock += ms }, get clock() { return clock }, get maxIngest() { return maxIngest }, get maximumPumpBytes() { return maximumPumpBytes }, get maximumReaders() { return maximumReaders }, get maximumMetadataCalls() { return maximumMetadataCalls }, get liveReaders() { return opened.size }, async compressOriginal() { const prior = originalPath; originalPath += '.zst'; await fs.writeFile(originalPath, zstdCompressSync(await fs.readFile(prior))); sourcePaths.set(await fs.realpath(originalPath), originalPath); await fs.rm(prior) }, async restart() { await adapter.close(); await core.close(); await server.close(); assert.equal(opened.size, 0); server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') }); await openAdapter() }, async appendOwnerRequest() { ownerTurn = 'B-next'; const next = generations * 4 + Number(original) * 4; await fs.appendFile(paths.at(-1), row(next, 'event_msg', start(ownerTurn)) + row(next + 1, 'event_msg', user(B, ownerTurn, 'Inspect one new native owner request'))); return read(B) } }
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

test('completed main and fork records in the fixed sibling archive reach their own public checks', async t => {
  for (const nativeOriginVersion of [1, 2]) await t.test('native-origin/' + nativeOriginVersion, async t => {
  const f = await fixture(t, null, { generations: 1, original: false, nativeOriginVersion, archivedMains: true })
  await f.adapter.refresh()
  assert.equal(f.adapter.discovery().counts.main, 1); assert.equal(f.adapter.discovery().counts.fork, 1)
  for (const thread of [B, F]) {
    const view = await f.catchUp(thread)
    assert.equal(view.identity.threadId, thread)
    assert.equal(view.observation.coverage, 'retained-history', JSON.stringify(view.observation))
    assert.equal(view.observation.phase, 'completed')
  }
  const snapshot = await f.core.snapshot(), base = snapshot.tasks.find(task => task.id === B), fork = snapshot.tasks.find(task => task.id === F)
  assert.equal(base.status, 'idle'); assert.equal(fork.status, 'idle')
  assert.equal(base.currentRequest, 'Retain native requirement 0')
  assert.equal(fork.currentRequest, 'Check this independent fork request')
  assert.equal(fork.instructionContext.userLimitRefs.length, 1, JSON.stringify(fork.instructionContext))
  assert.deepEqual(fork.instructionContext.gaps, [])
  assert.ok(fork.events.some(event => event.text === base.currentRequest && event.data.history && event.data.inherited && !event.authorizationEligible))
  assert.equal(fork.events.filter(event => event.data?.forkReceipt).length, 1)
  const before = snapshot.tasks.map(task => [task.id, task.lastActivity, task.events.length])
  f.advance(); await f.catchUp(B); await f.catchUp(F)
  assert.deepEqual((await f.core.snapshot()).tasks.map(task => [task.id, task.lastActivity, task.events.length]), before, 're-observing archived completed records does not manufacture new activity')
  })
})

test('version two current main and fork compressed records reach the public seam in both fixed storage roots', async t => {
  for (const archivedMains of [false, true]) await t.test(archivedMains ? 'archived' : 'sessions', async t => {
    const f = await fixture(t, null, { generations: 1, original: false, nativeOriginVersion: 2, compressedGenerations: true, compressedCurrent: true, archivedMains })
    await f.adapter.refresh()
    assert.equal(f.adapter.discovery().counts.main, 1); assert.equal(f.adapter.discovery().counts.fork, 1)
    await f.catchUp(B)
    const view = await f.catchUp(F)
    assert.equal(view.identity.threadId, F)
    assert.equal(view.forkProvenance.complete, true)
    assert.equal(view.observation.coverage, archivedMains ? 'retained-history' : 'caught-up', JSON.stringify(view.observation))
    const snapshot = await f.core.snapshot(), base = snapshot.tasks.find(task => task.id === B), fork = snapshot.tasks.find(task => task.id === F)
    assert.equal(base.currentRequest, 'Retain native requirement 0')
    assert.equal(fork.currentRequest, 'Check this independent fork request')
    assert.equal(fork.instructionContext.userLimitRefs.length, 1)
    assert.deepEqual(fork.instructionContext.gaps, [])
    assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
  })
})

test('legacy material observation does not admit compressed current main or fork records', async t => {
  const f = await fixture(t, null, { generations: 1, original: false, nativeOriginVersion: 1, compressedGenerations: true, compressedCurrent: true, archivedMains: true })
  for (const thread of [B, F]) {
    const view = await f.read(thread)
    assert.equal(view.identity.threadId, null)
    assert.equal(view.observation.gap, 'main-identity-unverified')
  }
  assert.equal((await f.core.snapshot()).tasks.length, 0)
  assert.equal(f.liveReaders, 0)
})

test('moving verified current records into the fixed archive preserves native identities and first capture times', async t => {
  const f = await fixture(t, null, { generations: 1, original: false, nativeOriginVersion: 2 })
  await f.catchUp(B); await f.catchUp(F)
  const events = snapshot => snapshot.tasks.flatMap(task => task.events).map(event => [event.taskId, event.data.nativeRecordId, event.evidence, event.capturedAt, event.text]).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
  const before = await f.core.snapshot(), originalEvents = events(before)
  await f.archiveMains(); f.advance(1000)
  for (const thread of [B, F]) {
    let view
    for (let pump = 0; pump < 32; pump++) {
      view = await f.read(thread)
      if (!view.observation.gap && ['caught-up', 'retained-history'].includes(view.observation.coverage)) break
    }
    assert.equal(view.identity.threadId, thread)
    assert.equal(view.observation.gap, null, JSON.stringify(view.observation))
    assert.equal(view.observation.coverage, thread === B ? 'retained-history' : 'caught-up', JSON.stringify(view.observation))
  }
  const after = await f.core.snapshot()
  assert.deepEqual(events(after), originalEvents)
  assert.deepEqual(after.tasks.map(task => [task.id, task.lastActivity]), before.tasks.map(task => [task.id, task.lastActivity]))
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

test('an in-progress inherited prefix resumes while already verified current material remains delivered', async t => {
  const f = await fixture(t, null, { generations: 1, historyPaddingBytes: 300000 })
  await f.read(F)
  const current = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(current.currentRequest, 'Check this independent fork request')
  assert.ok(current.instructionContext.gaps.includes('fork_context_pending'))
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


test('v2 compressed ancestry reaches the production adapter, HTTP core and authenticated MCP with codec-neutral evidence', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true })
  let view
  for (let pump = 0; pump < 32; pump++) { view = await f.read(F); assert.ok((view.observation.decodedBytes ?? 0) <= 262144); if (view.observation.coverage === 'caught-up') break }
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(view.forkProvenance.complete, true)
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(task.currentRequest, 'Check this independent fork request')
  assert.equal(task.instructionContext.userLimitRefs.length, 2)
  assert.ok(task.events.every(event => event.data.nativeGenerationKey && Number.isSafeInteger(event.data.nativeRecordStart)))
  assert.ok(task.events.filter(event => event.data.history).every(event => event.data.inherited))
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})


test('v2 simultaneous plain and compressed candidates require independently verified prefix equivalence', async t => {
  for (const variant of ['same', 'body', 'header', 'corrupt']) await t.test(variant, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, duplicateOriginal: variant })
    const view = await f.catchUp(F)
    if (variant === 'same') { assert.equal(view.observation.coverage, 'caught-up'); assert.equal(view.forkProvenance.complete, true) }
    else {
      assert.notEqual(view.observation.coverage, 'caught-up'); assert.notEqual(view.forkProvenance?.complete, true)
      const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
      assert.equal(task.currentRequest, 'Check this independent fork request')
      assert.equal(task.events.filter(event => event.data.history).length, 0)
      assert.deepEqual(task.instructionContext.userLimitRefs, []); assert.ok(task.instructionContext.gaps.length > 0)
    }
  })
})


test('an ACKed v2 ancestry cursor recovers a qualified plain-to-zstd conversion without recapturing history', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2 })
  assert.equal((await f.catchUp(F)).forkProvenance.complete, true)
  const first = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.data.nativeRecordId, event.evidence, event.capturedAt])
  await f.compressOriginal(); f.advance(); await f.restart()
  const view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
  const next = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.data.nativeRecordId, event.evidence, event.capturedAt])
  assert.deepEqual(next, first)
})


test('v2 compressed qualification precedes body decoding and the whole tree shares its decoded grant', async t => {
  await t.test('missing feature reads no Native body', async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true, compressionCapability: false }), view = await f.read(F)
    assert.equal(view.observation.gap, 'core-capability-unavailable'); assert.equal(f.maximumPumpBytes, 0)
  })
  await t.test('producer project disabled after header', async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true, disabledProducer: true, historyPaddingBytes: 1000000 }), view = await f.read(F)
    assert.notEqual(view.forkProvenance?.complete, true)
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    assert.equal(task.currentRequest, 'Check this independent fork request')
    assert.equal(task.events.filter(event => event.data.history).length, 0)
    assert.deepEqual(task.instructionContext.userLimitRefs, []); assert.ok(task.instructionContext.gaps.length > 0)
    assert.ok(f.maximumPumpBytes < 65536, 'only the bounded header and preceding qualification can be read')
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, task.events.length)
    assert.equal((await f.core.diagnostics()).retained.nativeOriginFences.generations, 1)
  })
  await t.test('one decoded budget for all readers and validation passes', async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true, historyPaddingBytes: 1200000 })
    let view, pumps = 0
    do { view = await f.read(F); assert.ok((view.observation.decodedBytes ?? 0) <= 262144); pumps++; if (view.observation.coverage === 'caught-up') break } while (pumps < 100)
    assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.ok(pumps > 5); assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumMetadataCalls <= 144)
  })
})


test('completed compressed histories release guards across pumps and cursor restoration', { timeout: 20000 }, async t => {
  const f = await fixture(t, null, { generations: 2, sharedOwner: false, nativeOriginVersion: 2, compressedOriginal: true, compressedGenerations: true })
  let view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up'); assert.equal(view.forkProvenance.complete, true)
  const first = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.data.nativeRecordId, event.evidence, event.capturedAt])
  assert.equal(f.liveReaders, 1, 'only the current plain reader remains after completed history guards')
  f.advance(); view = await f.read(F)
  assert.equal(view.observation.coverage, 'caught-up'); assert.equal(view.forkProvenance.complete, true); assert.equal(f.liveReaders, 1)
  await f.restart(); view = await f.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
  assert.equal(f.liveReaders, 1)
  const next = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.data.nativeRecordId, event.evidence, event.capturedAt])
  assert.deepEqual(next, first); assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
})

test('completed plain history growth is revalidated independently of newly appended current facts', async t => {
  for (const rewrite of [false, true]) await t.test(rewrite ? 'rewritten-prefix' : 'unchanged-prefix', async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
    assert.equal((await f.catchUp(F)).forkProvenance.complete, true)
    const first = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
    await f.growOriginal(rewrite); await f.appendForkOutput(); f.advance()
    let view
    for (let pump = 0; pump < 32; pump++) {
      view = await f.read(F)
      if (!rewrite && view.observation.coverage === 'caught-up' || rewrite && view.health?.state === 'failed') break
    }
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    if (rewrite) {
      assert.equal(view.health.state, 'failed')
      assert.ok(view.health.failures.some(failure => failure.error.code === 'native_origin_prefix_changed'))
      assert.deepEqual(task.events.filter(event => event.text !== 'New tail after ancestor growth').map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), first)
      assert.equal(task.events.filter(event => event.text === 'New tail after ancestor growth').length, 1)
      assert.equal(view.inheritance.state, 'pending')
      assert.ok(view.inheritance.gaps.includes('native_origin_prefix_changed'))
      assert.deepEqual(task.instructionContext.userLimitRefs, []); assert.ok(task.instructionContext.gaps.includes('fork_context_pending'))
    } else {
      assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
      assert.equal(task.events.filter(event => event.text === 'New tail after ancestor growth').length, 1)
      assert.equal(view.health?.state === 'failed', false)
    }
    assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
  })
})

test('old fork completion and interruption found across pumps preserve current and inherited request references as ended history', async t => {
  for (const currentTerminal of ['completed', 'aborted']) await t.test(currentTerminal, async t => {
    const f = await fixture(t, null, { generations: 1, original: false, nativeOriginVersion: 2, oldCurrentStart: true, currentPaddingBytes: 600000, currentTerminal })
    await f.read(F)
    assert.deepEqual((await f.core.snapshot()).tasks, [])
    let view
    for (let pump = 0; pump < 100; pump++) {
      view = await f.read(F)
      assert.equal((await f.core.snapshot()).overview.runningTasks, 0)
      if (view.observation.coverage === 'retained-history' && view.forkProvenance?.complete) break
    }
    assert.equal(view.observation.coverage, 'retained-history', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    assert.equal(task.status, 'idle'); assert.equal(task.currentRequest, 'Check this independent fork request')
    assert.equal(task.instructionContext.userLimitRefs.length, 1, JSON.stringify({ context: task.instructionContext, data: task.events.filter(event => event.kind === 'input' || event.data?.forkReceipt).map(event => event.data) })); assert.deepEqual(task.instructionContext.gaps, [])
    const start = task.events.find(event => event.data.nativeType === 'task_started' && !event.data.history), input = task.events.find(event => event.kind === 'input' && !event.data.history)
    assert.ok(task.instructionContext.binding.originRefs.some(ref => ref.eventId === start.id)); assert.ok(task.instructionContext.binding.originRefs.some(ref => ref.eventId === input.id))
    const original = task.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
    await f.restart()
    for (let pump = 0; pump < 100; pump++) { view = await f.read(F); if (view.observation.coverage === 'retained-history' && view.forkProvenance?.complete) break }
    assert.equal(view.forkProvenance?.complete, true, JSON.stringify({ observation: view.observation, health: view.health }))
    assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), original)
    assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
  })
})

test('a completed plain prefix using the exact pump budget waits rather than permanently failing its later guards', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, exactHistoryBytes: 262144 }), ready = await f.catchUp(F)
  assert.equal(ready.forkProvenance.complete, true)
  const saved = JSON.parse(await fs.readFile(join(f.dir, 'adapter/cursors', ready.identity.cursorKey + '.json'), 'utf8'))
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), sourceKey = (await f.core.snapshot()).sources[0].key
  const reader = new ForkHistoryReader(saved.reader.frames[0].source, saved.reader, { native: f.native, sessionsRoot: saved.reader.frames[0].source.allowedRoot, origins: { read: key => f.core.nativeOriginFence(sourceKey, key) } })
  try {
    await f.growOriginal(false)
    const view = await reader.pump()
    assert.equal(view.readBytes, 262144); assert.equal(view.decodedBytes, 262144)
    assert.equal(view.gap, null, JSON.stringify({ gap: view.gap, coverage: view.coverage }))
    assert.notEqual(view.coverage, 'gap')
    let complete = view
    for (let pump = 0; pump < 16 && complete.coverage !== 'caught-up'; pump++) { complete = await reader.pump(); assert.equal(complete.gap, null); assert.ok(complete.readBytes <= 262144); assert.ok(complete.decodedBytes <= 262144) }
    assert.equal(complete.coverage, 'caught-up'); assert.equal(complete.forkProvenance.complete, true)
  } finally { await reader.close() }
})

test('simultaneous current and completed ancestor revalidation never calls a reader with a zero budget', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, exactHistoryBytes: 262144 }), ready = await f.catchUp(F)
  const saved = JSON.parse(await fs.readFile(join(f.dir, 'adapter/cursors', ready.identity.cursorKey + '.json'), 'utf8'))
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), sourceKey = (await f.core.snapshot()).sources[0].key
  const reader = new ForkHistoryReader(saved.reader.frames[0].source, saved.reader, { native: f.native, sessionsRoot: saved.reader.frames[0].source.allowedRoot, origins: { read: key => f.core.nativeOriginFence(sourceKey, key) } })
  try {
    let warm
    for (let pump = 0; pump < 8; pump++) { warm = await reader.pump(); if (warm.coverage === 'caught-up') break }
    assert.equal(warm.coverage, 'caught-up')
    await f.appendForkOutput()
    const pending = await reader.pump({ budgetBytes: 256 })
    assert.equal(pending.gap, null); assert.equal(pending.coverage, 'catching-up'); assert.ok(pending.readBytes > 0)
    await f.growOriginal(false)
    const view = await reader.pump()
    assert.equal(view.readBytes, 262144); assert.equal(view.decodedBytes, 262144)
    assert.equal(view.gap, null, JSON.stringify({ gap: view.gap, error: view.error }))
    assert.notEqual(view.coverage, 'gap')
    let complete = view
    const material = [...view.materials]
    for (let pump = 0; pump < 16 && complete.coverage !== 'caught-up'; pump++) { complete = await reader.pump(); material.push(...complete.materials); assert.equal(complete.gap, null); assert.ok(complete.readBytes <= 262144); assert.ok(complete.decodedBytes <= 262144) }
    assert.equal(complete.coverage, 'caught-up'); assert.equal(material.filter(event => event.text === 'New tail after ancestor growth').length, 1)
  } finally { await reader.close() }
})

test('an ancestor end-guard defers inherited coverage while current delivery and its first capture survive recovery', async t => {
  for (const restart of [false, true]) await t.test(restart ? 'restart-before-delivery' : 'same-reader-delivery', async t => {
    const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), pump = ForkHistoryReader.prototype.pump
    let latest, guardedCurrent
    t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) { const view = await pump.apply(this, args); latest = view; guardedCurrent = this.currentGuardedOutput; return view })
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000 }), initial = await f.catchUp(F)
    assert.equal(initial.forkProvenance.complete, true)
    const initialTask = (await f.core.snapshot()).tasks.find(task => task.id === F), before = initialTask.events.map(event => [event.id, event.data.nativeRecordId, event.evidence, event.capturedAt, event.text]), through = latest.captureThroughOffset
    await f.appendForkOutput(); f.armOriginalGrowth(); f.advance()
    for (let pump = 0; pump < 8; pump++) { await f.read(F); if (f.originalGrewAfterRead) break }
    assert.equal(f.originalGrewAfterRead, true)
    if (!latest.materials.some(event => event.text === 'New tail after ancestor growth')) {
      assert.ok(guardedCurrent?.materials.some(event => event.text === 'New tail after ancestor growth'), 'the exhausted final fence retains the exact current package')
      assert.equal(latest.currentCoverage, 'catching-up')
      for (let pump = 0; pump < 8 && !latest.materials.some(event => event.text === 'New tail after ancestor growth'); pump++) await f.read(F)
    }
    assert.ok(latest.materials.some(event => event.text === 'New tail after ancestor growth'))
    assert.ok(latest.nativeOrigins.length > 0, 'current facts keep their own verified prefix')
    assert.ok(latest.captureThroughOffset > through, 'current delivery advances only its own watermark')
    const cursor = JSON.parse(await fs.readFile(join(f.dir, 'adapter/cursors', initial.identity.cursorKey + '.json'), 'utf8'))
    assert.equal(cursor.captureThroughOffset, latest.captureThroughOffset); assert.equal(cursor.reader.captureThroughOffset, latest.captureThroughOffset)
    assert.equal(cursor.deliveryThrough.current, latest.deliveryThrough.current)
    const delivered = (await f.core.snapshot()).tasks.find(task => task.id === F).events.find(event => event.text === 'New tail after ancestor growth')
    if (restart) { await f.restart(); f.advance() }
    const complete = await f.catchUp(F)
    assert.equal(complete.observation.coverage, 'caught-up', JSON.stringify(complete.observation))
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F), events = task.events
    assert.equal(events.filter(event => event.text === 'New tail after ancestor growth').length, 1)
    assert.deepEqual(events.find(event => event.text === 'New tail after ancestor growth'), delivered)
    assert.deepEqual(events.filter(event => event.text !== 'New tail after ancestor growth').map(event => [event.id, event.data.nativeRecordId, event.evidence, event.capturedAt, event.text]), before)
    assert.deepEqual(task.instructionContext, initialTask.instructionContext)
  })
})

test('four completed compressed trees cannot starve a fifth tree of the shared decoder leases', { timeout: 30000 }, async t => {
  const completed = []
  for (let index = 0; index < 4; index++) {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true }); completed.push(f)
    const view = await f.catchUp(F)
    assert.equal(view.observation.coverage, 'caught-up'); assert.equal(view.forkProvenance.complete, true)
  }
  const fifth = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true }), view = await fifth.catchUp(F)
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
  for (const f of [...completed, fifth]) { assert.equal((await f.read(F)).forkProvenance.complete, true); assert.equal(f.liveReaders, 1); assert.ok(f.maximumReaders <= 4) }
  for (const f of [...completed, fifth]) await f.close()
  assert.ok([...completed, fifth].every(f => f.liveReaders === 0))
})


test('unfinished compressed history keeps its decoder progress through verification guards', { timeout: 20000 }, async t => {
  const states = new WeakMap(), pump = NativeJsonlSource.prototype.pump, close = NativeJsonlSource.prototype.close
  let activeCloses = 0, validationPumps = 0
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    const result = await pump.apply(this, args)
    states.set(this, result)
    if (result.phase === 'validating' && result.position > 0 && result.position < result.decodedSize) validationPumps++
    return result
  })
  t.mock.method(NativeJsonlSource.prototype, 'close', async function (...args) {
    const state = states.get(this)
    if (!this.closed && state?.phase === 'validating' && state.position > 0 && state.position < state.decodedSize) activeCloses++
    return close.apply(this, args)
  })
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true, historyPaddingBytes: 1200000 })
  let view
  for (let pump = 0; pump < 100; pump++) { view = await f.read(F); assert.ok((view.observation.decodedBytes ?? 0) <= 262144); if (view.observation.coverage === 'caught-up') break }
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
  assert.ok(validationPumps > 3); assert.equal(activeCloses, 0, 'only completed guards may discard their decoder, never unfinished validation')
  assert.equal(f.liveReaders, 1); assert.ok(f.maximumReaders <= 4)
})


test('current fork facts reach the production panel before inherited history completes', async t => {
  for (const nativeOriginVersion of [1, 2]) await t.test('native-origin/' + nativeOriginVersion, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion, historyPaddingBytes: 600000, currentTool: true })
    const view = await f.read(F)
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F && task.lifecycleId === 'F')
    assert.equal(task?.currentRequest, 'Check this independent fork request', JSON.stringify({ view, task }))
    assert.equal(task.status, 'running')
    assert.equal(view.observation.currentCoverage, 'caught-up')
    assert.equal(view.forkProvenance.complete, false)
    assert.equal(view.inheritance.state, 'pending')
    assert.ok(task.instructionContext.gaps.includes('fork_context_pending'))
    assert.equal(view.currentFacts.request.text, task.currentRequest)
    assert.deepEqual(view.currentFacts.request.evidenceIds, [task.instructionContext.binding.inputRef.eventId])
    assert.match(view.currentFacts.activity.summary, /Current tool failed with exit 2/)
    assert.ok(task.events.some(event => event.kind === 'tool-result' && event.data.exitCode === 2 && !event.data.history))
    assert.notEqual(view.signal, 'green')
    const original = task.events.filter(event => !event.data.history).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    const completed = await f.catchUp(F)
    assert.equal(completed.forkProvenance.complete, true, JSON.stringify(completed.observation))
    const retained = (await f.core.snapshot()).tasks.find(task => task.id === F && task.lifecycleId === 'F')
    assert.deepEqual(retained.instructionContext.gaps, [])
    assert.equal(retained.instructionContext.userLimitRefs.length, 2)
    assert.deepEqual(retained.events.filter(event => !event.data.history).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), original)
    assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maxIngest <= 16)
  })
})


test('current-first delivery recovers a lost HTTP acknowledgement without swallowing lower inherited ordinals', async t => {
  for (const nativeOriginVersion of [1, 2]) await t.test('native-origin/' + nativeOriginVersion, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion, historyPaddingBytes: 600000, currentTool: true, lostCurrentAck: true })
    await f.read(F)
    const first = (await f.core.snapshot()).tasks.find(task => task.id === F && task.lifecycleId === 'F')
    assert.equal(first.currentRequest, 'Check this independent fork request')
    assert.ok(first.instructionContext.gaps.includes('fork_context_pending'))
    const identities = first.events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    f.advance(3000); await f.restart()
    let pending = await f.read(F)
    assert.equal(pending.currentFacts.request.text, first.currentRequest)
    const recovered = (await f.core.snapshot()).tasks.find(task => task.id === F && task.lifecycleId === 'F')
    assert.deepEqual(recovered.events.filter(event => !event.data.history).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), identities)
    const complete = await f.catchUp(F)
    assert.equal(complete.forkProvenance.complete, true, JSON.stringify(complete.observation))
    const final = (await f.core.snapshot()).tasks.find(task => task.id === F && task.lifecycleId === 'F')
    assert.deepEqual(final.instructionContext.gaps, [])
    assert.equal(final.instructionContext.userLimitRefs.length, 2)
    assert.deepEqual(final.events.filter(event => !event.data.history).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), identities)
    assert.ok(final.events.some(event => event.data.history && event.data.recordOrdinal < 4))
    assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
  })
})

test('a temporary ancestor metadata failure recovers while current facts remain observable', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000, currentTool: true })
  const readThread = f.native.readThread
  let unavailable = true
  t.mock.method(f.native, 'readThread', async function (threadId) {
    if (threadId === C && unavailable) throw Object.assign(new Error('controlled ancestor disconnect'), { code: 'EPIPE' })
    return readThread.call(this, threadId)
  })
  let view
  for (let pump = 0; pump < 8; pump++) { view = await f.read(F); if (view.inheritance?.gaps.includes('history-source-unavailable')) break }
  assert.equal(view.currentFacts.request.text, 'Check this independent fork request')
  assert.equal(view.observation.currentCoverage, 'caught-up')
  assert.ok(view.inheritance.gaps.includes('history-source-unavailable'))
  const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.filter(event => !event.data.history).map(event => [event.id, event.text, event.capturedAt])
  unavailable = false
  const complete = await f.catchUp(F)
  assert.equal(complete.forkProvenance.complete, true, JSON.stringify({ view: complete, diagnostics: f.adapter.diagnostics() }))
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.deepEqual(task.instructionContext.gaps, [])
  assert.equal(task.instructionContext.userLimitRefs.length, 2)
  assert.deepEqual(task.events.filter(event => !event.data.history).map(event => [event.id, event.text, event.capturedAt]), before)
  assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumPumpBytes <= 262144)
})


test('small ongoing current activity shares the original pump grant with incremental inherited history', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 300000 })
  let view = await f.read(F), inherited = 0
  for (let pump = 0; pump < 12; pump++) {
    await f.appendCurrentActivity(); f.advance(10); view = await f.read(F)
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    assert.equal(task.currentRequest, 'Check this independent fork request')
    inherited = Math.max(inherited, task.events.filter(event => event.data.history).length)
  }
  assert.ok(inherited > 0, 'continuous small current appends must not starve inherited delivery')
  const complete = await f.catchUp(F)
  assert.equal(complete.forkProvenance.complete, true, JSON.stringify(complete.inheritance))
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(task.events.filter(event => event.text.startsWith('Fresh activity ') && !event.data.history).length, 12)
  assert.deepEqual(task.instructionContext.gaps, [])
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4); assert.ok(f.maxIngest <= 16)
})

test('a current file replacement discovered during historical work stops the current observation', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true, historyPaddingBytes: 1200000 })
  const pump = NativeJsonlSource.prototype.pump
  let replaced = false
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    const value = await pump.apply(this, args)
    if (!replaced && this.source.historyRange && this.source.path.endsWith('rollout-' + C + '.jsonl.zst')) { replaced = true; await f.replaceCurrent() }
    return value
  })
  let view = await f.read(F)
  assert.equal(view.currentFacts, undefined, 'an unguarded current packet cannot escape when the historical phase exhausts its grant')
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0)
  for (let attempt = 0; attempt < 12; attempt++) { if (view.observation.gap === 'record-file-replaced') break; view = await f.read(F) }
  assert.equal(replaced, true)
  assert.equal(view.observation.gap, 'record-file-replaced', JSON.stringify(view))
  assert.equal(view.observation.phase, 'unknown')
  assert.equal(view.currentFacts, undefined)
  assert.equal(view.forkProvenance?.complete, undefined)
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})

test('legacy fork cursor migration preserves verified history and accepts later current facts', async t => {
  for (const nativeOriginVersion of [1, 2]) await t.test('native-origin/' + nativeOriginVersion, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion }), ready = await f.catchUp(F)
    const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.text, event.capturedAt, event.data.nativeRecordId])
    const path = join(f.dir, 'adapter/cursors', ready.identity.cursorKey + '.json'), cursor = JSON.parse(await fs.readFile(path, 'utf8'))
    assert.equal(cursor.format, 3); assert.equal(cursor.reader.format, 4)
    cursor.format = 2; cursor.reader.format = nativeOriginVersion === 2 ? 3 : 2
    delete cursor.deliveryThrough; delete cursor.reader.deliveryThrough
    await fs.writeFile(path, JSON.stringify(cursor)); await f.restart(); f.advance(1000); await f.appendCurrentActivity()
    const migrated = await f.catchUp(F)
    assert.equal(migrated.forkProvenance.complete, true, JSON.stringify(migrated.observation))
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    assert.deepEqual(task.instructionContext.gaps, [])
    assert.deepEqual(task.events.filter(event => !event.text.startsWith('Fresh activity ')).map(event => [event.id, event.text, event.capturedAt, event.data.nativeRecordId]), before)
    assert.equal(task.events.filter(event => event.text.startsWith('Fresh activity ')).length, 1)
    const saved = JSON.parse(await fs.readFile(path, 'utf8'))
    assert.equal(saved.format, 3); assert.equal(saved.reader.format, 4)
    assert.ok(saved.deliveryThrough.current > saved.deliveryThrough.history)
  })
})

test('current-first fork cursor formats reject missing independent delivery watermarks', async t => {
  for (const layer of ['outer', 'reader']) await t.test(layer, async t => {
    const f = await fixture(t, null, { generations: 1 }), ready = await f.catchUp(F)
    const path = join(f.dir, 'adapter/cursors', ready.identity.cursorKey + '.json'), cursor = JSON.parse(await fs.readFile(path, 'utf8'))
    if (layer === 'outer') delete cursor.deliveryThrough
    else delete cursor.reader.deliveryThrough
    await fs.writeFile(path, JSON.stringify(cursor)); await f.restart()
    const view = await f.read(F)
    assert.notEqual(view.forkProvenance?.complete, true)
    assert.equal(view.history.currentGap, layer === 'outer' ? 'cursor-format-invalid' : 'cursor-invalid')
    assert.equal(view.currentFacts, undefined)
  })
})

test('an ancestor transport error cannot release current facts after the current file was replaced', async t => {
  for (const nativeOriginVersion of [1, 2]) await t.test('native-origin/' + nativeOriginVersion, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion }), readThread = f.native.readThread
    let replaced = false
    t.mock.method(f.native, 'readThread', async function (threadId) {
      if (!replaced && threadId === id(16)) {
        replaced = true; await f.replaceCurrent()
        throw Object.assign(new Error('controlled ancestor transport failure after current replacement'), { code: 'EPIPE' })
      }
      return readThread.call(this, threadId)
    })
    const view = await f.read(F)
    assert.equal(replaced, true)
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0, 'current output still needs its own end fence when the ancestor query fails')
    assert.equal(view.currentFacts, undefined)
    assert.equal(view.observation.gap, 'record-file-replaced')
    assert.equal(view.observation.phase, 'unknown')
    assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
  })
})

test('restored current output shares one sixteen-event grant with new inherited output', async t => {
  const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, currentPaddingBytes: 600000 })
  let view
  for (let pump = 0; pump < 64; pump++) {
    view = await f.read(F)
    assert.ok(f.maxIngest <= 16, 'restoring current output must debit the grant before inherited delivery')
    if (view.forkProvenance?.complete && view.observation.coverage === 'caught-up') break
  }
  assert.equal(view.forkProvenance.complete, true, JSON.stringify(view.observation))
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(task.currentRequest, 'Check this independent fork request')
  assert.equal(task.events.filter(event => !event.data.history).length, 3)
  assert.equal(task.instructionContext.userLimitRefs.length, 5)
  assert.ok(task.events.every(event => Number.isSafeInteger(event.capturedAt)))
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})

test('restored inherited output shares one sixteen-event grant with new current output and lost response recovery', async t => {
  const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, historyPaddingBytes: 600000, lostAck: true })
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, pump = NativeJsonlSource.prototype.pump
  let appended = false, secondAppend = false, restarted = false, rounds = 0
  t.mock.method(ForkHistoryReader.prototype, 'pump', async function (options = {}) {
    rounds++
    const view = await readerPump.call(this, { ...options, budgetBytes: rounds === 1 ? 1536 : rounds === 2 ? 32768 : rounds === 8 ? 64 : 262144 })
    return view
  })
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    const value = await pump.apply(this, args)
    if (!appended && this.source.expectedHeader && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.position >= this.source.historyRange.endByteOffset) {
      appended = true; await f.growOriginal(false); await f.appendCurrentActivity()
    }
    return value
  })
  const first = new Map()
  let view
  for (let attempt = 0; attempt < 48; attempt++) {
    if (rounds === 7 && !secondAppend) { secondAppend = true; await f.appendCurrentActivity() }
    view = await f.read(F)
    assert.ok(f.maxIngest <= 16, 'restoring inherited output must share the remaining grant with new current delivery')
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    for (const event of task?.events ?? []) {
      assert.ok(Number.isSafeInteger(event.capturedAt))
      const identity = [event.data.nativeRecordId, event.capturedAt, event.text]
      if (first.has(event.id)) assert.deepEqual(identity, first.get(event.id))
      else first.set(event.id, identity)
    }
    if (!restarted && view.history?.currentGap === 'observation-unavailable') { restarted = true; f.advance(1000); await f.restart() }
    if (view.forkProvenance?.complete && view.observation.coverage === 'caught-up') break
  }
  assert.equal(appended, true)
  assert.equal(restarted, true, 'the accepted inherited HTTP response is deliberately lost once')
  assert.equal(view.forkProvenance.complete, true, JSON.stringify({ observation: view.observation, inheritance: view.inheritance, health: view.health }))
  const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(task.events.filter(event => !event.data.history).length, 5)
  assert.equal(task.events.filter(event => event.text.startsWith('Fresh activity ')).length, 2)
  assert.equal(task.instructionContext.userLimitRefs.length, 5)
  assert.equal(task.events.length, first.size)
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})

test('publishing another main chat cannot restore a fork binding invalidated by its ancestor', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
  assert.equal((await f.catchUp(F)).forkProvenance.complete, true)
  const before = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(before.instructionContext.userLimitRefs.length, 2)
  await f.growOriginal(true)
  let view
  for (let attempt = 0; attempt < 16; attempt++) { view = await f.read(F); if (view.inheritance?.gaps.includes('native_origin_prefix_changed')) break }
  const invalidated = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.deepEqual(invalidated.instructionContext.userLimitRefs, [])
  assert.ok(invalidated.instructionContext.gaps.includes('fork_context_pending'))
  await f.appendOwnerRequest()
  const after = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.deepEqual(after.instructionContext.binding.forkContext, invalidated.instructionContext.binding.forkContext)
  assert.deepEqual(after.instructionContext.userLimitRefs, [])
  assert.ok(after.instructionContext.gaps.includes('fork_context_pending'))
  assert.equal(after.currentRequest, before.currentRequest)
  assert.deepEqual(after.events.map(event => [event.id, event.capturedAt, event.text]), before.events.map(event => [event.id, event.capturedAt, event.text]))
})

test('permanent ancestor qualification gaps release decoder leases while current observation continues', { timeout: 30000 }, async t => {
  const pump = NativeJsonlSource.prototype.pump, broken = []
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    const value = await pump.apply(this, args)
    if (value.phase === 'validating' && value.position > 0 && value.position < value.decodedSize) {
      const owner = broken.find(item => this.source.path.startsWith(item.root + '/'))
      if (owner) owner.invalidate = true
    }
    return value
  })
  for (let index = 0; index < 4; index++) {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: true, historyPaddingBytes: 1200000 }), item = { f, root: await fs.realpath(f.dir), invalidate: false }
    broken.push(item)
    const readThread = f.native.readThread
    t.mock.method(f.native, 'readThread', async function (threadId) {
      const metadata = await readThread.call(this, threadId)
      return threadId === C && item.invalidate ? { ...metadata, thread: { ...metadata.thread, cwd: '/changed-ancestor-generation' } } : metadata
    })
    let view
    for (let attempt = 0; attempt < 16; attempt++) { view = await f.read(F); if (view.inheritance?.gaps.includes('history-source-generation-changed')) break }
    assert.equal(item.invalidate, true)
    assert.ok(view.inheritance.gaps.includes('history-source-generation-changed'))
    assert.equal(view.currentFacts.request.text, 'Check this independent fork request')
    assert.equal(f.liveReaders, 1, 'a permanent inheritance gap retains only the current reader')
    await f.appendCurrentActivity()
    for (let attempt = 0; attempt < 4; attempt++) await f.read(F)
    assert.equal((await f.core.snapshot()).tasks.find(task => task.id === F).events.filter(event => event.text.startsWith('Fresh activity ')).length, 1)
  }
  const fifth = await fixture(t, null, { generations: 1, original: false, nativeOriginVersion: 2, compressedCurrent: true })
  const view = await fifth.catchUp(F)
  assert.equal(view.forkProvenance.complete, true, JSON.stringify(view.observation))
  assert.equal(view.currentFacts.request.text, 'Check this independent fork request')
  for (const f of [...broken.map(item => item.f), fifth]) await f.close()
  assert.ok([...broken.map(item => item.f), fifth].every(f => f.liveReaders === 0))
})

test('inheritance coverage restrictions do not become failure receipts while definite transport and decode failures do', async t => {
  for (const kind of ['unknown-frame', 'disabled-project', 'unsupported-producer', 'transport', 'corrupt-frame']) await t.test(kind, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, compressedOriginal: kind.includes('frame'), disabledProducer: kind === 'disabled-project', originalProducer: kind === 'unsupported-producer' ? '0.160.2' : null })
    if (kind.includes('frame')) {
      const path = join(f.dir, 'sessions', 'rollout-' + C + '.jsonl.zst'), bytes = await fs.readFile(path)
      if (kind === 'unknown-frame') bytes[4] |= 1
      await fs.writeFile(path, kind === 'corrupt-frame' ? bytes.subarray(0, bytes.length - 2) : bytes)
    }
    if (kind === 'transport') {
      const readThread = f.native.readThread
      t.mock.method(f.native, 'readThread', async function (threadId) {
        if (threadId === C) throw Object.assign(new Error('controlled settled ancestor transport failure'), { code: 'EPIPE' })
        return readThread.call(this, threadId)
      })
    }
    let view
    for (let attempt = 0; attempt < 16; attempt++) { view = await f.read(F); if (view.inheritance?.gaps.some(gap => gap !== 'fork_context_pending')) break }
    assert.equal(view.currentFacts.request.text, 'Check this independent fork request')
    assert.equal(view.inheritance.state, 'pending')
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F), scope = { sourceKey: task.sourceKey, mainId: F, lifecycleId: 'F' }
    const first = await f.core.failureReceipts(scope)
    const definite = ['transport', 'corrupt-frame'].includes(kind)
    assert.equal(first.receipts.length, Number(definite), JSON.stringify({ gaps: view.inheritance.gaps, failures: first.receipts }))
    if (definite) assert.equal(first.receipts[0].error.code, kind === 'transport' ? 'EPIPE' : 'compressed-data-invalid')
    else assert.notEqual(view.health.state, 'failed')
    for (let repeat = 0; repeat < 2; repeat++) await f.read(F)
    assert.deepEqual((await f.core.failureReceipts(scope)).receipts, first.receipts, 'the same fault episode is not recorded or timed again')
    assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
  })
})

test('an ancestor error verifies current with the remaining grant or defers it until a later verified pump', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000 }), readThread = f.native.readThread
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, sourcePump = NativeJsonlSource.prototype.pump
  let decoded = 0, failed = false, guardedAfterFailure = false
  t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) { decoded = 0; return readerPump.apply(this, args) })
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) { if (failed && this.source.path.endsWith('fork.jsonl') && args[0]?.verifyOnly) guardedAfterFailure = true; const value = await sourcePump.apply(this, args); decoded += value.decodedBytes; return value })
  t.mock.method(f.native, 'readThread', async function (threadId) {
    if (!failed && threadId === id(16) && decoded > 250000) { failed = true; throw Object.assign(new Error('controlled ancestor failure after shared grant was spent'), { code: 'EPIPE' }) }
    return readThread.call(this, threadId)
  })
  const held = await f.read(F)
  assert.equal(failed, true)
  const accepted = (await f.core.snapshot()).tasks.flatMap(task => task.events)
  if (accepted.length) { assert.equal(guardedAfterFailure, true, 'current output needs a successful final fence after the ancestor error'); assert.equal(held.currentFacts.request.text, 'Check this independent fork request') }
  else assert.equal(held.currentFacts, undefined)
  f.advance(1000)
  const view = await f.catchUp(F), task = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(view.forkProvenance.complete, true, JSON.stringify(view.observation))
  assert.equal(task.currentRequest, 'Check this independent fork request')
  assert.equal(task.events.filter(event => !event.data.history).length, 3)
  const first = task.events.map(event => [event.id, event.capturedAt, event.text])
  f.advance(1000); await f.restart(); await f.catchUp(F)
  assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text]), first)
  assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})

test('completed small current prefix revalidation uses its remaining grant to observe continued appends', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, currentPaddingBytes: 4096 }), sourcePump = NativeJsonlSource.prototype.pump
  let decoded = 0, maximumDecoded = 0
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    try { const value = await sourcePump.apply(this, args); decoded += value.decodedBytes; return value }
    catch (error) { decoded += error.decodedBytes ?? 0; throw error }
  })
  const observed = new Map(), counts = []
  for (let sample = 0; sample < 16; sample++) {
    await f.appendCurrentActivity(); f.advance(10); decoded = 0
    await f.read(F)
    maximumDecoded = Math.max(maximumDecoded, decoded); assert.ok(decoded <= 262144)
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    counts.push({ current: task?.events.filter(event => !event.data.history).length ?? 0, inherited: task?.events.filter(event => event.data.history).length ?? 0 })
    for (const event of task?.events ?? []) {
      const value = [event.capturedAt, event.text, event.data.nativeRecordId]
      if (observed.has(event.id)) assert.deepEqual(value, observed.get(event.id))
      else observed.set(event.id, value)
    }
  }
  assert.ok(counts.at(-1).current > counts[1].current, JSON.stringify(counts))
  assert.equal(counts.at(-1).current, 19, 'the original three current records and all sixteen new outputs are observed')
  assert.ok(counts.at(-1).inherited > 0)
  assert.ok(maximumDecoded <= 262144); assert.ok(f.maximumPumpBytes <= 262144)
  assert.ok(f.maxIngest <= 16); assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumMetadataCalls <= 144)
})

test('continuing after small prefix validation preserves source integrity and original capture clocks', async t => {
  for (const mutation of ['replace', 'truncate', 'same-size-rewrite', 'rewrite-and-append']) await t.test(mutation, async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, currentPaddingBytes: 4096 }), sourcePump = NativeJsonlSource.prototype.pump
    let decoded = 0
    t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
      try { const value = await sourcePump.apply(this, args); decoded += value.decodedBytes; return value }
      catch (error) { decoded += error.decodedBytes ?? 0; throw error }
    })
    assert.equal((await f.catchUp(F)).forkProvenance.complete, true)
    const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    const path = join(f.dir, 'sessions', 'fork.jsonl'), bytes = await fs.readFile(path)
    if (mutation === 'replace') await f.replaceCurrent()
    else if (mutation === 'truncate') await fs.truncate(path, bytes.length - 1)
    else {
      const changed = Buffer.from(bytes.toString().replace('Check this independent fork request', 'Shift this independent fork request'))
      assert.equal(changed.length, bytes.length); assert.notDeepEqual(changed, bytes)
      await fs.writeFile(path, changed)
      if (mutation === 'rewrite-and-append') await f.appendCurrentActivity()
    }
    f.advance(10)
    let view
    for (let sample = 0; sample < 8; sample++) {
      decoded = 0; view = await f.read(F); assert.ok(decoded <= 262144)
      if (view.health?.state === 'failed') break
    }
    const expected = mutation === 'replace' ? 'record-file-replaced' : mutation === 'truncate' ? 'record-file-truncated' : mutation === 'same-size-rewrite' ? 'record-rewrite-suspected' : 'native_origin_prefix_changed'
    assert.equal(view.health.state, 'failed')
    assert.equal(view.observation.gap, expected)
    const after = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    assert.deepEqual(after, before)
    assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maxIngest <= 16)
    assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumMetadataCalls <= 144)
  })
})

test('normal ancestor guard exhaustion cannot release prepared current facts before their final source fence', async t => {
  for (const replace of [true, false]) await t.test(replace ? 'current-replaced' : 'unchanged-source-recovers', async t => {
    const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000 }), sourcePump = NativeJsonlSource.prototype.pump
    assert.equal((await f.catchUp(F)).forkProvenance.complete, true)
    const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    let crossed = false, decoded = 0
    t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
      try {
        const value = await sourcePump.apply(this, args); decoded += value.decodedBytes
        if (!crossed && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.physicalBytes > 200000) { crossed = true; if (replace) await f.replaceCurrent() }
        return value
      } catch (error) { decoded += error.decodedBytes ?? 0; throw error }
    })
    await f.growOriginal(false); await f.appendCurrentActivity(); f.advance(10)
    decoded = 0
    const first = await f.read(F)
    assert.equal(crossed, true, 'the completed ancestor revalidation must consume the remaining shared grant')
    assert.ok(decoded <= 262144)
    const received = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    if (replace) {
      assert.deepEqual(received, before, 'a normal budget return must not publish the prepared current packet after replacement')
      if (first.currentFacts) assert.ok(first.currentFacts.activity.evidenceIds.every(id => before.some(event => event[0] === id)))
    } else {
      const recovered = await f.catchUp(F)
      assert.equal(recovered.forkProvenance.complete, true, JSON.stringify(recovered.observation))
      const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
      assert.deepEqual(task.events.filter(event => !event.text.startsWith('Fresh activity ')).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), before)
      assert.equal(task.events.filter(event => event.text.startsWith('Fresh activity ')).length, 1)
      const once = task.events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
      f.advance(1000); await f.restart(); await f.catchUp(F)
      assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), once)
    }
    assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144)
    assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumMetadataCalls <= 144)
  })
})

test('a normally deferred full historical batch progresses while small current activity continues', async t => {
  const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, sourcePump = NativeJsonlSource.prototype.pump
  let grew = false, retainedHistory = 0, decoded = 0
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    try {
      const value = await sourcePump.apply(this, args); decoded += value.decodedBytes
      if (!grew && this.source.expectedHeader && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.position >= this.source.historyRange.endByteOffset) { grew = true; await f.growOriginal(false) }
      return value
    } catch (error) { decoded += error.decodedBytes ?? 0; throw error }
  })
  t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) {
    const view = await readerPump.apply(this, args)
    const retained = this.guardedOutput
    if (retained) retainedHistory = Math.max(retainedHistory, retained.historyMaterials.length + retained.historyLifecycle.length + retained.historyBoundaries.length)
    return view
  })
  let view
  for (let attempt = 0; attempt < 24 && retainedHistory < 16; attempt++) { decoded = 0; view = await f.read(F); assert.ok(decoded <= 262144) }
  assert.equal(retainedHistory, 16, 'source growth must normally establish the full deferred batch before the liveness workload')
  const taskBefore = (await f.core.snapshot()).tasks.find(task => task.id === F), beforeHistory = taskBefore.events.filter(event => event.data.history).length
  const original = new Map(taskBefore.events.map(event => [event.id, [event.capturedAt, event.text, event.data.nativeRecordId]])), counts = []
  for (let sample = 0; sample < 16; sample++) {
    await f.appendCurrentActivity(); f.advance(10); decoded = 0; view = await f.read(F)
    assert.ok(decoded <= 262144)
    assert.equal(view.observation.gap, null)
    assert.notEqual(view.health?.state, 'failed')
    const task = (await f.core.snapshot()).tasks.find(task => task.id === F)
    counts.push({ current: task.events.filter(event => !event.data.history).length, inherited: task.events.filter(event => event.data.history).length })
    for (const event of task.events) {
      const value = [event.capturedAt, event.text, event.data.nativeRecordId]
      if (original.has(event.id)) assert.deepEqual(value, original.get(event.id))
      else original.set(event.id, value)
    }
    assert.ok(f.maxIngest <= 16)
  }
  const during = counts.at(-1)
  assert.ok(during.current > counts[0].current)
  assert.ok(during.inherited > beforeHistory, JSON.stringify({ retainedHistory, beforeHistory, counts }))
  const quiet = await f.catchUp(F)
  assert.equal(quiet.forkProvenance?.complete, true, JSON.stringify({ label: 'quiet recovery', retainedHistory, beforeHistory, counts, observation: quiet.observation, history: quiet.history, health: quiet.health, inheritance: quiet.inheritance }))
  const completed = (await f.core.snapshot()).tasks.find(task => task.id === F)
  assert.equal(completed.events.filter(event => event.text.startsWith('Fresh activity ')).length, 16)
  for (const event of completed.events) if (original.has(event.id)) assert.deepEqual([event.capturedAt, event.text, event.data.nativeRecordId], original.get(event.id))
  const once = completed.events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
  f.advance(1000); await f.restart(); await f.catchUp(F)
  assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), once)
  assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4); assert.ok(f.maximumMetadataCalls <= 144)
})

test('deferred historical release recovery matrix retains first captures across current interleavings', async t => {
  for (const { appends, exactHistoryBytes = 0 } of [{ appends: 0 }, { appends: 2 }, { appends: 16 }, { appends: 0, exactHistoryBytes: 524288 }]) await t.test(exactHistoryBytes ? 'exact-two-pump-byte-boundary' : 'current-appends-' + appends, async t => {
    const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, historyPaddingBytes: 600000, exactHistoryBytes })
    const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, sourcePump = NativeJsonlSource.prototype.pump
    const ingest = f.core.ingest
    t.mock.method(f.core, 'ingest', async function (sourceKey, batch) {
      const proofs = new Map((batch.nativeOrigins ?? []).map(proof => [proof.key, proof]))
      const uncovered = (batch.events ?? []).filter(event => event.data?.history && (!proofs.get(event.data.nativeGenerationKey)?.complete || event.data.nativeRecordEnd > proofs.get(event.data.nativeGenerationKey).next.offset))
      if (uncovered.length) t.diagnostic(JSON.stringify({ marker: 'held-proof-feedback', appends, uncovered: uncovered.map(event => ({ id: event.id, recordEnd: event.data.nativeRecordEnd, proofEnd: proofs.get(event.data.nativeGenerationKey)?.next.offset ?? null })), proofCount: proofs.size }))
      return ingest.call(this, sourceKey, batch)
    })
    let grew = false, retainedHistory = 0, decoded = 0
    t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
      const value = await sourcePump.apply(this, args); decoded += value.decodedBytes
      if (!grew && this.source.expectedHeader && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.position >= this.source.historyRange.endByteOffset) { grew = true; await f.growOriginal(false) }
      return value
    })
    t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) {
      decoded = 0
      const view = await readerPump.apply(this, args), retained = this.guardedOutput
      assert.ok(decoded <= 262144, 'source decoded read budget')
      if (retained) retainedHistory = Math.max(retainedHistory, retained.historyMaterials.length + retained.historyLifecycle.length + retained.historyBoundaries.length)
      return view
    })
    for (let attempt = 0; attempt < 24 && retainedHistory < 16; attempt++) await f.read(F)
    assert.equal(retainedHistory, 16)
    for (let count = 0; count < appends; count++) { await f.appendCurrentActivity(); f.advance(10); await f.read(F) }
    const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    const quiet = await f.catchUp(F)
    t.diagnostic(JSON.stringify({ appends, exactHistoryBytes, retainedHistory, beforeCurrent: before.filter(event => event[2].startsWith('Fresh activity ')).length, quietComplete: quiet.forkProvenance?.complete ?? false, quietGap: quiet.observation.gap ?? null, quietCurrentGap: quiet.history?.currentGap ?? null, quietHealth: quiet.health?.state ?? null }))
    assert.equal(quiet.forkProvenance?.complete, true, JSON.stringify({ appends, observation: quiet.observation, inheritance: quiet.inheritance, health: quiet.health }))
    const after = (await f.core.snapshot()).tasks.find(task => task.id === F)
    for (const saved of before) assert.deepEqual(after.events.filter(event => event.id === saved[0]).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), [saved])
    assert.equal(after.events.filter(event => event.text.startsWith('Fresh activity ')).length, appends)
    assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
  })
})

test('a held historical package rejects rewrite and append inside its unacknowledged high prefix', async t => {
  const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, sourcePump = NativeJsonlSource.prototype.pump
  let grew = false, retainedHistory = 0, high = null, low = null
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    const value = await sourcePump.apply(this, args)
    if (!grew && this.source.expectedHeader && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.position >= this.source.historyRange.endByteOffset) {
      high = this.proof().next; low = this.expected.prefix; grew = true; await f.growOriginal(false)
    }
    return value
  })
  t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) {
    const view = await readerPump.apply(this, args), retained = this.guardedOutput
    if (retained) retainedHistory = Math.max(retainedHistory, retained.historyMaterials.length + retained.historyLifecycle.length + retained.historyBoundaries.length)
    return view
  })
  for (let attempt = 0; attempt < 24 && retainedHistory < 16; attempt++) await f.read(F)
  assert.equal(retainedHistory, 16)
  assert.ok(high.offset > low.offset)
  const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
  const path = join(f.dir, 'sessions', 'rollout-' + C + '.jsonl'), bytes = await fs.readFile(path), changeAt = Math.floor((low.offset + high.offset) / 2)
  assert.equal(bytes[changeAt], 'x'.charCodeAt(0)); bytes[changeAt] = 'y'.charCodeAt(0)
  await fs.writeFile(path, Buffer.concat([bytes, Buffer.from('\n')]))
  let view
  for (let attempt = 0; attempt < 24; attempt++) { view = await f.read(F); if (view.inheritance?.gaps.includes('native_origin_prefix_changed')) break }
  assert.ok(view.inheritance?.gaps.includes('native_origin_prefix_changed'), JSON.stringify({ low: low.offset, high: high.offset, changeAt, observation: view.observation, inheritance: view.inheritance, health: view.health }))
  assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), before)
  f.advance(1000); await f.restart(); await f.read(F)
  assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), before)
  assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})

test('a deferred high historical witness survives adapter restart without advancing its material acknowledgement', async t => {
  for (const changed of [false, true]) await t.test(changed ? 'rewritten-unacknowledged-span' : 'unchanged-source', async t => {
    const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
    const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, sourcePump = NativeJsonlSource.prototype.pump
    let grew = false, retainedHistory = 0, high = null, low = null
    t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
      const value = await sourcePump.apply(this, args)
      if (!grew && this.source.expectedHeader && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.position >= this.source.historyRange.endByteOffset) {
        high = this.proof().next; low = this.expected.prefix; grew = true; await f.growOriginal(false)
      }
      return value
    })
    t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) {
      const view = await readerPump.apply(this, args), retained = this.guardedOutput
      if (retained) retainedHistory = Math.max(retainedHistory, retained.historyMaterials.length + retained.historyLifecycle.length + retained.historyBoundaries.length)
      return view
    })
    let heldView
    for (let attempt = 0; attempt < 24 && retainedHistory < 16; attempt++) heldView = await f.read(F)
    assert.equal(retainedHistory, 16)
    const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
    const cursorPath = join(f.dir, 'adapter', 'cursors', heldView.identity.cursorKey + '.json'), saved = JSON.parse(await fs.readFile(cursorPath, 'utf8'))
    const frame = saved.reader.frames.find(frame => frame.source.threadId === C)
    assert.ok(frame.reader.offset < high.offset, 'the held high witness must not promote the material acknowledgement')
    assert.deepEqual(frame.originProof.next, high, 'the existing frame proof must retain the original high witness')
    f.advance(1000); await f.restart()
    if (changed) {
      const path = join(f.dir, 'sessions', 'rollout-' + C + '.jsonl'), bytes = await fs.readFile(path), at = Math.floor((low.offset + high.offset) / 2)
      assert.equal(bytes[at], 120); bytes[at] = 121
      await fs.writeFile(path, Buffer.concat([bytes, Buffer.from('\n')]))
    }
    const view = await f.catchUp(F), after = (await f.core.snapshot()).tasks.find(task => task.id === F)
    if (changed) {
      assert.ok(view.inheritance?.gaps.includes('native_origin_prefix_changed'), JSON.stringify({ inheritance: view.inheritance, observation: view.observation }))
      assert.deepEqual(after.events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), before)
    } else {
      assert.equal(view.forkProvenance?.complete, true, JSON.stringify({ inheritance: view.inheritance, observation: view.observation }))
      for (const saved of before) assert.deepEqual(after.events.filter(event => event.id === saved[0]).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), [saved])
    }
    assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
  })
})

test('a real held-prefix verifier IO failure records once and recovers without refreshing prior captures', async t => {
  const f = await fixture(t, null, { generations: 4, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), readerPump = ForkHistoryReader.prototype.pump, sourcePump = NativeJsonlSource.prototype.pump
  let grew = false, retainedHistory = 0, injected = false
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    if (retainedHistory === 16 && !injected && args[1] === undefined && this.source.path.endsWith('rollout-' + C + '.jsonl')) { injected = true; throw Object.assign(new Error('controlled raw historical IO failure'), { code: 'EPIPE' }) }
    const value = await sourcePump.apply(this, args)
    if (!grew && this.source.expectedHeader && this.source.path.endsWith('rollout-' + C + '.jsonl') && value.position >= this.source.historyRange.endByteOffset) { grew = true; await f.growOriginal(false) }
    return value
  })
  t.mock.method(ForkHistoryReader.prototype, 'pump', async function (...args) {
    const view = await readerPump.apply(this, args), retained = this.guardedOutput
    if (retained) retainedHistory = Math.max(retainedHistory, retained.historyMaterials.length + retained.historyLifecycle.length + retained.historyBoundaries.length)
    return view
  })
  for (let attempt = 0; attempt < 24 && retainedHistory < 16; attempt++) await f.read(F)
  assert.equal(retainedHistory, 16)
  const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
  const failed = await f.read(F)
  assert.equal(injected, true)
  assert.equal(failed.health.failures.filter(failure => failure.error.code === 'EPIPE').length, 1, JSON.stringify(failed.health))
  const recovered = await f.catchUp(F)
  assert.equal(recovered.forkProvenance?.complete, true, JSON.stringify({ health: recovered.health, inheritance: recovered.inheritance }))
  for (const saved of before) assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.id === F).events.filter(event => event.id === saved[0]).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), [saved])
  assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})

test('a partial historical prefix recovered from a public cursor can advance beyond its saved witness', async t => {
  const f = await fixture(t, null, { generations: 1, nativeOriginVersion: 2, historyPaddingBytes: 600000 })
  let partial, saved
  for (let pump = 0; pump < 24; pump++) {
    partial = await f.read(F)
    saved = JSON.parse(await fs.readFile(join(f.dir, 'adapter/cursors', partial.identity.cursorKey + '.json'), 'utf8'))
    const frame = saved.reader.frames.find(frame => frame.source.threadId === C)
    if (frame?.originProof?.next.offset > frame?.reader.offset && frame.originProof.next.offset < frame.source.historyRange.endByteOffset && !frame.complete) break
  }
  const frame = saved.reader.frames.find(frame => frame.source.threadId === C)
  assert.ok(frame.originProof.next.offset > frame.reader.offset && frame.originProof.next.offset < frame.source.historyRange.endByteOffset)
  const before = (await f.core.snapshot()).tasks.find(task => task.id === F).events.map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId])
  f.advance(1000); await f.restart()
  const ready = await f.catchUp(F)
  assert.equal(ready.forkProvenance?.complete, true, JSON.stringify({ observation: ready.observation, inheritance: ready.inheritance, health: ready.health }))
  const after = (await f.core.snapshot()).tasks.find(task => task.id === F)
  for (const saved of before) assert.deepEqual(after.events.filter(event => event.id === saved[0]).map(event => [event.id, event.capturedAt, event.text, event.data.nativeRecordId]), [saved])
  assert.ok(f.maxIngest <= 16); assert.ok(f.maximumPumpBytes <= 262144); assert.ok(f.maximumReaders <= 4)
})
