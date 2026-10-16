import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { zstdCompressSync } from 'node:zlib'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

async function fixture(t, { padding = '', compressed = false, invalidFrame = false, oldAnchor = false, terminal = null } = {}) {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-plain-integrity-')), path = join(dir, 'main.jsonl' + (compressed ? '.zst' : '')), clock = Date.now(), token = 'c'.repeat(64)
  const row = (ordinal, type, payload, at = clock - (oldAnchor ? 20000 : 1000)) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
  const bytes = row(0, 'session_meta', { id: 'main', cwd: '/owned', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, history_mode: 'paginated' })
    + row(1, 'event_msg', { type: 'task_started', turn_id: 'run', turn_attribution: { turn_id: 'run', turn_trigger: 'composer' } })
    + row(2, 'event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'UserMessage', id: 'user', content: [{ type: 'text', text: 'Inspect this request' }] } })
    + row(3, 'event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'AgentMessage', id: 'initial', content: [{ type: 'Text', text: 'Original owned body' + padding }] } })
    + (terminal ? row(4, 'event_msg', { type: terminal === 'completed' ? 'task_complete' : 'turn_aborted', turn_id: 'run' }) : '')
  const encode = content => compressed ? zstdCompressSync(Buffer.from(content)) : content
  const encoded = encode(bytes)
  await fs.writeFile(path, invalidFrame ? encoded.subarray(0, encoded.length - 2) : encoded)
  const target = await fs.realpath(path), open = fs.open
  let physicalBytes = 0
  t.mock.method(fs, 'open', async (path, ...args) => {
    const handle = await open(path, ...args)
    if (String(path) === target) { const read = handle.read; handle.read = async function (...values) { const result = await read.apply(this, values); physicalBytes += result.bytesRead; return result } }
    return handle
  })
  let server, core, adapter
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  const thread = { id: 'main', path, cwd: '/owned', originator: 'Codex Desktop', source: 'vscode', parentThreadId: null, forkedFromId: null }
  let anchorTurnId = 'run'
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: clock - 10000, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread() { return { thread, anchorTurnId } },
    async listThreads({ archived }) { return { data: archived ? [] : [thread], nextCursor: null } }, async close() {},
  }
  server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') })
  const openAdapter = () => startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'plain-integrity', token, collectMaterials: true, nativeOriginVersion: 2, pollMs: 0, now: () => clock })
  adapter = await openAdapter()
  const read = async () => {
    physicalBytes = 0
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main' } } }) })
    assert.equal(response.status, 200); const result = (await response.json()).result; assert.equal(result.isError, undefined)
    assert.ok(physicalBytes <= 262144, 'one adapter pump retains the physical budget across verification and new material')
    if (result.structuredContent.observation.decodedBytes !== undefined) assert.ok(result.structuredContent.observation.decodedBytes <= 262144, 'one adapter pump retains its independent decoded budget')
    return result.structuredContent
  }
  const catchUp = async (stopOnFailure = true) => { let view; for (let pump = 0; pump < 100; pump++) { view = await read(); if (stopOnFailure && view.health?.state === 'failed' || view.observation.coverage === 'caught-up' || view.observation.gap && !['main-header-unverified', 'current-turn-not-observed'].includes(view.observation.gap)) break } return view }
  const tail = row(4, 'event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'AgentMessage', id: 'tail', content: [{ type: 'Text', text: 'New verified tail' }] } })
  const source = async () => (await core.snapshot()).sources[0].key
  return { dir, path, bytes, tail, encode, core, adapter, read, catchUp, source, ending: (phase, turnId = 'run') => row(turnId === 'run' ? 4 : 7, 'event_msg', { type: phase === 'completed' ? 'task_complete' : 'turn_aborted', turn_id: turnId }, turnId === 'run' ? undefined : clock), confirmFresh() { anchorTurnId = 'fresh-run' }, async appendFresh(old = false, confirmed = true) { const turnId = old ? 'unconfirmed-old-run' : 'fresh-run'; if (confirmed) anchorTurnId = turnId; const at = clock - (old ? 20000 : 0); await fs.appendFile(path, row(5, 'event_msg', { type: 'task_started', turn_id: turnId, turn_attribution: { turn_id: turnId, turn_trigger: 'composer' } }, at) + row(6, 'event_msg', { type: 'item_completed', thread_id: 'main', turn_id: turnId, item: { type: 'UserMessage', id: 'fresh-user', content: [{ type: 'text', text: 'Inspect the new live request' }] } }, at)) }, async restart() { await adapter.close(); adapter = await openAdapter() } }
}

test('a plain path replaced during its physical read cannot publish old material or a native fence', { timeout: 10000 }, async t => {
  const f = await fixture(t), open = fs.open, target = await fs.realpath(f.path)
  let release, entered
  const gate = new Promise(resolve => { release = resolve }), started = new Promise(resolve => { entered = resolve })
  t.after(() => release())
  t.mock.method(fs, 'open', async (path, ...args) => {
    const handle = await open(path, ...args)
    if (String(path) === target) { const read = handle.read; handle.read = async function (...values) { entered(); await gate; return read.apply(this, values) } }
    return handle
  })
  const pending = f.read()
  try { await started; await fs.rename(f.path, f.path + '.old'); await fs.writeFile(f.path, f.bytes) }
  finally { release() }
  const view = await pending
  assert.equal(view.health.state, 'failed')
  assert.ok(view.health.failures.some(failure => failure.error.code === 'record-file-replaced'))
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0)
  const retained = (await f.core.diagnostics()).retained
  assert.equal(retained.nativeCaptureProgress.records, 0); assert.equal(retained.nativeOriginFences.generations, 0)
})

for (const mutation of ['header', 'body']) test(`an acknowledged plain ${mutation} rewrite hidden by append is refused before new material is published`, async t => {
  const f = await fixture(t); assert.equal((await f.catchUp()).observation.coverage, 'caught-up')
  const before = await f.core.snapshot(), original = before.tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), inode = (await fs.stat(f.path)).ino
  const rewritten = f.bytes.replace(mutation === 'header' ? '/owned' : 'Original owned body', mutation === 'header' ? '/other' : 'Replaced owned body')
  assert.equal(Buffer.byteLength(rewritten), Buffer.byteLength(f.bytes))
  await fs.writeFile(f.path, rewritten + f.tail); assert.equal((await fs.stat(f.path)).ino, inode)
  const view = await f.catchUp()
  assert.ok(['native_origin_header_changed', 'native_origin_prefix_changed'].includes(view.observation.gap), JSON.stringify(view.observation))
  assert.deepEqual((await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), original)
})

test('a normal append spends bounded prefix-validation work before publishing its new tail without a collection failure', async t => {
  const f = await fixture(t, { padding: 'p'.repeat(600000) }); assert.equal((await f.catchUp()).observation.coverage, 'caught-up')
  const original = (await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt])
  await fs.appendFile(f.path, f.tail)
  const first = await f.read()
  assert.equal(first.observation.coverage, 'catching-up', JSON.stringify(first.observation))
  assert.equal((await f.core.snapshot()).tasks[0].events.some(event => event.text === 'New verified tail'), false)
  const sourceKey = await f.source(); assert.deepEqual((await f.core.failureReceipts({ sourceKey })).receipts, [])
  assert.equal((await f.catchUp()).observation.coverage, 'caught-up')
  const events = (await f.core.snapshot()).tasks[0].events
  assert.deepEqual(events.slice(0, original.length).map(event => [event.data.nativeRecordId, event.capturedAt]), original)
  assert.equal(events.filter(event => event.text === 'New verified tail').length, 1)
  assert.deepEqual((await f.core.failureReceipts({ sourceKey })).receipts, [])
})

test('a confirmed bad compressed frame creates one immediate collection notice and recovers without another notice', async t => {
  const f = await fixture(t, { compressed: true, invalidFrame: true }), view = await f.catchUp(), sourceKey = await f.source()
  assert.equal(view.health.state, 'failed')
  let receipts = (await f.core.failureReceipts({ sourceKey })).receipts
  assert.equal(receipts.length, 1); assert.equal(receipts[0].error.code, 'compressed-data-invalid'); assert.equal(receipts[0].error.category, 'collection')
  await f.core.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true })
  const claims = await f.core.claimNotifications(sourceKey)
  assert.equal(claims.length, 1); assert.equal(claims[0].kind, 'monitoring-failure'); assert.deepEqual(claims[0].receiptIds, [receipts[0].id])
  await f.read(); assert.deepEqual(await f.core.claimNotifications(sourceKey), [])
  assert.equal((await f.core.failureReceipts({ sourceKey })).receipts.length, 1)
  await fs.writeFile(f.path, f.encode(f.bytes)); assert.equal((await f.catchUp(false)).observation.coverage, 'caught-up')
  assert.equal((await f.read()).health.state, 'recovered')
  receipts = (await f.core.failureReceipts({ sourceKey })).receipts
  assert.equal(receipts.length, 1); assert.equal(receipts[0].state, 'recovered')
  assert.deepEqual(await f.core.claimNotifications(sourceKey), [])
  assert.equal((await f.core.nativeNotificationRecords({ sourceKey })).records.length, 1)
})

for (const rewrite of [false, true]) test(`a checkpoint during bounded append revalidation remains untrusted after restart (${rewrite ? 'rewritten' : 'unchanged'})`, async t => {
  const f = await fixture(t, { padding: 'p'.repeat(600000) }); assert.equal((await f.catchUp()).observation.coverage, 'caught-up')
  const original = (await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
  await fs.appendFile(f.path, f.tail); assert.equal((await f.read()).observation.coverage, 'catching-up')
  await f.restart()
  if (rewrite) await fs.writeFile(f.path, f.bytes.replace('Original owned body', 'Replaced owned body') + f.tail)
  const first = await f.read()
  assert.equal((await f.core.snapshot()).tasks[0].events.some(event => event.text === 'New verified tail'), false)
  if (rewrite) {
    const view = await f.catchUp()
    assert.ok(['record-rewrite-suspected', 'native_origin_prefix_changed'].includes(view.observation.gap), JSON.stringify(view.observation))
    assert.deepEqual((await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), original)
  } else {
    assert.equal(first.observation.coverage, 'catching-up')
    assert.equal((await f.catchUp()).observation.coverage, 'caught-up')
    assert.equal((await f.core.snapshot()).tasks[0].events.filter(event => event.text === 'New verified tail').length, 1)
    assert.deepEqual((await f.core.failureReceipts({ sourceKey: await f.source() })).receipts, [])
  }
})

test('unsupported compressed framing and ordinary decoded-budget waiting do not create monitoring failure receipts', async t => {
  for (const unsupported of [true, false]) await t.test(unsupported ? 'unknown-frame' : 'bounded-validation', async t => {
    const f = await fixture(t, { compressed: true, padding: 'p'.repeat(600000) })
    if (unsupported) await fs.writeFile(f.path, Buffer.from('this is not a supported zstd frame'))
    await f.read()
    const sourceKey = await f.source()
    assert.deepEqual((await f.core.failureReceipts({ sourceKey })).receipts, [])
    assert.deepEqual((await f.core.nativeNotificationRecords({ sourceKey })).records, [])
    if (!unsupported) assert.equal((await f.catchUp()).observation.coverage, 'caught-up')
    assert.deepEqual((await f.core.failureReceipts({ sourceKey })).receipts, [])
  })
})

for (const terminal of ['completed', 'aborted']) test(`an old main anchor ${terminal} across pumps restores its request only as ended history across adapter restart`, async t => {
  const f = await fixture(t, { oldAnchor: true, terminal, padding: 'p'.repeat(600000) })
  await f.read(); await f.read()
  assert.deepEqual((await f.core.snapshot()).tasks, [], 'pre-scan cannot publish starts, input, activity or capture progress')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  await f.restart()
  let view
  for (let pump = 0; pump < 100; pump++) {
    view = await f.read()
    const snapshot = await f.core.snapshot()
    assert.equal(snapshot.overview.runningTasks, 0)
    assert.ok(snapshot.tasks.every(task => !['queued', 'analyzing'].includes(task.analysis.state)))
    if (view.observation.coverage === 'retained-history') break
  }
  assert.equal(view.observation.coverage, 'retained-history', JSON.stringify(view.observation))
  const task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'run')
  assert.equal(task.status, 'idle'); assert.equal(task.currentRequest, 'Inspect this request')
  const start = task.events.find(event => event.data.nativeType === 'task_started'), input = task.events.find(event => event.kind === 'input')
  assert.ok(start); assert.ok(input)
  assert.ok(task.instructionContext.binding.originRefs.some(ref => ref.eventId === start.id))
  assert.ok(task.instructionContext.binding.originRefs.some(ref => ref.eventId === input.id))
  const captured = task.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
  await f.restart()
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.observation.coverage === 'retained-history') break }
  assert.equal(view.observation.coverage, 'retained-history')
  assert.deepEqual((await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'run').events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), captured)
  assert.deepEqual((await f.core.failureReceipts({ sourceKey: await f.source() })).receipts, [])
})

test('an unfinished old main anchor stays unknown at EOF and a later real completion remains observable', async t => {
  const f = await fixture(t, { oldAnchor: true, padding: 'p'.repeat(600000) })
  let view
  for (let pump = 0; pump < 12; pump++) { view = await f.read(); assert.equal((await f.core.snapshot()).overview.runningTasks, 0) }
  assert.equal(view.observation.phase, 'unknown')
  assert.equal(view.observation.metadataOffset, Buffer.byteLength(f.bytes))
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  await fs.appendFile(f.path, f.ending('completed'))
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.observation.coverage === 'retained-history') break }
  assert.equal(view.observation.coverage, 'retained-history', JSON.stringify(view.observation))
  const task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'run')
  assert.equal(task.status, 'idle'); assert.equal(task.currentRequest, 'Inspect this request')
})

for (const old of [false, true]) test(`an unfinished old anchor independently admits only a verified host-local successor (${old ? 'old-start' : 'fresh-start'})`, async t => {
  const f = await fixture(t, { oldAnchor: true, padding: 'p'.repeat(600000) })
  let view
  for (let pump = 0; pump < 12; pump++) view = await f.read()
  assert.equal(view.observation.phase, 'unknown')
  assert.equal(view.observation.metadataOffset, Buffer.byteLength(f.bytes))
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  await f.appendFresh(old)
  let snapshot, fresh
  for (let pump = 0; pump < 100; pump++) {
    view = await f.read(); snapshot = await f.core.snapshot(); fresh = snapshot.tasks.find(task => task.lifecycleId === 'fresh-run')
    const prior = snapshot.tasks.find(task => task.lifecycleId === 'run')
    if (prior) assert.equal(prior.status, 'unknown')
    if (fresh?.status === 'running' && fresh.currentRequest === 'Inspect the new live request') break
  }
  if (old) {
    assert.equal(snapshot.overview.runningTasks, 0)
    assert.equal(snapshot.tasks.some(task => task.lifecycleId === 'unconfirmed-old-run'), false)
    assert.equal(view.observation.phase, 'unknown')
    return
  }
  assert.equal(fresh?.status, 'running', JSON.stringify(view.observation))
  assert.equal(fresh.currentRequest, 'Inspect the new live request')
  const start = fresh.events.find(event => event.data.nativeType === 'task_started'), input = fresh.events.find(event => event.kind === 'input')
  assert.ok(start); assert.ok(input)
  assert.equal(input.text, 'Inspect the new live request')
  assert.equal(input.data.inputOrigin, 'user-entry')
  assert.equal(fresh.coverage.complete, true)
  assert.deepEqual(fresh.instructionContext.binding.originRefs.map(ref => ref.eventId).sort(), [start.id, input.id].sort())
  assert.ok(fresh.instructionContext.binding.originRefs.every(ref => ref.lifecycleId === 'fresh-run'))
  const prior = snapshot.tasks.find(task => task.lifecycleId === 'run')
  assert.equal(prior?.status, 'unknown')
  assert.ok(prior.coverage.gaps.includes('historical-execution-unconfirmed'))
  assert.equal(prior.events.find(event => event.kind === 'input').text, 'Inspect this request')
  assert.equal(prior.events.some(event => ['task_complete', 'turn_aborted'].includes(event.data.nativeType)), false)
  assert.equal(snapshot.overview.runningTasks, 1)
  const captured = snapshot.tasks.map(task => [task.lifecycleId, task.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])])
  await f.restart()
  for (let pump = 0; pump < 24; pump++) view = await f.read()
  snapshot = await f.core.snapshot()
  assert.equal(view.observation.phase, 'started')
  assert.equal(snapshot.tasks.find(task => task.lifecycleId === 'run').status, 'unknown')
  assert.ok(snapshot.tasks.find(task => task.lifecycleId === 'run').coverage.gaps.includes('historical-execution-unconfirmed'))
  assert.deepEqual(snapshot.tasks.map(task => [task.lifecycleId, task.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])]), captured)
  assert.deepEqual((await f.core.failureReceipts({ sourceKey: await f.source() })).receipts, [])
})

test('a verified Native anchor arriving after the cold scan reached the fresh start at EOF can still admit that execution', async t => {
  const f = await fixture(t, { oldAnchor: true, padding: 'p'.repeat(600000) })
  for (let pump = 0; pump < 12; pump++) await f.read()
  await f.appendFresh(false, false)
  let view
  for (let pump = 0; pump < 12; pump++) view = await f.read()
  assert.equal(view.observation.coldReadStage, 'scan')
  assert.equal(view.observation.metadataOffset, (await fs.stat(f.path)).size)
  assert.deepEqual((await f.core.snapshot()).tasks, [], 'a start with the wrong Native anchor cannot publish or authorize materials')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  f.confirmFresh()
  let snapshot, fresh
  for (let pump = 0; pump < 100; pump++) {
    view = await f.read(); snapshot = await f.core.snapshot(); fresh = snapshot.tasks.find(task => task.lifecycleId === 'fresh-run')
    if (fresh?.status === 'running' && fresh.currentRequest === 'Inspect the new live request') break
  }
  assert.equal(fresh?.status, 'running', JSON.stringify(view.observation))
  assert.equal(fresh.currentRequest, 'Inspect the new live request')
  const start = fresh.events.find(event => event.data.nativeType === 'task_started'), input = fresh.events.find(event => event.kind === 'input')
  assert.equal(fresh.coverage.complete, true)
  assert.deepEqual(fresh.instructionContext.binding.originRefs.map(ref => ref.eventId).sort(), [start.id, input.id].sort())
  const prior = snapshot.tasks.find(task => task.lifecycleId === 'run')
  assert.equal(prior.status, 'unknown')
  assert.ok(prior.coverage.gaps.includes('historical-execution-unconfirmed'))
})

test('a verified Native anchor arriving after the cold scan read a completed successor at EOF restores ended history', async t => {
  const f = await fixture(t, { oldAnchor: true, padding: 'p'.repeat(600000) })
  for (let pump = 0; pump < 12; pump++) await f.read()
  await f.appendFresh(false, false)
  await fs.appendFile(f.path, f.ending('completed', 'fresh-run'))
  let view
  for (let pump = 0; pump < 12; pump++) view = await f.read()
  assert.equal(view.observation.coldReadStage, 'scan')
  assert.equal(view.observation.metadataOffset, (await fs.stat(f.path)).size)
  assert.deepEqual((await f.core.snapshot()).tasks, [], 'the wrong Native anchor cannot publish even a completed successor')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  f.confirmFresh()
  let snapshot, fresh
  for (let pump = 0; pump < 100; pump++) {
    view = await f.read(); snapshot = await f.core.snapshot(); fresh = snapshot.tasks.find(task => task.lifecycleId === 'fresh-run')
    assert.equal(snapshot.overview.runningTasks, 0)
    assert.ok(snapshot.tasks.every(task => !['queued', 'analyzing'].includes(task.analysis.state)))
    if (fresh?.status === 'idle' && view.observation.coverage === 'retained-history') break
  }
  assert.equal(fresh?.status, 'idle', JSON.stringify(view.observation))
  assert.equal(fresh.currentRequest, 'Inspect the new live request')
  const start = fresh.events.find(event => event.data.nativeType === 'task_started'), input = fresh.events.find(event => event.kind === 'input')
  assert.deepEqual(fresh.instructionContext.binding.originRefs.map(ref => ref.eventId).sort(), [start.id, input.id].sort())
  assert.equal(fresh.events.filter(event => event.data.nativeType === 'task_complete').length, 1)
  assert.equal(view.observation.phase, 'completed')
  assert.equal(view.execution.lifecycleId, 'fresh-run'); assert.equal(view.execution.historical, true)
  const prior = snapshot.tasks.find(task => task.lifecycleId === 'run')
  assert.equal(prior.status, 'unknown')
  assert.ok(prior.coverage.gaps.includes('historical-execution-unconfirmed'))
})

test('an old terminal proof does not suppress a fresh host-local execution appended after the historical replay', async t => {
  const f = await fixture(t, { oldAnchor: true, terminal: 'completed', padding: 'p'.repeat(600000) })
  let view
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.observation.coverage === 'retained-history') break }
  assert.equal(view.observation.coverage, 'retained-history')
  const old = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'run'), oldEvents = old.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
  await f.appendFresh()
  let fresh
  for (let pump = 0; pump < 100; pump++) {
    await f.read(); fresh = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'fresh-run')
    if (fresh?.status === 'running' && fresh.currentRequest === 'Inspect the new live request') break
  }
  assert.equal(fresh?.status, 'running'); assert.equal(fresh.currentRequest, 'Inspect the new live request')
  const snapshot = await f.core.snapshot(), retained = snapshot.tasks.find(task => task.lifecycleId === 'run')
  assert.equal(snapshot.overview.runningTasks, 1); assert.equal(retained.status, 'idle'); assert.equal(retained.lastActivity, old.lastActivity)
  assert.deepEqual(retained.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), oldEvents)
})

test('an old terminal proof never exempts another unfinished old anchor or presents old history as its current conclusion', async t => {
  const f = await fixture(t, { oldAnchor: true, terminal: 'completed', padding: 'p'.repeat(600000) })
  let view
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.observation.coverage === 'retained-history') break }
  assert.equal(view.observation.coverage, 'retained-history')
  await f.appendFresh(true)
  for (let pump = 0; pump < 24; pump++) view = await f.read()
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.overview.runningTasks, 0)
  assert.equal(snapshot.tasks.some(task => task.lifecycleId === 'unconfirmed-old-run'), false)
  assert.equal(view.history.currentGap, 'historical-execution-unconfirmed')
  assert.equal(view.execution.historical, true)
  assert.ok(view.summary.startsWith('当前执行尚未确认'))
})

test('an old-anchor pre-scan retains the already checked file identity across its phase transition', async t => {
  const f = await fixture(t, { oldAnchor: true, terminal: 'completed', padding: 'p'.repeat(600000) })
  await f.read(); assert.deepEqual((await f.core.snapshot()).tasks, [])
  await fs.rename(f.path, f.path + '.old'); await fs.writeFile(f.path, f.bytes)
  let view
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.health?.state === 'failed') break }
  assert.equal(view.health.state, 'failed')
  assert.ok(view.health.failures.some(failure => failure.error.code === 'record-file-replaced'))
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
})

for (const replaced of [false, true]) test(`an old-anchor replay inherits its scan identity before reopening material IO (${replaced ? 'replaced' : 'unchanged'})`, async t => {
  const f = await fixture(t, { oldAnchor: true, terminal: 'completed', padding: 'p'.repeat(600000) })
  let view
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.observation.coldReadStage === 'verify') break }
  assert.equal(view.observation.coldReadStage, 'verify')
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  if (replaced) {
    const before = await fs.stat(f.path)
    await fs.rename(f.path, f.path + '.old'); await fs.writeFile(f.path, f.bytes)
    assert.notEqual((await fs.stat(f.path)).ino, before.ino)
  }
  for (let pump = 0; pump < 100; pump++) { view = await f.read(); if (view.health?.state === 'failed' || view.observation.coverage === 'retained-history') break }
  if (replaced) {
    assert.equal(view.health.state, 'failed', JSON.stringify(view.observation))
    assert.ok(view.health.failures.some(failure => failure.error.code === 'record-file-replaced'))
    assert.deepEqual((await f.core.snapshot()).tasks, [])
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  } else {
    assert.equal(view.observation.coverage, 'retained-history')
    assert.equal((await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'run').status, 'idle')
    assert.deepEqual((await f.core.failureReceipts({ sourceKey: await f.source() })).receipts, [])
  }
})

test('same-size plain rewrites with restored mtime cannot bypass the remaining ctime guard', async t => {
  const f = await fixture(t, { padding: 'p'.repeat(600000) }), fixed = Math.floor(Date.now() / 1000) - 1
  await fs.utimes(f.path, fixed, fixed); await f.read()
  const before = await fs.stat(f.path), events = (await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
  await fs.writeFile(f.path, f.bytes.replace('/owned', '/other')); await fs.utimes(f.path, fixed, fixed)
  const after = await fs.stat(f.path)
  assert.equal(after.ino, before.ino); assert.equal(after.size, before.size); assert.equal(after.mtimeMs, before.mtimeMs); assert.notEqual(after.ctimeMs, before.ctimeMs)
  const view = await f.catchUp()
  assert.equal(view.health.state, 'failed')
  assert.ok(view.health.failures.some(failure => failure.error.code === 'record-rewrite-suspected'))
  assert.deepEqual((await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), events)
})
