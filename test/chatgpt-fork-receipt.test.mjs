import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, appendFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

async function fixture(t, count = 50, options = {}) {
  let clock = Date.now(), turn = 'c', ordinal = 1 + count * 3
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-fork-receipt-')), sessionsRoot = join(dir, 'sessions'), archive = join(dir, 'archived_sessions')
  await mkdir(sessionsRoot); await mkdir(archive)
  const row = (n, type, payload, at = clock) => JSON.stringify({ ordinal: n, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
  const header = id => ({ id, history_mode: 'paginated', cwd: '/p', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion })
  const start = id => ({ type: 'task_started', turn_id: id, turn_attribution: { turn_id: id, turn_trigger: 'composer' } })
  const user = (thread, id) => ({ type: 'item_completed', thread_id: thread, turn_id: id, item: { type: 'UserMessage', id: id + 'u', content: [{ type: 'text', text: 'Read only ' + id }] } })
  let inherited = row(0, 'session_meta', header('o'), clock - 100000)
  for (let i = 0; i < count; i++) {
    const id = 'h' + i, n = 1 + i * 3
    const originalStart = options.missingStart ? { type: 'thread_settings_applied', thread_id: 'o' } : options.goal ? { ...start(id), turn_attribution: { turn_id: id, turn_trigger: 'goal' } } : start(id)
    inherited += row(n, 'event_msg', originalStart, clock - 100000) + row(n + 1, 'event_msg', user('o', id), clock - 100000) + row(n + 2, 'event_msg', { type: 'task_complete', turn_id: id }, clock - 100000)
  }
  const originalPath = join(archive, 'o.jsonl'), middlePath = join(archive, 'm.jsonl'), path = join(sessionsRoot, 'f.jsonl')
  let parentId = 'o', historyBytes = Buffer.byteLength(inherited)
  await writeFile(originalPath, inherited)
  if (options.nested) {
    const end = ordinal
    inherited = row(ordinal++, 'session_meta', { ...header('m'), forked_from_id: 'o', forked_from_ordinal_exclusive: end, history_base: { thread_id: 'o', end_ordinal_exclusive: end, end_byte_offset: historyBytes } }, clock - 100000) + row(ordinal++, 'event_msg', start('middle'), clock - 100000) + row(ordinal++, 'event_msg', user('m', 'middle'), clock - 100000) + row(ordinal++, 'event_msg', { type: 'task_complete', turn_id: 'middle' }, clock - 100000)
    await writeFile(middlePath, inherited); parentId = 'm'; historyBytes = Buffer.byteLength(inherited)
  }
  await writeFile(path, row(ordinal++, 'session_meta', { ...header('f'), forked_from_id: parentId, forked_from_ordinal_exclusive: ordinal - 1, history_base: { thread_id: parentId, end_ordinal_exclusive: ordinal - 1, end_byte_offset: historyBytes } }) + row(ordinal++, 'event_msg', start(turn)) + row(ordinal++, 'event_msg', user('f', turn)) + row(ordinal++, 'event_msg', { type: 'item_completed', thread_id: 'f', turn_id: turn, item: { type: 'AgentMessage', id: 'a', content: [{ type: 'Text', text: 'Reading' }] } }))
  const hostStartedAt = clock - 1000
  const metadata = id => ({ id, path: id === 'f' ? path : id === 'm' ? middlePath : originalPath, cwd: '/p', parentThreadId: null, forkedFromId: id === 'f' ? parentId : id === 'm' ? 'o' : null, source: 'vscode', originator: 'Codex Desktop' })
  const native = { async sampleHost() { return { available: true, hostRunId: 'h', hostStartedAt, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread(id) { return { thread: metadata(id), anchorTurnId: id === 'f' ? turn : null } }, async listThreads({ archived }) { return { data: archived ? [] : [metadata('f')], nextCursor: null } }, async close() {} }
  let server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }), core = await connectCore({ dataDir: join(dir, 'core') })
  let droppedAck = null
  if (options.lostAck) {
    const ingest = core.ingest
    core.ingest = async (...args) => {
      const result = await ingest(...args)
      if (!droppedAck && args[1].events?.some(event => event.data?.forkReceipt)) {
        droppedAck = args[1].events.map(event => ({ id: event.id, lifecycleId: event.taskLifecycleId, capturedAt: event.capturedAt }))
        throw new Error('synthetic loss after an accepted core HTTP receipt')
      }
      return result
    }
  }
  if (options.legacyCore) { const health = core.health; core.health = async () => { const value = await health(); return { ...value, features: value.features.filter(feature => feature !== 'chatgpt-fork-context/1') } } }
  const openAdapter = () => startChatGPTAdapter({ core, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'a', token: 'b'.repeat(64), pollMs: 0, collectMaterials: true, now: () => clock })
  let adapter = await openAdapter()
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const read = async () => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'b'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'f' } } }) })
    assert.equal(response.status, 200); const value = await response.json(); assert.equal(value.result.isError, undefined); return value.result.structuredContent
  }
  await adapter.refresh()
  for (let pump = 0; pump < 300; pump++) { const view = await read(); if (view.observation.coverage === 'caught-up') break; if (view.health?.state === 'failed' || view.observation.gap === 'core-capability-unavailable') break }
  return {
    get core() { return core }, get droppedAck() { return droppedAck }, read, now: () => clock,
    advance(milliseconds) { clock += milliseconds },
    async restart() { await adapter.close(); adapter = await openAdapter(); await adapter.refresh() },
    async restartCore() {
      await adapter.close(); await core.close(); await server.close()
      server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') })
      adapter = await openAdapter(); await adapter.refresh()
    },
    async next(id) {
      clock += 10
      await appendFile(path, row(ordinal++, 'event_msg', { type: 'task_complete', turn_id: turn }) + row(ordinal++, 'event_msg', start(id)) + row(ordinal++, 'event_msg', user('f', id)))
      turn = id; await read()
    },
  }
}

test('native fork retains fifty user constraints across twenty observed executions with two own origin refs', async t => {
  const f = await fixture(t)
  let snapshot = await f.core.snapshot(), current = snapshot.tasks.find(task => task.lifecycleId === 'c')
  assert.equal(current.currentRequest, 'Read only c')
  assert.equal(current.instructionContext.userLimitRefs.length, 50, JSON.stringify(current.instructionContext))
  assert.equal(current.instructionContext.binding.originRefs.length, 2)
  const original = current.events.find(event => event.data?.history && event.kind === 'input'), capturedAt = original.capturedAt
  for (let i = 0; i < 20; i++) await f.next('n' + i)
  await f.restart()
  await f.restartCore()
  assert.equal((await f.read()).observation.coverage, 'caught-up')
  snapshot = await f.core.snapshot(); current = snapshot.tasks.find(task => task.lifecycleId === 'n19')
  assert.equal(current.currentRequest, 'Read only n19')
  assert.equal(current.instructionContext.userLimitRefs.length, 50, JSON.stringify(current.instructionContext))
  assert.equal(snapshot.sources[0].hierarchyContext.instructionBindings.every(binding => binding.originRefs.length === 2), true)
  assert.equal(snapshot.tasks.flatMap(task => task.events).filter(event => event.id === original.id && event.data?.history).length, 1)
  assert.equal(snapshot.tasks.flatMap(task => task.events).find(event => event.id === original.id).capturedAt, capturedAt)
  assert.equal(current.events.some(event => event.data?.history), false)
})

test('an accepted core HTTP receipt lost at the adapter seam replays the same proof fold and capture times', async t => {
  const f = await fixture(t, 1, { lostAck: true }), view = await f.read(), current = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'c')
  assert.ok(f.droppedAck)
  assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(current.instructionContext.userLimitRefs.length, 1)
  assert.deepEqual(current.instructionContext.gaps, [])
  for (const first of f.droppedAck) {
    const retained = current.events.filter(event => event.id === first.id && event.taskLifecycleId === first.lifecycleId)
    assert.equal(retained.length, 1)
    assert.equal(retained[0].capturedAt, first.capturedAt)
  }
})

async function imported(f, mutate = () => {}) {
  const snapshot = await f.core.snapshot(), context = structuredClone(snapshot.sources[0].hierarchyContext)
  const batch = { tasks: snapshot.tasks.map(task => ({ id: task.id, sessionId: task.sessionId, lifecycleId: task.lifecycleId, title: task.title, status: task.status })), events: structuredClone(snapshot.tasks.flatMap(task => task.events)), hierarchyContext: context }
  mutate(batch)
  const source = await f.core.registerSource({ registrationId: 'copy-' + Math.random(), hostRunId: 'h', name: 'Fixture fork', policy: 'chatgpt-main/1' })
  await f.core.ingest(source, batch)
  return { source, batch, task: async () => (await f.core.snapshot()).tasks.find(task => task.sourceKey === source && task.lifecycleId === 'c') }
}

test('compact fork context remains unknown when one or all producers, starts, or its receipt expire', async t => {
  for (const missing of ['start', 'user', 'all-users', 'receipt', 'current-start']) await t.test(missing, async t => {
    const f = await fixture(t, 2), cutoff = f.now() - 86400000 + 1000
    const copy = await imported(f, batch => {
      let selected = false
      for (const event of batch.events) {
        const data = event.data ?? {}, match = missing === 'current-start' ? !data.history && data.nativeType === 'task_started' : data.history && (missing === 'receipt' ? Boolean(data.forkReceipt) : missing === 'start' ? data.nativeType === 'task_started' : event.kind === 'input')
        if (match && (!selected || missing === 'all-users')) { event.capturedAt = cutoff; selected = true }
      }
    })
    assert.equal((await copy.task()).instructionContext.userLimitRefs.length, 2)
    f.advance(2000)
    for (let read = 0; read < 2; read++) {
      const current = await copy.task()
      assert.equal(current.currentRequest, 'Read only c')
      assert.equal(current.instructionContext.userLimitRefs.length, 0)
      assert.ok(current.instructionContext.gaps.some(gap => gap.startsWith('fork_')))
    }
  })
})

test('public HTTP compact context never authorizes altered count, digest, lineage, trigger, or excerpt', async t => {
  for (const mutation of ['count', 'digest', 'generation', 'owner', 'ordinal', 'goal', 'relay', 'agent', 'truncated', 'redacted', 'budget']) await t.test(mutation, async t => {
    const f = await fixture(t, 1)
    const copy = await imported(f, batch => {
      const boundary = batch.events.find(event => event.data?.forkReceipt), receipt = boundary.data.forkReceipt
      const user = batch.events.find(event => event.data?.history && event.kind === 'input'), start = batch.events.find(event => event.data?.history && event.data.nativeType === 'task_started')
      if (mutation === 'count') receipt.users = 0
      if (mutation === 'digest') receipt.digest = '0'.repeat(64)
      if (mutation === 'generation') user.data.historyGeneration = '0'.repeat(64)
      if (mutation === 'owner') boundary.data.historyOwnerThreadId = 'unrelated'
      if (mutation === 'ordinal') user.data.recordOrdinal = receipt.endOrdinalExclusive
      if (mutation === 'goal') start.data.turnTrigger = 'goal'
      if (mutation === 'relay') user.data.senderSessionId = 'relay'
      if (mutation === 'agent') user.data.nativeType = 'item_completed/AgentMessage'
      if (mutation === 'truncated') user.data.truncated = true
      if (mutation === 'redacted') user.data.redacted = true
      if (mutation === 'budget') receipt.overBudget = true
    })
    const current = await copy.task()
    assert.equal(current.currentRequest, 'Read only c')
    assert.equal(current.instructionContext.userLimitRefs.length, 0)
    assert.ok(current.instructionContext.gaps.length > 0)
  })
})

test('replayed native proofs never acquire a new observation container or first capture after expiry', async t => {
  const f = await fixture(t, 1), copy = await imported(f, batch => {
    for (const event of batch.events) if (event.data?.history) event.capturedAt = f.now() - 86400000 + 1000
  })
  const original = (await copy.task()).events.find(event => event.data?.history && event.kind === 'input')
  f.advance(2000)
  const events = copy.batch.events.filter(event => event.data?.history || ['task_started', 'item_completed/UserMessage'].includes(event.data?.nativeType)).map(event => ({ ...structuredClone(event), id: event.data.history ? event.id : 'next-' + event.id, taskLifecycleId: 'next', capturedAt: f.now(), data: { ...event.data, runId: 'next' } }))
  // These two own records represent a genuinely new native execution. History
  // retains its original physical identity and must not receive a fresh clock.
  for (const event of events.filter(event => !event.data.history)) event.data.nativeRecordId = (event.data.nativeType === 'task_started' ? '1' : '2').repeat(64)
  const own = events.filter(event => !event.data.history), ref = event => ({ taskId: 'f', lifecycleId: 'next', eventId: event.id })
  await f.core.ingest(copy.source, { tasks: [{ id: 'f', sessionId: 'f', lifecycleId: 'next', title: 'Fork', status: 'running' }], events, hierarchyContext: { schemaVersion: 1, revision: copy.batch.hierarchyContext.revision + 1, sessions: [{ sessionId: 'f' }], relations: [], activations: [{ taskId: 'f', lifecycleId: 'next', sessionId: 'f', runId: 'next', state: 'running', stateRefs: [ref(own[0])] }], instructionBindings: [{ taskId: 'f', lifecycleId: 'next', inputRef: ref(own[1]), phase: 'consumed', origin: 'user-entry', originRefs: own.map(ref), ancestorLimitRefs: [], forkContext: structuredClone(copy.batch.hierarchyContext.instructionBindings[0].forkContext) }], coverage: { complete: true, branchGaps: [] } } })
  const next = (await f.core.snapshot()).tasks.find(task => task.sourceKey === copy.source && task.lifecycleId === 'next')
  assert.equal(next.events.some(event => event.data?.history), false)
  assert.equal(next.instructionContext.userLimitRefs.length, 0)
  assert.ok(next.instructionContext.gaps.includes('fork_receipt_missing_or_expired'))
  assert.equal((await f.core.snapshot()).tasks.filter(task => task.sourceKey === copy.source).flatMap(task => task.events).some(event => event.data?.history && event.id === original.id), false)
})

test('two copies of one native producer in a first HTTP batch cannot extend its first capture', async t => {
  const f = await fixture(t, 1), copy = await imported(f, batch => {
    const original = batch.events.find(event => event.data?.history && event.kind === 'input')
    original.capturedAt = f.now() - 86400000 + 1000
    batch.tasks.push({ id: 'f', sessionId: 'f', lifecycleId: 'duplicate', title: 'Fork', status: 'unknown' })
    batch.events.push({ ...structuredClone(original), taskLifecycleId: 'duplicate', capturedAt: f.now(), data: { ...original.data, runId: 'duplicate' } })
  })
  f.advance(2000)
  const current = await copy.task()
  assert.deepEqual(current.instructionContext.userLimitRefs, [])
  assert.ok(current.instructionContext.gaps.length > 0)
  const duplicate = (await f.core.snapshot()).tasks.find(task => task.sourceKey === copy.source && task.lifecycleId === 'duplicate')
  assert.equal(duplicate.events.length, 0)
})

test('expiry of an implicit historical start cannot reuse a previously completed analysis', async t => {
  const f = await fixture(t, 1), copy = await imported(f, batch => {
    batch.events.find(event => event.data?.history && event.data.nativeType === 'task_started').capturedAt = f.now() - 86400000 + 1000
  })
  const before = await copy.task()
  assert.equal(before.analysis.resultDisposition, 'current', JSON.stringify(before.analysis))
  f.advance(2000)
  const after = await copy.task()
  assert.ok(after.instructionContext.gaps.length > 0)
  assert.notEqual(after.analysis.resultDisposition, 'current')
  assert.notEqual(after.analysis.resultDisposition, 'reused')
})

test('recognizable native producer without a parsed start is an explicit unconfirmed expectation', async t => {
  const f = await fixture(t, 1, { missingStart: true }), task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'c')
  assert.equal(task.currentRequest, 'Read only c')
  assert.equal(task.instructionContext.userLimitRefs.length, 0)
  assert.ok(task.instructionContext.gaps.length > 0)
  assert.equal(task.events.find(event => event.data?.forkReceipt).data.forkReceipt.unconfirmed, 1)
})

test('native goal text remains historical without user authorization', async t => {
  const f = await fixture(t, 1, { goal: true }), task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'c')
  assert.equal(task.currentRequest, 'Read only c')
  assert.equal(task.instructionContext.userLimitRefs.length, 0)
  assert.equal(task.events.filter(event => event.data?.history).some(event => event.authorizationEligible), false)
})

test('compact receipt DTO rejects unbounded or malformed fields through public HTTP', async t => {
  const f = await fixture(t, 1)
  assert.ok((await f.core.health()).features.includes('chatgpt-fork-context/1'))
  for (const mutation of ['extra', 'number', 'marker']) await assert.rejects(imported(f, batch => {
    const receipt = batch.events.find(event => event.data?.forkReceipt).data.forkReceipt
    if (mutation === 'extra') receipt.refs = new Array(200).fill('unbounded')
    if (mutation === 'number') receipt.users = 'one'
    if (mutation === 'marker') delete batch.events.find(event => event.data?.forkHistoryPending).data.forkGeneration
  }), error => error.status === 400)
})

test('material collection stops before an older core can discard compact fork semantics', async t => {
  const f = await fixture(t, 1, { legacyCore: true })
  assert.equal((await f.read()).observation.gap, 'core-capability-unavailable')
  assert.deepEqual((await f.core.snapshot()).tasks, [])
})

test('native fork proof budget exhaustion preserves collection and declares context unknown', async t => {
  const f = await fixture(t, 1025), task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'c')
  assert.equal(task.currentRequest, 'Read only c')
  assert.equal((await f.read()).observation.coverage, 'caught-up')
  assert.equal(task.instructionContext.binding.originRefs.length, 2)
  assert.deepEqual(task.instructionContext.userLimitRefs, [])
  assert.ok(task.instructionContext.gaps.includes('fork_context_budget_exceeded'))
  assert.equal(task.events.find(event => event.data?.forkReceipt).data.forkReceipt.users, 1025)
})


test('compact nested native receipts require every header-owner generation and cannot omit an inherited range', async t => {
  const f = await fixture(t, 2, { nested: true }), current = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'c')
  assert.equal(current.instructionContext.userLimitRefs.length, 3, JSON.stringify(current.instructionContext))
  for (const mutation of ['omitted-parent', 'missing-receipt', 'wrong-generation']) {
    const copy = await imported(f, batch => {
      const root = batch.events.find(event => event.data?.historyOwnerThreadId === 'f' && event.data.forkReceipt)
      if (mutation === 'omitted-parent') root.data.forkReceipt.parentId = null
      if (mutation === 'missing-receipt') batch.events = batch.events.filter(event => event.data?.historyOwnerThreadId !== 'm')
      if (mutation === 'wrong-generation') root.data.forkReceipt.ownerGeneration = '0'.repeat(64)
    })
    assert.deepEqual((await copy.task()).instructionContext.userLimitRefs, [])
    assert.ok((await copy.task()).instructionContext.gaps.length)
  }
})
