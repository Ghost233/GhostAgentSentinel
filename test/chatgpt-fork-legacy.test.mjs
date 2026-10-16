import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const ALPHA2 = '0.162.0-alpha.2'

async function fixture(t, { producer = ALPHA2, trigger = null, form = 'typed', host = supportedProfile, missingCapability = false, lostAck = false } = {}) {
  let clock = Date.now(), turn = 'current', ordinal = 8, calls = 0, physicalBytes = 0, maximumBytes = 0, maximumEvents = 0, dropped = false
  t.mock.method(Date, 'now', () => clock)
  const hostStartedAt = clock - 20000
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-legacy-fork-')), sessionsRoot = join(dir, 'sessions'), archive = join(dir, 'archived_sessions'), path = join(sessionsRoot, 'fork.jsonl'), original = join(archive, 'original.jsonl')
  await fs.mkdir(sessionsRoot); await fs.mkdir(archive)
  const row = (n, type, payload, at = clock) => JSON.stringify({ ordinal: n, type, payload, timestamp: new Date(at).toISOString() }) + '\n'
  const header = (id, cli = supportedProfile.cliVersion) => ({ id, cwd: '/legacy-project', originator: 'Codex Desktop', source: 'vscode', cli_version: cli, history_mode: 'paginated' })
  const start = (id, value) => ({ type: 'task_started', turn_id: id, ...(value ? { turn_attribution: { turn_id: id, turn_trigger: value } } : {}) })
  const typed = (thread, id, text) => ({ type: 'item_completed', thread_id: thread, turn_id: id, item: { type: 'UserMessage', id: id + '-user', content: [{ type: 'text', text }] } })
  const producerRow = form === 'typed' ? row(2, 'event_msg', typed('original', 'old-run', 'Legacy user requirement'), clock - 100000) : form === 'role-user' ? row(2, 'response_item', { type: 'message', role: 'user', id: 'old-mirror', content: [{ type: 'input_text', text: 'Legacy role mirror' }], internal_chat_message_metadata_passthrough: { turn_id: 'old-run' } }, clock - 100000) : row(2, 'event_msg', { type: 'user_message', turn_id: 'old-run', message: 'Legacy event message' }, clock - 100000)
  const prefix = row(0, 'session_meta', header('original', producer), clock - 100000) + row(1, 'event_msg', start('old-run', trigger), clock - 100000) + producerRow + row(3, 'event_msg', { type: 'task_complete', turn_id: 'old-run' }, clock - 100000)
  await fs.writeFile(original, prefix)
  await fs.writeFile(path, row(4, 'session_meta', { ...header('fork'), forked_from_id: 'original', history_base: { thread_id: 'original', end_ordinal_exclusive: 4, end_byte_offset: Buffer.byteLength(prefix) } }) + row(5, 'event_msg', start(turn, 'composer')) + row(6, 'event_msg', typed('fork', turn, 'Inspect independent current request')) + row(7, 'event_msg', { type: 'item_completed', thread_id: 'fork', turn_id: turn, item: { type: 'AgentMessage', id: 'current-output', content: [{ type: 'Text', text: 'New current activity' }] } }))
  const watched = new Set(await Promise.all([path, original].map(value => fs.realpath(value)))), opened = new Set(), open = fs.open
  t.mock.method(fs, 'open', async (...args) => {
    const handle = await open(...args)
    if (watched.has(String(args[0]))) {
      opened.add(handle); const read = handle.read, close = handle.close
      handle.read = async function (...values) { const result = await read.apply(this, values); physicalBytes += result.bytesRead; return result }
      handle.close = async function (...values) { try { return await close.apply(this, values) } finally { opened.delete(handle) } }
    }
    return handle
  })
  const native = { async sampleHost() { return { available: true, hostRunId: 'legacy-host', hostStartedAt, profile: host } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread(id) { calls++; return { thread: { id, path: id === 'fork' ? path : original, cwd: '/legacy-project', parentThreadId: null, forkedFromId: id === 'fork' ? 'original' : null, source: 'vscode', originator: 'Codex Desktop' }, anchorTurnId: id === 'fork' ? turn : 'old-run' } }, async close() {} }
  let server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }), core = await connectCore({ dataDir: join(dir, 'core') }), adapter
  const openAdapter = async () => {
    const connection = { ...core, async ingest(...args) {
      maximumEvents = Math.max(maximumEvents, args[1].events?.length ?? 0)
      const value = await core.ingest(...args)
      if (lostAck && !dropped && args[1].events?.some(event => event.data?.forkReceipt)) { dropped = true; throw new Error('controlled legacy acknowledgement loss') }
      return value
    } }
    if (missingCapability) connection.health = async () => { const health = await core.health(); return { ...health, features: health.features.filter(feature => feature !== 'chatgpt-fork-legacy/1') } }
    adapter = await startChatGPTAdapter({ core: connection, native, dataDir: join(dir, 'adapter'), sessionsRoot, installationId: 'legacy-install', token: '6'.repeat(64), pollMs: 0, now: () => clock, collectMaterials: true })
  }
  await openAdapter()
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await fs.rm(dir, { recursive: true, force: true }); assert.equal(opened.size, 0) })
  const read = async () => {
    physicalBytes = 0
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + '6'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'fork' } } }) })
    assert.equal(response.status, 200); const result = await response.json(); assert.equal(result.result.isError, undefined)
    maximumBytes = Math.max(maximumBytes, physicalBytes); assert.ok(physicalBytes <= 262144); assert.ok(maximumEvents <= 16)
    return result.result.structuredContent
  }
  return { get core() { return core }, read, get calls() { return calls }, get maximumBytes() { return maximumBytes }, async caughtUp() { let view; for (let i = 0; i < 20; i++) { view = await read(); if (view.observation.coverage === 'caught-up' || view.observation.gap === 'core-capability-unavailable' || view.observation.gap === 'unsupported-host-version') break } return view }, async restart() { await adapter.close(); await core.close(); await server.close(); server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') }); await openAdapter() }, advance(ms) { clock += ms }, async next() { clock += 10; const previous = turn; turn += '-next'; await fs.appendFile(path, row(ordinal++, 'event_msg', { type: 'task_complete', turn_id: previous }) + row(ordinal++, 'event_msg', start(turn, 'composer')) + row(ordinal++, 'event_msg', typed('fork', turn, 'Inspect another independent request'))); return read() } }
}

test('an exact historical alpha.2 producer remains user-unverified while the unchanged alpha.17.2 baseline retains typed context', async t => {
  for (const [name, options, users, unconfirmed] of [
    ['current-baseline', { producer: supportedProfile.cliVersion, trigger: 'composer' }, 1, 0],
    ['observed-alpha2-no-attribution', {}, 1, 1],
    ['alpha2-even-with-composer', { trigger: 'composer' }, 1, 0],
    ['alpha2-role-user-only', { form: 'role-user' }, 0, 0],
    ['alpha2-user-message-only', { form: 'user-message' }, 0, 0],
  ]) await t.test(name, async t => {
    const f = await fixture(t, options), view = await f.caughtUp()
    assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation)); assert.equal(view.forkProvenance.complete, true)
    const task = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'current'), receipt = task.events.find(event => event.data?.forkReceipt).data.forkReceipt
    assert.equal(task.currentRequest, 'Inspect independent current request'); assert.equal(task.events.find(event => event.kind === 'input' && !event.data.history).authorizationEligible, true)
    assert.equal(receipt.users, users); assert.equal(receipt.unconfirmed, unconfirmed)
    if (name === 'current-baseline') { assert.equal(receipt.version, 1); assert.equal(Object.hasOwn(receipt, 'userProofMode'), false); assert.deepEqual(task.instructionContext.gaps, []); assert.equal(task.instructionContext.userLimitRefs.length, 1) }
    else { assert.equal(receipt.version, 2); assert.equal(receipt.userProofMode, 'legacy-unverified'); assert.deepEqual(task.instructionContext.userLimitRefs, []); assert.ok(task.instructionContext.gaps.includes('fork_user_context_unverified')); assert.notEqual(view.signal, 'green'); assert.ok(task.events.filter(event => event.data?.history).every(event => event.authorizationEligible !== true)) }
  })
})

test('other historical versions, a changed current Host and an older core never silently consume the legacy semantics', async t => {
  for (const [name, options] of [['unknown-alpha3', { producer: '0.162.0-alpha.3' }], ['current-host-alpha2', { host: { ...supportedProfile, cliVersion: ALPHA2 } }], ['missing-legacy-capability', { missingCapability: true }], ['missing-legacy-capability-modern', { producer: supportedProfile.cliVersion, trigger: 'composer', missingCapability: true }]]) await t.test(name, async t => {
    const f = await fixture(t, options), view = await f.caughtUp()
    assert.notEqual(view.forkProvenance?.complete, true)
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.history), false)
    if (name.startsWith('missing-legacy-capability')) { assert.equal(view.observation.gap, 'core-capability-unavailable'); assert.equal(f.calls, 0); assert.equal(f.maximumBytes, 0) }
    if (name === 'current-host-alpha2') { assert.equal(view.observation.gap, 'unsupported-host-version'); assert.equal(f.maximumBytes, 0) }
  })
})

test('legacy proof mode is a strict version-two HTTP field rather than an optional hint', async t => {
  const f = await fixture(t); await f.caughtUp()
  const snapshot = await f.core.snapshot(), task = snapshot.tasks.find(task => task.lifecycleId === 'current'), context = snapshot.sources.find(source => source.key === task.sourceKey).hierarchyContext
  for (const [name, mutate] of [['missing', receipt => { delete receipt.userProofMode }], ['other-enum', receipt => { receipt.userProofMode = 'typed-producers-v1' }], ['object', receipt => { receipt.userProofMode = {} }]]) await t.test(name, async () => {
    const source = await f.core.registerSource({ registrationId: 'invalid-legacy-' + name, hostRunId: 'host', name: 'Own invalid legacy DTO', policy: 'chatgpt-main/1' }), events = structuredClone(task.events)
    mutate(events.find(event => event.data?.forkReceipt).data.forkReceipt)
    await assert.rejects(f.core.ingest(source, { tasks: [{ id: task.id, sessionId: task.id, lifecycleId: task.lifecycleId, title: 'Own fork', status: 'running' }], events, hierarchyContext: structuredClone(context) }), error => error.status === 422 && error.code === 'hierarchy_context_invalid')
  })
})

test('legacy ACK replay and core-adapter recovery preserve the semantic gap and first clocks until real evidence expiry', async t => {
  for (const form of ['typed', 'role-user']) await t.test(form, async t => {
    const f = await fixture(t, { form, lostAck: true }), initialView = await f.caughtUp(), initial = await f.core.snapshot(), first = initial.tasks.find(task => task.lifecycleId === 'current')
    assert.equal(initialView.forkProvenance.complete, true)
    const original = first.events.filter(event => event.data?.history).map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence])
    assert.ok(original.length)
    f.advance(500); await f.restart(); await f.caughtUp(); await f.next(); await f.caughtUp()
    const after = await f.core.snapshot(), current = after.tasks.find(task => task.lifecycleId === 'current-next')
    assert.equal(current.currentRequest, 'Inspect another independent request'); assert.ok(current.instructionContext.gaps.includes('fork_user_context_unverified')); assert.deepEqual(current.instructionContext.userLimitRefs, [])
    const history = after.tasks.flatMap(task => task.events).filter(event => event.data?.history)
    for (const [id, at, text, evidence] of original) assert.ok(history.some(event => event.data.nativeRecordId === id && event.capturedAt === at && event.text === text && event.evidence === evidence))
    assert.ok(history.every(event => event.authorizationEligible !== true))
    assert.ok(history.some(event => event.data?.forkReceipt?.version === 2 && event.data.forkReceipt.userProofMode === 'legacy-unverified'))
    f.advance(86400001); await f.next(); const expiredView = await f.caughtUp(), expired = await f.core.snapshot(), fresh = expired.tasks.find(task => task.lifecycleId === 'current-next-next')
    assert.equal(fresh.currentRequest, 'Inspect another independent request')
    assert.ok(fresh.instructionContext.gaps.includes('fork_receipt_missing_or_expired')); assert.deepEqual(fresh.instructionContext.userLimitRefs, [])
    assert.notEqual(expiredView.signal, 'green')
    assert.equal(expired.tasks.flatMap(task => task.events).some(event => original.some(([id]) => event.data?.nativeRecordId === id)), false)
  })
})
