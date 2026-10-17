import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { setTimeout as delay } from 'node:timers/promises'

for (const [anchored, guard] of [[true, null], [false, null], [false, 'recovered-before-claim'], [false, 'paused'], [false, 'project-disabled'], [false, 'project-unknown'], [true, 'already-ended'], [true, 'ended'], [true, 'next-execution']]) test(`a production collection EIO ${anchored ? 'after' : 'before'} its lifecycle anchor respects ${guard ?? 'eligible health notice scope'}`, async t => {
  const clock = Date.now(), dir = await fs.mkdtemp(join(tmpdir(), 'collection-notice-')), token = 'a'.repeat(64), path = join(dir, 'main.jsonl')
  let server, core, adapter, broken = false, project = 'enabled'
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  const row = (type, payload) => JSON.stringify({ type, timestamp: new Date(clock).toISOString(), payload }) + '\n'
  await fs.writeFile(path, row('session_meta', { id: 'main' }) + row('event_msg', { type: 'task_started', turn_id: 'run', turn_attribution: { turn_id: 'run', turn_trigger: 'composer' } }) + (guard === 'already-ended' ? row('event_msg', { type: 'task_complete', turn_id: 'run' }) : ''))
  const thread = { id: 'main', path, cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null }
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: clock - 1000, profile: supportedProfile } },
    async readEnablement(cwd) { return { user: 'enabled', effective: cwd ? project : 'enabled' } },
    async readThread() { return { thread, anchorTurnId: 'run' } }, async close() {},
    async listThreads() { return { data: [thread], nextCursor: null } },
  }
  const open = fs.open, realPath = await fs.realpath(path)
  t.mock.method(fs, 'open', async (...args) => {
    const handle = await open(...args)
    if (String(args[0]) === realPath) { const read = handle.read; handle.read = async function (...params) { if (broken) { if (guard?.startsWith('project-')) project = guard === 'project-disabled' ? 'disabled' : 'unknown'; throw Object.assign(new Error('controlled record IO failure'), { code: 'EIO' }) } return read.apply(this, params) } }
    return handle
  })
  server = await startServer({ dataDir: join(dir, 'core'), now: () => clock })
  core = await connectCore({ dataDir: join(dir, 'core') })
  const submissions = [], notificationIO = {
    async settings() { return { authorization: 'authorized', alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: clock } },
    async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: clock } },
    async submit(value) { submissions.push(value); return { requestId: value.requestId, accepted: true, authorization: 'authorized', observedAt: clock } },
    async cancel() { return { pendingIds: [], observedAt: clock } }, close() {},
  }
  adapter = await startChatGPTAdapter({ core, native, notificationIO, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'health-notice', token, collectMaterials: true, pollMs: 0, now: () => clock })
  if (guard === 'paused') {
    const previous = await core.registerSource({ registrationId: 'chatgpt:health-notice', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
    await core.ingest(previous, { tasks: [{ id: 'main', lifecycleId: 'previous', title: 'Previously monitored chat', status: 'idle' }] })
    const control = await core.conversationMonitoring({ registrationId: 'chatgpt:health-notice', taskId: 'main' })
    await core.setConversationMonitoring({ ...control, paused: true })
  }
  const read = async (name = 'sentinel_current_chat', args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args, _meta: { thread_id: 'main' } } }) })
    assert.equal(response.status, 200); const body = await response.json(); assert.equal(body.result.isError, undefined); return body.result.structuredContent
  }
  if (anchored) { const initial = await read(); assert.equal(initial.execution.lifecycleId, 'run'); if (guard === 'already-ended') assert.equal(initial.execution.status, 'ended') }
  broken = true
  await fs.appendFile(path, row('event_msg', { type: 'agent_message', message: 'owned new output' }))
  const view = await read(), snapshot = await core.snapshot(), sourceKey = snapshot.sources[0].key
  const receipts = (await core.failureReceipts({ sourceKey })).receipts
  if (!guard?.startsWith('project-')) assert.equal(view.health.state, 'failed')
  assert.equal(receipts.length, 1); assert.equal(receipts[0].error.code, 'EIO')
  if (anchored) assert.equal(snapshot.tasks.find(task => task.id === 'main').status, 'unknown')
  assert.equal(receipts[0].mainId, 'main'); assert.equal(receipts[0].lifecycleId, anchored ? 'run' : null)
  if (guard?.startsWith('project-')) {
    await adapter.refresh()
    let records
    for (let wait = 0; wait < 100; wait++) { records = (await core.nativeNotificationRecords({ sourceKey })).records; if (records[0]?.native?.suppression) break; await delay(2) }
    assert.equal(submissions.length, 0)
    assert.equal(records[0].native.suppression.reason, guard === 'project-disabled' ? 'project-disabled' : 'project-unconfirmed')
    if (guard === 'project-unknown') {
      const attempt = records[0].native.attemptId
      project = 'enabled'; broken = false; await adapter.refresh()
      for (let wait = 0; wait < 100; wait++) { records = (await core.nativeNotificationRecords({ sourceKey })).records; if (records[0]?.status === 'submitted') break; await delay(2) }
      assert.equal(submissions.length, 1); assert.equal(records[0].delivery, 'system-accepted')
      assert.equal(records[0].native.attemptId, attempt); assert.deepEqual(records[0].receiptIds, [receipts[0].id])
      assert.equal(records[0].lifecycleId, null, 'qualification recovery never rewrites the original failure as a new execution')
    }
    return
  }
  if (guard === 'recovered-before-claim') { broken = false; assert.equal((await read()).health.state, 'recovered') }
  await core.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true })
  if (guard === 'ended' || guard === 'next-execution') await core.ingest(sourceKey, { tasks: [{ id: 'main', lifecycleId: guard === 'ended' ? 'run' : 'next', title: 'Main', status: guard === 'ended' ? 'idle' : 'running' }] })
  const claims = await core.claimNotifications(sourceKey)
  if (guard === 'paused') { assert.deepEqual(claims, []); return }
  assert.equal(claims.length, 1, JSON.stringify(await core.nativeNotificationRecords({ sourceKey })))
  assert.equal(claims[0].kind, 'monitoring-failure'); assert.deepEqual(claims[0].receiptIds, [receipts[0].id])
  assert.equal(claims[0].mainId, 'main', 'a verified main keeps its pause and project scope even before its lifecycle is known')
  assert.equal(claims[0].lifecycleId, anchored ? 'run' : null, 'an unanchored reader does not fabricate an execution')
  await read(); assert.deepEqual(await core.claimNotifications(sourceKey), [], 'the same failure is not notified twice')
  if (!anchored) {
    broken = false; assert.equal((await read()).health.state, 'recovered', 'the recovered pre-anchor failure remains visible in its own chat')
    const recovered = (await core.failureReceipts({ sourceKey })).receipts[0]
    assert.equal(recovered.state, 'recovered')
    assert.equal((await core.nativeNotificationRecords({ sourceKey })).records[0].status, 'claimed', 'a new execution does not discard the first monitoring failure or manufacture another claim')
    assert.deepEqual(await core.claimNotifications(sourceKey), [])
    const page = await read('sentinel_notification_records', { expectedThreadId: 'main' })
    assert.equal(page.records.length, 1, 'the same chat can inspect its original pre-anchor health notice')
    assert.equal(page.records[0].mainId, 'main'); assert.equal(page.records[0].lifecycleId, null)
  }
})

// Uses fixed notification IO and the real fresh-install runtime path.
test('six verified never-monitored main chats keep six ENOENT receipts and submit one immediate initial-enrollment summary', { timeout: 10_000 }, async t => {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-initial-enrollment-red-'))
  const sessionsRoot = join(dir, 'sessions'), archiveRoot = join(dir, 'archived_sessions'), coreDataDir = join(dir, 'core')
  let server, client, runtime, primary
  const submissions = [], clock = Date.now(), hostStartedAt = clock - 10_000
  try {
    await fs.mkdir(sessionsRoot); await fs.mkdir(archiveRoot)
    const threads = Array.from({ length: 6 }, (_, i) => ({ id: 'owned-initial-' + i, path: join(archiveRoot, i + '.jsonl'), cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null, archived: true }))
    const native = {
      async sampleHost() { return { available: true, hostRunId: 'owned-initial-host', hostStartedAt, profile: supportedProfile } },
      async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
      async listThreads({ archived }) { return { data: archived ? threads : [], nextCursor: null } },
      async readThread(id) { return { thread: threads.find(thread => thread.id === id), anchorTurnId: null } },
      async close() {},
    }
    const notificationIO = {
      async settings() { return { authorization: 'authorized', alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: Date.now() } },
      async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: Date.now(), historyGap: false, pendingHasMore: false, deliveredHasMore: false } },
      async submit(value) { submissions.push(value); return { requestId: value.requestId, accepted: true, authorization: 'authorized', observedAt: Date.now() } },
      async cancel() { return { pendingIds: [], observedAt: Date.now() } },
      close() {},
    }
    server = await startServer({ dataDir: coreDataDir })
    client = await connectCore({ dataDir: coreDataDir })
    const { configPath } = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir, sessionsRoot, metadataSocket: join(dir, 'unused-owned-metadata.sock'), pluginId: 'owned-initial-rollup@sentinel-test' })
    runtime = await startFromInstallation({ configPath, pollMs: 0, native, notificationIO })
    assert.equal(runtime.reused, false)
    await runtime.refresh()
    const deadline = performance.now() + 2000
    while (runtime.diagnostics().pendingOperations) {
      assert.ok(performance.now() < deadline, 'the owned first refresh settles without a new notification delay')
      await delay(2)
    }
    const snapshot = await client.snapshot(), source = snapshot.sources.find(value => value.descriptor.policy === 'chatgpt-main/1')
    assert.ok(source, 'the production adapter registered its real test source')
    const first = await client.failureReceipts({ sourceKey: source.key }), notices = await client.nativeNotificationRecords({ sourceKey: source.key })
    assert.equal(first.hasMore, false); assert.equal(first.historyGap, false)
    assert.equal(first.receipts.length, 6)
    assert.equal(new Set(first.receipts.map(value => value.mainId)).size, 6)
    assert.ok(first.receipts.every(value => value.domain === 'collection' && value.error.code === 'ENOENT' && value.state === 'active' && value.lifecycleId === null && value.recoveredAt === null))
    assert.equal(snapshot.tasks.length, 0, 'no nonexistent execution was fabricated')
    assert.equal(submissions.length, 1, 'the first eligible failure submits immediately; five other initial failures only join the summary')
    assert.ok(submissions.every(value => value.metadata.mainId !== null), 'aggregation must retain a real main scope')
    assert.equal(new Set(notices.records.flatMap(value => value.receiptIds)).size, 6)
    const originalIds = first.receipts.map(value => value.id).sort()
    for (let round = 0; round < 3; round++) await runtime.refresh()
    const end = performance.now() + 2000
    while (runtime.diagnostics().pendingOperations) {
      assert.ok(performance.now() < end, 'the owned repeated refresh settles')
      await delay(2)
    }
    const after = await client.failureReceipts({ sourceKey: source.key })
    assert.deepEqual(after.receipts.map(value => value.id).sort(), originalIds)
    assert.equal(submissions.length, 1, 'repeated polls do not resubmit the summary')
    t.diagnostic(JSON.stringify({ syntheticOnly: true, mainChats: 6, independentFailures: 6, notificationSubmissions: submissions.length, rounds: 4, nativeIO: 'fixed-no-OS', actualModelCalls: 0 }))
  } catch (error) { primary = error }
  const cleanup = []
  for (const [label, close] of [['runtime', () => runtime?.close()], ['client', () => client?.close()], ['server', () => server?.close()], ['directory', () => fs.rm(dir, { recursive: true, force: true })]]) {
    try { await close() } catch (error) { cleanup.push(Object.assign(new Error('owned fixture cleanup: ' + label, { cause: error }), { code: error.code })) }
  }
  t.diagnostic(JSON.stringify({ cleanupErrors: cleanup.map(error => error.code ?? error.name), primaryFailurePreserved: Boolean(primary), cleanupCount: cleanup.length }))
  if (primary || cleanup.length) throw new AggregateError([...(primary ? [primary] : []), ...cleanup], primary ? primary.message : 'Owned fixture cleanup failed')
})
