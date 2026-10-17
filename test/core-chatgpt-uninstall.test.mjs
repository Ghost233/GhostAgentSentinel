import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { NativePrefix } from '../src/native-origin.mjs'
import { nativeRecordIdentity } from '../src/fork-proof.mjs'
import { DatabaseSync } from 'node:sqlite'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-scoped-uninstall-'))
  let clock = Date.now(), server, core
  t.mock.method(Date, 'now', () => clock)
  const start = async () => { server = await startServer({ dataDir: dir, now: () => clock, leaseMs: 1e9 }); core = await connectCore({ dataDir: dir }) }
  t.after(async () => { await core?.close(); await server?.close(); await rm(dir, { recursive: true, force: true }) })
  await start()
  return { dir, get core() { return core }, get now() { return clock }, advance(ms = 1000) { clock += ms }, async post(body) { const response = await fetch(server.url + '/v1/chatgpt/installation/purge', { method: 'POST', headers: { authorization: 'Bearer ' + server.token, 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: response.status, body: await response.json() } }, async restart() { await core.close(); await server.close(); await start() } }
}

async function seed(f, descriptor, id) {
  const source = await f.core.registerSource(descriptor)
  await f.core.ingest(source, { tasks: [{ id, lifecycleId: 'run', title: 'Owned ' + id, status: 'running' }], events: [{ id: 'body', taskId: id, taskLifecycleId: 'run', kind: 'output', capturedAt: f.now, occurredAt: f.now, text: 'private body ' + id }] })
  return source
}

test('public installation purge refuses a connected target and removes only its exact stopped ChatGPT scope', async t => {
  const f = await fixture(t)
  const target = await seed(f, { registrationId: 'chatgpt:install-a', hostRunId: 'target-host', name: 'Target', policy: 'chatgpt-main/1', enroll: true }, 'target')
  const other = await seed(f, { registrationId: 'chatgpt:install-a-extra', hostRunId: 'other-host', name: 'Other ChatGPT', policy: 'chatgpt-main/1' }, 'other')
  const dsh = await seed(f, { registrationId: 'chatgpt:install-a', hostRunId: 'dsh-host', name: 'DSH collision without ChatGPT policy' }, 'dsh')
  const before = await f.core.snapshot(), refused = await f.post({ installationId: 'install-a' })
  assert.equal(refused.status, 409)
  assert.equal(refused.body.code, 'chatgpt_installation_connected')
  await assert.rejects(f.core.purgeChatGPTInstallation({ installationId: 'install-a' }), error => error.status === 409 && error.code === 'chatgpt_installation_connected')
  assert.deepEqual(await f.core.snapshot(), before)
  await f.core.unregisterSource(target, 'owned helper stopped')
  const result = await f.core.purgeChatGPTInstallation({ installationId: 'install-a' })
  assert.equal(result.status, 'complete')
  assert.equal(result.logical.status, 'purged')
  assert.equal(result.logical.removed.sources, 1)
  assert.equal(result.logical.removed.tasks, 1)
  assert.equal(result.logical.removed.events, 1)
  assert.equal(result.physical.status, 'checkpointed')
  const after = await f.core.snapshot()
  assert.deepEqual(after.sources.map(source => source.key).sort(), [other, dsh].sort())
  assert.equal(JSON.stringify(after).includes('private body target'), false)
  assert.ok(JSON.stringify(after).includes('private body other'))
  assert.ok(JSON.stringify(after).includes('private body dsh'))
  assert.equal((await f.core.sourcePlan()).entries.some(entry => entry.registrationId === 'chatgpt:install-a'), true, 'a same-registration DSH policy retains the shared enrollment entry')
  await f.core.heartbeat(dsh)
  await f.core.ingest(dsh, { tasks: [{ id: 'dsh', lifecycleId: 'run', title: 'Still owned DSH', status: 'running' }] })
  await f.restart()
  assert.equal(JSON.stringify(await f.core.snapshot()).includes('private body target'), false)
  const repeated = await f.core.purgeChatGPTInstallation({ installationId: 'install-a' })
  assert.equal(repeated.status, 'complete')
  assert.equal(repeated.logical.removed.sources, 0)
})

async function nativeSeed(f, installationId) {
  const registrationId = 'chatgpt:' + installationId, hostRunId = 'host-' + installationId
  const source = await f.core.registerSource({ registrationId, hostRunId, name: 'Owned native ' + installationId, policy: 'chatgpt-main/1', nativeOriginVersion: 2, enroll: true })
  const hash = value => createHash('sha256').update(value).digest('hex'), key = hash('storage-' + installationId), prefix = new NativePrefix()
  prefix.update(Buffer.from('owned native bytes ' + installationId))
  const proof = { key, headerHash: hash('header-' + installationId), revision: 0, base: null, verifiedBase: null, next: prefix.snapshot(), complete: true }, nativeType = 'item_completed/AgentMessage'
  await f.core.ingest(source, { nativeOrigins: [proof], tasks: [{ id: 'native', lifecycleId: 'run', title: 'Owned native', status: 'idle' }], events: [{ id: 'native-body', taskId: 'native', taskLifecycleId: 'run', kind: 'output', capturedAt: f.now, occurredAt: f.now, text: 'private native ' + installationId, data: { sourceKind: 'chatgpt-native', nativeType, nativeGenerationKey: key, nativeRecordStart: 0, nativeRecordEnd: proof.next.offset, nativeRecordId: nativeRecordIdentity('native-storage/2:' + key, null, 0, nativeType) } }] })
  const failure = await f.core.recordCollectionOutcome({ sourceKey: source, mainId: 'native', lifecycleId: 'run', channel: 'default', roundId: 'failure-' + installationId, outcome: 'failed', settledAt: f.now, error: { code: 'EIO', stage: 'collection' } })
  await f.core.setNativeNotificationEligibility({ sourceKey: source, hostRunId, appRunning: true, pluginEnabled: true })
  const [notice] = await f.core.claimNotifications(source)
  assert.ok(notice)
  const noticeResult = { notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: 'ghostsentinel:' + 'a'.repeat(24) + ':' + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now }
  await f.core.recordNativeNotificationResult({ ...noticeResult, event: 'authorization', authorization: 'authorized' })
  assert.equal((await f.core.recordNativeNotificationResult({ ...noticeResult, event: 'submitting' })).maySubmit, true)
  return { source, registrationId, key, failure, notice, noticeResult }
}

test('purge clears native progress, fences, preferences and failure links while late notice results cannot revive them', async t => {
  const f = await fixture(t), target = await nativeSeed(f, 'native-a'), other = await nativeSeed(f, 'native-a-extra')
  const preference = await f.core.conversationMonitoring({ registrationId: target.registrationId, taskId: 'native' })
  await f.core.setConversationMonitoring({ ...preference, paused: true })
  const otherFence = await f.core.nativeOriginFence(other.source, other.key)
  const otherNotices = await f.core.nativeNotificationRecords({ sourceKey: other.source })
  await f.core.unregisterSource(target.source, 'owned helper stopped')
  const result = await f.core.purgeChatGPTInstallation({ installationId: 'native-a' })
  assert.equal(result.status, 'complete')
  const diagnostics = await f.core.diagnostics()
  assert.equal(diagnostics.retained.nativeCaptureProgress.records, 1)
  assert.equal(diagnostics.retained.nativeOriginFences.generations, 1)
  assert.equal(diagnostics.retained.nativeOriginFences.registrations, 1)
  assert.equal(diagnostics.failureReceipts.receipts, 1)
  assert.equal(diagnostics.nativeNotifications.failureLinks, 1)
  assert.equal(diagnostics.configuration.conversationMonitoringScopes, 0)
  assert.deepEqual(await f.core.nativeOriginFence(other.source, other.key), otherFence)
  assert.deepEqual(await f.core.nativeNotificationRecords({ sourceKey: other.source }), otherNotices)
  assert.equal((await f.core.failureReceipts({ sourceKey: other.source })).receipts[0].id, other.failure.receipt.id)
  assert.equal((await f.core.sourcePlan()).entries.some(entry => entry.registrationId === target.registrationId), false)
  await assert.rejects(f.core.recordNativeNotificationResult({ ...target.noticeResult, event: 'accepted' }), /does not match/)
  await assert.rejects(f.core.recordCollectionOutcome({ sourceKey: target.source, mainId: null, lifecycleId: null, roundId: 'late-old-failure', outcome: 'failed', settledAt: f.now, error: { code: 'EIO', stage: 'collection' } }), /known ChatGPT source/)
  assert.equal((await f.core.diagnostics()).retained.notifications, 1)
  const fresh = await f.core.registerSource({ registrationId: target.registrationId, hostRunId: 'fresh-host', name: 'Fresh install', policy: 'chatgpt-main/1', nativeOriginVersion: 2 })
  assert.equal(await f.core.nativeOriginFence(fresh, target.key), null)
  await f.core.ingest(fresh, { tasks: [{ id: 'native', lifecycleId: 'fresh', title: 'Fresh install', status: 'idle' }] })
  assert.equal((await f.core.conversationMonitoring({ registrationId: target.registrationId, taskId: 'native' })).paused, false)
  await f.restart()
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 1)
  assert.equal((await f.core.diagnostics()).nativeNotifications.failureLinks, 1)
})

test('empty or expanded purge scopes cannot become a global or prefix deletion', async t => {
  const f = await fixture(t)
  await seed(f, { registrationId: 'chatgpt:exact-install', hostRunId: 'host', name: 'Owned', policy: 'chatgpt-main/1' }, 'retained')
  assert.ok((await f.core.health()).features.includes('chatgpt-installation-purge/1'))
  const before = await f.core.snapshot()
  for (const scope of [{}, { installationId: '' }, { installationId: '   ' }, { installationId: null }, { installationId: 'exact-install', sourceKeys: [] }, { installationId: 'exact-install', all: true }]) assert.equal((await f.post(scope)).status, 400)
  for (const installationId of ['exact', '*']) assert.equal((await f.core.purgeChatGPTInstallation({ installationId })).logical.removed.sources, 0)
  assert.deepEqual(await f.core.snapshot(), before)
})

test('orphan native progress and paused preferences are purged even after normal source retention removed its source', async t => {
  const f = await fixture(t), target = await nativeSeed(f, 'expired-install')
  const preference = await f.core.conversationMonitoring({ registrationId: target.registrationId, taskId: 'native' })
  await f.core.setConversationMonitoring({ ...preference, paused: true })
  await f.core.unregisterSource(target.source)
  f.advance(86400001)
  assert.equal((await f.core.snapshot()).sources.length, 0)
  const before = await f.core.diagnostics()
  assert.equal(before.retained.nativeCaptureProgress.records, 1)
  assert.equal(before.retained.nativeOriginFences.generations, 1)
  assert.equal(before.configuration.conversationMonitoringScopes, 1)
  const result = await f.core.purgeChatGPTInstallation({ installationId: 'expired-install' })
  assert.equal(result.logical.removed.sources, 0)
  assert.equal(result.logical.removed.nativeCaptureProgress, 1)
  assert.equal(result.logical.removed.nativeOriginFences, 1)
  assert.equal(result.logical.removed.conversationSettings, 1)
  const after = await f.core.diagnostics()
  assert.equal(after.retained.nativeCaptureProgress.records, 0)
  assert.equal(after.retained.nativeOriginFences.registrations, 0)
  assert.equal(after.configuration.conversationMonitoringScopes, 0)
  assert.equal((await f.core.sourcePlan()).entries.length, 0)
  await f.restart()
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
})

test('storage IO supplement: a generic SQLite deletion failure preserves public state and reports retryable residue', async t => {
  const f = await fixture(t), target = await nativeSeed(f, 'retry-install')
  const dsh = await seed(f, { registrationId: 'owned-retry-dsh', hostRunId: 'dsh-host', name: 'DSH' }, 'dsh')
  await f.core.unregisterSource(target.source)
  const before = await f.core.snapshot(), database = new DatabaseSync(join(f.dir, 'observations.sqlite'))
  try {
    // External storage failure only; all business assertions below use Core HTTP.
    database.exec("CREATE TRIGGER reject_owned_purge AFTER DELETE ON records BEGIN SELECT RAISE(FAIL, 'controlled purge deletion failure'); END")
    const failed = await f.core.purgeChatGPTInstallation({ installationId: 'retry-install' })
    assert.equal(failed.status, 'failed')
    assert.equal(failed.logical.status, 'retained')
    assert.equal(failed.logical.removed.sources, 0)
    assert.equal(failed.logical.remaining.nativeCaptureProgress, 1)
    assert.equal(failed.physical.status, 'not-attempted')
    assert.equal(failed.retryable, true)
    assert.match(failed.errors[0].message, /controlled purge deletion failure/)
    assert.deepEqual(await f.core.snapshot(), before)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 1)
    assert.equal((await f.core.diagnostics()).retained.nativeOriginFences.generations, 1)
    assert.equal((await f.core.diagnostics()).failureReceipts.receipts, 1)
    assert.equal((await f.core.diagnostics()).nativeNotifications.failureLinks, 1)
    await f.restart()
    assert.ok(JSON.stringify(await f.core.snapshot()).includes('private native retry-install'), 'rollback must survive a real Core reopen, not only retain an in-memory projection')
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 1)
    await f.core.registerSource({ registrationId: 'owned-retry-dsh', hostRunId: 'dsh-host', name: 'DSH' })
    await f.core.heartbeat(dsh)
    database.exec('DROP TRIGGER reject_owned_purge')
    const retried = await f.core.purgeChatGPTInstallation({ installationId: 'retry-install' })
    assert.equal(retried.status, 'complete')
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
    assert.equal((await f.core.snapshot()).tasks[0].id, 'dsh')
  } finally { database.exec('DROP TRIGGER IF EXISTS reject_owned_purge'); database.close() }
})

test('storage IO supplement: a pinned SQLite snapshot reports busy WAL cleanup without interrupting DSH', async t => {
  const f = await fixture(t)
  const target = await seed(f, { registrationId: 'chatgpt:wal-install', hostRunId: 'target', name: 'Owned', policy: 'chatgpt-main/1' }, 'target')
  const dsh = await seed(f, { registrationId: 'owned-wal-dsh', hostRunId: 'dsh-host', name: 'DSH' }, 'dsh')
  await f.core.unregisterSource(target)
  const instance = (await f.core.health()).instanceId, reader = new DatabaseSync(join(f.dir, 'observations.sqlite'))
  try {
    // Pin a SQLite read transaction without reading application columns or bodies.
    reader.exec('BEGIN'); reader.prepare('SELECT 1 FROM sqlite_schema LIMIT 1').get()
    const busy = await f.core.purgeChatGPTInstallation({ installationId: 'wal-install' })
    assert.equal(busy.status, 'incomplete')
    assert.equal(busy.logical.status, 'purged')
    assert.equal(busy.physical.status, 'busy')
    assert.equal(busy.retryable, true)
    assert.equal(JSON.stringify(await f.core.snapshot()).includes('private body target'), false)
    await f.core.heartbeat(dsh)
    await f.core.ingest(dsh, { tasks: [{ id: 'dsh', lifecycleId: 'run', title: 'Still running', status: 'running' }] })
    reader.exec('COMMIT')
    const retried = await f.core.purgeChatGPTInstallation({ installationId: 'wal-install' })
    assert.equal(retried.status, 'complete')
    assert.equal(retried.logical.removed.sources, 0)
    assert.equal((await f.core.health()).instanceId, instance)
    assert.equal((await f.core.snapshot()).tasks[0].status, 'running')
  } finally { if (reader.isTransaction) reader.exec('ROLLBACK'); reader.close() }
})

test('completed summaries and a cancelled analysis cannot restore purged installation material when the provider finishes late', async t => {
  const f = await fixture(t), held = []
  let holdJudgment = false, completedLate = 0
  const model = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    const wire = JSON.parse(body)
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') {
      const evidence = JSON.parse(wire.messages[1].content).evidence
      const selectedOptionIds = evidence.flatMap(group => { const kind = group.options.some(option => option.kind === 'authorization') ? 'authorization' : group.options.some(option => option.kind === 'action') ? 'action' : 'context'; return group.options.filter(option => option.kind === kind).slice(0, 1).map(option => option.optionId) })
      res.end(JSON.stringify({ model: wire.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds, gaps: [] }) } }] }))
    } else {
      const json = decodeJevRequest(wire), finish = () => { if (holdJudgment) completedLate++; if (!res.destroyed) res.end(JSON.stringify({ model: json.model, answers: Object.fromEntries(Object.keys(json.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) })) }
      if (holdJudgment) held.push(finish); else finish()
    }
  })
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve))
  t.after(async () => { for (const finish of held.splice(0)) finish(); model.closeAllConnections(); await new Promise(resolve => model.close(resolve)) })
  const baseUrl = 'http://127.0.0.1:' + model.address().port
  await f.core.configureAnalysis({ llm: { baseUrl, model: 'owned-extractor' }, jev: { baseUrl, model: 'owned-judge' } })
  const descriptor = { registrationId: 'chatgpt:analysis-install', hostRunId: 'first-host', name: 'Owned analysis', policy: 'chatgpt-main/1' }
  const batch = () => {
    const ref = eventId => ({ taskId: 'main', lifecycleId: 'run', eventId }), event = (id, kind, text, data) => ({ id, taskId: 'main', taskLifecycleId: 'run', kind, text, capturedAt: f.now, occurredAt: f.now, data: { sourceKind: 'chatgpt-native', runId: 'run', ...data } })
    return { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'run', title: 'Owned', status: 'running' }], events: [event('start', 'status', 'Started', { nativeType: 'task_started', sourceForm: 'event_msg', turnTrigger: 'composer', status: 'running' }), event('request', 'input', 'secret uninstall request: read only', { nativeType: 'item_completed/UserMessage', sourceForm: 'event_msg/item_completed', messageId: 'message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }), event('output', 'output', 'Reading the owned input only', { nativeType: 'item_completed/AgentMessage', sourceForm: 'event_msg/item_completed' })], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], relations: [], activations: [{ taskId: 'main', lifecycleId: 'run', sessionId: 'main', runId: 'run', state: 'running', stateRefs: [ref('start')] }], instructionBindings: [{ taskId: 'main', lifecycleId: 'run', inputRef: ref('request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('start'), ref('request')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } }
  }
  const until = async predicate => { const deadline = performance.now() + 3000; while (!await predicate()) { assert.ok(performance.now() < deadline, 'owned provider must reach the public state'); await delay(5) } }
  const first = await f.core.registerSource(descriptor)
  await f.core.ingest(first, batch())
  await until(async () => Boolean((await f.core.snapshot()).tasks[0].analysis.summary))
  await f.core.unregisterSource(first)
  const completed = await f.core.purgeChatGPTInstallation({ installationId: 'analysis-install' })
  assert.equal(completed.logical.removed.summaries, 1)
  assert.equal(JSON.stringify(await f.core.gsSummary(first, 'main')).includes('secret uninstall request'), false)
  holdJudgment = true; f.advance()
  const next = await f.core.registerSource({ ...descriptor, hostRunId: 'next-host' })
  await f.core.ingest(next, batch())
  await until(() => held.length === 1)
  await f.core.unregisterSource(next, 'owned helper stopped')
  assert.equal((await f.core.purgeChatGPTInstallation({ installationId: 'analysis-install' })).status, 'complete')
  for (const finish of held.splice(0)) finish()
  await delay(20)
  assert.equal(completedLate, 1)
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  assert.equal((await f.core.diagnostics()).retained.notifications, 0)
  assert.equal((await f.core.diagnostics()).failureReceipts.receipts, 0)
})
