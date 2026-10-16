import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
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
