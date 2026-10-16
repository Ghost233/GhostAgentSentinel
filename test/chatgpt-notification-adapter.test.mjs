import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

test('the production adapter keeps denied notices pending and its app-only permission action does not block the panel', async t => {
  let clock = Date.now(); t.mock.method(Date, 'now', () => clock)
  const hostBirth = clock - 1000
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-notification-adapter-')), token = 'd'.repeat(64), path = join(dir, 'main.jsonl')
  const row = (type, payload) => JSON.stringify({ type, timestamp: new Date(clock).toISOString(), payload }) + '\n'
  await writeFile(path, row('session_meta', { id: 'main' }) + row('event_msg', { type: 'task_started', turn_id: 'run', turn_attribution: { turn_id: 'run', turn_trigger: 'composer' } }))
  const thread = { id: 'main', path, cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null }
  let directoryFailed = true, settingsCalls = 0, authorizationCalls = 0, permission = 'denied', grant, cancelAuthorization, authorizationStarted = false
  const waiting = new Promise((resolve, reject) => { grant = resolve; cancelAuthorization = reject }), submits = []
  const settings = () => ({ authorization: permission, alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: clock })
  const notificationIO = {
    async settings() { settingsCalls++; return settings() },
    async requestAuthorization() { authorizationCalls++; authorizationStarted = true; await waiting; permission = 'authorized'; return settings() },
    async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: clock } },
    async submit(value) { submits.push(value); return { requestId: value.requestId, authorization: permission, accepted: true, observedAt: clock } },
    async cancel() { return { pendingIds: [], observedAt: clock } },
    cancelPendingOperations() { if (authorizationStarted) cancelAuthorization(Object.assign(new Error('Cancelled owned test IO'), { code: 'native_unavailable' })) }, close() {},
  }
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'notification-host', hostStartedAt: hostBirth, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread() { return { thread, anchorTurnId: 'run' } },
    async listThreads() { if (directoryFailed) throw Object.assign(new Error('Owned directory failure'), { code: 'EACCES' }); return { data: [thread], nextCursor: null } }, async close() {},
  }
  const server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }), core = await connectCore({ dataDir: join(dir, 'core') })
  const adapter = await startChatGPTAdapter({ core, native, notificationIO, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'notification-adapter', token, collectMaterials: true, pollMs: 0, now: () => clock })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const call = async (name = 'sentinel_current_chat', args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, signal: AbortSignal.timeout(2000), body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args, _meta: { thread_id: 'main' } } }) })
    assert.equal(response.status, 200); const value = await response.json(); assert.equal(value.result?.isError, undefined, JSON.stringify(value)); return value.result.structuredContent
  }
  await adapter.refresh()
  const first = await call()
  assert.ok(first.notifications, 'the same production panel exposes the native notification state')
  for (let wait = 0; !settingsCalls && wait < 100; wait++) await delay(2)
  assert.equal(settingsCalls, 1)
  assert.equal(authorizationCalls, 0, 'background collection must never open the permission prompt')
  const sourceKey = (await core.snapshot()).sources[0].key
  assert.equal((await core.nativeNotificationRecords({ sourceKey })).records[0].status, 'pending')
  directoryFailed = false; await adapter.refresh()
  const requesting = await call('sentinel_request_notification_permission', { expectedThreadId: 'main' })
  assert.equal(requesting.notifications.permission.requestInFlight, true, 'the response returns while native permission IO is still held')
  grant()
  for (let wait = 0; wait < 100; wait++) { if ((await call()).notifications.permission.authorization === 'authorized') break; await delay(2) }
  clock += 3001; await adapter.refresh()
  let records
  for (let wait = 0; wait < 100; wait++) { records = (await core.nativeNotificationRecords({ sourceKey })).records; if (records[0]?.status === 'submitted') break; await delay(2) }
  assert.equal(authorizationCalls, 1)
  assert.equal(submits.length, 1, 'the recovered first failure remains deliverable once permission is granted')
  assert.equal(records[0].delivery, 'system-accepted')
  assert.equal(records[0].humanSeen, 'unknown')
  const visible = await call('sentinel_notification_records', { expectedThreadId: 'main' })
  assert.equal(visible.identity.threadId, 'main')
  assert.equal(visible.records[0].id, records[0].id)
  assert.equal(visible.records[0].delivery, 'system-accepted')
  assert.equal(visible.records[0].humanSeen, 'unknown')
  assert.match(visible.records[0].body, /EACCES/)
})
