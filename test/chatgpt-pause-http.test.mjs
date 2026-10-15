import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-pause-http-'))
  let server = await startServer({ dataDir }), client = await connectCore({ dataDir })
  const scope = { registrationId: 'chatgpt:http', taskId: 'native-chat' }
  const source = await client.registerSource({ registrationId: scope.registrationId, hostRunId: 'host-one', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await client.ingest(source, { tasks: [{ id: scope.taskId, sessionId: 'shared-tree', lifecycleId: 'one', title: 'Native chat', status: 'running' }] })
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  return { scope, get client() { return client }, get server() { return server }, async restart() { await client.close(); await server.close(); server = await startServer({ dataDir }); client = await connectCore({ dataDir }) }, async request(path, body, authorized = true) {
    const response = await fetch(server.url + path, { method: body === undefined ? 'GET' : 'POST', headers: { ...(authorized ? { authorization: 'Bearer ' + server.token } : {}), 'content-type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) })
    return { status: response.status, body: await response.json() }
  } }
}

test('the advertised private HTTP conversation control persists and rejects stale opposite writes without changing global monitoring', async t => {
  const f = await fixture(t)
  assert.ok((await f.client.health()).features.includes('conversation-monitoring/1'))
  const original = await f.client.conversationMonitoring(f.scope)
  assert.equal(original.paused, false)
  const paused = await f.client.setConversationMonitoring({ ...f.scope, paused: true, revision: original.revision })
  assert.equal(paused.paused, true)
  assert.equal((await f.client.snapshot()).monitoring.paused, false)
  assert.equal((await f.client.snapshot()).tasks[0].conversationMonitoring.paused, true)
  await f.restart()
  assert.deepEqual(await f.client.conversationMonitoring(f.scope), paused)
  const source = await f.client.registerSource({ registrationId: f.scope.registrationId, hostRunId: 'host-two', name: 'ChatGPT reopened', policy: 'chatgpt-main/1' })
  await f.client.ingest(source, { tasks: [{ id: f.scope.taskId, lifecycleId: 'two', title: 'New execution', status: 'running' }] })
  assert.equal((await f.client.snapshot()).tasks.find(task => task.sourceKey === source).analysis.state, 'paused')
  const resumed = await f.client.setConversationMonitoring({ ...f.scope, paused: false, revision: paused.revision })
  await assert.rejects(f.client.setConversationMonitoring({ ...f.scope, paused: true, revision: paused.revision }), error => error.status === 409 && error.code === 'conversation_monitoring_conflict')
  assert.deepEqual(await f.client.setConversationMonitoring({ ...f.scope, paused: false, revision: paused.revision }), resumed)
  assert.equal((await f.client.snapshot()).monitoring.paused, false)
})

test('conversation HTTP control requires private auth, a known native scope, boolean state and exact query/body fields', async t => {
  const f = await fixture(t), query = '/v1/conversation/monitoring?' + new URLSearchParams(f.scope)
  assert.equal((await f.request(query, undefined, false)).status, 401)
  const legacy = await f.client.registerSource({ registrationId: 'dsh:http', hostRunId: 'host', name: 'DSH' })
  await f.client.ingest(legacy, { tasks: [{ id: f.scope.taskId, lifecycleId: 'legacy', title: 'DSH', status: 'running' }] })
  for (const scope of [{ registrationId: 'unknown', taskId: f.scope.taskId }, { registrationId: f.scope.registrationId, taskId: 'unverified-chat' }, { registrationId: 'dsh:http', taskId: f.scope.taskId }]) assert.equal((await f.request('/v1/conversation/monitoring?' + new URLSearchParams(scope))).status, 400)
  for (const invalid of [query + '&taskId=second', query + '&lifecycleId=one', '/v1/conversation/monitoring?registrationId=chatgpt%3Ahttp']) assert.equal((await f.request(invalid)).status, 400)
  const state = (await f.request(query)).body
  for (const body of [{ ...f.scope, paused: 'true', revision: state.revision }, { ...f.scope, paused: true }, { ...f.scope, paused: true, revision: state.revision, sessionId: 'shared-tree' }]) assert.equal((await f.request('/v1/conversation/monitoring', body)).status, 400)
  assert.deepEqual((await f.request(query)).body, state)
  assert.equal((await f.client.snapshot()).monitoring.paused, false)
})
