import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { createCore } from '../src/core/index.mjs'
import { createAnalyzer } from '../src/analysis/index.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

const nativeBatch = (at, lifecycleId = 'run', revision = 1) => {
  const ref = eventId => ({ taskId: 'main', lifecycleId, eventId })
  const event = (id, kind, text, data) => ({ id, taskId: 'main', taskLifecycleId: lifecycleId, kind, text, capturedAt: at, occurredAt: at, data })
  return { tasks: [{ id: 'main', sessionId: 'main', lifecycleId, title: 'Main', status: 'running' }], events: [
    event('start', 'status', 'Started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: lifecycleId, turnTrigger: 'composer', status: 'running' }),
    event('request', 'input', 'Read only this log', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: lifecycleId, messageId: 'user-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }),
    event('output', 'output', 'Reading this log', {}),
  ], hierarchyContext: { schemaVersion: 1, revision, sessions: [{ sessionId: 'main' }], relations: [], activations: [{ taskId: 'main', lifecycleId, sessionId: 'main', runId: lifecycleId, state: 'running', stateRefs: [ref('start')] }], instructionBindings: [{ taskId: 'main', lifecycleId, inputRef: ref('request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('start'), ref('request')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } }
}

test('an actual settled model HTTP failure remains readable after the next round recovers and the core restarts', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-failure-core-'))
  let calls = 0, core
  const model = createServer(async (req, res) => {
    if (req.method === 'GET') { res.writeHead(404, { 'content-type': 'application/json' }); res.end('{"error":{"code":"not_found"}}'); return; }
    let body = ''; for await (const chunk of req) body += chunk
    res.setHeader('content-type', 'application/json')
    if (++calls === 1) { res.writeHead(503).end('{"error":{"code":"controlled"}}'); return }
    if (req.url === '/v1/chat/completions') res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(JSON.parse(body), { selectedOptionIds: [], gaps: [] })) } }] }))
    else res.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(decodeJevRequest(JSON.parse(body)).questions).map(id => [id, { type: 'choice', choice: 'clear' }])) }))
  })
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve))
  t.after(async () => { await core?.close(); model.closeAllConnections(); await new Promise(resolve => model.close(resolve)); await rm(dir, { recursive: true, force: true }) })
  const baseUrl = 'http://127.0.0.1:' + model.address().port
  core = await createCore({ dataDir: dir, cooldownMs: 0, analyzer: createAnalyzer({ llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' } }) })
  const sourceKey = await core.registerSource({ registrationId: 'chatgpt:test', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await core.ingest(sourceKey, nativeBatch(Date.now()))
  let observed
  for (let n = 0; n < 100; n++) {
    observed = (await core.snapshot()).tasks[0]
    if (calls >= 3 && observed.analysis.summary && !observed.analysis.error) break
    await delay(5)
  }
  assert.equal(observed.analysis.error, null, 'the failed snapshot has already been replaced by recovery')
  assert.ok(calls >= 3)
  const scope = { sourceKey, mainId: 'main', lifecycleId: 'run' }, page = await core.failureReceipts(scope)
  assert.equal(page.receipts.length, 1)
  assert.equal(page.receipts[0].error.code, 'http_error')
  assert.equal(page.receipts[0].error.status, 503)
  assert.equal(page.receipts[0].state, 'recovered')
  assert.ok(page.receipts[0].firstRoundId)
  assert.deepEqual(await core.failureReceipts(scope), page, 'repeat reads do not create another episode')
  assert.deepEqual((await core.failureReceipts({ ...scope, lifecycleId: 'another-run' })).receipts, [])
  assert.deepEqual((await core.failureReceipts({ ...scope, afterSequence: page.nextSequence })).receipts, [])
  await core.close()
  core = await createCore({ dataDir: dir })
  assert.deepEqual(await core.failureReceipts(scope), page)
  const diagnostics = await core.diagnostics()
  assert.equal(diagnostics.failureReceipts.receipts, 1)
})
