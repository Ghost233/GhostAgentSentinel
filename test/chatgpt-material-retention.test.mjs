import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, writeFile, appendFile, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

const DAY = 86_400_000
const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const row = (type, payload, at) => JSON.stringify({ type, payload, timestamp: new Date(at).toISOString() }) + '\n'
function turn(thread, id, text, at, outputs = 1) {
  return row('event_msg', { type: 'task_started', turn_id: id, turn_attribution: { turn_id: id, turn_trigger: 'composer' } }, at)
    + row('event_msg', { type: 'item_completed', thread_id: thread, turn_id: id, item: { type: 'UserMessage', id: id + '-user', content: [{ type: 'text', text }] } }, at + 1)
    + Array.from({ length: outputs }, (_, index) => row('response_item', { type: 'message', id: id + '-output-' + index, role: 'assistant', content: [{ type: 'output_text', text: id + ' observed output ' + index }], internal_chat_message_metadata_passthrough: { turn_id: id } }, at + index + 2)).join('')
}
async function fixture(t, entries) {
  let clock = Date.now()
  t.mock.method(Date, 'now', () => clock)
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-material-retention-')), coreDir = join(dir, 'core'), adapterDir = join(dir, 'adapter')
  const hostBirth = clock - 10000, threads = new Map()
  for (const entry of entries) {
    const path = join(dir, entry.id + '.jsonl')
    await writeFile(path, row('session_meta', { id: entry.id }, clock) + turn(entry.id, entry.turn, entry.text, clock, entry.outputs ?? 1))
    threads.set(entry.id, { path, anchorTurnId: entry.turn })
  }
  let server = await startServer({ dataDir: coreDir, now: () => clock, leaseMs: DAY * 3 }), core, adapter
  const batches = [], deliveries = []
  let dropNextEvents = false
  const proxy = createServer(async (req, res) => {
    try {
      let body = ''; for await (const chunk of req) body += chunk
      const decoded = body ? JSON.parse(body) : null
      if (req.url === '/v1/source/ingest' && decoded?.batch?.events?.length) batches.push(structuredClone(decoded.batch))
      const response = await fetch(server.url + req.url, { method: req.method, headers: { authorization: req.headers.authorization, 'content-type': 'application/json' }, body: body || undefined })
      const result = await response.text()
      if (req.url === '/v1/source/ingest' && decoded?.batch?.events?.length) {
        deliveries.push({ status: response.status, result: JSON.parse(result), ids: decoded.batch.events.map(event => event.id) })
        if (dropNextEvents) { dropNextEvents = false; res.destroy(); return }
      }
      res.writeHead(response.status, { 'content-type': 'application/json' }).end(result)
    } catch (error) { if (!res.destroyed) res.writeHead(500, { 'content-type': 'application/json' }).end(JSON.stringify({ error: error.message })) }
  })
  await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve))
  const connect = async () => {
    const runtimePath = join(coreDir, 'runtime.json'), runtime = JSON.parse(await readFile(runtimePath, 'utf8'))
    await writeFile(runtimePath, JSON.stringify({ ...runtime, url: 'http://127.0.0.1:' + proxy.address().port }))
    core = await connectCore({ dataDir: coreDir })
  }
  await connect()
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'material-host', hostStartedAt: hostBirth, profile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread(id) { const entry = threads.get(id); return entry ? { thread: { id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: entry.path, cwd: dir }, anchorTurnId: entry.anchorTurnId } : null },
    async close() {},
  }
  const start = async (collectMaterials = true) => {
    await adapter?.close()
    adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: adapterDir, installationId: 'material-retention', token: '7'.repeat(64), now: () => clock, collectMaterials, pollMs: 0 })
  }
  const read = async id => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + '7'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: id } } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result.structuredContent
  }
  const restart = async () => {
    await adapter?.close(); adapter = null
    await core.close(); await server.close()
    server = await startServer({ dataDir: coreDir, now: () => clock, leaseMs: DAY * 3 })
    await connect(); await start()
  }
  t.after(async () => { await adapter?.close(); await core?.close(); await server.close(); proxy.closeAllConnections(); await new Promise(resolve => proxy.close(resolve)); await rm(dir, { recursive: true, force: true }) })
  return { dir, coreDir, adapterDir, threads, batches, deliveries, start, read, restart, get core() { return core }, now: () => clock, advance: ms => { clock += ms }, loseNextResponse: () => { dropNextEvents = true } }
}

test('material batches beyond sixteen records retain every first capture and use a cursor namespace distinct from metadata observation', async t => {
  const f = await fixture(t, [{ id: 'main', turn: 'turn', text: 'Read the selected log only', outputs: 40 }])
  await f.start(false)
  const metadata = await f.read('main'), metadataPath = join(f.adapterDir, 'cursors', metadata.identity.cursorKey + '.json'), metadataCursor = await readFile(metadataPath, 'utf8')
  await f.start(true)
  let view = await f.read('main')
  assert.notEqual(view.identity.cursorKey, metadata.identity.cursorKey)
  assert.equal(view.observation.coverage, 'catching-up', JSON.stringify({ view, deliveries: f.deliveries }))
  const catchingUp = (await f.core.snapshot()).tasks.find(task => task.id === 'main')
  assert.equal(catchingUp.status, 'unknown')
  assert.equal(catchingUp.coverage.complete, false)
  assert.ok(catchingUp.coverage.gaps.some(gap => /baseline.*unconfirmed/i.test(gap)))
  assert.equal(catchingUp.analysis.completedAt, null)
  for (let pump = 0; view.observation.coverage === 'catching-up' && pump < 10; pump++) { f.advance(1000); view = await f.read('main') }
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify({ view, deliveries: f.deliveries }))
  assert.equal(view.observation.status, 'running')
  assert.equal(await readFile(metadataPath, 'utf8'), metadataCursor)
  const task = (await f.core.snapshot()).tasks.find(task => task.id === 'main')
  assert.equal(task.currentRequest, 'Read the selected log only')
  assert.equal(task.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.equal(task.events.filter(event => event.kind === 'output').length, 40)
  const materialBatches = f.batches.filter(batch => batch.events.some(event => event.data?.sourceKind === 'chatgpt-native'))
  assert.deepEqual(materialBatches.map(batch => batch.events.length), [16, 16, 10])
  const firstCapture = new Map(materialBatches.flatMap(batch => batch.events).map(event => [event.id, event.capturedAt]))
  assert.equal(firstCapture.size, 42)
  for (const event of task.events.filter(event => event.data?.sourceKind === 'chatgpt-native')) assert.equal(event.capturedAt, firstCapture.get(event.id))
  assert.equal(new Set(firstCapture.values()).size, 3)
  const checkpointText = await readFile(join(f.adapterDir, 'cursors', view.identity.cursorKey + '.json'), 'utf8'), checkpoint = JSON.parse(checkpointText)
  assert.equal(checkpoint.format, 2)
  assert.ok(checkpoint.captures.length <= 16)
  assert.ok(Buffer.byteLength(checkpointText) <= 16384)
})

test('a lost core HTTP receipt replays native materials with their first capture time and cannot revive them after a day', async t => {
  const f = await fixture(t, [{ id: 'retained', turn: 'retained-turn', text: 'Inspect the current trace only' }])
  await f.start()
  f.loseNextResponse()
  assert.equal((await f.read('retained')).observation.gap, 'observation-unavailable')
  assert.equal(f.deliveries[0].status, 200, 'the core accepted the real batch before the proxy discarded its receipt')
  assert.equal(f.deliveries[0].result.accepted, 3)
  const first = f.batches[0].events.map(({ id, capturedAt }) => ({ id, capturedAt }))
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'retained').events.length, 3)
  f.advance(1000)
  await f.restart()
  f.loseNextResponse()
  assert.equal((await f.read('retained')).observation.gap, 'observation-unavailable')
  assert.deepEqual(f.batches.at(-1).events.map(({ id, capturedAt }) => ({ id, capturedAt })), first)
  assert.equal(f.deliveries.at(-1).status, 200)
  assert.equal(f.deliveries.at(-1).result.duplicate, 3)
  const retained = (await f.core.snapshot()).tasks.find(task => task.id === 'retained')
  for (const event of retained.events) assert.equal(event.capturedAt, first.find(item => item.id === event.id).capturedAt)
  f.advance(DAY + 1)
  await f.restart()
  assert.equal((await f.core.diagnostics()).retained.events, 0)
  await f.read('retained')
  assert.deepEqual(f.batches.at(-1).events.map(({ id, capturedAt }) => ({ id, capturedAt })), first)
  const after = await f.core.snapshot()
  assert.equal((await f.core.diagnostics()).retained.events, 0)
  assert.ok(after.tasks.every(task => task.currentRequest === '' && task.events.length === 0))
  assert.deepEqual(await f.core.claimNotifications(after.sources[0].key), [])
})

async function controlledModels(t) {
  const requests = []
  const server = createServer(async (req, res) => {
    let text = ''; for await (const chunk of req) text += chunk
    const body = JSON.parse(text), decoded = req.url === '/v1/systemone' ? decodeJevRequest(body) : body
    requests.push({ path: req.url, body: decoded })
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
    else res.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(decoded.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  const baseUrl = 'http://127.0.0.1:' + server.address().port
  return { requests, config: { llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' } } }
}
async function settled(f, ids) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const tasks = (await f.core.snapshot()).tasks.filter(task => ids.some(([id, lifecycle]) => task.id === id && task.lifecycleId === lifecycle))
    if (tasks.length === ids.length && tasks.every(task => task.analysis.completedAt !== null && ['cooldown', 'unavailable', 'idle'].includes(task.analysis.state))) return tasks
    await delay(3)
  }
  assert.fail('controlled analysis did not settle')
}

test('two main chats keep their source context while completed turns stop analysis and never grant proof to a different current turn', async t => {
  const f = await fixture(t, [{ id: 'alpha', turn: 'alpha-one', text: 'Read alpha.log only' }, { id: 'beta', turn: 'beta-one', text: 'Read beta.log only' }]), models = await controlledModels(t)
  await f.core.configureAnalysis(models.config)
  await f.start()
  await f.read('alpha'); await f.read('beta')
  const initial = await settled(f, [['alpha', 'alpha-one'], ['beta', 'beta-one']])
  assert.equal((await f.core.snapshot()).sources.length, 1)
  for (const task of initial) {
    assert.equal(task.currentRequest, task.id === 'alpha' ? 'Read alpha.log only' : 'Read beta.log only')
    assert.equal(task.events.find(event => event.kind === 'input').authorizationEligible, true)
  }
  let snapshot = await f.core.snapshot(), context = snapshot.sources[0].hierarchyContext
  assert.deepEqual(context.sessions.map(session => session.sessionId).sort(), ['alpha', 'beta'])
  assert.equal(context.activations.length, 2)
  assert.equal(context.instructionBindings.length, 2)
  assert.equal(models.requests.filter(request => request.path === '/v1/chat/completions').length, 2)
  assert.equal(models.requests.filter(request => request.path === '/v1/systemone').length, 2)
  for (const request of models.requests.filter(request => request.path === '/v1/chat/completions')) {
    const text = JSON.stringify(JSON.parse(request.body.messages[1].content).evidence)
    assert.notEqual(text.includes('Read alpha.log only'), text.includes('Read beta.log only'), 'each extraction sees exactly its own chat request')
  }
  const beforeTransition = models.requests.length
  f.advance(1000)
  await appendFile(f.threads.get('alpha').path, row('event_msg', { type: 'task_complete', turn_id: 'alpha-one' }, f.now())
    + turn('alpha', 'alpha-two', 'Read alpha-next.log only', f.now() + 1, 20))
  f.threads.get('alpha').anchorTurnId = 'alpha-two'
  let current = await f.read('alpha')
  assert.equal(current.observation.coverage, 'catching-up')
  const pending = (await f.core.snapshot()).tasks.find(task => task.id === 'alpha' && task.lifecycleId === 'alpha-two')
  assert.equal(pending.status, 'unknown')
  assert.equal(pending.coverage.complete, false)
  assert.equal(pending.analysis.completedAt, null)
  assert.equal(models.requests.length, beforeTransition, 'a partially collected new turn must not trigger analysis or reschedule the ended turn')
  for (let pump = 0; current.observation.coverage === 'catching-up' && pump < 10; pump++) { f.advance(1000); current = await f.read('alpha') }
  assert.equal(current.observation.phase, 'started')
  assert.equal(current.observation.coverage, 'caught-up')
  await settled(f, [['alpha', 'alpha-two']])
  snapshot = await f.core.snapshot(); context = snapshot.sources[0].hierarchyContext
  const old = snapshot.tasks.find(task => task.id === 'alpha' && task.lifecycleId === 'alpha-one'), next = snapshot.tasks.find(task => task.id === 'alpha' && task.lifecycleId === 'alpha-two'), beta = snapshot.tasks.find(task => task.id === 'beta')
  assert.equal(old.status, 'idle')
  assert.equal(context.activations.find(activation => activation.taskId === 'alpha' && activation.lifecycleId === 'alpha-one').state, 'ended')
  assert.equal(next.currentRequest, 'Read alpha-next.log only')
  assert.equal(next.events.filter(event => event.kind === 'input').length, 1)
  assert.equal(next.events.filter(event => event.kind === 'output').length, 20)
  assert.equal(next.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.equal(beta.currentRequest, 'Read beta.log only')
  assert.equal(beta.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.equal(context.instructionBindings.length, 3)
  assert.equal(models.requests.length, beforeTransition + 2, 'only the newly running turn receives one extractor and one JEV call')
  f.advance(1000)
  await appendFile(f.threads.get('alpha').path, row('event_msg', { type: 'task_complete', turn_id: 'alpha-two' }, f.now()))
  assert.equal((await f.read('alpha')).observation.phase, 'completed')
  await f.read('alpha'); await f.read('beta')
  snapshot = await f.core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.id === 'alpha' && task.lifecycleId === 'alpha-two').status, 'idle')
  assert.equal(snapshot.sources[0].hierarchyContext.activations.filter(activation => activation.taskId === 'alpha').every(activation => activation.state === 'ended'), true)
  assert.equal(models.requests.length, beforeTransition + 2, 'ending and revisiting historical turns must not run inference again')
})
