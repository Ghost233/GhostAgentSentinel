import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createAnalyzer } from '../src/analysis/index.mjs'

const at = 1_800_000_000_000
const task = { id: 'main', lifecycleId: 'turn', title: 'Main', status: 'running', currentRequestId: 'request' }
const event = (id, kind, offset, data = {}) => ({ id, kind, taskId: task.id, taskLifecycleId: task.lifecycleId, text: id, occurredAt: at + offset, capturedAt: at + offset, data })
const input = (source, events) => ({ source, task, events, coverage: { complete: true, gaps: [] } })

test('ChatGPT silence and unpaired tools stay unknown while DSH retains its original long-wait rule', async () => {
  for (const activity of [event('output', 'output', -400_000), event('tool', 'tool-start', -400_000, { callId: 'open' })]) {
    const events = [event('request', 'input', -500_000), activity]
    const chat = await createAnalyzer({ now: () => at }).analyze(input({ policy: 'chatgpt-main/1' }, events))
    const check = chat.checks.find(check => check.id === 'long_wait')
    assert.equal(check.status, 'unknown')
    assert.equal(check.applicability.state, 'unknown')
    assert.match(check.reason, /等待原因未确认.*400/)
    assert.deepEqual(check.evidenceIds, [activity.id])
    const dsh = await createAnalyzer({ now: () => at }).analyze(input({}, events))
    assert.equal(dsh.checks.find(check => check.id === 'long_wait').status, 'attention')
  }
})

import { createServer } from 'node:http'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { createCore } from '../src/core/index.mjs'

async function nativeInput(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-material-io-')), path = join(dir, 'native.jsonl')
  const row = (type, payload) => JSON.stringify({ type, timestamp: new Date(at).toISOString(), payload }) + '\n'
  await writeFile(path, row('session_meta', { id: 'main' })
    + row('event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } })
    + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn', item: { type: 'UserMessage', id: 'user-producer', content: [{ type: 'text', text: 'Read only the requested log' }] } })
    + row('response_item', { type: 'message', id: 'assistant-output', role: 'assistant', content: [{ type: 'output_text', text: 'Read the requested log and found the failing assertion' }], internal_chat_message_metadata_passthrough: { turn_id: 'turn' } }))
  const reader = new BoundedReader({ path, allowedRoot: dir, threadId: 'main', parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', hostRunId: 'host', hostStartedAt: at - 10000, profile: supportedProfile, anchorTurnId: 'turn' }, null, { mode: 'material' })
  t.after(async () => { await reader.close(); await rm(dir, { recursive: true, force: true }) })
  const view = await reader.pump()
  assert.equal(view.coverage, 'caught-up')
  const events = view.materials.map((event, index) => ({ id: 'material-' + index, taskId: 'main', taskLifecycleId: 'turn', kind: event.kind, occurredAt: event.at, capturedAt: at, text: event.text, data: event.data }))
  return { dir, input: { source: { policy: 'chatgpt-main/1' }, task: { ...task, currentRequestId: events[0].id }, events, coverage: { complete: true, gaps: [] } }, view }
}

async function modelIO(t, { choice = 'clear', failure = false, hold = false } = {}) {
  const requests = []
  let requestReached
  const reached = new Promise(resolve => { requestReached = resolve })
  const server = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    const wire = JSON.parse(body), decoded = req.url === '/v1/systemone' ? decodeJevRequest(wire) : wire
    requests.push({ path: req.url, body: decoded }); requestReached()
    if (hold) return
    res.setHeader('content-type', 'application/json')
    if (failure) { res.writeHead(503).end(JSON.stringify({ error: { code: 'controlled-unavailable' } })); return }
    if (req.url === '/v1/chat/completions') res.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(wire, { selectedOptionIds: [], gaps: [] })) } }], usage: { prompt_tokens: 10, completion_tokens: 5 } }))
    else res.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(decoded.questions).map(id => [id, { type: 'choice', choice }])), usage: { input_tokens: 20, output_tokens: 1 } }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  const baseUrl = 'http://127.0.0.1:' + server.address().port
  return { requests, reached, analyzer: now => createAnalyzer({ llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' }, now }) }
}

test('native material reaches the existing Chat/JEV IO and distinguishes normal, valid unknown, failure and unconfigured dependencies', async t => {
  for (const mode of ['clear', 'unknown', 'failure', 'unconfigured']) await t.test(mode, async t => {
    const f = await nativeInput(t)
    if (mode === 'unconfigured') {
      const result = await createAnalyzer({ now: () => at }).analyze(f.input)
      assert.equal(result.checks.length, 6)
      assert.deepEqual(result.coverage.dependencies, ['llm_unconfigured', 'jev_unconfigured'])
      assert.equal(result.checks.find(check => check.id === 'request_drift').status, 'unknown')
      return
    }
    const io = await modelIO(t, { choice: mode === 'unknown' ? 'unknown' : 'clear', failure: mode === 'failure' })
    if (mode === 'failure') {
      await assert.rejects(io.analyzer(() => at).analyze(f.input), error => error.code === 'http_error' && error.stage === 'llm')
      assert.equal(io.requests.length, 1)
      return
    }
    const result = await io.analyzer(() => at).analyze(f.input)
    assert.equal(result.checks.length, 6)
    const drift = result.checks.find(check => check.id === 'request_drift')
    assert.equal(drift.status, mode)
    assert.equal(drift.judgmentState, mode === 'clear' ? 'clear' : 'insufficient')
    assert.equal(result.checks.find(check => check.id === 'long_wait').status, 'clear')
    for (const id of ['stalled_progress', 'failure_loop', 'validation_cost', 'validation_gap']) assert.equal(result.checks.find(check => check.id === id).applicability.state, 'not-triggered')
    assert.deepEqual(io.requests.map(request => request.path), ['/v1/chat/completions', '/v1/systemone'])
    const evidence = JSON.parse(io.requests[0].body.messages[1].content).evidence
    assert.ok(evidence.some(item => item.text === 'Read only the requested log'))
    assert.ok(evidence.some(item => item.text === 'Read the requested log and found the failing assertion'))
    assert.equal(io.requests[1].body.state.request, 'Read only the requested log')
    assert.deepEqual(result.coverage.dependencies, [])
  })
})

test('cancelling an actual Chat HTTP request rejects that analysis without converting it into a model failure', async t => {
  const f = await nativeInput(t), io = await modelIO(t, { hold: true }), controller = new AbortController()
  const pending = io.analyzer(() => at).analyze(f.input, { signal: controller.signal })
  await io.reached
  controller.abort(new DOMException('controlled analysis cancellation', 'AbortError'))
  await assert.rejects(pending, error => error.name === 'AbortError' && !error.code?.startsWith?.('http'))
  assert.equal(io.requests.length, 1)
})

async function settled(core) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const actor = (await core.snapshot()).tasks[0]
    if (['cooldown', 'failed'].includes(actor.analysis.state)) return actor
    await new Promise(resolve => setTimeout(resolve, 2))
  }
  assert.fail('controlled analysis did not settle')
}

test('recent normal activity returns from unconfirmed waiting through the public core without a permanent coverage gap', async t => {
  const f = await nativeInput(t), io = await modelIO(t)
  let time = at
  const core = await createCore({ dataDir: join(f.dir, 'core'), analyzer: io.analyzer(() => time), now: () => time, leaseMs: 1_000_000 })
  t.after(() => core.close())
  const source = await core.registerSource({ registrationId: 'native-io', hostRunId: 'host', name: 'Native', policy: 'chatgpt-main/1' })
  const ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId })
  const start = { id: 'start', taskId: 'main', taskLifecycleId: 'turn', kind: 'status', occurredAt: at, capturedAt: at, text: 'task_started', data: { nativeType: 'task_started', runId: 'turn', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', turnTrigger: 'composer', status: 'running' } }
  await core.ingest(source, { tasks: [{ id: 'main', lifecycleId: 'turn', sessionId: 'main', title: 'Main', status: 'running' }], events: [start, ...f.input.events], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('material-0'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('start'), ref('material-0')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } })
  assert.equal((await settled(core)).gs.light, 'green')
  time += 60_000
  assert.equal((await core.snapshot()).tasks[0].gs.label, '等待原因未确认')
  time += 1
  await core.ingest(source, { events: [{ id: 'fresh-output', taskId: 'main', taskLifecycleId: 'turn', kind: 'output', occurredAt: time, capturedAt: time, text: 'The requested log investigation is complete' }] })
  const actor = await settled(core)
  assert.equal(actor.gs.light, 'green')
  assert.equal(actor.gs.waiting, null)
  assert.deepEqual(actor.coverage.gaps, [])
})
