import test from 'node:test'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { performance } from 'node:perf_hooks'
import { createServer } from 'node:http'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'
import { startServer } from '../src/server.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-subagent-spawn-in-process', 'dsh-session-query', 'dsh-user-approval', 'dsh-user-questions'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
const until = async (predicate, label) => {
  const deadline = performance.now() + 3000
  while (!await predicate()) { assert.ok(performance.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 2)) }
}

async function requiresParentWaitAudit(t) {
  const ctx = new Context()
  try {
    await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
    for (const module of modules) await ctx.plugin(module, { agents: [] })
    assert.ok(ctx.get('subagents'), 'real SDK subagent service mounted')
    if (ctx.subagents.parentWaitAuditVersion === 1 && typeof ctx.subagents.observeWaitCaller === 'function') return true
    t.skip('stock public SDK has no parentwait audit; unsupported')
    return false
  } finally { await ctx.fiber.dispose() }
}

async function fixture(t, beforeAdapter = () => {}, judgment = 'clear', waitForOutput = true, parentTool = null, deferAnalysis = false) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-waits-')), ctx = new Context(), streams = [], handles = [], release = []
  let adapter, server, models, lastSnapshot, config
  const modelCalls = [], nativeWaitAudits = [], observations = []
  t.after(async () => {
    const failures = []
    if (adapter && !adapter.diagnostics().adapter.closed) lastSnapshot = await adapter.snapshot()
    for (const step of [() => { for (const stream of streams) { stream.done = true; stream.wake?.() }; for (const finish of release) finish() }, async () => { for (const handle of handles) await handle.dispose() }, async () => { if (adapter) assert.equal((await adapter.close()).status, 'closed') }, () => ctx.fiber.dispose(), () => server?.close(), async () => { if (models?.listening) await new Promise(resolve => models.close(resolve)) }, () => rm(dataDir, { recursive: true, force: true })]) { try { await step() } catch (error) { failures.push(error) } }
    if (adapter) {
      const closed = adapter.diagnostics()
      for (const field of ['activeRequests', 'timers', 'handlers', 'subscriptions', 'sentTasks', 'flushTimers', 'shutdownTimers']) assert.equal(closed.adapter[field], 0)
      for (const [field, value] of Object.entries(closed.collector)) if (field !== 'closed') assert.equal(value, 0, `closed collector ${field}`)
      assert.equal(closed.hierarchyReader.pending, 0)
    }
    if (process.env.SENTINEL_NATIVE_WAIT_RECEIPTS) await writeFile(join(process.env.SENTINEL_NATIVE_WAIT_RECEIPTS, `${config.registrationId}.json`), JSON.stringify({ scope: 'self-owned real native SDK Approval/UserQuestions and Agent/Session, production adapter, public HTTP core, controlled local model HTTP; no installed Host or actual model', hostRunId: lastSnapshot?.adapter.hostRunId, sourceKey: lastSnapshot?.adapter.sourceKey, publicSnapshot: lastSnapshot, nativeWaitAudits, observations, nativeToolCalls: handles.flatMap(handle => handle.agent.session.snapshotEvents().filter(event => event.type === 'tool/call').map(event => ({ sessionId: handle.agent.session.id, seq: event.seq, time: event.time, callId: event.data.callId, arguments: event.data.arguments }))), nativeApprovalAudits: handles.flatMap(handle => handle.agent.session.snapshotEvents().filter(event => event.type.startsWith('approval/')).map(event => ({ sessionId: handle.agent.session.id, type: event.type, seq: event.seq, time: event.time, id: event.data.id }))), modelCalls, cleanup: { failures: failures.map(error => error.message), diagnostics: adapter.diagnostics(), modelServerListening: models.listening } }, null, 2))
    if (failures.length) throw new AggregateError(failures, 'native waits cleanup failed')
  })
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  class Actor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      const first = streams.length === 0, state = { done: false, model: options.model }; streams.push(state)
      const abort = () => { state.done = true; state.wake?.() }
      options.signal.addEventListener('abort', abort, { once: true })
      try { if (first && parentTool) { const id = randomUUID(); yield { type: 'block-start', index: 1, blockType: 'tool-call' }; yield { type: 'tool-call-delta', index: 1, id, name: parentTool, argumentsDelta: '{"prompt":"Inspect delegated child completion only."}' }; yield { type: 'block-end', index: 1, block: { type: 'tool-call', id, name: parentTool, arguments: '{"prompt":"Inspect delegated child completion only."}' } }; yield { type: 'finish', reason: { kind: 'tool-calls' } }; return }; yield { type: 'text-delta', index: 0, text: 'Inspecting the authorized request.' }; while (!state.done) await new Promise(resolve => { state.wake = resolve }); yield { type: 'finish', reason: { kind: 'stop' } } }
      finally { options.signal.removeEventListener('abort', abort) }
    }
  }
  ctx.llm.registerAdapter(['native-waits-fixture'], new Actor())
  for (const type of ['subagent/wait-start', 'subagent/wait-targets', 'subagent/wait-end', 'subagent/wait-gap', 'subagent/wait-caller-start', 'subagent/wait-caller-end']) ctx.on(type, info => { nativeWaitAudits.push({ type, waitId: info.waitId, parentId: info.parent.id, parentStatusAtStart: info.parentStatusAtStart, parentSeqAtStart: info.parentSeqAtStart, timestamp: info.timestamp, targets: info.targets?.map(target => ({ id: target.id, runId: target.runId })), coverage: info.coverage, outcome: info.outcome, operationSettled: info.operationSettled, callId: info.execution?.callId }) })
  await beforeAdapter(ctx, release)
  server = await startServer({ dataDir })
  config = { dataDir, registrationId: `native-waits-${randomUUID()}` }
  adapter = createAdapter(ctx, config, { flushMs: 2, heartbeatMs: 100 })
  await until(() => adapter.connected, 'native wait adapter connected')
  if (deferAnalysis) await adapter.pause()
  models = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk
    const body = JSON.parse(raw)
    modelCalls.push({ path: request.url, model: body.model, inputChars: raw.length })
    response.setHeader('content-type', 'application/json')
    if (judgment === 'failed') { response.statusCode = 500; response.end(JSON.stringify({ error: 'controlled native wait analyzer failure' })); return }
    if (request.url === '/v1/chat/completions') response.end(JSON.stringify({ model: 'native-wait-select', choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
    else response.end(JSON.stringify({ model: 'native-wait-judge', answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: judgment, probabilities: { attention: 0, clear: judgment === 'clear' ? 1 : 0, unknown: judgment === 'unknown' ? 1 : 0 } }])) }))
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  const configureAnalysis = async () => { await adapter.configureAnalysis({ llm: { baseUrl, model: 'native-wait-select' }, jev: { baseUrl, model: 'native-wait-judge' } }); if (deferAnalysis) await adapter.resume() }
  if (!deferAnalysis) await configureAnalysis()
  const create = async (sessionId = randomUUID()) => {
    const handle = await ctx.agents.create({ sessionId, agentOptions: { provider: 'native-waits-fixture', model: randomUUID() } }); handles.push(handle)
    handle.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only and ask before proceeding.' }] }))
    await until(() => streams.length && handle.agent.status === 'running', 'actual native turn running')
    await until(async () => (await adapter.snapshot()).tasks.some(task => task.id === handle.agent.id && task.status === 'running' && task.instructionContext?.binding?.phase === 'consumed'), 'actual request binding published')
    if (waitForOutput) await until(async () => (await adapter.snapshot()).tasks.some(task => task.id === handle.agent.id && task.status === 'running' && task.analysis.completedAt != null && task.events.some(event => event.kind === 'output' && (judgment === 'failed' ? task.analysis.state === 'failed' : task.analysis.inputEventIds?.includes(event.id)))), 'controlled HTTP judgment of actual native activity completed')
    return handle
  }
  const handle = await create()
  return { ctx, get adapter() { return adapter }, agent: handle.agent, handle, release, streams, configureAnalysis, observations, create, async task() { lastSnapshot = await adapter.snapshot(); return lastSnapshot.tasks.find(task => task.id === handle.agent.id && task.status === 'running') }, async replaceAdapter() { adapter = createAdapter(ctx, config, { flushMs: 2, heartbeatMs: 100 }); await until(() => adapter.connected, 'replacement native wait adapter connected') } }
}

test('a real native ApprovalService audit pair reaches public GS with its actual approval ID and closes when the provider returns', async t => {
  let finish, calls = 0
  const f = await fixture(t, (ctx, releases) => {
    ctx.on('approval/request', () => { calls++; return new Promise(resolve => { finish = resolve; releases.push(() => resolve('cancelled')) }) }, { global: true })
  })
  const callId = randomUUID(), result = f.ctx.approval.request({ agent: f.agent, toolName: 'native_readonly_check', callId })
  await until(() => finish instanceof Function, 'actual native approval provider pending')
  const asked = f.agent.session.snapshotEvents().find(event => event.type === 'approval/asked')
  assert.equal(typeof asked.data.id, 'string')
  let row
  await until(async () => { row = await f.task(); return row?.gs?.waiting?.kind === 'user' }, 'native approval audit opens public user wait')
  const event = row.events.find(event => event.data.nativeType === 'approval/asked')
  assert.equal(event.data.approvalId, asked.data.id)
  assert.equal(event.data.callId, callId)
  assert.equal(event.data.history, undefined)
  assert.equal(row.gs.light, 'green')
  assert.equal(row.analysis.checks.find(check => check.id === 'request_drift').status, 'clear')
  finish('allowed-once')
  assert.equal(await result, 'allowed-once')
  assert.equal(calls, 1)
  await until(async () => { row = await f.task(); return row?.events.some(event => event.data.nativeType === 'approval/decided') && row.gs.waiting === null }, 'native approval decision closes public wait')
  assert.equal(row.events.find(event => event.data.nativeType === 'approval/decided').data.approvalId, asked.data.id)
})

test('the actual UserQuestions waterfall preserves a preinstalled provider and publishes a paired native request wait', async t => {
  let finish, calls = 0, delegated = 0
  const answer = { answers: [{ id: 'mode', selected: ['Inspect'] }] }
  const f = await fixture(t, (ctx, releases) => {
    ctx.on('user-questions/request', (_request, next) => { delegated++; return next() }, { global: true })
    ctx.on('user-questions/request', request => { calls++; assert.ok(request.agent); return new Promise(resolve => { finish = resolve; releases.push(() => resolve(answer)) }) }, { global: true })
  })
  const callId = randomUUID(), result = f.ctx.userQuestions.askTimed({ agent: f.agent, questions: [{ id: 'mode', question: 'Choose the next step.', options: [{ label: 'Inspect' }] }] }, callId, 1000)
  await until(() => finish instanceof Function, 'actual question provider pending')
  let row
  await until(async () => { row = await f.task(); return row?.gs?.waiting?.kind === 'user' }, 'question waterfall opens public user wait')
  const start = row.events.find(event => event.data.nativeType === 'user-questions/request-start')
  assert.match(start.data.waitId, /^[0-9a-f]{8}-[0-9a-f-]{27}$/i)
  assert.equal(start.data.callId, callId)
  assert.equal(row.gs.light, 'green')
  assert.equal(row.analysis.checks.find(check => check.id === 'request_drift').status, 'clear')
  finish(answer)
  assert.equal(await result, answer)
  assert.equal(calls, 1)
  assert.equal(delegated, 1)
  await until(async () => { row = await f.task(); return row?.events.some(event => event.data.nativeType === 'user-questions/request-end') && row.gs.waiting === null }, 'provider return closes actual question wait')
  const end = row.events.find(event => event.data.nativeType === 'user-questions/request-end')
  assert.equal(end.data.waitId, start.data.waitId)
  assert.equal(end.taskLifecycleId, start.taskLifecycleId)
})

test('missing native answerers finish Approval and Question audits without leaving a current user wait', async t => {
  const f = await fixture(t)
  assert.equal(await f.ctx.approval.request({ agent: f.agent, toolName: 'native_readonly_check' }), 'unavailable')
  await assert.rejects(f.ctx.userQuestions.ask({ agent: f.agent, questions: [{ id: 'mode', question: 'Choose the next step.' }] }), error => error.code === 'NO_PROVIDER')
  const callId = randomUUID()
  assert.deepEqual(await f.ctx.userQuestions.askTimed({ agent: f.agent, questions: [{ id: 'mode', question: 'Choose the next step.' }] }, callId, 20), { pending: true, callId })
  let row
  await until(async () => { row = await f.task(); return row?.events.filter(event => event.data.nativeType === 'user-questions/request-end').length === 2 && row.events.some(event => event.data.nativeType === 'approval/decided') }, 'no-provider audit pairs published')
  assert.equal(row.gs.waiting, null)
  assert.notEqual(row.gs.label, '等待你')
  const starts = row.events.filter(event => event.data.nativeType === 'user-questions/request-start')
  assert.equal(starts.length, 2)
  assert.equal(new Set(starts.map(event => event.data.waitId)).size, 2)
})

test('native question pending, abort and provider rejection each close their audit and preserve the service result', async t => {
  const { UserQuestionError } = req('@deepseek-ai/dsh-user-questions')
  let mode = 'throw', calls = 0, entered
  const original = new Error('actual native question provider rejected'), abortReason = new Error('caller aborted native question')
  const f = await fixture(t, (ctx, releases) => {
    ctx.on('user-questions/request', request => {
      calls++
      if (mode === 'throw') throw original
      if (mode === 'pending') throw new UserQuestionError('foreground provider timed out', 'ASK_TIMED_OUT')
      return new Promise((resolve, reject) => { entered = true; releases.push(() => resolve({ answers: [] })); request.signal.addEventListener('abort', () => reject(request.signal.reason), { once: true }) })
    }, { global: true })
  })
  const questions = [{ id: 'mode', question: 'Choose the next step.' }]
  await assert.rejects(f.ctx.userQuestions.ask({ agent: f.agent, questions }), error => error === original)
  mode = 'pending'
  const callId = randomUUID()
  assert.deepEqual(await f.ctx.userQuestions.askTimed({ agent: f.agent, questions }, callId, 1000), { pending: true, callId })
  mode = 'abort'
  const controller = new AbortController(), result = f.ctx.userQuestions.ask({ agent: f.agent, questions, signal: controller.signal })
  const rejected = assert.rejects(result, error => error.code === 'ASK_ABORTED' && error.cause === abortReason)
  await until(() => entered, 'actual native abortable provider entered')
  controller.abort(abortReason)
  await rejected
  let row
  await until(async () => { row = await f.task(); return row?.events.filter(event => event.data.nativeType === 'user-questions/request-end').length === 3 }, 'all actual provider settlements published')
  assert.equal(calls, 3)
  assert.equal(row.gs.waiting, null)
  for (const start of row.events.filter(event => event.data.nativeType === 'user-questions/request-start')) assert.ok(row.events.some(end => end.data.nativeType === 'user-questions/request-end' && end.data.waitId === start.data.waitId && end.taskLifecycleId === start.taskLifecycleId))
})

test('an observation getter failure and an unowned native question preserve the actual provider result without collecting question bodies', async t => {
  let calls = 0
  const answer = { answers: [{ id: 'mode', selected: ['Inspect'] }] }
  const f = await fixture(t, ctx => { ctx.on('user-questions/request', () => { calls++; return answer }, { global: true }) })
  const questions = [{ id: 'mode', question: 'Choose the next step.', detail: 'Authorization: Bearer native-wait-secret' }]
  const wait = { get callId() { throw new Error('observer cannot read optional call identity') } }
  assert.equal(await f.ctx.userQuestions.ask({ agent: f.agent, questions, wait }), answer)
  assert.ok((await f.adapter.snapshot()).adapter.gaps.some(gap => gap.includes('原生用户问询观察失败')))
  assert.equal(await f.ctx.userQuestions.ask({ questions }), answer)
  assert.equal(calls, 2)
  const snapshot = await f.adapter.snapshot(), row = snapshot.tasks.find(task => task.id === f.agent.id)
  assert.equal(row.gs.waiting, null)
  assert.ok(!row.events.some(event => event.data.nativeType === 'user-questions/request-start'))
  assert.ok(snapshot.adapter.gaps.some(gap => gap.includes('缺少已核对原生 Agent')))
  assert.ok(!JSON.stringify(snapshot).includes('native-wait-secret'))
})

test('native approval history remains historical on replacement and its original event time cannot be renewed after 24 hours', async t => {
  let finish
  const f = await fixture(t, (ctx, releases) => { ctx.on('approval/request', () => new Promise(resolve => { finish = resolve; releases.push(() => resolve('cancelled')) }), { global: true }) })
  assert.equal((await f.adapter.close()).status, 'closed')
  const result = f.ctx.approval.request({ agent: f.agent, toolName: 'native_readonly_check' })
  await until(() => finish instanceof Function, 'actual approval began while adapter detached')
  const asked = f.agent.session.snapshotEvents().find(event => event.type === 'approval/asked')
  await f.replaceAdapter()
  let row
  await until(async () => { row = await f.task(); return row?.events.some(event => event.data.nativeType === 'approval/asked' && event.data.history === true) }, 'read-only native approval audit tagged as history')
  const history = row.events.find(event => event.data.nativeType === 'approval/asked')
  assert.equal(history.capturedAt, asked.time)
  assert.equal(row.gs.waiting, null)
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true, now: () => asked.time + 86_400_001 })
  t.after(() => collector.close())
  collector.observe('session/observed', { sessionId: f.agent.session.id, header: f.agent.session.header, inheritedEventCount: 0, events: [asked], cursor: asked.seq, readComplete: false, gaps: [] })
  assert.ok(!collector.batch().events.some(event => event.data.nativeType === 'approval/asked'))
  assert.ok(collector.batch().gaps.some(gap => gap.includes('审批审计超过 24 小时')))
  finish('allowed-once')
  assert.equal(await result, 'allowed-once')
  await until(async () => (await f.task())?.events.some(event => event.data.nativeType === 'approval/decided'), 'live decision after historical ask published')
  assert.equal((await f.task()).gs.waiting, null)
})

test('a disposed native lifecycle late question return cannot close the current lifecycle wait', async t => {
  const finishes = []
  const answer = { answers: [{ id: 'mode', selected: ['Inspect'] }] }
  const f = await fixture(t, (ctx, releases) => { ctx.on('user-questions/request', () => new Promise(resolve => { finishes.push(resolve); releases.push(() => resolve(answer)) }), { global: true }) })
  const questions = [{ id: 'mode', question: 'Choose the next step.' }], first = f.ctx.userQuestions.ask({ agent: f.agent, questions })
  let old
  await until(async () => { old = await f.task(); return old?.gs.waiting?.kind === 'user' }, 'old native lifecycle wait published')
  await f.handle.dispose()
  const current = await f.create(f.agent.session.id), second = f.ctx.userQuestions.ask({ agent: current.agent, questions })
  let row
  await until(async () => { row = (await f.adapter.snapshot()).tasks.find(task => task.id === current.agent.id && task.lifecycleId !== old.lifecycleId && task.status === 'running'); return row?.gs.waiting?.kind === 'user' }, 'new native lifecycle wait published')
  const currentStart = row.events.find(event => event.data.nativeType === 'user-questions/request-start')
  finishes[0](answer)
  assert.equal(await first, answer)
  row = (await f.adapter.snapshot()).tasks.find(task => task.key === row.key)
  assert.equal(row.gs.waiting.evidenceIds[0], currentStart.id)
  assert.ok(!row.events.some(event => event.data.nativeType === 'user-questions/request-end'))
  finishes[1](answer)
  assert.equal(await second, answer)
  await until(async () => (await f.adapter.snapshot()).tasks.find(task => task.key === row.key)?.gs.waiting === null, 'current lifecycle own result closes its wait')
})

test('closing the production observer leaves a pending native provider untouched and late return cannot repopulate closed resources', async t => {
  let finish, observedSignal
  const answer = { answers: [{ id: 'mode', selected: ['Inspect'] }] }
  const f = await fixture(t, (ctx, releases) => { ctx.on('user-questions/request', request => { observedSignal = request.signal; return new Promise(resolve => { finish = resolve; releases.push(() => resolve(answer)) }) }, { global: true }) })
  const controller = new AbortController(), result = f.ctx.userQuestions.ask({ agent: f.agent, questions: [{ id: 'mode', question: 'Choose the next step.' }], signal: controller.signal })
  await until(async () => (await f.task())?.gs.waiting?.kind === 'user', 'native provider remains pending before observer close')
  assert.equal((await f.adapter.close()).status, 'closed')
  const closed = f.adapter.diagnostics()
  assert.equal(observedSignal, controller.signal)
  assert.equal(controller.signal.aborted, false)
  finish(answer)
  assert.equal(await result, answer)
  assert.deepEqual(f.adapter.diagnostics(), closed)
})

test('twenty genuine native questions pair exactly once and never imply a parent join', async t => {
  let calls = 0
  const answer = { answers: [{ id: 'mode', selected: ['Inspect'] }] }
  const f = await fixture(t, ctx => { ctx.on('user-questions/request', () => { calls++; return answer }, { global: true }) })
  for (let index = 0; index < 20; index++) assert.equal(await f.ctx.userQuestions.ask({ agent: f.agent, questions: [{ id: 'mode', question: 'Choose the next step.' }] }), answer)
  let row
  await until(async () => { row = await f.task(); return row?.events.filter(event => event.data.nativeType === 'user-questions/request-end').length === 20 && f.adapter.diagnostics().collector.bufferedEvents === 0 }, 'twenty natural native question pairs acknowledged')
  const starts = row.events.filter(event => event.data.nativeType === 'user-questions/request-start')
  assert.equal(calls, 20)
  assert.equal(starts.length, 20)
  assert.equal(new Set(starts.map(event => event.data.waitId)).size, 20)
  assert.equal(row.gs.waiting, null)
  const snapshot = await f.adapter.snapshot()
  const supported = f.ctx.subagents.parentWaitAuditVersion === 1
  assert.deepEqual(snapshot.adapter.parentWaitAudit, { supported, version: supported ? 1 : null })
  assert.ok(snapshot.adapter.gaps.some(gap => gap.includes(supported ? 'join 审计不提供豁免' : '不能证明父级正在等待子代理')))
  assert.ok(!snapshot.tasks.flatMap(task => task.events).some(event => event.data.nativeType === 'execution/wait-start'))
})

for (const judgment of ['unknown', 'failed']) test(`a ${judgment} controlled HTTP judgment remains gray while the actual native user wait stays visible`, async t => {
  let finish
  const answer = { answers: [{ id: 'mode', selected: ['Inspect'] }] }
  const f = await fixture(t, (ctx, releases) => { ctx.on('user-questions/request', () => new Promise(resolve => { finish = resolve; releases.push(() => resolve(answer)) }), { global: true }) }, judgment)
  const result = f.ctx.userQuestions.ask({ agent: f.agent, questions: [{ id: 'mode', question: 'Choose the next step.' }] })
  let row
  await until(async () => { row = await f.task(); return row?.gs.waiting?.kind === 'user' }, 'actual native wait stays independently visible')
  assert.equal(row.gs.light, 'gray', JSON.stringify({ gs: row.gs, checks: row.analysis.checks }))
  assert.ok(row.gs.waiting.durationMs >= 0)
  const drift = row.analysis.checks.find(check => check.id === 'request_drift')
  assert.equal(drift.status, 'unknown')
  if (judgment === 'unknown') assert.equal(drift.applicability.state, 'triggered')
  else assert.equal(drift.judgmentState, 'failed')
  finish(answer)
  assert.equal(await result, answer)
})

test('native ApprovalService cancellation and a throwing provider produce their own closed audit outcomes', async t => {
  let mode = 'abort', entered, calls = 0
  const f = await fixture(t, (ctx, releases) => { ctx.on('approval/request', () => { calls++; if (mode === 'throw') throw new Error('actual native approval provider failed'); return new Promise(resolve => { entered = true; releases.push(() => resolve('unavailable')) }) }, { global: true }) })
  const controller = new AbortController(), result = f.ctx.approval.request({ agent: f.agent, toolName: 'native_readonly_check', signal: controller.signal })
  await until(() => entered, 'actual abortable approval provider entered')
  controller.abort()
  assert.equal(await result, 'cancelled')
  mode = 'throw'
  assert.equal(await f.ctx.approval.request({ agent: f.agent, toolName: 'native_readonly_check' }), 'unavailable')
  let row
  await until(async () => { row = await f.task(); return row?.events.filter(event => event.data.nativeType === 'approval/decided').length === 2 }, 'native fail-closed approval audits published')
  assert.equal(calls, 2)
  assert.equal(row.gs.waiting, null)
  const audits = f.agent.session.snapshotEvents().filter(event => event.type === 'approval/decided')
  assert.deepEqual(audits.map(event => event.data.outcome), ['cancelled', 'unavailable'])
})

for (const mode of ['await', 'fire-and-forget', 'await-close', 'await-lateattach', 'await-prompt-fidelity']) test(`real native parent ${mode} distinguishes join operation from a correlated blocking tool caller`, async t => {
  if (!await requiresParentWaitAudit(t)) return
  let activation, pending, joined, waitId, callerExecution, parent, audits = []
  const childModel = randomUUID()
  const f = await fixture(t, (ctx, release) => {
    assert.equal(ctx.subagents.parentWaitAuditVersion, 1)
    assert.equal(typeof ctx.subagents.observeWaitCaller, 'function')
    for (const type of ['subagent/wait-start', 'subagent/wait-targets', 'subagent/wait-end', 'subagent/wait-caller-start', 'subagent/wait-caller-end']) ctx.on(type, info => { audits.push({ type, waitId: info.waitId, parent: info.parent, targets: info.targets, execution: info.execution, operationSettled: info.operationSettled }) })
    ctx.tools.register({ name: 'readonly_actual_parent_join', description: 'Self-owned native await fixture; no installed default caller claim.', parameters: { prompt: { type: 'string' } },
      output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
      async execute(_args, exec) {
        callerExecution = exec; parent = exec.agent
        activation = await ctx.subagents.startActivation({ provider: 'spawn', label: 'Wait child', delivery: 'caller', signal: new AbortController().signal,
          request: { parent: exec.agent, prompt: [{ type: 'text', text: 'Inspect delegated data only.' }], maxDepth: 4, agentOptions: { model: childModel } } })
        if (mode.startsWith('await')) {
          try { joined = await ctx.subagents.waitForChildren(exec.agent, info => { waitId = info.waitId; ctx.subagents.observeWaitCaller('start', waitId, exec) }) }
          finally { if (waitId) ctx.subagents.observeWaitCaller('end', waitId, exec) }
        } else {
          pending = ctx.subagents.waitForChildren(exec.agent)
          await new Promise(resolve => release.push(resolve))
          joined = await pending
        }
        return 'native-child-joined'
      },
    })
  }, mode === 'await-prompt-fidelity' ? 'clear' : 'unknown', false, 'readonly_actual_parent_join', true)
  t.after(async () => { await activation?.dispose(); await pending })
  assert.equal(parent, f.agent)
  assert.deepEqual(callerExecution.arguments, { prompt: 'Inspect delegated child completion only.' }, 'actual registry parsed argument identity')
  await until(() => audits.some(item => item.type === 'subagent/wait-start'), 'actual SDK join opens')
  const start = audits.find(item => item.type === 'subagent/wait-start')
  assert.equal(start.parent, f.agent)
  assert.equal(f.agent.status, 'running')
  assert.equal(start.targets[0].id, activation.childId)
  let row, snapshot
  await until(async () => { snapshot = await f.adapter.snapshot(); row = snapshot.tasks.find(task => task.id === f.agent.id); return row?.events.some(event => event.data.nativeType === 'subagent/wait-start') && snapshot.tasks.some(task => task.sessionId === activation.childId && task.status === 'running' && task.currentRequest) }, 'actual join and child request reach HTTP Core')
  assert.deepEqual(snapshot.adapter.parentWaitAudit, { supported: true, version: 1 })
  if (mode.startsWith('await')) {
    assert.equal(audits.find(item => item.type === 'subagent/wait-caller-start').execution, callerExecution)
    await until(async () => { row = await f.task(); return row?.gs.waiting?.kind === 'children' }, 'trusted native await opens public child wait')
    assert.equal(row.gs.light, 'green', JSON.stringify(row.gs))
    assert.equal(row.gs.waiting.semanticState, 'pending')
    f.observations.push({ phase: 'trusted-await-pending-green', row: structuredClone(row) })
    assert.ok(row.events.some(event => event.data.nativeType === 'execution/wait-start' && event.data.waitId === start.waitId))
  } else {
    assert.equal(audits.some(item => item.type.includes('caller')), false)
    assert.equal(row.gs.waiting, null)
    assert.ok(!row.events.some(event => event.data.nativeType === 'execution/wait-start'))
    assert.ok(snapshot.sources.find(source => source.key === snapshot.adapter.sourceKey).hierarchyContext.coverage.branchGaps.some(gap => gap.reason.includes('阻塞调用者')))
    f.observations.push({ phase: 'bare-join-no-caller', row: structuredClone(row), hierarchy: snapshot.sources.find(source => source.key === snapshot.adapter.sourceKey).hierarchyContext })
  }
  if (mode === 'await-lateattach') {
    assert.equal((await f.adapter.close()).status, 'closed')
    await f.replaceAdapter()
    await until(async () => { row = await f.task(); return row?.status === 'running' }, 'replacement adapter observes the real current parent')
    assert.equal(row.gs.waiting, null)
    snapshot = await f.adapter.snapshot()
    assert.equal(snapshot.sources.find(source => source.key === snapshot.adapter.sourceKey).hierarchyContext.waitBindings?.length ?? 0, 0)
    const childStream = f.streams.find(stream => stream.model === childModel); childStream.done = true; childStream.wake?.()
    await until(() => joined === true, 'original join completes across observer replacement')
    await until(async () => { snapshot = await f.adapter.snapshot(); return snapshot.sources.find(source => source.key === snapshot.adapter.sourceKey).hierarchyContext.coverage.branchGaps.some(gap => gap.reason.includes('开始')) }, 'late attach keeps missing original start explicit')
    assert.equal(snapshot.tasks.find(task => task.id === f.agent.id).gs.waiting, null)
    f.observations.push({ phase: 'late-attach-no-rebuilt-wait', snapshot })
    return
  }
  if (mode === 'await-prompt-fidelity') {
    const start = row.events.find(event => event.kind === 'tool-start' && event.data.callId === callerExecution.callId)
    assert.equal(start.text, callerExecution.arguments.prompt, 'the actual native call prompt must survive first canonical capture')
  }
  await f.configureAnalysis()
  if (mode.startsWith('await')) {
    await until(async () => { row = await f.task(); return row?.gs.waiting?.semanticState === (mode === 'await-prompt-fidelity' ? 'complete' : 'insufficient') }, 'semantic judgment remains independent from actual wait proof')
    assert.equal(row.gs.light, mode === 'await-prompt-fidelity' ? 'green' : 'gray')
    assert.equal(row.gs.waiting.kind, 'children')
    f.observations.push({ phase: mode === 'await-prompt-fidelity' ? 'native-action-retained-clear-judgment' : 'same-wait-semantic-insufficient-gray', row: structuredClone(row), liveArguments: callerExecution.arguments })
  }
  if (mode === 'await-close') {
    const child = f.ctx.agents.get(activation.childId)
    assert.equal((await f.adapter.close()).status, 'closed')
    const closed = f.adapter.diagnostics()
    assert.equal(child.status, 'running')
    assert.equal(callerExecution.signal.aborted, false)
    f.observations.push({ phase: 'observer-closed-native-child-active', childStatus: child.status, callerAborted: callerExecution.signal.aborted, diagnostics: closed })
    const childStream = f.streams.find(stream => stream.model === childModel); childStream.done = true; childStream.wake?.()
    await until(() => joined === true, 'native join completes after observer shutdown')
    assert.deepEqual(f.adapter.diagnostics(), closed)
    return
  }
  const childStream = f.streams.find(stream => stream.model === childModel)
  assert.ok(childStream); childStream.done = true; childStream.wake?.()
  if (mode === 'fire-and-forget') for (const finish of f.release) finish()
  await until(() => joined === true, 'native join returns its original result')
  await until(async () => { row = await f.task(); return row?.events.some(event => event.data.nativeType === 'subagent/wait-end') && row.gs.waiting === null }, 'native settled end closes observation')
  if (mode.startsWith('await')) assert.ok(row.events.some(event => event.data.nativeType === 'execution/wait-end' && event.data.waitId === start.waitId))
  assert.ok(audits.find(item => item.type === 'subagent/wait-end').operationSettled)
})

test('an actual idle parent join does not promote the parent to running or create a caller exemption', async t => {
  if (!await requiresParentWaitAudit(t)) return
  const f = await fixture(t), childModel = randomUUID(), nativeAudits = []
  for (const stream of f.streams) { stream.done = true; stream.wake?.() }
  await f.agent.whenIdle()
  f.ctx.on('subagent/wait-start', info => nativeAudits.push(info))
  const activation = await f.ctx.subagents.startActivation({ provider: 'spawn', label: 'Idle-parent child', delivery: 'caller', signal: new AbortController().signal,
    request: { parent: f.agent, prompt: [{ type: 'text', text: 'Inspect delegated data only.' }], maxDepth: 4, agentOptions: { model: childModel } } })
  let joined
  const pending = f.ctx.subagents.waitForChildren(f.agent).then(value => { joined = value })
  t.after(async () => { await activation.dispose(); await pending })
  await until(() => nativeAudits.length > 0, 'actual idle parent native join audit')
  assert.equal(nativeAudits[0].parentStatusAtStart, 'idle')
  assert.equal(f.agent.status, 'idle')
  let row
  await until(async () => { row = (await f.adapter.snapshot()).tasks.find(task => task.id === f.agent.id); return row?.events.some(event => event.data.nativeType === 'subagent/wait-start') }, 'idle join reaches public HTTP observation')
  assert.equal(row.status, 'idle')
  assert.equal(row.gs.light, null)
  assert.equal(row.gs.waiting, null)
  assert.ok(!row.events.some(event => event.data.nativeType === 'execution/wait-start'))
  f.observations.push({ phase: 'idle-parent-join-not-running', row: structuredClone(row) })
  const stream = f.streams.find(stream => stream.model === childModel); stream.done = true; stream.wake?.()
  await until(() => joined === true, 'original idle parent join result unchanged')
})

test('stock SDK preserves real child lifecycle and tree while parent wait audit remains an explicit public gap', async t => {
  const f = await fixture(t, () => {}, 'unknown', false, null, true), native = []
  assert.ok(f.ctx.get('subagents'), 'real SDK subagent service mounted')
  assert.notEqual(f.ctx.subagents.parentWaitAuditVersion, 1)
  assert.notEqual(typeof f.ctx.subagents.observeWaitCaller, 'function')
  for (const type of ['subagent/start', 'subagent/end']) f.ctx.on(type, info => native.push({ type, id: info.id, runId: info.runId }))
  const childModel = randomUUID()
  const activation = await f.ctx.subagents.startActivation({ provider: 'spawn', label: 'Stock child', delivery: 'caller', signal: new AbortController().signal,
    request: { parent: f.agent, prompt: [{ type: 'text', text: 'Inspect delegated data only.' }], maxDepth: 4, agentOptions: { model: childModel } } })
  t.after(() => activation.dispose())
  let snapshot, child
  await until(async () => { snapshot = await f.adapter.snapshot(); child = snapshot.tasks.find(task => task.id === activation.childId); return child?.status === 'running' && child.hierarchy?.parentSessionId === f.agent.session.id }, 'stock child and parent relation reach production Core')
  assert.deepEqual(snapshot.adapter.parentWaitAudit, { supported: false, version: null })
  assert.ok(snapshot.adapter.gaps.some(gap => gap.includes('不能证明父级正在等待子代理')))
  assert.ok(native.some(event => event.type === 'subagent/start' && event.id === activation.childId))
  const assertNoWait = view => {
    assert.ok(view.tasks.every(task => task.gs?.waiting?.kind !== 'children'))
    assert.ok(!view.tasks.flatMap(task => task.events).some(event => /^(subagent\/wait-|execution\/wait-)/.test(event.data?.nativeType ?? '')))
  }
  assertNoWait(snapshot)
  const stream = f.streams.find(stream => stream.model === childModel)
  assert.ok(stream); stream.done = true; stream.wake?.()
  await until(() => native.some(event => event.type === 'subagent/end' && event.id === activation.childId), 'stock child publishes natural end')
  await until(async () => { snapshot = await f.adapter.snapshot(); return snapshot.tasks.flatMap(task => task.events).some(event => event.data?.nativeType === 'subagent/end') }, 'stock child end reaches production Core')
  assertNoWait(snapshot)
  assert.equal(native.find(event => event.type === 'subagent/start').runId, native.find(event => event.type === 'subagent/end').runId)
})
