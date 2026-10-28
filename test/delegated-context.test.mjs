import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { req } from './dsh-native-runtime.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-subagent-spawn-in-process'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
async function until(predicate, label) {
  const deadline = Date.now() + 5000
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 10)) }
}

async function nativeFixture(t, { action = 'Inspect HTTP tests without changing files.', parentSource = { kind: 'user' }, delegatedPrompt = 'Check HTTP tests only.', throughTool = false } = {}) {
  const requests = [], streams = new Map(), descendants = [], dataDir = await mkdtemp(join(tmpdir(), 'sentinel-delegated-context-'))
  let ctx, server, adapter, parent, activation
  const models = createServer(async (req, res) => {
    if (req.method === 'GET') return res.writeHead(404, { 'content-type': 'application/json' }).end('{}');
    let body = ''; for await (const chunk of req) body += chunk
    const wire = JSON.parse(body), json = req.url === '/v1/systemone' ? decodeJevRequest(wire) : wire; requests.push({ path: req.url, body: json })
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') {
      const groups = JSON.parse(json.messages[1].content).evidence
      const selectedOptionIds = groups.flatMap(group => {
        const kind = group.options.some(option => option.kind === 'authorization') ? 'authorization' : group.options.some(option => option.kind === 'action') ? 'action' : 'context'
        return group.options.filter(option => option.kind === kind).slice(0, 1).map(option => option.optionId)
      })
      res.end(JSON.stringify({ model: json.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(json, { selectedOptionIds, gaps: [] })) } }] }))
    } else {
      const hasLimit = json.state.events.some(event => (event.textRef === 'request' ? json.state.request : event.text) === 'Inspect repository. Do not modify files.' && event.authorizationEligible === true)
      const badDelegation = json.state.events.some(event => event.kind === 'tool-start' && event.text === 'Change database files.')
      const choice = hasLimit ? badDelegation || action.startsWith('I changed') && json.state.instruction?.origin === 'delegated' ? 'attention' : 'clear' : 'unknown'
      res.end(JSON.stringify({ model: json.model, answers: Object.fromEntries(Object.keys(json.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? choice : 'unknown' }])) }))
    }
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  t.after(async () => {
    const failures = []
    for (const release of [() => { for (const stream of streams.values()) { stream.done = true; stream.wake?.() } }, async () => { for (const item of descendants.slice().reverse()) await item.dispose() }, () => activation?.dispose?.(), () => parent?.dispose(), async () => { if (adapter) { const result = await adapter.close(); assert.equal(result.status, 'closed'); assert.equal(result.failures, 0); assert.equal(result.pendingRequests, 0) } }, () => ctx?.fiber.dispose(), () => server?.close(), () => new Promise(resolve => { models.closeAllConnections(); models.close(resolve) }), () => rm(dataDir, { recursive: true, force: true })]) {
      try { await release() } catch (error) { failures.push(error) }
    }
    if (failures.length) throw new AggregateError(failures, 'delegated context cleanup failed')
  })
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  server = await startServer({ dataDir, cooldownMs: 1 })
  const control = await connectCore({ dataDir })
  try { await configureInstallationModels(control, 'native-context', { llm: { baseUrl, model: 'controlled-extractor' }, jev: { baseUrl, model: 'controlled-judge' } }) } finally { await control.close() }
  ctx = new Context()
  class Actor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      const stream = { done: false }; streams.set(options.model, stream)
      const abort = () => { stream.done = true; stream.wake?.() }
      options.signal.addEventListener('abort', abort, { once: true })
      try {
        yield { type: 'text-delta', index: 0, text: options.model !== 'parent' ? action : 'Inspecting the requested repository.' }
        while (!stream.done) await new Promise(resolve => { stream.wake = resolve })
        yield { type: 'finish', reason: { kind: 'stop' } }
      } finally { options.signal.removeEventListener('abort', abort) }
    }
  }
  await ctx.plugin(LlmRuntime)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  if (throughTool) { const persistence = req('@deepseek-ai/dsh-session-persistence-jsonl'); await ctx.plugin(persistence.default ?? persistence, { root: join(dataDir, 'native-sessions'), compression: 'none' }) }
  ctx.llm.registerAdapter(['context-fixture'], new Actor())
  adapter = createAdapter(ctx, { dataDir, registrationId: 'native-context' }, { flushMs: 5, heartbeatMs: 25 })
  await until(() => adapter.connected, 'adapter connected')
  parent = await ctx.agents.create({ sessionId: 'context-parent', agentOptions: { provider: 'context-fixture', model: 'parent' } })
  parent.agent.followup(createUserMessage({ source: parentSource, content: [{ type: 'text', text: 'Inspect repository. Do not modify files.' }] }))
  await until(() => streams.has('parent'), 'parent running')
  if (throughTool) {
    const tool = req('@deepseek-ai/dsh-tool-subagent'); await ctx.plugin(tool.default ?? tool, { provider: 'spawn', maxDepth: 4, agentOptions: { model: 'child' } })
    const result = await ctx.tools.execute({ name: 'subagent', callId: 'native-delegation', arguments: { description: 'HTTP checks', prompt: delegatedPrompt }, agent: parent.agent, signal: new AbortController().signal })
    assert.equal(result.isError, false, JSON.stringify(result))
    activation = { childId: result.value.subagentId }
  } else activation = await ctx.subagents.startActivation({ provider: 'spawn', label: 'HTTP checks', delivery: 'caller', signal: new AbortController().signal,
    request: { parent: parent.agent, prompt: [{ type: 'text', text: delegatedPrompt }], maxDepth: 4, agentOptions: { model: 'child' } } })
  await until(() => streams.has('child'), 'child running')
  return { requests, adapter, activation, async delegateGrandchild() {
    const nested = await ctx.subagents.startActivation({ provider: 'spawn', label: 'Deep HTTP checks', delivery: 'caller', signal: new AbortController().signal, request: { parent: ctx.agents.get(activation.childId), prompt: [{ type: 'text', text: 'Inspect the HTTP response only.' }], maxDepth: 4, agentOptions: { model: 'grandchild' } } }); descendants.push(nested); await until(() => streams.has('grandchild'), 'native grandchild running'); return nested
  } }
}

test('native consumed delegation is analyzed against its actual task and original ancestor user limit through the public core', async t => {
  const f = await nativeFixture(t, { throughTool: true })
  let child
  await until(async () => {
    child = (await f.adapter.snapshot()).tasks.find(task => task.sessionId === f.activation.childId)
    return child?.analysis.checks.find(check => check.id === 'request_drift')?.status === 'clear'
  }, 'normal delegated division must be analyzed').catch(error => { console.error(JSON.stringify({ request: child?.currentRequest, context: child?.instructionContext, error: child?.analysis.error, requestCount: f.requests.length }, null, 2)); throw error })
  await until(async () => { const parent = (await f.adapter.snapshot()).tasks.find(task => task.sessionId === 'context-parent'); const drift = parent.analysis.checks.find(check => check.id === 'request_drift'); return drift.status === 'clear' && drift.evidenceIds.some(id => parent.events.some(event => event.id === id && event.data?.toolName === 'subagent')) }, 'normal division also clears the parent actual delegation check')
  assert.match(child.currentRequest, /^Check HTTP tests only\./)
  const rootJudge = f.requests.find(request => request.path === '/v1/systemone' && request.body.state.instruction?.inputRef.taskId === 'context-parent')
  assert.equal(JSON.stringify(rootJudge.body.state).split('Inspect repository. Do not modify files.').length - 1, 1)
  const drift = child.analysis.checks.find(check => check.id === 'request_drift')
  const parentRef = drift.evidenceRefs.find(reference => reference.taskId !== child.id)
  assert.ok(parentRef, 'short evidence keeps its original ancestor owner')
  assert.ok(!child.events.some(event => event.text === 'Inspect repository. Do not modify files.'))
  const judge = f.requests.find(request => request.path === '/v1/systemone' && request.body.state.events.some(event => event.id === request.body.state.requestId && event.text.startsWith('Check HTTP tests only.')))
  assert.equal(judge.body.state.events.filter(event => event.text === 'Inspect repository. Do not modify files.').length, 1)
  assert.ok(judge.body.state.events.filter(event => event.taskId === child.id && event.kind === 'input').every(event => event.authorizationEligible === false))
})


test('a native child action explicitly outside the inherited user limit receives first-round drift with original source references', async t => {
  const f = await nativeFixture(t, { action: 'I changed the database files.' })
  let child
  await until(async () => {
    child = (await f.adapter.snapshot()).tasks.find(task => task.sessionId === f.activation.childId)
    return child?.analysis.checks.find(check => check.id === 'request_drift')?.status === 'attention'
  }, 'explicit delegated drift')
  assert.ok(child.analysis.checks.find(check => check.id === 'request_drift').evidenceRefs.some(ref => ref.taskId === 'context-parent'))
  assert.ok(child.alerts.some(alert => alert.checkId === 'request_drift' && alert.status === 'attention'))
})

test('native agent relay provenance cannot grant inherited user permission and preserves a related unknown', async t => {
  const f = await nativeFixture(t, { parentSource: { kind: 'user', senderSessionId: 'other-agent' } })
  let child
  await until(async () => {
    child = (await f.adapter.snapshot()).tasks.find(task => task.sessionId === f.activation.childId)
    return child?.analysis.summary?.currentTask?.startsWith('Check HTTP tests only.') && child?.instructionContext.gaps.length && f.requests.some(request => request.path === '/v1/chat/completions' && JSON.parse(request.body.messages[1].content).coverage.instructionOrigin === 'delegated')
  }, 'missing original user proof')
  assert.equal(child.analysis.checks.find(check => check.id === 'request_drift').status, 'unknown')
  assert.ok(child.instructionContext.gaps.some(gap => ['ancestor_limit_origin_or_retention_missing', 'ancestor_user_limits_unconfirmed'].includes(gap)))
  assert.equal(child.analysis.error, null)
  const extractor = f.requests.filter(request => request.path === '/v1/chat/completions').map(request => JSON.parse(request.body.messages[1].content)).find(body => body.coverage.instructionOrigin === 'delegated')
  assert.ok(extractor.evidence.every(group => group.options.every(option => option.kind !== 'authorization')))
})


test('the native parent independently judges its delegation against the original request while a child retains its distinct task', async t => {
  const f = await nativeFixture(t, { delegatedPrompt: 'Change database files.', throughTool: true })
  let snapshot
  await until(async () => {
    snapshot = await f.adapter.snapshot()
    return snapshot.tasks.find(task => task.sessionId === 'context-parent')?.analysis.checks.find(check => check.id === 'request_drift')?.status === 'attention'
  }, 'parent delegation must be checked against its original read-only request')
  const parent = snapshot.tasks.find(task => task.sessionId === 'context-parent')
  assert.ok(parent.events.some(event => event.kind === 'tool-start' && event.text === 'Change database files.'))
  assert.ok(parent.analysis.checks.find(check => check.id === 'request_drift').evidenceIds.some(id => parent.events.some(event => event.id === id && event.data?.toolName === 'subagent')))
  await until(async () => (await f.adapter.snapshot()).tasks.find(task => task.sessionId === f.activation.childId)?.currentRequest === 'Change database files.', 'child actual delegated anchor')
  assert.ok(!parent.events.some(event => event.kind === 'input' && event.text === 'Change database files.'))
})


test('a third native delegation level inherits the original user limit once without treating its parent task as user permission', async t => {
  const f = await nativeFixture(t), activation = await f.delegateGrandchild()
  let grandchild
  await until(async () => {
    grandchild = (await f.adapter.snapshot()).tasks.find(task => task.sessionId === activation.childId)
    return grandchild?.analysis.checks.find(check => check.id === 'request_drift')?.status === 'clear'
  }, 'deep original ancestor limit inheritance')
  assert.equal(grandchild.instructionContext.ancestorLimitRefs.length, 1)
  assert.equal(grandchild.instructionContext.ancestorLimitRefs[0].taskId, 'context-parent')
  const judge = f.requests.find(request => request.path === '/v1/systemone' && request.body.state.instruction?.inputRef.taskId === grandchild.id).body.state
  assert.ok(judge.events.every(event => event.taskId === grandchild.id || event.taskId === 'context-parent'))
  assert.equal(judge.events.filter(event => event.text === 'Inspect repository. Do not modify files.').length, 1)
})
