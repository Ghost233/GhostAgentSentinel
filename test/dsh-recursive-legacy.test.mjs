import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createServer } from 'node:http'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { createCore as createLegacyCore } from './fixtures/legacy-core/src/core/index.mjs'
import { createAnalyzer as createLegacyAnalyzer } from './fixtures/legacy-core/src/analysis/index.mjs'
const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-subagent-spawn-in-process'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
const until = async (predicate, label) => { const end = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < end, label); await new Promise(resolve => setTimeout(resolve, 10)) } }

test('an actual legacy core preserves consumed root input while accepted, notice, relay and delegated inputs cannot replace or authorize tasks, including final flush', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-legacy-')), ctx = new Context(), streams = new Map(), modelRequests = [], batches = []
  let core, adapter, root, activation, closed = false
  const modelServer = createServer(async (request, response) => {
    let body = ''; for await (const chunk of request) body += chunk
    const json = JSON.parse(body); modelRequests.push(json)
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/chat/completions') response.end(JSON.stringify({ model: 'extractor', choices: [{ message: { content: JSON.stringify({ selectedOptionIds: [], gaps: [] }) }, finish_reason: 'stop' }] }))
    else response.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(json.questions).map(id => [id, { type: 'choice', choice: 'unknown', confidence: 1 }])) }))
  })
  t.after(async () => {
    const failures = []
    for (const release of [() => { for (const stream of streams.values()) { stream.done = true; stream.wake?.() } }, () => activation?.dispose(), () => root?.dispose(), async () => { if (adapter && !closed) assert.equal((await adapter.close()).status, 'closed') }, () => ctx.fiber.dispose(), () => core?.close(), () => new Promise(resolve => { modelServer.closeAllConnections(); modelServer.close(resolve) }), () => rm(dataDir, { recursive: true, force: true })]) { try { await release() } catch (error) { failures.push(error) } }
    if (failures.length) throw new AggregateError(failures, 'legacy fixture cleanup failed')
  })
  await new Promise(resolve => modelServer.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${modelServer.address().port}`
  core = await createLegacyCore({ dataDir, cooldownMs: 1, analyzer: createLegacyAnalyzer({ llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' } }) })
  class Actor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) { const stream = { done: false }; streams.set(options.model, stream); const abort = () => { stream.done = true; stream.wake?.() }; options.signal.addEventListener('abort', abort, { once: true }); try { yield { type: 'text-delta', index: 0, text: 'native fixture active' }; while (!stream.done) await new Promise(resolve => { stream.wake = resolve }); yield { type: 'finish', reason: { kind: 'stop' } } } finally { options.signal.removeEventListener('abort', abort) } }
  }
  await ctx.plugin(LlmRuntime); for (const module of modules) await ctx.plugin(module, { agents: [] }); ctx.llm.registerAdapter(['legacy-fixture'], new Actor())
  adapter = createAdapter(ctx, { registrationId: 'legacy' }, { flushMs: 5, heartbeatMs: 20, connect: async () => ({ health: async () => ({ features: [] }), registerSource: value => core.registerSource(value), heartbeat: key => core.heartbeat(key), ingest(key, batch) { batches.push(structuredClone(batch)); return core.ingest(key, batch) }, unregisterSource: (key, reason) => core.unregisterSource(key, reason), snapshot: () => core.snapshot(), close: async () => {} }) })
  await until(() => adapter.connected, 'legacy adapter connected')
  root = await ctx.agents.create({ sessionId: 'legacy-root', agentOptions: { provider: 'legacy-fixture', model: 'root' } })
  root.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Actual root request: inspect only.' }] }))
  await until(async () => (await core.snapshot()).tasks.find(task => task.sessionId === 'legacy-root')?.currentRequest === 'Actual root request: inspect only.', 'actual consumed root legacy behavior')
  await until(() => modelRequests.some(request => request.messages?.some(message => message.content.includes('Actual root request: inspect only.'))), 'actual legacy extraction processed root')
  activation = await ctx.subagents.startActivation({ provider: 'spawn', delivery: 'caller', label: 'child', signal: new AbortController().signal, request: { parent: root.agent, prompt: [{ type: 'text', text: 'Delegated child cannot authorize.' }], agentOptions: { model: 'child' } } })
  await until(() => streams.has('child'), 'real delegated child running')
  root.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Accepted only update cannot replace.' }] }))
  root.agent.session.append('user/message', createUserMessage({ source: { kind: 'runtime-context', form: 'notice' }, content: [{ type: 'text', text: 'Runtime notice cannot authorize.' }] }), { surfaceOp: 'append' })
  root.agent.session.append('user/message', createUserMessage({ source: { kind: 'agent-message', form: 'relay', senderSessionId: activation.childId }, content: [{ type: 'text', text: 'Agent relay cannot authorize.' }] }), { surfaceOp: 'append' })
  await until(() => adapter.diagnostics().adapter.tickInFlight === false && batches.some(batch => batch.events.some(event => event.data?.inputOrigin === 'agent-relay')), 'all native source phases sent')
  let snapshot = await core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.sessionId === 'legacy-root').currentRequest, 'Actual root request: inspect only.')
  assert.equal(snapshot.tasks.find(task => task.sessionId === activation.childId).currentRequest, '')
  assert.ok(batches.every(batch => !batch.hierarchyContext))
  const inputEvents = batches.flatMap(batch => batch.events).filter(event => event.kind === 'input')
  assert.ok(inputEvents.every(event => event.text === 'Actual root request: inspect only.'))
  const offered = modelRequests.filter(request => request.messages).flatMap(request => JSON.parse(request.messages[1].content).evidence)
  assert.ok(offered.some(group => group.text === 'Actual root request: inspect only.' && group.options.some(option => option.kind === 'authorization')))
  assert.ok(offered.every(group => !/Delegated child|Accepted only|Runtime notice|Agent relay/.test(group.text)))
  root.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Final accepted update cannot replace.' }] }))
  assert.equal((await adapter.close()).status, 'closed'); closed = true
  snapshot = await core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.sessionId === 'legacy-root').currentRequest, 'Actual root request: inspect only.')
  assert.ok(batches.flatMap(batch => batch.events).filter(event => event.kind === 'input').every(event => event.text === 'Actual root request: inspect only.'))
  assert.ok(snapshot.sources[0].gaps.some(gap => /不支持.*输入|输入.*不支持/.test(gap)))
})
