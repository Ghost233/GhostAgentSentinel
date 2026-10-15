import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { req } from './dsh-native-runtime.mjs'
import { mountGS } from './gs-session-client-runtime.mjs'
import { startServer } from '../src/server.mjs'
import { createAdapter } from '../dsh/host.mjs'
const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-subagent-spawn-in-process', 'dsh-subagent-fork-in-process', 'dsh-session-query'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
const until = async (predicate, label) => { const end = Date.now() + 5000; while (!await predicate()) { assert.ok(Date.now() < end, label); await new Promise(resolve => setTimeout(resolve, 10)) } }

test('five native delegation levels, siblings and a real fork reach GS through the production adapter while a plain fork stays outside', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-native-recursive-')), ctx = new Context(), streams = new Map(), activations = []
  let server, adapter, root
  const receipt = { scope: 'self-owned in-process native DSH tasks, actual spawn/fork providers, production adapter, authenticated HTTP public snapshot, actual client JS with native SlotCore/Remote in JSDOM; no installed Host/GUI or actual model', observations: [], cleanup: { failures: [] } }
  t.after(async () => {
    const failures = []
    for (const release of [() => { for (const stream of streams.values()) { stream.done = true; stream.wake?.() } }, ...activations.slice().reverse().map(activation => () => activation.dispose()), () => root?.dispose(), async () => { if (adapter) { const result = await adapter.close(); receipt.shutdown = result; assert.equal(result.status, 'closed', JSON.stringify(result)) } }, () => ctx.fiber.dispose(), () => server?.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await release() } catch (error) { failures.push(error) } }
    receipt.cleanup.failures = failures.map(error => error.message); receipt.completedAt = Date.now()
    if (process.env.SENTINEL_GS_NATIVE_RECEIPT) await writeFile(process.env.SENTINEL_GS_NATIVE_RECEIPT, JSON.stringify(receipt, null, 2))
    if (failures.length) throw new AggregateError(failures, 'recursive native cleanup failed')
  })
  class Actor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      const stream = { done: false }; streams.set(options.model, stream)
      const abort = () => { stream.done = true; stream.wake?.() }
      options.signal.addEventListener('abort', abort, { once: true })
      try {
        yield { type: 'text-delta', index: 0, text: `native execution ${options.model}` }
        while (!stream.done) await new Promise(resolve => { stream.wake = resolve })
        yield { type: 'finish', reason: { kind: 'stop' } }
      } finally { options.signal.removeEventListener('abort', abort) }
    }
  }
  await ctx.plugin(LlmRuntime)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  ctx.llm.registerAdapter(['recursive-fixture'], new Actor())
  server = await startServer({ dataDir })
  adapter = createAdapter(ctx, { dataDir, registrationId: 'recursive-native' }, { flushMs: 5, heartbeatMs: 25 })
  await until(() => adapter.connected, 'adapter connected')
  root = await ctx.agents.create({ sessionId: 'recursive-root', agentOptions: { provider: 'recursive-fixture', model: 'root' } })
  root.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only; preserve files.' }] }))
  await until(() => streams.has('root'), 'native root running')
  async function delegate(parent, model, provider = 'spawn') {
    const activation = await ctx.subagents.startActivation({ provider, label: 'same label', delivery: 'caller', signal: new AbortController().signal,
      request: { parent, prompt: [{ type: 'text', text: `Inspect delegated ${model} only.` }], maxDepth: 8, agentOptions: { model } } })
    activations.push(activation); await until(() => streams.has(model), `${model} running`)
    return ctx.agents.get(activation.childId)
  }
  const child = await delegate(root.agent, 'child'), grandchild = await delegate(child, 'grandchild'), deeper = await delegate(grandchild, 'deeper')
  await delegate(deeper, 'deepest')
  await delegate(root.agent, 'sibling')
  await delegate(root.agent, 'forked', 'fork')
  const branch = ctx.sessions.fork(root.agent.session, undefined, 'ordinary-branch')
  let snapshot, group, childScope
  await until(async () => {
    snapshot = await adapter.snapshot(); group = snapshot.hierarchyContext?.groups?.find(group => group.rootSessionId === root.agent.session.id)
    childScope = snapshot.hierarchyContext?.scopes?.find(scope => scope.sessionId === child.session.id)
    return group?.runningCount === 7 && childScope
  }, 'all actual delegated nodes grouped')
  assert.equal(group.nodes.length, 7)
  assert.equal(Math.max(...group.nodes.map(node => node.depth)), 4)
  assert.deepEqual(childScope.counts, { green: 0, yellow: 0, red: 0, gray: 4 })
  assert.ok(!group.nodes.some(node => node.sessionId === branch.id))
  receipt.observations.push({ phase: 'seven-native-active', group: structuredClone(group), childScope: structuredClone(childScope) })
  const frontend = await mountGS(t, adapter, child.session.id)
  await frontend.until(() => frontend.dom.window.document.querySelector('[data-gs-count="gray"]')?.textContent === '4', 'native GS current counts')
  await frontend.click(frontend.dom.window.document.querySelector('[data-gs-entry]'))
  assert.equal(frontend.dom.window.document.querySelectorAll('[data-gs-node]').length, 4)
  assert.equal(frontend.dom.window.document.querySelector('[data-gs-scope]').dataset.gsScope, child.session.id)
  const selected = frontend.dom.window.document.querySelectorAll('[data-gs-node]')[3]
  await frontend.click(selected)
  assert.equal(frontend.dom.window.document.querySelector('[data-gs-count="gray"]').textContent, '4')
  await frontend.click(frontend.dom.window.document.querySelector('[data-gs-return-root]'))
  assert.equal(frontend.dom.window.document.querySelectorAll('[data-gs-node]').length, 7)
  streams.get('root').done = true; streams.get('root').wake?.(); await root.agent.whenIdle()
  await until(async () => (await adapter.snapshot()).hierarchyContext.groups.find(group => group.rootSessionId === root.agent.session.id)?.runningCount === 6, 'ended root retains active descendants')
  receipt.observations.push({ phase: 'root-ended-descendants-active', group: (await adapter.snapshot()).hierarchyContext.groups.find(group => group.rootSessionId === root.agent.session.id) })
  receipt.reader = adapter.diagnostics().hierarchyReader
})

test('a real external activation has a no-session GS leaf and no synthetic local Agent', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-native-external-')), ctx = new Context()
  let server, adapter, parent, activation, finish
  t.after(async () => {
    const failures = []
    for (const release of [() => finish?.({ output: [], stopReason: 'aborted' }), () => activation?.dispose(), () => parent?.dispose(), async () => { if (adapter) assert.equal((await adapter.close()).status, 'closed') }, () => ctx.fiber.dispose(), () => server?.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await release() } catch (error) { failures.push(error) } }
    if (failures.length) throw new AggregateError(failures, 'external native cleanup failed')
  })
  await ctx.plugin(LlmRuntime)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  server = await startServer({ dataDir })
  adapter = createAdapter(ctx, { dataDir, registrationId: 'external-native' }, { flushMs: 5, heartbeatMs: 25 })
  await until(() => adapter.connected, 'external adapter connected')
  parent = await ctx.agents.create({ sessionId: 'external-parent', agentOptions: { provider: 'unused', model: 'unused' } })
  ctx.subagents.registerProvider({ name: 'external-fixture', capabilities: { agentOptions: false, outputSchema: false, depthLimit: false, toolFilter: false, persona: false }, inheritsParentContext: false,
    async start({ signal }) {
      const result = new Promise(resolve => { finish = resolve })
      signal.addEventListener('abort', () => finish({ output: [], stopReason: 'aborted' }), { once: true })
      return { id: 'external-execution', result, dispose() { finish({ output: [], stopReason: 'aborted' }) } }
    },
  })
  activation = await ctx.subagents.startActivation({ provider: 'external-fixture', delivery: 'parent', label: 'External work', signal: new AbortController().signal,
    request: { parent: parent.agent, prompt: [{ type: 'text', text: 'Inspect independently.' }] } })
  let snapshot, group
  await until(async () => { snapshot = await adapter.snapshot(); group = snapshot.hierarchyContext?.groups?.find(group => group.rootSessionId === 'external-parent'); return group?.nodes.some(node => node.external && node.state === 'running') }, 'native external execution leaf')
  assert.equal(finish instanceof Function, true)
  assert.equal(ctx.sessions.get(activation.childId), undefined)
  assert.equal(ctx.agents.get(activation.childId), undefined)
  assert.equal(group.nodes.find(node => node.external).sessionId, null)
  assert.equal(group.nodes.find(node => node.external).parentSessionId, 'external-parent')
  assert.equal(group.nodes.filter(node => node.external).length, 1)
  assert.deepEqual(group.counts, { green: 0, yellow: 0, red: 0, gray: 1 })
  assert.equal(adapter.diagnostics().hierarchyReader.coldReads, 0, 'external leaves must not cause synthetic session reads')
  assert.ok(snapshot.hierarchyContext.scopes.find(scope => scope.sessionId === 'external-parent').coverage.branchGaps.some(gap => gap.reason.includes('外部执行内部材料不可观察')))
  await parent.dispose()
  finish({ output: [], stopReason: 'completed' })
  await activation.result
  await until(async () => (await adapter.snapshot()).hierarchyContext.groups.find(group => group.rootSessionId === 'external-parent')?.runningCount === 0, 'disposed parent and ended external do not leave a running phantom')
})
