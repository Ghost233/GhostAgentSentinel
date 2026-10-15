import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { readFile, access } from 'node:fs/promises'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { req } from './dsh-native-runtime.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createAdapter } from '../dsh/host.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-subagent-spawn-in-process'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
async function until(predicate, label) {
  const deadline = Date.now() + 5000
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 10)) }
}

test('a real native delegation crosses the production adapter and HTTP into one overview group, retaining a running child after its parent ends', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-hierarchy-'))
  const streams = new Map()
  let ctx, server, adapter, parent, activation, shutdownFault
  const receipt = { scope: 'in-process native DSH Host, actual spawn provider, production adapter, authenticated HTTP core and public snapshot; no installed Host or actual model', observations: [], shutdowns: [], shutdownBoundaries: [], dataDir }
  const connectFixture = async options => {
    const connection = await connectCore(options)
    return { ...connection, async unregisterSource(...args) {
      // Close the real HTTP service only after the adapter has sealed its work.
      if (shutdownFault) {
        const boundary = { phase: shutdownFault, endpoint: 'source/unregister', serverCloseCompleted: false }
        receipt.shutdownBoundaries.push(boundary)
        await server.close(); boundary.serverCloseCompleted = true; shutdownFault = null
        try { return await connection.unregisterSource(...args) }
        catch (error) { boundary.rpcFailure = { name: error.name, code: error.code ?? null }; throw error }
      }
      return connection.unregisterSource(...args)
    } }
  }
  const closeAdapter = async phase => {
    if (process.env.SENTINEL_HIERARCHY_CLEANUP_FAULT === phase) shutdownFault = phase
    const result = await adapter.close()
    receipt.shutdowns.push({ phase, result })
    if (result?.status !== 'closed' || result.timedOut !== false || result.failures !== 0 || result.pendingRequests !== 0 || result.tickInFlight !== false) throw new Error(`adapter shutdown incomplete: ${phase}: ${JSON.stringify(result)}`)
    return result
  }
  t.after(async () => {
    const failures = []
    for (const release of [() => { for (const stream of streams.values()) { stream.done = true; stream.wake?.() } }, () => activation?.dispose(), () => parent?.dispose(), () => adapter && closeAdapter('final'), () => ctx?.fiber.dispose(), () => server?.close(), () => rm(dataDir, { recursive: true, force: true })]) {
      try { await release() } catch (error) { failures.push(error) }
    }
    if (process.env.SENTINEL_HIERARCHY_RECEIPT) await writeFile(process.env.SENTINEL_HIERARCHY_RECEIPT, JSON.stringify({ ...receipt, cleanup: { failures: failures.map(error => error.message), adapter: adapter?.diagnostics() }, completedAt: Date.now() }, null, 2))
    if (failures.length) throw new AggregateError(failures, 'native hierarchy cleanup failed')
  })
  server = await startServer({ dataDir })
  ctx = new Context()
  class Actor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      const stream = { done: false }; streams.set(options.model, stream)
      const abort = () => { stream.done = true; stream.wake?.() }
      options.signal.addEventListener('abort', abort, { once: true })
      try {
        yield { type: 'text-delta', index: 0, text: 'native actor active' }
        while (!stream.done) await new Promise(resolve => { stream.wake = resolve })
        yield { type: 'finish', reason: { kind: 'stop' } }
      } finally { options.signal.removeEventListener('abort', abort) }
    }
  }
  await ctx.plugin(LlmRuntime)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  ctx.llm.registerAdapter(['hierarchy-fixture'], new Actor())
  adapter = createAdapter(ctx, { dataDir, registrationId: 'native-hierarchy' }, { connect: connectFixture, flushMs: 5, heartbeatMs: 25 })
  await until(() => adapter.connected, 'adapter connected')
  parent = await ctx.agents.create({ sessionId: 'native-parent', agentOptions: { provider: 'hierarchy-fixture', model: 'parent' } })
  parent.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only. Do not modify files.' }] }))
  await until(() => streams.has('parent'), 'parent running')
  activation = await ctx.subagents.startActivation({ provider: 'spawn', label: 'same label', delivery: 'caller', signal: new AbortController().signal,
    request: { parent: parent.agent, prompt: [{ type: 'text', text: 'Inspect the delegated task only.' }], maxDepth: 4, agentOptions: { model: 'child' } } })
  await until(() => streams.has('child'), 'child running')
  let snapshot
  await until(async () => {
    snapshot = await adapter.snapshot()
    return snapshot.hierarchyContext?.groups?.some(group => group.rootSessionId === 'native-parent' && group.nodes.length === 2) && snapshot.tasks.find(task => task.sessionId === activation.childId)?.instructionContext?.binding?.origin === 'delegated'
  }, 'native delegation grouped')
  const group = snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'native-parent')
  receipt.coreInstanceId = server.instanceId; receipt.hostRunId = snapshot.adapter.hostRunId; receipt.sourceKey = snapshot.adapter.sourceKey
  receipt.observations.push({ phase: 'parent-and-child-running', group: structuredClone(group), tasks: snapshot.tasks.map(task => ({ id: task.id, key: task.key, lifecycleId: task.lifecycleId, hierarchy: task.hierarchy, analysis: task.analysis, coverage: task.coverage, currentRequest: task.currentRequest })) })
  assert.equal(group.runningCount, 2)
  const child = snapshot.tasks.find(task => task.sessionId === activation.childId)
  assert.equal(child.hierarchy.parentSessionId, 'native-parent')
  assert.equal(child.hierarchy.instructionOrigin, 'delegated')
  assert.equal(child.analysis.checks.filter(check => check.id !== 'long_wait').every(check => check.status === 'unknown'), true)
  let waitSnapshot
  await until(async () => { waitSnapshot = await adapter.snapshot(); return waitSnapshot.tasks.find(task => task.sessionId === activation.childId).analysis.checks.find(check => check.id === 'long_wait').status === 'clear' }, 'grounded recent native child activity clears the wait check without semantic models').catch(error => { receipt.observations.push({ phase: 'wait-check-unavailable', generatedAt: waitSnapshot.generatedAt, child: waitSnapshot.tasks.find(task => task.sessionId === activation.childId) }); throw error })
  assert.ok(child.events.filter(event => event.kind === 'input').every(event => event.authorizationEligible === false))
  const runId = child.hierarchy.runId
  assert.ok(runId)
  streams.get('parent').done = true; streams.get('parent').wake?.()
  await parent.agent.whenIdle()
  await until(async () => (await adapter.snapshot()).hierarchyContext.groups.find(group => group.rootSessionId === 'native-parent')?.runningCount === 1, 'ended parent does not erase active descendant')
  receipt.observations.push({ phase: 'parent-ended-child-running', group: (await adapter.snapshot()).hierarchyContext.groups.find(group => group.rootSessionId === 'native-parent') })
  const lifecycleId = child.lifecycleId
  await closeAdapter('replacement')
  if (process.env.SENTINEL_HIERARCHY_CLEANUP_FAULT === 'replacement') return
  adapter = createAdapter(ctx, { dataDir, registrationId: 'native-hierarchy' }, { connect: connectFixture, flushMs: 5, heartbeatMs: 25 })
  await until(() => adapter.connected, 'adapter replacement connected')
  await until(async () => (await adapter.snapshot()).tasks.find(task => task.sessionId === activation.childId)?.hierarchy?.runId === runId, 'reconnect retains actual native activation')
  assert.equal((await adapter.snapshot()).tasks.find(task => task.sessionId === activation.childId).lifecycleId, lifecycleId)
  receipt.observations.push({ phase: 'adapter-replaced-without-new-execution', tasks: (await adapter.snapshot()).tasks.map(task => ({ id: task.id, lifecycleId: task.lifecycleId, hierarchy: task.hierarchy })) })
  streams.get('child').done = true; streams.get('child').wake?.()
  await activation.result
  await until(async () => (await adapter.snapshot()).hierarchyContext.groups.find(group => group.rootSessionId === 'native-parent')?.runningCount === 0, 'all actors ended')
  snapshot = await adapter.snapshot()
  assert.equal(snapshot.tasks.find(task => task.sessionId === activation.childId).hierarchy.runId, runId)
  assert.ok(snapshot.tasks.find(task => task.sessionId === 'native-parent').events.some(event => event.data?.nativeType === 'subagent/end'))
  receipt.observations.push({ phase: 'all-ended', group: snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'native-parent') })
})

if (!process.env.SENTINEL_HIERARCHY_CLEANUP_FAULT) for (const phase of ['replacement', 'final']) test(`native fixture records incomplete ${phase} shutdown as a failure while releasing all its remaining resources`, async t => {
  const receiptDir = await mkdtemp(join(tmpdir(), 'sentinel-hierarchy-cleanup-')), receiptPath = join(receiptDir, 'receipt.json')
  t.after(() => rm(receiptDir, { recursive: true, force: true }))
  const result = await new Promise(resolve => {
    const env = { ...process.env, SENTINEL_HIERARCHY_CLEANUP_FAULT: phase, SENTINEL_HIERARCHY_RECEIPT: receiptPath }; delete env.NODE_TEST_CONTEXT
    const child = spawn(process.execPath, ['--test', '--test-name-pattern=^a real native delegation', fileURLToPath(import.meta.url)], { env, stdio: ['ignore', 'pipe', 'pipe'] })
    let output = '', stdout = '', stderr = '', forced = false
    child.stdout.on('data', chunk => { stdout += chunk; output += chunk }); child.stderr.on('data', chunk => { stderr += chunk; output += chunk })
    const timeout = setTimeout(() => { forced = true; child.kill('SIGTERM') }, 5000)
    child.once('close', (code, signal) => { clearTimeout(timeout); resolve({ code, signal, output, stdout, stderr, forced }) })
  })
  assert.equal(result.forced, false, `failure cleanup must finish naturally\n${result.output}`)
  const receipt = JSON.parse(await readFile(receiptPath, 'utf8'))
  if (process.env.SENTINEL_HIERARCHY_NEGATIVE_RECEIPTS) await writeFile(join(process.env.SENTINEL_HIERARCHY_NEGATIVE_RECEIPTS, `${phase}.json`), JSON.stringify({ result, receipt }, null, 2))
  assert.equal(result.signal, null)
  assert.equal(result.code, 1, `an incomplete resolved close result must fail the native fixture\n${result.output}`)
  assert.ok(receipt.shutdowns.some(entry => entry.phase === phase && entry.result.status === 'incomplete'), result.output)
  assert.ok(receipt.shutdownBoundaries.some(boundary => boundary.phase === phase && boundary.serverCloseCompleted === true && boundary.rpcFailure), result.output)
  assert.ok(receipt.cleanup.failures.some(failure => failure.includes('adapter shutdown incomplete')))
  assert.equal(receipt.cleanup.adapter.adapter.closed, true)
  assert.equal(receipt.cleanup.adapter.adapter.handlers, 0)
  assert.equal(receipt.cleanup.adapter.collector.hierarchyEvidence, 0)
  assert.ok(receipt.observations.length >= 2, 'retain native observations made before cleanup failed')
  await assert.rejects(access(receipt.dataDir))
})
