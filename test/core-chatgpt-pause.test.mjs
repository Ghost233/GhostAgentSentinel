import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createCore } from '../src/core/index.mjs'

const at = 1_800_000_000_000
const chat = (id, lifecycleId = 'one') => ({ id, lifecycleId, sessionId: 'shared-tree', title: id, status: 'running' })
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-conversation-pause-'))
  let clock = at, core = await createCore({ dataDir, now: () => clock, leaseMs: 1e9 })
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }) })
  return { get core() { return core }, advance: ms => { clock += ms }, async reopen() { await core.close(); core = await createCore({ dataDir, now: () => clock, leaseMs: 1e9 }) } }
}

test('conversation pause follows registration and native chat identity across turns and core/host restart without pausing its fork or another source', async t => {
  const f = await fixture(t)
  const scope = { registrationId: 'chatgpt:install', taskId: 'main-a' }
  const source = await f.core.registerSource({ registrationId: scope.registrationId, hostRunId: 'host-one', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await f.core.ingest(source, { tasks: [chat('main-a'), chat('fork-a')] })
  const original = await f.core.conversationMonitoring(scope)
  assert.equal(original.paused, false)
  const paused = await f.core.setConversationMonitoring({ ...scope, paused: true, revision: original.revision })
  assert.equal(paused.paused, true)
  assert.notEqual(paused.revision, original.revision)
  assert.deepEqual(await f.core.setConversationMonitoring({ ...scope, paused: true, revision: original.revision }), paused, 'repeating the same desired state is idempotent')
  await f.core.ingest(source, { tasks: [chat('main-a', 'two')] })
  let snapshot = await f.core.snapshot()
  assert.equal(snapshot.monitoring.paused, false)
  assert.ok(snapshot.tasks.filter(task => task.id === 'main-a').every(task => task.conversationMonitoring.paused && task.analysis.state === 'paused'))
  assert.equal(snapshot.tasks.find(task => task.id === 'fork-a').conversationMonitoring.paused, false)
  await f.reopen()
  assert.deepEqual(await f.core.conversationMonitoring(scope), paused)
  const reopened = await f.core.registerSource({ registrationId: scope.registrationId, hostRunId: 'host-two', name: 'ChatGPT reopened', policy: 'chatgpt-main/1' })
  await f.core.ingest(reopened, { tasks: [chat('main-a', 'three')] })
  const other = await f.core.registerSource({ registrationId: 'chatgpt:other-install', hostRunId: 'host-two', name: 'Other', policy: 'chatgpt-main/1' })
  await f.core.ingest(other, { tasks: [chat('main-a')] })
  snapshot = await f.core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.sourceKey === reopened && task.id === 'main-a').conversationMonitoring.paused, true)
  assert.equal(snapshot.tasks.find(task => task.sourceKey === other).conversationMonitoring.paused, false)
  const resumed = await f.core.setConversationMonitoring({ ...scope, paused: false, revision: paused.revision })
  assert.equal(resumed.paused, false)
  await assert.rejects(f.core.setConversationMonitoring({ ...scope, paused: true, revision: paused.revision }), error => error.code === 'conversation_monitoring_conflict')
  assert.deepEqual(await f.core.conversationMonitoring(scope), resumed)
})

import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { createAnalyzer } from '../src/analysis/index.mjs'
const DAY = 86_400_000
async function until(check, label) {
  const deadline = performance.now() + 5000
  while (!await check()) { assert.ok(performance.now() < deadline, label); await delay(3) }
}
async function analysisFixture(t, { hold = true, choice = 'clear' } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-conversation-pause-io-'))
  let clock = Date.now(), holding = hold, decision = choice, core
  const requests = [], held = [], chats = new Map()
  const provider = createServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk
    const body = JSON.parse(raw); requests.push({ path: req.url, body })
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') {
      const job = { body, done: false, closedEarly: false, reply() { if (job.done) return; job.done = true; if (!res.destroyed) res.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] })) } }
      res.once('close', () => { if (!job.done) job.closedEarly = true })
      held.push(job); if (!holding) job.reply()
    } else res.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: decision }])) }))
  })
  await new Promise(resolve => provider.listen(0, '127.0.0.1', resolve))
  const baseUrl = 'http://127.0.0.1:' + provider.address().port
  const analyzer = createAnalyzer({ llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' }, now: () => clock })
  core = await createCore({ dataDir, analyzer, now: () => clock, leaseMs: DAY * 3 })
  const source = await core.registerSource({ registrationId: 'chatgpt:pause-io', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  const legacy = await core.registerSource({ registrationId: 'dsh:pause-io', hostRunId: 'host', name: 'DSH' })
  let revision = 0
  const observe = async id => {
    const ref = eventId => ({ taskId: id, lifecycleId: id + '-one', eventId })
    const event = (suffix, kind, text, data = {}) => ({ id: id + '-' + suffix, taskId: id, taskLifecycleId: id + '-one', kind, occurredAt: clock, capturedAt: clock, text, data })
    const events = [event('start', 'status', 'Started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: id + '-one', turnTrigger: 'composer', status: 'running' }), event('input', 'input', 'Read ' + id + '.log only', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: id + '-one', messageId: id + '-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }), event('output', 'output', 'Reading ' + id + '.log')]
    chats.set(id, { activation: { taskId: id, lifecycleId: id + '-one', sessionId: id, runId: id + '-one', state: 'running', stateRefs: [ref(id + '-start')] }, binding: { taskId: id, lifecycleId: id + '-one', inputRef: ref(id + '-input'), origin: 'user-entry', phase: 'consumed', originRefs: [ref(id + '-start'), ref(id + '-input')], ancestorLimitRefs: [] } })
    await core.ingest(source, { tasks: [{ ...chat(id, id + '-one'), sessionId: id }], events, hierarchyContext: { schemaVersion: 1, revision: ++revision, sessions: [...chats.keys()].map(sessionId => ({ sessionId })), activations: [...chats.values()].map(item => item.activation), relations: [], instructionBindings: [...chats.values()].map(item => item.binding), coverage: { complete: true, branchGaps: [] } } })
  }
  const observeDSH = async () => core.ingest(legacy, { tasks: [chat('dsh')], events: ['input', 'output'].map(kind => ({ id: 'dsh-' + kind, taskId: 'dsh', taskLifecycleId: 'one', kind, occurredAt: clock, capturedAt: clock, text: kind === 'input' ? 'Read DSH log' : 'Reading DSH log' })) })
  const scope = id => ({ registrationId: 'chatgpt:pause-io', taskId: id })
  const control = async (id, paused) => core.setConversationMonitoring({ ...scope(id), paused, revision: (await core.conversationMonitoring(scope(id))).revision })
  t.after(async () => {
    await core.pause(); holding = false; for (const job of held) job.reply()
    await until(async () => (await core.diagnostics()).runtime.cancelledUnsettledRequests === 0, 'controlled submitted responses settle during cleanup')
    await core.close(); provider.closeAllConnections(); await new Promise(resolve => provider.close(resolve)); await rm(dataDir, { recursive: true, force: true })
  })
  return { core, source, legacy, requests, held, scope, control, observe, observeDSH, now: () => clock, advance: ms => { clock += ms }, autoReply: () => { holding = false }, decision: value => { decision = value }, async actor(id) { return (await core.snapshot()).tasks.find(task => task.id === id) } }
}

test('pausing a queued conversation removes its scheduling eligibility while B and DSH continue through real inference IO', async t => {
  const f = await analysisFixture(t)
  await f.observeDSH(); await until(() => f.held.length === 1, 'DSH extraction entered the controlled provider')
  await f.observe('A'); await f.observe('B')
  assert.equal((await f.core.snapshot()).monitoring.queueLength, 2)
  await f.control('A', true)
  assert.equal((await f.core.snapshot()).monitoring.queueLength, 1)
  assert.equal((await f.actor('A')).analysis.state, 'paused')
  f.autoReply(); f.held[0].reply()
  await until(async () => (await f.actor('B')).analysis.completedAt !== null, 'B completes after DSH')
  assert.equal(f.held.length, 2, 'paused A never enters the actual provider')
  assert.equal((await f.actor('dsh')).analysis.state, 'cooldown')
  assert.equal((await f.actor('A')).analysis.completedAt, null)
})

test('pausing in-flight A seals late inference, keeps physical admission occupied, continues capture and resumes from current material after B and DSH', async t => {
  const f = await analysisFixture(t)
  await f.observe('A'); await until(() => f.held.length === 1, 'A extraction is physically submitted')
  await f.observe('B'); await f.observeDSH()
  await f.control('A', true)
  let snapshot = await f.core.snapshot(), a = snapshot.tasks.find(task => task.id === 'A')
  assert.equal(a.analysis.state, 'paused')
  assert.equal(a.gs.label, '本聊天分析已暂停')
  assert.equal(a.gs.light, 'gray')
  assert.equal(a.analysis.cancelledComputeMayContinue, true)
  assert.deepEqual(snapshot.monitoring.admission, { limit: 1, unsettledCalls: 1, blocked: true })
  assert.equal(snapshot.monitoring.pendingCancelledRequests, 1)
  assert.equal(f.held[0].done, false)
  assert.equal(f.held[0].closedEarly, false)
  f.advance(1)
  await f.core.ingest(f.source, { events: [{ id: 'A-paused-output', taskId: 'A', taskLifecycleId: 'A-one', kind: 'output', occurredAt: f.now(), capturedAt: f.now(), text: 'Fresh observed A activity during pause' }] })
  assert.ok((await f.actor('A')).events.some(event => event.id === 'A-paused-output'))
  assert.equal(f.held.length, 1)
  f.autoReply(); f.held[0].reply()
  await until(async () => (await f.actor('B')).analysis.completedAt !== null && (await f.actor('dsh')).analysis.completedAt !== null, 'independent B and DSH complete after the sealed physical call ends')
  a = await f.actor('A')
  assert.equal(a.analysis.completedAt, null, 'late A result never becomes a completed paused result')
  assert.equal(a.analysis.summary, null)
  assert.equal(a.analysis.state, 'paused')
  assert.equal(f.held.length, 3)
  await f.control('A', false)
  await until(async () => (await f.actor('A')).analysis.completedAt !== null, 'manual resume schedules a new current round')
  assert.equal(f.held.length, 4)
  const latestEvidence = JSON.parse(f.held.at(-1).body.messages[1].content).evidence
  assert.ok(latestEvidence.some(event => event.text === 'Fresh observed A activity during pause'))
  assert.equal((await f.core.snapshot()).monitoring.pendingCancelledRequests, 0)
})

test('conversation pause cancels pending and claimed notifications while preserving B and DSH delivery and sealing late acknowledgements', async t => {
  for (const phase of ['pending', 'claimed']) await t.test(phase, async t => {
    const f = await analysisFixture(t, { hold: false, choice: 'attention' })
    await f.observe('A'); await f.observe('B'); await f.observeDSH()
    await until(async () => (await f.actor('A')).analysis.completedAt !== null && (await f.actor('B')).analysis.completedAt !== null && (await f.actor('dsh')).analysis.completedAt !== null, 'all independent alert rounds finish')
    f.advance(3000)
    const aKey = (await f.actor('A')).key
    let oldA
    if (phase === 'claimed') oldA = (await f.core.claimNotifications(f.source)).find(notification => notification.members.some(member => member.taskKey === aKey))
    await f.control('A', true)
    if (oldA) assert.equal((await f.core.ackNotification(oldA.id, { status: 'submitted' })).status, 'cancelled')
    const current = await f.core.claimNotifications(f.source)
    assert.ok(current.every(notification => notification.members.every(member => member.taskKey !== aKey)))
    if (phase === 'pending') assert.equal(current.length, 1, 'B remains eligible when only A is paused')
    assert.ok((await f.core.claimNotifications(f.legacy)).length > 0, 'DSH remains independently eligible')
    f.advance(600_001)
    const repeated = await f.core.claimNotifications(f.source)
    assert.ok(repeated.every(notification => notification.members.every(member => member.taskKey !== aKey)))
    assert.equal((await f.actor('A')).analysis.state, 'paused')
    assert.equal((await f.core.snapshot()).monitoring.paused, false)
    if (oldA) {
      await f.control('A', false)
      await until(async () => (await f.actor('A')).analysis.completedAt === f.now(), 'manual resume obtains a fresh current round')
      assert.equal((await f.core.ackNotification(oldA.id, { status: 'submitted' })).status, 'cancelled', 'a pre-pause receipt cannot reactivate after manual resume')
    }
  })
})

test('the persistent conversation index is bounded and refuses a new scope without expiring or unpausing existing choices', async t => {
  const f = await fixture(t), configuration = (await f.core.diagnostics()).configuration
  assert.ok(Number.isInteger(configuration.conversationMonitoringLimit) && configuration.conversationMonitoringLimit > 0)
  const limit = configuration.conversationMonitoringLimit, registrationId = 'chatgpt:bounded-pause'
  const source = await f.core.registerSource({ registrationId, hostRunId: 'host', name: 'Bounded', policy: 'chatgpt-main/1' })
  await f.core.ingest(source, { tasks: Array.from({ length: limit + 1 }, (_, index) => chat('chat-' + index)) })
  for (let index = 0; index < limit; index++) {
    const scope = { registrationId, taskId: 'chat-' + index }, state = await f.core.conversationMonitoring(scope)
    await f.core.setConversationMonitoring({ ...scope, paused: true, revision: state.revision })
  }
  const overflow = { registrationId, taskId: 'chat-' + limit }, state = await f.core.conversationMonitoring(overflow)
  await assert.rejects(f.core.setConversationMonitoring({ ...overflow, paused: true, revision: state.revision }), error => error.code === 'conversation_monitoring_capacity')
  assert.equal((await f.core.conversationMonitoring(overflow)).paused, false)
  const first = { registrationId, taskId: 'chat-0' }, paused = await f.core.conversationMonitoring(first)
  f.advance(DAY + 1)
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'chat-0').conversationMonitoring.paused, true)
  assert.deepEqual(await f.core.conversationMonitoring(first), paused)
  const full = (await f.core.diagnostics()).configuration
  assert.equal(full.conversationMonitoringScopes, limit)
  assert.equal(full.pausedConversations, limit)
  const resumed = await f.core.setConversationMonitoring({ ...first, paused: false, revision: paused.revision })
  assert.equal(resumed.paused, false, 'capacity never blocks manual resume of an existing choice')
  await assert.rejects(f.core.setConversationMonitoring({ ...first, paused: true, revision: paused.revision }), error => error.code === 'conversation_monitoring_conflict')
  assert.equal((await f.core.diagnostics()).configuration.conversationMonitoringScopes, limit)
})

test('a previously verified paused scope remains readable and manually resumable after all observation records expire and the core reopens', async t => {
  const f = await fixture(t), scope = { registrationId: 'chatgpt:expired-observation', taskId: 'known-chat' }
  const source = await f.core.registerSource({ registrationId: scope.registrationId, hostRunId: 'host', name: 'Known', policy: 'chatgpt-main/1' })
  await f.core.ingest(source, { tasks: [chat(scope.taskId)] })
  const original = await f.core.conversationMonitoring(scope), paused = await f.core.setConversationMonitoring({ ...scope, paused: true, revision: original.revision })
  f.advance(DAY + 1); await f.reopen()
  assert.deepEqual((await f.core.snapshot()).tasks, [], 'expired observation bodies are not kept alive by a pause preference')
  assert.deepEqual(await f.core.conversationMonitoring(scope), paused)
  const resumed = await f.core.setConversationMonitoring({ ...scope, paused: false, revision: paused.revision })
  assert.equal(resumed.paused, false)
  await assert.rejects(f.core.conversationMonitoring({ ...scope, taskId: 'never-observed' }), /Unknown ChatGPT conversation scope/)
  assert.equal((await f.core.diagnostics()).configuration.conversationMonitoringScopes, 1)
  assert.equal((await f.core.diagnostics()).configuration.pausedConversations, 0)
})
