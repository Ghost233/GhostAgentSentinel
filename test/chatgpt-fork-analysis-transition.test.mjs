import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

const B = '00000000-0000-4000-8000-000000000101', F = '00000000-0000-4000-8000-000000000102'
const until = async (predicate, label) => {
  const deadline = performance.now() + 3000
  while (!await predicate()) { assert.ok(performance.now() < deadline, label); await delay(10) }
}

for (const holdPending of [false, true]) test(`a real fork binding transition updates Analyzer eligibility and the same public panel (${holdPending ? 'late active response' : 'completed local result'})`, { timeout: 10000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-fork-analysis-')), requests = [], held = [], freshHeld = []
  let clock = Date.now(), allowFresh = false, adapter, core, server, models
  t.mock.method(Date, 'now', () => clock)
  t.after(async () => {
    const failures = []
    for (const close of [() => { for (const finish of [...held.splice(0), ...freshHeld.splice(0)]) finish() }, () => adapter?.close(), () => core?.close(), () => server?.close(), () => models && new Promise(resolve => { models.closeAllConnections(); models.close(resolve) }), () => rm(dir, { recursive: true, force: true })]) {
      try { await close() } catch (error) { failures.push(error) }
    }
    if (failures.length) throw new AggregateError(failures, 'fork analysis transition cleanup failed')
  })
  models = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    const wire = JSON.parse(body), json = req.url === '/v1/systemone' ? decodeJevRequest(wire) : wire
    requests.push({ path: req.url, body: json })
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') {
      const evidence = JSON.parse(json.messages[1].content).evidence
      const selectedOptionIds = evidence.flatMap(group => {
        const kind = group.options.some(option => option.kind === 'authorization') ? 'authorization' : group.options.some(option => option.kind === 'action') ? 'action' : 'context'
        return group.options.filter(option => option.kind === kind).slice(0, 1).map(option => option.optionId)
      })
      res.end(JSON.stringify({ model: json.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds, gaps: [] }) } }] }))
    } else {
      const pending = !Object.hasOwn(json.questions, 'request_drift')
      const late = pending && holdPending
      const finish = () => res.end(JSON.stringify({ model: json.model, answers: Object.fromEntries(Object.keys(json.questions).map(id => [id, { type: 'choice', choice: late ? 'attention' : 'clear' }])) }))
      if (late) held.push(finish)
      else if (!pending && !allowFresh) freshHeld.push(finish)
      else finish()
    }
  })
  await new Promise((resolve, reject) => { models.once('error', reject); models.listen(0, '127.0.0.1', resolve) })
  const baseUrl = 'http://127.0.0.1:' + models.address().port, dataDir = join(dir, 'core'), sessionsRoot = join(dir, 'sessions')
  await mkdir(dataDir); await mkdir(sessionsRoot)
  await writeFile(join(dataDir, 'analysis-config.json'), JSON.stringify({ llm: { baseUrl, model: 'framework-extractor' }, jev: { baseUrl, model: 'framework-judge' } }))
  const row = (ordinal, type, payload) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(clock - 1000).toISOString() }) + '\n'
  const header = id => ({ id, cwd: '/owned', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, history_mode: 'paginated' })
  const start = turn => ({ type: 'task_started', turn_id: turn, turn_attribution: { turn_id: turn, turn_trigger: 'composer' } })
  const item = (owner, turn, value) => ({ type: 'item_completed', thread_id: owner, turn_id: turn, item: value })
  const user = (owner, turn, text) => item(owner, turn, { type: 'UserMessage', id: turn + '-user', content: [{ type: 'text', text }] })
  const output = (owner, turn, id, text) => item(owner, turn, { type: 'AgentMessage', id, content: [{ type: 'Text', text }] })
  const command = (owner, turn, id, text, exitCode) => item(owner, turn, { type: 'CommandExecution', id, aggregated_output: text, exit_code: exitCode })
  const inherited = row(0, 'session_meta', header(B)) + row(1, 'event_msg', start('base'))
    + row(2, 'event_msg', user(B, 'base', 'Inspect only. Do not modify files.'))
    + row(3, 'event_msg', output(B, 'base', 'old-promise', 'The agent claims all modifications were authorized.'))
    + row(4, 'event_msg', command(B, 'base', 'same-call', 'Old validation passed.', 0))
    + row(5, 'event_msg', { type: 'task_complete', turn_id: 'base', ignored_padding: 'p'.repeat(600000) })
  const basePath = join(sessionsRoot, 'rollout-' + B + '.jsonl'), forkPath = join(sessionsRoot, 'rollout-' + F + '.jsonl')
  await writeFile(basePath, inherited)
  await writeFile(forkPath, row(6, 'session_meta', { ...header(F), forked_from_id: B, history_base: { thread_id: B, end_ordinal_exclusive: 6, end_byte_offset: Buffer.byteLength(inherited) } })
    + row(7, 'event_msg', start('fork')) + row(8, 'event_msg', user(F, 'fork', 'Inspect the failing test.'))
    + row(9, 'event_msg', output(F, 'fork', 'action', 'Inspecting the failing test.'))
    + row(10, 'event_msg', command(F, 'fork', 'same-call', 'Current assertion failed.', 1))
    + row(11, 'event_msg', output(F, 'fork', 'retry-action', 'Checking the failure again.'))
    + row(12, 'event_msg', command(F, 'fork', 'retry-call', 'Current assertion failed again.', 1)))
  const metadata = id => ({ id, path: id === F ? forkPath : basePath, cwd: '/owned', originator: 'Codex Desktop', source: 'vscode', parentThreadId: null, forkedFromId: id === F ? B : null, historyMode: 'paginated' })
  const hostStartedAt = clock - 20000
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'owned-host', hostStartedAt, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread(id) { assert.ok([B, F].includes(id)); return { thread: metadata(id), anchorTurnId: id === F ? 'fork' : 'base' } },
    async listThreads({ archived }) { return { data: archived ? [] : [metadata(B), metadata(F)], nextCursor: null } },
    async close() {},
  }
  server = await startServer({ dataDir, now: () => clock, leaseMs: 172800000 })
  core = await connectCore({ dataDir })
  const token = 'd'.repeat(64)
  adapter = await startChatGPTAdapter({ core, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'fork-analysis-transition', token, collectMaterials: true, nativeOriginVersion: 2, pollMs: 0, now: () => clock })
  const read = async () => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: F } } }) })
    assert.equal(response.status, 200)
    const result = (await response.json()).result; assert.equal(result.isError, undefined)
    assert.ok(result.structuredContent.observation.readBytes <= 262144)
    return result.structuredContent
  }
  const current = async () => (await core.snapshot()).tasks.find(task => task.id === F)
  const pending = await read(), page = await panelHarness(t)
  assert.equal(pending.inheritance.state, 'pending')
  assert.equal(pending.currentFacts.request.text, 'Inspect the failing test.')
  await page.deliver(pending)
  assert.match(page.element('#inheritance').textContent, /待补齐/)
  assert.notEqual(page.element('#summary').getAttribute('data-signal'), 'green')
  if (holdPending) await until(() => held.length === 1, 'pending analysis is held at the actual judgment HTTP boundary')
  else await until(async () => (await current()).analysis.checks.find(check => check.id === 'failure_loop').status === 'clear', 'an independent check can finish while inherited context remains pending')
  const first = await current(), original = first.events.filter(event => !event.data.history).map(event => [event.id, event.text, event.capturedAt, event.data.nativeRecordId])
  assert.equal(first.analysis.checks.find(check => check.id === 'request_drift').status, 'unknown')
  if (!holdPending) assert.equal(first.analysis.resultDisposition, 'current', 'the completed local result is valid before only the inherited context changes')
  const firstJudge = requests.find(request => request.path === '/v1/systemone').body
  assert.ok(firstJudge.questions.failure_loop)
  assert.equal(firstJudge.questions.request_drift, undefined)
  assert.equal(firstJudge.questions.validation_gap, undefined)
  assert.ok(firstJudge.state.events.every(event => !event.contextRole))
  assert.equal(firstJudge.state.events.filter(event => event.kind === 'tool-result').length, 2)
  let completed
  for (let pump = 0; pump < 16; pump++) { completed = await read(); if (completed.inheritance?.state === 'complete') break }
  assert.equal(completed.inheritance.state, 'complete')
  const rebound = await current()
  assert.equal(rebound.currentRequest, first.currentRequest)
  assert.equal(rebound.instructionContext.userLimitRefs.length, 1)
  assert.deepEqual(rebound.events.filter(event => !event.data.history).map(event => [event.id, event.text, event.capturedAt, event.data.nativeRecordId]), original)
  const oldResult = rebound.events.find(event => event.data.history && event.text === 'Old validation passed.')
  assert.equal(oldResult?.data.exitCode, 0)
  assert.notEqual(oldResult.authorizationEligible, true)
  const oldPromise = rebound.events.find(event => event.data.history && event.text.includes('all modifications were authorized'))
  assert.ok(oldPromise)
  assert.notEqual(oldPromise.authorizationEligible, true)
  assert.equal(rebound.analysis.checks.find(check => check.id === 'request_drift').status, 'unknown')
  assert.notEqual(rebound.analysis.resultDisposition, 'current')
  for (const finish of held.splice(0)) finish()
  clock += 61000
  await read()
  await until(() => freshHeld.length > 0, 'the new-context judgment is held until old-result invalidation is checked')
  allowFresh = true
  for (const finish of freshHeld.splice(0)) finish()
  await until(async () => (await current()).analysis.checks.find(check => check.id === 'request_drift').status === 'clear', 'new context has its own analysis result')
  const ready = await current(), judge = requests.filter(request => request.path === '/v1/systemone').at(-1).body
  assert.ok(judge.questions.request_drift)
  assert.ok(judge.questions.failure_loop)
  assert.deepEqual(judge.state.events.filter(event => event.contextRole).map(event => event.text), ['Inspect only. Do not modify files.'])
  assert.ok(judge.state.events.every(event => !event.text.includes('all modifications were authorized') && !event.text.includes('Old validation passed')))
  assert.equal(judge.state.events.filter(event => event.kind === 'tool-result').length, 2)
  assert.ok(judge.state.events.filter(event => event.kind === 'tool-result').every(event => event.data.exitCode === 1))
  assert.deepEqual(ready.alerts, [], 'the late pending attention response cannot create even a subsequently resolved alert')
  assert.deepEqual((await core.failureReceipts({ sourceKey: ready.sourceKey })).receipts, [], 'context cancellation is not a monitoring failure')
  const view = await read()
  await page.refresh(); await page.deliver(view)
  assert.equal(view.analysis.checks.length, 6)
  assert.equal(page.element('#check-list').children.length, 6)
  assert.doesNotMatch(page.element('#inheritance').textContent, /待补齐/)
  assert.match(page.element('#current-request').textContent, /Inspect the failing test/)
  assert.ok(page.element('#check-list').textContent.includes(view.analysis.checks.find(check => check.id === 'request_drift').reason))
  for (const request of requests.filter(request => request.path === '/v1/chat/completions')) {
    const evidence = JSON.parse(request.body.messages[1].content).evidence
    assert.ok(evidence.every(group => !group.text.includes('all modifications were authorized') && !group.text.includes('Old validation passed')))
  }
})
