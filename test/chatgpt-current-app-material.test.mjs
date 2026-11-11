import { startsSummaryRound, withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createServer } from 'node:http'
import { zstdCompressSync } from 'node:zlib'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { candidateHostProfile, supportedProfile } from '../chatgpt/storage-compatibility.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { MaterialProjection, materialRecord } from '../chatgpt/material-projection.mjs'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

const at = 1_800_000_000_000, owner = '00000000-0000-4000-8000-000000000163', parent = '00000000-0000-4000-8000-000000000162'
const app20770 = { appVersion: '26.1013.40934', appBuild: '20770', cliVersion: '0.163.0-alpha.10.2' }
const app21828 = { appVersion: '26.1021.31345', appBuild: '21828', cliVersion: '0.164.0-alpha.11.1' }
const app25471 = { appVersion: '26.1103.32239', appBuild: '25471', cliVersion: '0.167.0-alpha.3.2' }
const row = (ordinal, type, payload, time = at) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(time).toISOString() }) + '\n'
const header = (id, producer = candidateHostProfile.cliVersion) => ({ id, cwd: '/synthetic-project', originator: 'Codex Desktop', source: 'vscode', cli_version: producer, history_mode: 'paginated' })
const start = (id, trigger = 'composer') => ({ type: 'task_started', turn_id: id, turn_attribution: { turn_id: id, turn_trigger: trigger } })
const user = (thread, turn, text = 'Inspect only the synthetic log') => ({ type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'UserMessage', id: turn + '-user', content: [{ type: 'text', text }] } })
const agent = (thread, turn, text = 'The synthetic log has been inspected') => ({ type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'AgentMessage', id: turn + '-agent', content: [{ type: 'Text', text }] } })

async function fixture(t, { profile = candidateHostProfile, producer = candidateHostProfile.cliVersion, inheritedProducer = producer, unknownHistory = false, suffix = '', records, fork = false, compressed = false, pollMs = 0, backgroundChats = 0, cooldownMs = 0, onQualificationLost } = {}) {
  let clock = at
  t.mock.method(Date, 'now', () => clock)
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-current-app-')), sessionsRoot = join(dir, 'sessions'), archived = join(dir, 'archived_sessions'), dataDir = join(dir, 'core'), path = join(sessionsRoot, 'rollout-' + owner + '.jsonl' + (compressed ? '.zst' : '')), parentPath = join(archived, 'rollout-' + parent + '.jsonl')
  await Promise.all([fs.mkdir(sessionsRoot), fs.mkdir(archived), fs.mkdir(dataDir)])
  let server, core, adapter
  const wire = createServer(async (req, res) => {
    if (req.method === 'GET') return res.writeHead(404, { 'content-type': 'application/json' }).end('{}');
    let body = ''; for await (const chunk of req) body += chunk
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') { const wire = JSON.parse(body); if (startsSummaryRound(wire)) state.summaryCalls++; await state.modelGate; if (state.modelFailure) return res.writeHead(503).end('{"error":"controlled model unavailable"}'); res.end(JSON.stringify({ model: 'fixed-extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(wire, { selectedOptionIds: [], gaps: [] })) } }] })) }
    else { state.jevCalls++; const request = decodeJevRequest(JSON.parse(body)); res.end(JSON.stringify({ model: 'fixed-judge', answers: Object.fromEntries(Object.keys(request.questions).map(id => [id, { type: 'choice', choice: id === 'execution_blocking' ? state.blockingChoice ?? 'unknown' : 'clear' }])) })) }
  })
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); wire.closeAllConnections(); await new Promise(resolve => wire.close(resolve)); await fs.rm(dir, { recursive: true, force: true }) })
  await new Promise(resolve => wire.listen(0, '127.0.0.1', resolve))
  const baseUrl = 'http://127.0.0.1:' + wire.address().port
  const inherited = row(0, 'session_meta', header(parent, inheritedProducer), at - 100000) + row(1, 'event_msg', start('inherited'), at - 100000) + row(2, 'event_msg', user(parent, 'inherited', 'Keep the synthetic inherited limit'), at - 100000)
    + (unknownHistory === 'record' ? row(3, 'future_record', { secret: 'unselected-ancestor-body' }, at - 100000) : unknownHistory === 'event' ? row(3, 'event_msg', { type: 'future_event', secret: 'unselected-ancestor-body' }, at - 100000) :row(3, 'event_msg', { type: 'item_completed', thread_id: parent, turn_id: 'inherited', item: unknownHistory ? { type: 'FutureHistoryItem', secret: 'unselected-ancestor-body' } : { type: 'CommandExecution', id: 'old-validation', aggregated_output: 'Historical validation only', exit_code: 0 } }, at - 100000)) + row(4, 'event_msg', { type: 'task_complete', turn_id: 'inherited' }, at - 100000)
  await fs.writeFile(parentPath, inherited)
  const base = fork ? 5 : 0
  const bytes = row(base, 'session_meta', { ...header(owner, producer), ...(fork ? { forked_from_id: parent, forked_from_ordinal_exclusive: 5, history_base: { thread_id: parent, end_ordinal_exclusive: 5, end_byte_offset: Buffer.byteLength(inherited) } } : {}) })
    + (records ?? row(base + 1, 'event_msg', start('turn')) + row(base + 2, 'event_msg', user(owner, 'turn')) + row(base + 3, 'event_msg', agent(owner, 'turn'))) + suffix
  await fs.writeFile(path, compressed ? zstdCompressSync(Buffer.from(bytes)) : bytes)
  const extraPaths = new Map()
  for (let index = 0; index < backgroundChats; index++) {
    const id = 'background-' + index, extraPath = join(sessionsRoot, 'rollout-' + id + '.jsonl')
    await fs.writeFile(extraPath, row(0, 'session_meta', header(id, producer)) + row(1, 'event_msg', start('turn-' + id)) + row(2, 'event_msg', user(id, 'turn-' + id)) + row(3, 'event_msg', agent(id, 'turn-' + id)))
    extraPaths.set(id, extraPath)
  }
  server = await startServer({ dataDir, now: () => clock, cooldownMs }); core = await connectCore({ dataDir })
  await configureInstallationModels(core, 'chatgpt:current-app-material', { llm: { baseUrl, model: 'fixed-extractor' }, jev: { baseUrl, model: 'fixed-judge' } })
  const state = { profile, hostRunId: 'current-app-fixture', hostStartedAt: at - 10000, anchorTurnId: 'turn', enabled: true, available: true, threadReads: 0, threadReadIds: [], currentRecordReads: 0, summaryCalls: 0, jevCalls: 0, coreCalls: {}, snapshotSites: {} }
  const native = { async sampleHost() { return { available: state.available, hostRunId: state.hostRunId, hostStartedAt: state.hostStartedAt, profile: state.profile } }, async readThread(id) { state.threadReads++; state.threadReadIds.push(id); return { thread: { id, path: id === owner ? path : extraPaths.get(id) ?? parentPath, cwd: '/synthetic-project', parentThreadId: null, forkedFromId: id === owner && fork ? parent : null, source: 'vscode', originator: 'Codex Desktop' }, anchorTurnId: id === owner ? state.anchorTurnId : extraPaths.has(id) ? 'turn-' + id : null } }, async readEnablement() { return { user: state.enabled ? 'enabled' : 'disabled', effective: state.enabled ? 'enabled' : 'disabled' } }, async close() {} }
  if (pollMs || backgroundChats) native.listThreads = async ({ archived }) => ({ data: archived ? [] : await Promise.all([owner, ...extraPaths.keys()].map(async id => (await native.readThread(id)).thread)), nextCursor: null })
  const adapterCore = new Proxy(core, { get(target, name) { const value = target[name]; return typeof value !== 'function' ? value : (...args) => { state.coreCalls[name] = (state.coreCalls[name] ?? 0) + 1; if (name === 'snapshot') { const site = new Error().stack.split('\n').find(line => line.includes('/chatgpt/adapter.mjs'))?.trim() ?? 'other'; state.snapshotSites[site] = (state.snapshotSites[site] ?? 0) + 1 }; return value(...args) } } })
  const open = fs.open, watched = new Set([await fs.realpath(path), await fs.realpath(parentPath)])
  t.mock.method(fs, 'open', async (...args) => { const handle = await open(...args); if (watched.has(String(args[0]))) { const read = handle.read; handle.read = async function (...values) { const got = await read.apply(this, values); state.currentRecordReads += got.bytesRead; return got } }; return handle })
  const reopen = async () => { await adapter?.close(); adapter = await startChatGPTAdapter({ core: adapterCore, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'current-app-material', token: 'c'.repeat(64), pollMs, collectMaterials: true, nativeOriginVersion: 2, now: () => clock, ...(onQualificationLost ? { onQualificationLost } : {}) }) }
  await reopen()
  const read = async () => { const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: owner } } }) }); assert.equal(response.status, 200); const value = await response.json(); assert.equal(value.result.isError, undefined); return value.result.structuredContent }
  const caughtUp = async () => { let view; for (let i = 0; i < 30; i++) { view = await read(); if (view.observation.coverage === 'caught-up' || view.observation.coverage === 'retained-history' && ['completed', 'aborted'].includes(view.observation.phase) || view.observation.gap?.startsWith('unsupported-') || ['cursor-invalid', 'record-malformed-or-over-limit'].includes(view.observation.gap)) break }; return view }
  return { core, state, native, path, dir, read, caughtUp, reopen, advance() { clock += 1000 }, get adapter() { return adapter } }
}

test('installation diagnostics report bounded backfill positions without pumping records or inferring an active execution', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'large', output: 'x'.repeat(800000) }) })
  assert.equal((await f.read()).observation.coverage, 'catching-up')
  const bytes = f.state.currentRecordReads, threadReads = f.state.threadReads
  const response = await fetch(f.adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_installation_diagnostics', arguments: {} } }) })
  const result = (await response.json()).result.structuredContent
  assert.equal(result.collection.backfill.pendingChats, 1)
  assert.equal(result.collection.backfill.knownChats, 1)
  assert.equal(result.collection.backfill.totalBytes, (await fs.stat(f.path)).size)
  assert.ok(result.collection.backfill.readBytes >= 0 && result.collection.backfill.readBytes < result.collection.backfill.totalBytes)
  assert.equal(result.analysis.counts.total, 0)
  assert.equal(f.state.currentRecordReads, bytes, 'a diagnostic read does not advance collection')
  assert.equal(f.state.threadReads, threadReads)
  assert.ok(!JSON.stringify(result).includes(f.path))
  await f.caughtUp()
  const later = await fetch(f.adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'sentinel_installation_diagnostics', arguments: {} } }) })
  assert.equal((await later.json()).result.structuredContent.collection.backfill, undefined)
})

test('a JEV blocking decision reaches the summary with expandable original evidence and becomes historical when new material arrives', async t => {
  const f = await fixture(t)
  f.state.blockingChoice = 'unblocked_1'
  let view = await f.caughtUp()
  for (let i = 0; i < 100 && !view.recentModelActivity?.summary?.blocking; i++) {
    await new Promise(resolve => setTimeout(resolve, 5))
    view = await f.read()
  }
  const decision = view.recentModelActivity?.summary?.blocking
  assert.equal(decision?.status, 'unblocked')
  assert.equal(decision.historical, false)
  assert.equal(decision.reason, '会话回复记录了可继续推进的进展')
  assert.equal(decision.evidence[0].text, 'The synthetic log has been inspected')
  assert.equal(f.state.jevCalls, 1)
  const page = await panelHarness(t)
  await page.deliver(view)
  assert.match(page.element('#recent-blocking-status').textContent, /^未阻塞 · /)
  assert.doesNotMatch(page.element('#recent-blocking-status').textContent, /The synthetic log/)
  assert.equal(page.element('#recent-blocking-details').hidden, false)
  assert.equal(page.element('#recent-blocking-details').open, false)
  assert.match(page.element('#recent-blocking-evidence').textContent, /The synthetic log has been inspected/)

  let release
  f.state.modelGate = new Promise(resolve => { release = resolve })
  try {
    f.advance()
    const later = agent(owner, 'turn', 'A later action now needs review'); later.item.id = 'later-blocking-review'
    await fs.appendFile(f.path, row(4, 'event_msg', later))
    view = await f.caughtUp()
    assert.equal(view.recentModelActivity.summary.blocking.historical, true)
    await page.refresh(); await page.deliver(view)
    assert.match(page.element('#recent-blocking-status').textContent, /^上次判断 · 未阻塞/)
    assert.equal(f.state.jevCalls, 1)
  } finally { release() }
})

test('querying and coalescing continue while LLM work is held and the next analysis ignores the query interval', async t => {
  const f = await fixture(t)
  await f.core.configureInstallationScheduling('chatgpt:current-app-material', { concurrency: 1, queryConcurrency: 2, queryIntervalMs: 5000 })
  let release; f.state.modelGate = new Promise(resolve => { release = resolve })
  try {
    await f.caughtUp()
    for (let i = 0; i < 100 && f.state.summaryCalls !== 1; i++) await new Promise(resolve => setTimeout(resolve, 5))
    assert.equal(f.state.summaryCalls, 1)
    const reads = f.state.threadReads; f.advance()
    const later = agent(owner, 'turn', 'A second diagnostic fact was found'); later.item.id = 'second-agent'
    await fs.appendFile(f.path, row(4, 'event_msg', later))
    for (let i = 0; i < 4; i++) await f.caughtUp()
    assert.ok(f.state.threadReads > reads)
    assert.equal(f.state.summaryCalls, 1, 'polling never duplicates an already active conversation')
    release()
    for (let i = 0; i < 150; i++) {
      const task = (await f.core.snapshot()).tasks.find(task => task.id === owner)
      if (f.state.summaryCalls === 2 && task.analysis.throughRevision === task.revision) break
      await new Promise(resolve => setTimeout(resolve, 5))
    }
    assert.equal(f.state.summaryCalls, 2, 'the coalesced version starts without waiting five seconds or another query')
  } finally { release() }
})

test('a verified active execution gets one final summary after ending and never re-enters the analysis queue', async t => {
  const f = await fixture(t, { cooldownMs: 60000 })
  await f.caughtUp()
  let task
  for (let i = 0; i < 100; i++) { task = (await f.core.snapshot()).tasks.find(task => task.id === owner); if (task?.analysis.summary?.generated || task?.analysis.summary?.partial) break; await new Promise(resolve => setTimeout(resolve, 5)) }
  assert.equal(f.state.summaryCalls, 1)
  f.advance()
  await fs.appendFile(f.path, row(4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
  await f.caughtUp()
  for (let i = 0; i < 150; i++) { task = (await f.core.snapshot()).tasks.find(task => task.id === owner); if (task?.analysis.finalSummaryCompletedAt) break; await new Promise(resolve => setTimeout(resolve, 5)) }
  assert.equal(f.state.summaryCalls, 2, 'an active-to-ended transition schedules the final summary without waiting for the query interval or cooldown')
  assert.ok(task.analysis.finalSummaryCompletedAt)
  assert.equal(task.status, 'idle')
  assert.equal(task.gs.light, null)
  assert.equal((await f.core.snapshot()).overview.runningTasks, 0)
  const view = await f.caughtUp(), page = await panelHarness(t)
  assert.equal(view.recentModelActivity.summary.final, true)
  await page.deliver(view)
  assert.equal(page.element('#recent-summary-state').textContent, '已收尾')
  for (let i = 0; i < 120; i++) { f.advance(); await f.core.heartbeat(task.sourceKey) }
  for (let i = 0; i < 3; i++) await f.caughtUp()
  assert.equal(f.state.summaryCalls, 2)
})

test('an idle metadata transition keeps draining until its native end record can produce the final summary', async t => {
  const f = await fixture(t, { cooldownMs: 60000 })
  await f.caughtUp()
  for (let i = 0; i < 100 && !(await f.core.snapshot()).tasks[0]?.analysis.completedAt; i++) await new Promise(resolve => setTimeout(resolve, 5))
  const read = f.native.readThread.bind(f.native)
  f.native.readThread = async id => ({ ...await read(id), activity: 'idle' })
  const waiting = await f.caughtUp(), page = await panelHarness(t)
  await page.deliver(waiting)
  assert.match(page.element('#availability').textContent, /已静止.*收尾摘要/)
  assert.ok(f.adapter.diagnostics().openReaders > 0, 'idle metadata cannot discard an end record that has not been read yet')
  assert.equal((await f.core.snapshot()).overview.runningTasks, 0)
  f.advance(); await fs.appendFile(f.path, row(4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
  await f.caughtUp()
  for (let i = 0; i < 150 && !(await f.core.snapshot()).tasks[0]?.analysis.finalSummaryCompletedAt; i++) await new Promise(resolve => setTimeout(resolve, 5))
  assert.ok((await f.core.snapshot()).tasks[0].analysis.finalSummaryCompletedAt)
  assert.equal(f.state.summaryCalls, 2)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('native additional_tools and normal excerpts reach per-check judgment and the real MCP panel for plain and compressed main/fork records', async t => {
  for (const fork of [false, true]) for (const compressed of [false, true]) await t.test(`${fork ? 'fork' : 'main'}/${compressed ? 'zstd' : 'plain'}`, async t => {
    const ordinal = fork ? 9 : 4
    const later = agent(owner, 'turn', 'The requested log was inspected. ' + 'x'.repeat(400)); later.item.id = 'later-agent'
    const suffix = row(ordinal, 'response_item', { type: 'additional_tools', role: 'developer', tools: [{ name: 'PRIVATE_SCHEMA_MARKER', description: 'Authorization: Bearer DO_NOT_CAPTURE_SCHEMA' }] })
      + row(ordinal + 1, 'event_msg', later)
    const f = await fixture(t, { fork, compressed, suffix })
    await f.caughtUp()
    let task
    for (let i = 0; i < 100; i++) {
      task = (await f.core.snapshot()).tasks.find(task => task.id === owner)
      if (task?.analysis.completedAt) break
      await new Promise(resolve => setTimeout(resolve, 5))
    }
    assert.ok(f.state.summaryCalls > 0); assert.ok(f.state.jevCalls > 0, JSON.stringify({error:task.analysis.error,coverage:task.analysis.coverage,checks:task.analysis.checks.map(c=>({id:c.id,reason:c.reason}))}))
    assert.equal(task.analysis.checks.find(check => check.id === 'request_drift').status, 'clear')
    assert.equal(task.gs.light, 'green')
    assert.doesNotMatch(JSON.stringify(task.events), /PRIVATE_SCHEMA_MARKER|DO_NOT_CAPTURE_SCHEMA|response_item\/additional_tools/)
    const view = await f.caughtUp(), panel = await panelHarness(t)
    assert.equal(view.signal, 'green')
    assert.ok(view.analysis.partial?.text)
    await panel.deliver(view)
    assert.match(panel.element('#summary').textContent, /^🟢.*观察窗口/)
    assert.equal(panel.element('#recent-summary-state').textContent, '已总结')
  })
})

test('a successful bounded summary with real JEV decisions recovers an earlier analysis fault', async t => {
  const f = await fixture(t, { cooldownMs: 1000 })
  f.state.modelFailure = true
  await f.caughtUp()
  let snapshot, task
  for (let i = 0; i < 100; i++) {
    snapshot = await f.core.snapshot(); task = snapshot.tasks.find(task => task.id === owner)
    if (task?.analysis.state === 'failed') break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.equal(task.analysis.state, 'failed')
  const sourceKey = task.sourceKey
  assert.ok((await f.core.failureReceipts({ sourceKey })).receipts.some(receipt => receipt.domain === 'analysis' && receipt.state === 'active'))
  const later = agent(owner, 'turn', 'Verified recovery activity. ' + 'x'.repeat(400)); later.item.id = 'recovery-agent'
  await fs.appendFile(f.path, row(4, 'event_msg', later))
  await f.caughtUp()
  f.state.modelFailure = false; f.advance(); f.advance()
  await f.caughtUp()
  for (let i = 0; i < 100; i++) {
    task = (await f.core.snapshot()).tasks.find(task => task.id === owner)
    if (task?.analysis.summary?.partial && task.analysis.coverage?.judgedCheckIds?.length) break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.ok(task.analysis.summary?.partial)
  assert.ok(task.analysis.coverage.judgedCheckIds?.length, JSON.stringify({coverage:task.analysis.coverage, state:task.status, checks:task.analysis.checks.map(c=>({id:c.id,reason:c.reason}))}))
  assert.equal(task.analysis.error, null)
  assert.ok((await f.core.failureReceipts({ sourceKey })).receipts.filter(receipt => receipt.domain === 'analysis').every(receipt => receipt.state === 'recovered'))
})

test('a fresh read failure remains current even after its execution becomes unknown, and verified EOF clears only that current failure', async t => {
  const f = await fixture(t), recordPath = await fs.realpath(f.path), open = fs.open
  let broken = false
  t.mock.method(fs, 'open', async (...args) => { const handle = await open(...args); if (String(args[0]) === recordPath) { const read = handle.read; handle.read = async function (...params) { if (broken) throw Object.assign(new Error('Controlled record read failure'), { code: 'EIO' }); return read.apply(this, params) } }; return handle })
  await f.caughtUp()
  const diagnostics = async () => {
    const response = await fetch(f.adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_installation_diagnostics', arguments: {} } }) })
    return (await response.json()).result.structuredContent
  }
  await fs.appendFile(f.path, row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'AgentMessage', id: 'later-output', content: [{ type: 'Text', text: 'New activity after the verified prefix' }] } }))
  broken = true
  await f.read()
  assert.equal((await f.core.snapshot()).tasks[0].status, 'unknown')
  const failed = await diagnostics()
  assert.equal(failed.collection.state, 'failed')
  assert.equal(failed.collection.error.code, 'EIO')
  assert.equal(failed.collection.pendingHistory, undefined, 'a fresh failure is not labeled as old execution history')
  broken = false
  const recoveredView = await f.caughtUp()
  const recovered = await diagnostics()
  assert.equal(recovered.collection.state, 'connected', JSON.stringify({ collection: recovered.collection, observation: recoveredView.observation }))
  assert.equal(recovered.collection.error, null)
})

test('unchanged background chats do not multiply whole-Core snapshots or repeat ingestion', async t => {
  const f = await fixture(t, { backgroundChats: 27 })
  await f.adapter.refresh(); await f.adapter.refresh()
  const before = (await f.core.snapshot()).tasks.map(task => [task.id, task.lifecycleId, task.currentRequest, task.events.map(event => [event.id, event.capturedAt])]).sort()
  f.state.coreCalls = {}; f.state.snapshotSites = {}
  await f.adapter.refresh()
  t.diagnostic(JSON.stringify({ calls: f.state.coreCalls, snapshotSites: f.state.snapshotSites }))
  assert.ok((f.state.coreCalls.snapshot ?? 0) <= 2, 'one unchanged background batch must not read complete snapshots for every chat')
  assert.equal(f.state.coreCalls.ingest ?? 0, 0, 'unchanged native records must not rebuild source hierarchy')
  assert.equal(f.state.coreCalls.heartbeat, 1)
  const after = (await f.core.snapshot()).tasks.map(task => [task.id, task.lifecycleId, task.currentRequest, task.events.map(event => [event.id, event.capturedAt])]).sort()
  assert.deepEqual(after, before)
})

test('fast catch-up does not repeat discovery or idle-chat qualification between maintenance rounds', async t => {
  const padding = Array.from({ length: 20 }, (_, index) => row(index + 1, 'token_usage_record', { padding: 'x'.repeat(65536) })).join('')
  const f = await fixture(t, { pollMs: 10000, backgroundChats: 27, records: padding + row(21, 'event_msg', start('turn')) + row(22, 'event_msg', user(owner, 'turn')) + row(23, 'event_msg', agent(owner, 'turn')) })
  await f.adapter.refresh()
  f.state.threadReadIds = []; f.state.coreCalls = {}; f.state.snapshotSites = {}
  const deadline = performance.now() + 3000
  let current
  do { current = (await f.core.snapshot()).tasks.find(task => task.id === owner && task.currentRequest); if (current) break; await new Promise(resolve => setTimeout(resolve, 5)) } while (performance.now() < deadline)
  assert.equal(current?.currentRequest, 'Inspect only the synthetic log')
  const idleReads = f.state.threadReadIds.filter(id => id !== owner).length
  t.diagnostic(JSON.stringify({ idleMetadataReads: idleReads, calls: f.state.coreCalls, snapshotSites: f.state.snapshotSites }))
  assert.equal(idleReads, 0, 'a backlogged reader must not requalify every idle chat on each physical chunk')
  assert.ok((f.state.coreCalls.snapshot ?? 0) <= 2, 'only material publication needs its before/after snapshots during this catch-up window')
  await f.adapter.close(); assert.equal(f.adapter.diagnostics().refreshTimers, 0)
})

test('unchanged observation cannot hide an explicit pause or resume response', async t => {
  const f = await fixture(t); let view = await f.caughtUp()
  for (const paused of [true, false]) {
    const response = await fetch(f.adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_set_chat_analysis', arguments: { expectedThreadId: owner, paused, revision: view.monitoring.revision }, _meta: { thread_id: owner } } }) })
    const packet = await response.json()
    assert.equal(packet.result.isError, undefined)
    view = packet.result.structuredContent
    assert.equal(view.monitoring.paused, paused, 'the committed control response must reflect the new state immediately')
  }
})

test('cached collection health cannot retain an old model failure after a successful unchanged-material retry', async t => {
  const f = await fixture(t, { cooldownMs: 60000 })
  const waitTask = async predicate => { const deadline = performance.now() + 2000; let task; do { task = (await f.core.snapshot()).tasks.find(t => t.id === owner); if (task && predicate(task)) return task; await new Promise(resolve => setTimeout(resolve, 5)) } while (performance.now() < deadline); assert.fail('controlled analysis did not reach expected state: ' + task?.analysis.state) }
  f.state.modelFailure = true; await f.caughtUp(); await waitTask(task => task.analysis.state === 'failed')
  const changed = agent(owner, 'turn', 'A new owned activity after the failure'); changed.item.id = 'after-model-failure'
  await fs.appendFile(f.path, row(4, 'event_msg', changed)); await f.caughtUp()
  f.state.modelFailure = false
  const source = (await f.core.snapshot()).sources.find(source => source.status === 'connected')
  for (let second = 0; second < 60; second++) { f.advance(); await f.core.heartbeat(source.key) }
  const setting = await f.core.installationAnalysisConfig('chatgpt:current-app-material')
  await f.core.configureInstallationAnalysis(setting.registrationId, setting.config, setting.revision)
  await waitTask(task => Boolean(task.analysis.summary?.generated))
  f.state.coreCalls = {}
  const recovered = await f.read()
  assert.notEqual(recovered.health?.state, 'failed', 'analysis health comes from the fresh Core result, not an old collection cache')
  assert.ok((f.state.coreCalls.snapshot ?? 0) <= 1, 'a recovered model cannot force unchanged material through full publication again')
})

test('background catch-up schedules bounded work without waiting the idle polling interval between chunks', async t => {
  const padding = Array.from({ length: 12 }, (_, index) => row(index + 1, 'token_usage_record', { padding: 'x'.repeat(65536) })).join('')
  const f = await fixture(t, { pollMs: 10000, records: padding + row(13, 'event_msg', start('turn')) + row(14, 'event_msg', user(owner, 'turn')) + row(15, 'event_msg', agent(owner, 'turn')) })
  const deadline = performance.now() + 3000
  let task
  do { task = (await f.core.snapshot()).tasks.find(task => task.id === owner && task.currentRequest); if (task) break; await new Promise(resolve => setTimeout(resolve, 5)) } while (performance.now() < deadline)
  assert.equal(task?.currentRequest, 'Inspect only the synthetic log', 'a real backlog must progress before the 10-second idle poll is due')
  assert.ok(f.adapter.diagnostics().refreshTimers <= 1)
  await f.adapter.close()
  assert.equal(f.adapter.diagnostics().refreshTimers, 0)
})

test('ongoing prefix verification cannot cancel a model round for the unchanged verified material', async t => {
  let release
  const gate = new Promise(resolve => { release = resolve })
  const f = await fixture(t, { suffix: row(4, 'token_usage_record', { padding: 'p'.repeat(600000) }) })
  f.state.modelGate = gate
  try {
    await f.caughtUp()
    const deadline = performance.now() + 2000
    while (!f.state.summaryCalls && performance.now() < deadline) { await f.core.snapshot(); await new Promise(resolve => setTimeout(resolve, 5)) }
    assert.equal(f.state.summaryCalls, 1)
    await fs.appendFile(f.path, row(5, 'token_usage_record', { heartbeat: 1 }))
    const waiting = await f.read()
    assert.equal(waiting.observation.recordRevalidating, true)
    assert.equal(waiting.signal, 'gray')
    assert.equal((await f.core.snapshot()).tasks.find(task => task.id === owner).analysis.state, 'analyzing', 'no new execution evidence has replaced this frozen model input')
    await f.read(); await f.read()
    release()
    let task
    const completed = performance.now() + 2000
    do { task = (await f.core.snapshot()).tasks.find(task => task.id === owner); if (task.analysis.summary?.generated) break; await new Promise(resolve => setTimeout(resolve, 5)) } while (performance.now() < completed)
    assert.ok(task.analysis.summary?.generated)
    assert.equal(f.state.summaryCalls, 1); assert.equal(f.state.jevCalls, 1)
    const shown = await f.read()
    assert.equal(shown.analysis.generated.text, task.analysis.summary.generated.text)
  } finally { release() }
})

test('production HTTP recovery preserves legacy main and fork captures and accepts the next real execution', async t => {
  for (const fork of [false, true]) for (const recovery of ['Host', 'device-and-Host', 'device']) await t.test(`${fork ? 'fork' : 'main'}-${recovery}`, async t => {
    const f = await fixture(t, { fork, suffix: row(fork ? 9 : 4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) })
    const before = await f.caughtUp(), prior = (await f.core.snapshot()).tasks.flatMap(task => task.events).map(event => ({ id: event.data.nativeRecordId, capturedAt: event.capturedAt })).sort((a, b) => a.id.localeCompare(b.id))
    assert.equal(before.observation.phase, 'completed')
    const cursorPath = join(f.dir, 'adapter', 'cursors', before.identity.cursorKey + '.json')
    await f.adapter.close()
    const saved = JSON.parse(await fs.readFile(cursorPath, 'utf8'))
    for (const frame of fork ? saved.reader.frames : [{ reader: saved.reader }]) {
      const reader = frame.reader
      delete reader.storageScope
      if (recovery.includes('device') && reader.io?.io && reader.fileId) {
        const previousDevice = String(Number(reader.fileId.split(':')[0]) - 3)
        reader.fileId = previousDevice + ':' + reader.fileId.split(':')[1]; reader.io.io.fileId = reader.fileId
        reader.io.io.signature = previousDevice + ':' + reader.io.io.signature.split(':').slice(1).join(':')
        if (frame.source?.expectedFileId) frame.source.expectedFileId = previousDevice + ':' + frame.source.expectedFileId.split(':')[1]
      }
    }
    await fs.writeFile(cursorPath, JSON.stringify(saved), { mode: 0o600 })
    if (recovery.includes('Host')) { f.state.hostRunId = 'reopened-app-fixture'; f.state.hostStartedAt = at + 1000 }
    f.advance(); f.advance()
    await f.reopen()
    const restored = await f.caughtUp()
    assert.notEqual(restored.observation.gap, 'cursor-invalid')
    assert.equal(restored.observation.phase, 'completed')
    const recovered = await f.core.snapshot()
    assert.deepEqual(recovered.tasks.flatMap(task => task.events).map(event => ({ id: event.data.nativeRecordId, capturedAt: event.capturedAt })).sort((a, b) => a.id.localeCompare(b.id)), prior, 'old captures are neither replayed nor re-timed into the new source')
    for (const source of recovered.sources) assert.equal((await f.core.failureReceipts({ sourceKey: source.key })).receipts.length, 0)
    f.state.anchorTurnId = 'next-turn'; f.advance(); f.advance()
    const ordinal = fork ? 10 : 5
    await fs.appendFile(f.path, row(ordinal, 'event_msg', start('next-turn'), at + 4000) + row(ordinal + 1, 'event_msg', user(owner, 'next-turn', 'A genuinely new request'), at + 4000))
    let next
    for (let pump = 0; pump < 30; pump++) { next = await f.read(); if (next.execution.lifecycleId === 'next-turn' && next.observation.coverage === 'caught-up') break }
    assert.equal(next.execution.lifecycleId, 'next-turn')
    assert.equal(next.execution.status, 'running')
    const current = (await f.core.snapshot()).tasks.find(task => task.id === owner && task.lifecycleId === 'next-turn')
    assert.equal(current.currentRequest, 'A genuinely new request')
  })
})

test('persisted cursor scopes survive an admitted App version upgrade without replaying captures', async t => {
  for (const [fork, producer, compressed = false] of [[false, candidateHostProfile.cliVersion], [true, candidateHostProfile.cliVersion], [false, supportedProfile.cliVersion], [false, candidateHostProfile.cliVersion, true], [true, candidateHostProfile.cliVersion, true]]) await t.test((fork ? 'fork' : 'main') + '/' + producer + (compressed ? '/zstd' : ''), async t => {
    const f = await fixture(t, { profile: app20770, producer, fork, compressed, suffix: row(fork ? 9 : 4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) })
    const before = await f.caughtUp(), prior = (await f.core.snapshot()).tasks.flatMap(task => task.events).map(event => ({ id: event.data.nativeRecordId, capturedAt: event.capturedAt })).sort((a, b) => a.id.localeCompare(b.id))
    const cursorPath = join(f.dir, 'adapter', 'cursors', before.identity.cursorKey + '.json')
    await f.adapter.close()
    const saved = JSON.parse(await fs.readFile(cursorPath, 'utf8'))
    // Persisted v0.2.2 hashes for these fixed record names, headers and ranges.
    const frames = fork ? saved.reader.frames : [{ reader: saved.reader }]
    const stableScopes = frames.map(frame => frame.reader.storageScope)
    const legacyScopes = fork ? ['8b0b9a153a9d67e973f787bb1df0b893bc233e9beb7858927f2a7d071a5a277c', '0fad8c1b5dd04805fb59486cf22b45f841c143c5d79c27a5b5ff0190174b8750'] : ['fe093b3d89f6ecd47e7d2dea3f0af9b5d1bb092802f90c9525d5b930b4fa848a']
    frames.forEach((frame, index) => { frame.reader.storageScope = legacyScopes[index] })
    await fs.writeFile(cursorPath, JSON.stringify(saved), { mode: 0o600 })
    f.state.profile = app21828; f.state.hostRunId = 'upgraded-app'; f.state.hostStartedAt = at + 1000
    f.advance(); f.advance(); await f.reopen()
    const restored = await f.caughtUp()
    assert.notEqual(restored.observation.gap, 'cursor-invalid')
    assert.equal(restored.observation.phase, 'completed')
    const migrated = JSON.parse(await fs.readFile(cursorPath, 'utf8'))
    assert.deepEqual((fork ? migrated.reader.frames : [{ reader: migrated.reader }]).map(frame => frame.reader.storageScope), stableScopes)
    assert.deepEqual((await f.core.snapshot()).tasks.flatMap(task => task.events).map(event => ({ id: event.data.nativeRecordId, capturedAt: event.capturedAt })).sort((a, b) => a.id.localeCompare(b.id)), prior)
    f.state.anchorTurnId = 'upgraded-turn'; f.advance(); f.advance()
    const ordinal = fork ? 10 : 5
    const appended = row(ordinal, 'event_msg', start('upgraded-turn'), at + 4000) + row(ordinal + 1, 'event_msg', user(owner, 'upgraded-turn', 'A new request after the App upgrade'), at + 4000)
    await fs.appendFile(f.path, compressed ? zstdCompressSync(Buffer.from(appended)) : appended)
    let next
    for (let pump = 0; pump < 30; pump++) { next = await f.read(); if (next.execution?.lifecycleId === 'upgraded-turn' && next.observation.coverage === 'caught-up') break }
    if (compressed) {
      assert.equal(next.observation.gap, 'record-rewrite-suspected', 'completed compressed archives remain immutable')
      assert.ok((await f.core.snapshot()).tasks.every(task => task.lifecycleId !== 'upgraded-turn'))
      return
    }
    assert.equal(next.execution.lifecycleId, 'upgraded-turn', JSON.stringify({ summary: next.summary, observation: next.observation }))
    assert.equal(next.execution.status, 'running')
  })
})

test('version metadata changes cannot bypass an unrelated scope or a tampered saved header', async t => {
  for (const invalid of ['scope', 'producer']) await t.test(invalid, async t => {
    const f = await fixture(t, { profile: app20770 }), before = await f.caughtUp()
    const path = join(f.dir, 'adapter', 'cursors', before.identity.cursorKey + '.json')
    await f.adapter.close()
    const saved = JSON.parse(await fs.readFile(path, 'utf8'))
    saved.reader.storageScope = invalid === 'scope' ? '0'.repeat(64) : 'fe093b3d89f6ecd47e7d2dea3f0af9b5d1bb092802f90c9525d5b930b4fa848a'
    if (invalid === 'producer') saved.reader.header.cliVersion = 'unknown-producer'
    await fs.writeFile(path, JSON.stringify(saved), { mode: 0o600 })
    f.state.profile = app21828; f.state.currentRecordReads = 0
    await f.reopen()
    const view = await f.read()
    assert.equal(view.observation.gap, 'cursor-invalid')
    if (invalid === 'scope') assert.equal(f.state.currentRecordReads, 0)
    assert.equal((await f.core.snapshot()).tasks.some(task => task.events.some(event => event.text === 'unknown-producer')), false)
  })
})

test('an upgraded unanchored cursor exposes revalidation in the real panel while all input material stays sealed', async t => {
  const records = Array.from({ length: 12 }, (_, n) => row(n + 1, 'token_usage_record', { padding: 'x'.repeat(65536) })).join('')
    + row(13, 'event_msg', start('turn')) + row(14, 'event_msg', user(owner, 'turn'))
  const f = await fixture(t, { profile: app20770, records })
  await f.read(); await f.read()
  const directory = join(f.dir, 'adapter', 'cursors'), cursors = await fs.readdir(directory)
  assert.equal(cursors.length, 1)
  const path = join(directory, cursors[0])
  await f.adapter.close()
  const saved = JSON.parse(await fs.readFile(path, 'utf8'))
  assert.equal(saved.reader.state.anchored, false)
  assert.ok(saved.reader.offset > 262144)
  saved.reader.storageScope = 'fe093b3d89f6ecd47e7d2dea3f0af9b5d1bb092802f90c9525d5b930b4fa848a'
  await fs.writeFile(path, JSON.stringify(saved), { mode: 0o600 })
  f.state.profile = app21828; f.state.hostRunId = 'upgraded-unanchored'; f.advance()
  await f.reopen(); const view = await f.read()
  const source = (await f.core.snapshot()).sources.find(value => value.descriptor.registrationId === 'chatgpt:current-app-material' && value.status === 'connected')
  const fence = await f.core.nativeOriginFence(source.key, saved.reader.io.key)
  assert.ok(fence.prefix.offset >= saved.reader.offset, 'the retained Core proof covers the saved progress')
  assert.equal(view.observation.recordRevalidating, true)
  assert.equal(view.observation.coverage, 'catching-up')
  assert.equal(view.observation.gap, 'cursor-revalidating')
  const page = await panelHarness(t); await page.deliver(view)
  assert.match(page.element('#summary').textContent, /当前记录重验中/)
  assert.ok((await f.core.snapshot()).tasks.every(task => !task.currentRequest && task.events.length === 0))
})

test('compatible interfaces and record structure admit unlisted host and producer versions', async t => {
  for (const options of [{}, { compressed: true }, { fork: true }, { fork: true, compressed: true }]) await t.test(JSON.stringify(options), async t => {
    const f = await fixture(t, { profile: { appVersion: 'future-app', appBuild: 'future-build', cliVersion: 'future-cli' }, producer: 'future-record-producer', inheritedProducer: 'another-future-producer', ...options }), view = await f.caughtUp()
    assert.equal(view.identity.threadId, owner)
    assert.equal(view.observation.coverage, 'caught-up')
    const task = (await f.core.snapshot()).tasks[0]
    assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.equal(task.coverage.complete, true)
  })
})

test('missing observation interfaces and incompatible record structure remain explicit gaps', async t => {
  const missing = await fixture(t)
  missing.native.readThread = undefined
  assert.equal((await missing.read()).observation.gap, 'metadata-capability-unavailable')
  const malformed = await fixture(t)
  const records = (await fs.readFile(malformed.path, 'utf8')).split('\n'), first = JSON.parse(records[0])
  first.payload.history_mode = 'unknown-layout'; records[0] = JSON.stringify(first)
  await fs.writeFile(malformed.path, records.join('\n'))
  assert.equal((await malformed.read()).observation.gap, 'unsupported-record-schema')
  assert.equal((await malformed.core.snapshot()).tasks.some(task => task.currentRequest), false)
})

test('unknown native events retain explicit gaps while later known records continue across restart', async t => {
  for (const unknown of [row(4, 'future_record', { secret: 'unselected-future-body' }), row(4, 'event_msg', { type: 'future_event', turn_id: 'turn', secret: 'unselected-future-body' }), row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'FutureItem', text: 'unselected-future-body' } })]) await t.test(JSON.parse(unknown).type + '/' + JSON.parse(unknown).payload.type, async t => {
    const later = agent(owner, 'turn', 'Known activity after the unknown event'); later.item.id = 'later-known-output'
    const f = await fixture(t, { suffix: unknown + row(5, 'event_msg', later) }), view = await f.caughtUp()
    assert.equal(view.observation.coverage, 'caught-up')
    const task = (await f.core.snapshot()).tasks[0]
    assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.equal(task.coverage.complete, false)
    assert.notEqual(task.gs.light, 'green')
    assert.ok(task.events.some(event => event.kind === 'gap' && /unsupported-/.test(event.text)))
    assert.ok(task.events.some(event => event.text === 'Known activity after the unknown event'))
    assert.doesNotMatch(JSON.stringify(task), /unselected-future-body/)
    const captures = task.events.map(event => [event.data.nativeRecordId, event.capturedAt])
    await f.reopen(); await f.caughtUp()
    const recovered = (await f.core.snapshot()).tasks[0]
    assert.deepEqual(recovered.events.map(event => [event.data.nativeRecordId, event.capturedAt]), captures)
    assert.equal(recovered.coverage.complete, false)
    const page = await panelHarness(t); await page.deliver(await f.read())
    assert.match(page.element('#gaps').textContent, /unsupported-/)
  })
})

test('unknown records explicitly attributed to another turn do not contaminate current coverage', async t => {
  const f = await fixture(t, { suffix: row(4, 'event_msg', { type: 'future_event', turn_id: 'different-turn', secret: 'foreign-event-body' }) + row(5, 'future_record', { turn_id: 'different-turn', secret: 'foreign-record-body' }) })
  await f.caughtUp()
  const task = (await f.core.snapshot()).tasks[0]
  assert.equal(task.coverage.complete, true)
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.doesNotMatch(JSON.stringify(task), /foreign-event-body|foreign-record-body|unsupported-event-type|unsupported-record-type/)
})

test('unknown inherited events preserve known fork material while retaining incomplete coverage', async t => {
 for (const unknownHistory of [true, 'record', 'event']) await t.test(String(unknownHistory), async t => {
  const f = await fixture(t, { fork: true, unknownHistory, producer: 'future-producer', inheritedProducer: 'future-ancestor' }), view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks[0]
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.ok(task.events.some(event => event.kind === 'gap' && event.data?.history))
  assert.notEqual(task.gs.light, 'green')
  assert.doesNotMatch(JSON.stringify(task), /unselected-ancestor-body/)
  assert.equal(task.coverage.complete, false)
  f.advance(); f.state.anchorTurnId = 'next'
  await fs.appendFile(f.path, row(9, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) + row(10, 'event_msg', start('next')) + row(11, 'event_msg', user(owner, 'next')))
  await f.caughtUp()
  const next = (await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'next')
  assert.equal(next.coverage.complete, false)
  assert.notEqual(next.gs.light, 'green')
  const page = await panelHarness(t); await page.deliver(await f.read())
  assert.match(page.element('#gaps').textContent, /unsupported-/)
 })
})

test('the exact current App reaches the production Core HTTP and panel with a typed user request', async t => {
  const f = await fixture(t), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0]
  assert.equal(view.identity.threadId, owner)
  assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.equal(task.events.find(event => event.kind === 'output').text, 'The synthetic log has been inspected')
  assert.equal(task.coverage.complete, true)
})

test('the exact App build 20770 retains main and fork material ownership', async t => {
  for (const fork of [false, true]) await t.test(fork ? 'fork' : 'main', async t => {
    const f = await fixture(t, { profile: app20770, fork }), view = await f.caughtUp()
    assert.equal(view.identity.threadId, owner)
    assert.equal(view.observation.coverage, 'caught-up')
    const tasks = (await f.core.snapshot()).tasks
    assert.equal(tasks.length, 1)
    const task = tasks[0]
    assert.equal(task.id, owner)
    assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.equal(task.coverage.complete, true)
    assert.equal(task.instructionContext.userLimitRefs.length, fork ? 1 : 0)
    assert.ok(task.events.filter(event => event.data?.history).every(event => !task.activity.evidenceIds.includes(event.id)))
  })
})

test('App build 21828 and its 0.164 producer reach the production panel with bounded main and fork materials', async t => {
  for (const [name, options] of [['plain main', {}], ['compressed main', { compressed: true }], ['current fork', { fork: true }], ['compressed fork with retained prior producer', { fork: true, compressed: true, inheritedProducer: candidateHostProfile.cliVersion }]]) await t.test(name, async t => {
    const f = await fixture(t, { profile: app21828, producer: app21828.cliVersion, ...options }), view = await f.caughtUp()
    assert.equal(view.identity.threadId, owner)
    assert.equal(view.observation.coverage, 'caught-up')
    const tasks = (await f.core.snapshot()).tasks
    assert.equal(tasks.length, 1)
    const task = tasks[0]
    assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.equal(task.events.find(event => event.kind === 'input' && !event.data?.history).authorizationEligible, true)
    assert.equal(task.coverage.complete, true)
    assert.equal(task.instructionContext.userLimitRefs.length, options.fork ? 1 : 0)
    assert.ok(task.events.filter(event => event.data?.history).every(event => !task.activity.evidenceIds.includes(event.id)))
  })
})

test('App build 25471 and its 0.167 producer reach the production panel with bounded main and fork materials', async t => {
  for (const [name, options] of [['plain main', {}], ['compressed main', { compressed: true }], ['current fork', { fork: true }], ['compressed fork with retained prior producer', { fork: true, compressed: true, inheritedProducer: app21828.cliVersion }]]) await t.test(name, async t => {
    const f = await fixture(t, { profile: app25471, producer: app25471.cliVersion, ...options }), view = await f.caughtUp()
    assert.equal(view.identity.threadId, owner)
    assert.equal(view.observation.coverage, 'caught-up')
    const tasks = (await f.core.snapshot()).tasks
    assert.equal(tasks.length, 1)
    const task = tasks[0]
    assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.equal(task.events.find(event => event.kind === 'input' && !event.data?.history).authorizationEligible, true)
    assert.equal(task.coverage.complete, true)
    assert.equal(task.instructionContext.userLimitRefs.length, options.fork ? 1 : 0)
    assert.ok(task.events.filter(event => event.data?.history).every(event => !task.activity.evidenceIds.includes(event.id)))
  })
})

test('current clock sleep extensions retain a bounded tool result without interrupting collection', async t => {
  const suffix = row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'Extension', kind: 'clock.sleep', id: 'sleep-result', durationMs: 45000, message: 'Unselected private wait details' } })
  const f = await fixture(t, { profile: app25471, producer: app25471.cliVersion, suffix }), view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'native-item:sleep-result')
  assert.equal(result.kind, 'tool-result'); assert.equal(result.data.toolName, 'clock.sleep')
  assert.match(result.text, /45000/)
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(JSON.stringify(task).includes('Unselected private wait details'), false)
})

test('unknown clock extensions and unverified durations remain explicit material gaps', async t => {
  for (const item of [{ kind: 'clock.unknown', durationMs: 45000 }, { kind: 'clock.sleep' }, { kind: 'clock.sleep', durationMs: -1 }, { kind: 'clock.sleep', durationMs: 43200001 }]) await t.test(JSON.stringify(item), async t => {
    const suffix = row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'Extension', id: 'invalid-sleep', ...item } })
    const f = await fixture(t, { profile: app25471, producer: app25471.cliVersion, suffix }), view = await f.caughtUp()
    assert.match(view.observation.gap, item.kind === 'clock.sleep' ? /^unsupported-clock-result-shape$/ : /^unsupported-item-type:/)
    assert.equal(view.signal, 'gray')
  })
})

test('current web search extensions preserve bounded tool evidence without importing queries, extra results or user authority', async t => {
  const suffix = row(4, 'event_msg', { type: 'item_started', thread_id: owner, turn_id: 'turn', item: { type: 'Extension', kind: 'web.search', id: 'web-result' } })
    + row(5, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'Extension', kind: 'web.search', id: 'web-result', query: 'private-search-query', action: { type: 'search', query: 'private-search-query' }, results: [{ type: 'search', snippet: 'Synthetic search evidence <img>', title: 'private-title', url: 'https://example.test/private' }, { snippet: 'private-second-result' }] } })
    + row(6, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'Extension', kind: 'web.search', id: 'empty-web-result', action: { type: 'openPage', url: 'https://example.test/private' }, results: [] } })
    + row(7, 'event_msg', { type: 'task_complete', turn_id: 'turn' })
  const f = await fixture(t, { profile: app21828, producer: app21828.cliVersion, suffix }), view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'retained-history')
  assert.equal(view.execution.historical, true)
  const task = (await f.core.snapshot()).tasks[0], found = task.events.find(event => event.data?.callId === 'native-item:web-result')
  assert.equal(found.kind, 'tool-result'); assert.equal(found.text, 'Synthetic search evidence <img>')
  assert.equal(found.data.toolName, 'web.search'); assert.equal(found.data.truncated, true)
  assert.equal(task.coverage.complete, false, 'an excerpt from only the first search result is not complete coverage')
  assert.equal(task.events.find(event => event.data?.callId === 'native-item:empty-web-result').text, '网页工具未返回结果')
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.doesNotMatch(JSON.stringify(task), /private-search-query|private-second-result|private-title|example\.test/)
})

test('observed web find and omitted text-result forms keep reading without fabricating complete evidence', async t => {
  const cases = [
    ['findInPage', [{ type: 'text_result', snippet: 'A bounded current page match' }], 'tool-result'],
    ['other', [{ type: 'text_result', snippet: 'private-other-snippet' }], 'gap'],
    ['openPage', [{ type: 'text_result', title: 'private-title', ref_id: 'private-reference' }], 'gap'],
  ]
  for (const [action, results, kind] of cases) await t.test(action, async t => {
    const following = agent(owner, 'turn', 'Current execution continues'); following.item.id = 'after-web-omission'
    const item = { type: 'Extension', kind: 'web.search', id: 'observed-web', action: { type: action, pattern: 'private-pattern', url: 'https://example.test/private' }, results }
    const f = await fixture(t, { suffix: row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item }) + row(5, 'event_msg', following) })
    const view = await f.caughtUp(), task = (await f.core.snapshot()).tasks.find(task => task.id === owner), result = task.events.find(event => event.data?.callId === 'native-item:observed-web')
    assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
    assert.equal(result.kind, kind)
    assert.ok(task.events.some(event => event.text === 'Current execution continues'))
    if (kind === 'tool-result') assert.equal(result.text, 'A bounded current page match')
    else { assert.equal(result.data.truncated, true); assert.equal(task.coverage.complete, false); assert.match(result.data.nativeType, /\/omitted$/) }
    assert.doesNotMatch(JSON.stringify(task), /private-pattern|private-title|private-reference|private-other-snippet|example\.test/)
  })
})

test('unknown web extension kinds and malformed result shapes retain explicit gaps without reserving a result body', async t => {
  for (const item of [{ kind: 'unknown.extension', action: { type: 'search' }, results: [{ snippet: 'private-unapproved-result' }] }, { kind: 'web.search', action: { type: 'unknown-action' }, results: [{ snippet: 'private-unapproved-result' }] }, { kind: 'web.search', action: { type: 'search' }, results: [{ snippet: { text: 'private-unapproved-result' } }] }, { kind: 'web.search', action: { type: 'search' }, results: [{ snippet: 42 }] }]) await t.test(item.kind + '/' + item.action.type, async t => {
    const f = await fixture(t, { profile: app21828, producer: app21828.cliVersion, suffix: row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'Extension', id: 'unapproved-web', ...item } }) }), view = await f.read()
    assert.match(view.observation.gap, /unsupported-(?:item-type|web-result-shape)/)
    const snapshot = await f.core.snapshot()
    assert.ok(snapshot.tasks.every(task => task.events.every(event => event.data?.callId !== 'native-item:unapproved-web')))
    assert.doesNotMatch(JSON.stringify(snapshot), /private-unapproved-result|unknown\.extension|unknown-action/)
  })
})

test('verified custom tool input_text arrays become one bounded result without becoming input or verification proof', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'array-result', output: [{ type: 'input_text', text: 'Synthetic result excerpt' }, { text: 'A second fixed text segment', type: 'input_text' }] }) })
  const view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'array-result')
  assert.equal(result.kind, 'tool-result'); assert.equal(result.text, 'Synthetic result excerpt\nA second fixed text segment')
  assert.equal(result.data.nativeType, 'response_item/custom_tool_call_output')
  assert.notEqual(result.authorizationEligible, true)
  for (const key of ['inputOrigin', 'inputPhase', 'exitCode', 'sourceVersion', 'validationScope', 'command']) assert.equal(result.data[key], undefined)
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.coverage.complete, true)
  assert.equal(task.events.filter(event => event.kind === 'input').length, 1)
})

test('observed function tool input_text arrays reach Core as bounded results without input or verification authority', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'function_call_output', call_id: 'function-array', output: [{ type: 'input_text', text: 'Synthetic function result' }, { text: 'A second fixed result segment', type: 'input_text' }] }) })
  const view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'function-array')
  assert.equal(result.kind, 'tool-result'); assert.equal(result.text, 'Synthetic function result\nA second fixed result segment')
  assert.equal(result.data.nativeType, 'response_item/function_call_output')
  assert.notEqual(result.authorizationEligible, true)
  for (const key of ['inputOrigin', 'inputPhase', 'exitCode', 'sourceVersion', 'validationScope', 'command']) assert.equal(result.data[key], undefined)
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.coverage.complete, true)
  assert.equal(task.events.filter(event => event.kind === 'input').length, 1)
})

test('dotted property names cannot impersonate structural array elements or the payload output field', async t => {
  await t.test('sibling-with-dotted-name', async t => {
    const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'real-array', output: [{ type: 'input_text', text: 'Actual structural text' }], 'output.1': { type: 'input_text', text: 'PHANTOM-SIBLING-BODY' } }) })
    assert.equal((await f.caughtUp()).observation.coverage, 'caught-up')
    const result = (await f.core.snapshot()).tasks[0].events.find(event => event.data?.callId === 'real-array')
    assert.equal(result.text, 'Actual structural text')
    assert.doesNotMatch(JSON.stringify(result), /PHANTOM/)
  })
  await t.test('root-with-dotted-name', async t => {
    const suffix = JSON.stringify({ ordinal: 4, type: 'response_item', timestamp: new Date(at).toISOString(), payload: { type: 'custom_tool_call_output', call_id: 'phantom-array' }, 'payload.output': [{ type: 'input_text', text: 'PHANTOM-ROOT-BODY' }] }) + '\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-tool-result-shape')
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.callId === 'phantom-array'), false)
  })
  await t.test('payload-kind-alias', async t => {
    const suffix = JSON.stringify({ ordinal: 4, type: 'response_item', timestamp: new Date(at).toISOString(), payload: { type: 'future_tool_call_output', call_id: 'phantom-kind', output: [{ type: 'input_text', text: 'UNOBSERVED-KIND-BODY' }] }, 'payload.type': 'custom_tool_call_output' }) + '\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.coverage, 'caught-up')
    const task = (await f.core.snapshot()).tasks[0], gap = task.events.find(event => event.data?.nativeType === 'response_item/future_tool_call_output')
    assert.equal(gap.kind, 'gap'); assert.equal(task.coverage.complete, false)
    assert.equal(task.events.some(event => event.data?.callId === 'phantom-kind'), false)
    assert.doesNotMatch(JSON.stringify(task.events), /UNOBSERVED-KIND-BODY/)
    const projection = new MaterialProjection(); projection.write(Buffer.from(suffix))
    assert.equal(projection.finish().material.fields['payload.output'], undefined)
  })
  await t.test('root-kind-alias', async t => {
    const suffix = JSON.stringify({ ordinal: 4, type: 'world_state', timestamp: new Date(at).toISOString(), payload: { type: 'custom_tool_call_output', call_id: 'phantom-root-kind', output: [{ type: 'input_text', text: 'UNOBSERVED-RECORD-BODY' }] }, '': { type: 'response_item' } }) + '\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.observation.coverage, 'caught-up'); assert.equal(view.observation.gap, null)
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.callId === 'phantom-root-kind'), false)
  })
})

test('current-producer fork material reaches the production panel with independent user input and inherited evidence kept historical', async t => {
  const f = await fixture(t, { fork: true, suffix: row(9, 'response_item', { type: 'custom_tool_call_output', call_id: 'current-array', output: [{ type: 'input_text', text: 'Only current synthetic tool activity' }] }) })
  const view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(view.forkProvenance.complete, true)
  const snapshot = await f.core.snapshot(), task = snapshot.tasks.find(value => value.lifecycleId === 'turn')
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.instructionContext.userLimitRefs.length, 1)
  const inherited = task.events.filter(event => event.data?.history)
  assert.ok(inherited.some(event => event.text === 'Keep the synthetic inherited limit'))
  const oldResult = inherited.find(event => event.text === 'Historical validation only')
  assert.ok(oldResult); assert.equal(oldResult.data.inherited, true)
  assert.ok(!task.activity.evidenceIds.includes(oldResult.id))
  assert.equal(task.events.find(event => event.data?.callId === 'current-array').data.history, undefined)
  assert.ok(task.events.filter(event => event.kind === 'input' && !event.data?.history).every(event => event.text === task.currentRequest && event.authorizationEligible === true))
  assert.ok(snapshot.tasks.every(value => value.id === owner))
})

test('version metadata does not gate compatible native material or revoke its owner', async t => {
  for (const profile of [null, {}, { appVersion: 'future', appBuild: 'future', cliVersion: 'future' }, { ...candidateHostProfile, appBuild: 20586 }]) await t.test(JSON.stringify(profile), async t => {
    const lost = [], f = await fixture(t, { profile, onQualificationLost: reason => lost.push(reason) }), view = await f.caughtUp()
    assert.equal(view.identity.threadId, owner); assert.equal(view.observation.coverage, 'caught-up')
    assert.ok(f.state.currentRecordReads > 0); assert.ok(f.state.threadReads > 0)
    assert.deepEqual(lost, []); assert.equal(f.adapter.diagnostics().closed, false)
    assert.equal((await f.core.snapshot()).tasks[0].currentRequest, 'Inspect only the synthetic log')
  })
})

test('a genuine global plugin disable still closes the adapter and preserves an independently connected DSH source', async t => {
  const lost = [], f = await fixture(t, { onQualificationLost: reason => lost.push(reason) })
  await f.caughtUp()
  const dsh = await f.core.registerSource({ registrationId: 'independent-dsh', hostRunId: 'dsh-host', name: 'Owned DSH fixture' })
  f.state.enabled = false
  await f.adapter.refresh(); await f.adapter.close()
  assert.deepEqual(lost, ['plugin-disabled'])
  assert.equal(f.adapter.diagnostics().closed, true); assert.equal(f.adapter.diagnostics().openReaders, 0)
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.sources.find(source => source.key === dsh).status, 'connected')
})

test('mixed text and image tool results preserve text, flag missing images and continue to the current execution and model requests', async t => {
  const image = 'data:image/png;base64,' + 'PRIVATE-IMAGE-DATA'.repeat(40000), f = await fixture(t)
  await f.caughtUp(); f.advance()
  const callsBefore = { summary: f.state.summaryCalls, jev: f.state.jevCalls }
  await fs.appendFile(f.path, row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'mixed-image', output: [{ type: 'input_text', text: 'Observed bounded text' }, { type: 'input_image', image_url: image, detail: 'original' }, { type: 'input_text', text: 'Text after the image' }] })
    + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' })
    + row(6, 'event_msg', start('current'))
    + row(7, 'event_msg', user(owner, 'current', 'Inspect the new request'))
    + row(8, 'event_msg', agent(owner, 'current', 'Current execution is advancing')))
  f.state.anchorTurnId = 'current'
  const view = await f.caughtUp(), snapshot = await f.core.snapshot(), old = snapshot.tasks.find(task => task.lifecycleId === 'turn'), current = snapshot.tasks.find(task => task.lifecycleId === 'current')
  assert.equal(view.observation.gap, null, 'a known mixed image result must not stop the production reader')
  assert.equal(view.observation.coverage, 'caught-up')
  const result = old.events.find(event => event.data?.callId === 'mixed-image')
  assert.equal(result.text, 'Observed bounded text\nText after the image')
  assert.equal(result.data.truncated, true)
  assert.ok(result.data.verificationGaps.includes('tool-result-image-omitted'))
  assert.equal(old.coverage.complete, false)
  assert.equal(current.status, 'running'); assert.equal(current.currentRequest, 'Inspect the new request')
  assert.doesNotMatch(JSON.stringify(snapshot), /PRIVATE-IMAGE|data:image|image_url/)
  const deadline = performance.now() + 3000
  while (!(await f.core.snapshot()).tasks.find(task => task.lifecycleId === 'current')?.analysis.completedAt) { assert.ok(performance.now() < deadline, 'current execution reaches summary and judgment HTTP'); await new Promise(resolve => setTimeout(resolve, 5)) }
  assert.ok(f.state.summaryCalls > callsBefore.summary); assert.ok(f.state.jevCalls > callsBefore.jev)
})

test('unverified and malformed tool arrays stay gray across later completion and adapter recovery without reserving body identities', async t => {
  const cases = [
    ['empty', []], ['scalar-element', ['Not a typed element']], ['missing-type', [{ text: 'unverified-body' }]], ['missing-text', [{ type: 'input_text' }]],
    ['null-text', [{ type: 'input_text', text: null }]], ['number-text', [{ type: 'input_text', text: 7 }]], ['object-text', [{ type: 'input_text', text: { nested: 'unverified-body' } }]],
    ['missing-image-url', [{ type: 'input_image', detail: 'original' }]], ['invalid-image-url', [{ type: 'input_image', image_url: { url: 'unverified-body' } }]], ['invalid-image-detail', [{ type: 'input_image', image_url: 'unverified-body', detail: 'unknown' }]], ['mixed', [{ type: 'input_text', text: 'unverified-body' }, { type: 'input_audio', audio_url: 'unverified-body' }]],
    ['extra-field', [{ type: 'input_text', text: 'unverified-body', extra: { authorization: 'Bearer PRIVATE-UNVERIFIED' } }]],
  ]
  for (const type of ['function_call_output', 'custom_tool_call_output']) for (const [name, output] of cases) await t.test(type + '/' + name, async t => {
    const f = await fixture(t); await f.caughtUp()
    const before = (await f.core.snapshot()).tasks[0], old = before.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), progress = (await f.core.diagnostics()).retained.nativeCaptureProgress.records
    f.advance(); await fs.appendFile(f.path, row(4, 'response_item', { type, call_id: 'bad-array', output }) + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-tool-result-shape')
    assert.equal(after.coverage.complete, false); assert.equal(after.status, 'unknown'); assert.equal(after.lastActivity, before.lastActivity)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), old)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, progress)
    await f.reopen(); view = await f.read(); after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), old)
    assert.doesNotMatch(JSON.stringify(after.events), /unverified-body|PRIVATE-UNVERIFIED|task_complete/)
  })
})

for (const type of ['function_call_output', 'custom_tool_call_output']) for (const text of ['', 'Known text before the image']) test(type + ' image omission preserves an explicit gap after model completion (' + (text ? 'mixed' : 'image-only') + ')', async t => {
  const output = [...(text ? [{ type: 'input_text', text }] : []), { type: 'input_image', image_url: 'data:image/png;base64,PRIVATE-OMITTED-IMAGE', detail: 'high' }]
  const f = await fixture(t, { suffix: row(4, 'response_item', { type, call_id: 'image-result', output }) })
  assert.equal((await f.caughtUp()).observation.coverage, 'caught-up')
  let task
  const deadline = performance.now() + 3000
  do { task = (await f.core.snapshot()).tasks[0]; if (task.analysis.summary?.partial) break; assert.ok(performance.now() < deadline, 'partial image observations still reach model analysis: ' + JSON.stringify({ status: task.status, hasRequest: Boolean(task.currentRequest), state: task.analysis.state, error: task.analysis.error?.code, summaryCalls: f.state.summaryCalls, jevCalls: f.state.jevCalls, summaryKeys: Object.keys(task.analysis.summary ?? {}), checks: task.analysis.checks.map(check => ({ status: check.status, reason: check.reason })) })); await new Promise(resolve => setTimeout(resolve, 5)) } while (true)
  const result = task.events.find(event => event.data?.callId === 'image-result')
  assert.equal(result.text, text); assert.equal(result.data.truncated, true)
  assert.ok(result.data.verificationGaps.includes('tool-result-image-omitted'))
  assert.equal(task.coverage.complete, false); assert.equal(task.gs.light, 'green')
  assert.ok(f.state.jevCalls > 0)
  assert.match(task.gs.label, /已检查项未见异常/)
  for (const id of ['validation_cost', 'validation_gap']) {
    assert.equal(task.analysis.checks.find(check => check.id === id).status, 'unknown')
    assert.ok(task.gs.assessment.unknown.includes(id))
  }
  assert.equal(task.analysis.summary.generated, undefined); assert.ok(task.analysis.summary.partial.evidenceRefs.length)
  const partialView = await f.caughtUp(); assert.ok(partialView.analysis.partial?.text); assert.equal(partialView.analysis.generated, null)
  assert.doesNotMatch(JSON.stringify(task), /PRIVATE-OMITTED-IMAGE|image_url|data:image/)
})

test('observed function arrays coexist with scalar function and custom results without replacing earlier material', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'function_call_output', call_id: 'function-scalar', output: 'Function scalar result' }) + row(5, 'response_item', { type: 'custom_tool_call_output', call_id: 'custom-scalar', output: 'Custom scalar result' }) })
  assert.equal((await f.caughtUp()).observation.coverage, 'caught-up')
  const first = (await f.core.snapshot()).tasks[0], results = first.events.filter(event => event.kind === 'tool-result')
  assert.deepEqual(results.map(event => event.text), ['Function scalar result', 'Custom scalar result'])
  await fs.appendFile(f.path, row(6, 'response_item', { type: 'function_call_output', call_id: 'observed-array', output: [{ type: 'input_text', text: 'Function array result' }] }))
  const view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
  assert.equal(view.observation.gap, null); assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(after.coverage.complete, true)
  assert.deepEqual(after.events.filter(event => event.kind === 'tool-result').map(event => event.text), [...results.map(event => event.text), 'Function array result'])
})

test('array excerpts share one hard text budget and late authentication fields redact the whole output with an explicit coverage gap', async t => {
  for (const type of ['function_call_output', 'custom_tool_call_output']) for (const mode of ['many-elements', 'late-authentication']) await t.test(type + '/' + mode, async t => {
    const output = mode === 'many-elements' ? Array.from({ length: 5000 }, () => ({ type: 'input_text', text: 'A bounded excerpt segment' })) : [{ type: 'input_text', text: 'Prefix before a later sensitive element' }, { type: 'input_text', text: 'x'.repeat(400000) + ' Authorization: Bearer PRIVATE-LATE-ARRAY-TOKEN' }]
    const f = await fixture(t, { suffix: row(4, 'response_item', { type, call_id: 'bounded-array', output }) + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) }), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'bounded-array')
    assert.ok(['caught-up', 'retained-history'].includes(view.observation.coverage)); assert.equal(task.coverage.complete, false)
    assert.equal(result.kind, 'tool-result'); assert.ok(result.text.length <= 240); assert.equal(result.data.truncated, true)
    assert.notEqual(result.authorizationEligible, true)
    assert.equal(result.data.exitCode, undefined)
    if (mode === 'late-authentication') { assert.equal(result.data.redacted, true); assert.equal(result.text, '认证材料已省略，当前证据不完整'); assert.doesNotMatch(JSON.stringify(task.events), /PRIVATE-LATE|Prefix before|Bearer/) }
  })
})

test('duplicate array fields cannot fall back to an earlier scalar or manufacture a complete result', async t => {
  const outputs = [
    '"output":[{"type":"input_text","text":"first","text":"second"}]',
    '"output":[{"type":"input_text","type":"input_text","text":"duplicate-type"}]',
    '"output":"earlier-scalar","output":[{"type":"input_text"}]',
    '"output":[{"type":"input_text","text":"earlier-array"}],"output":"later-scalar"',
  ]
  for (const type of ['function_call_output', 'custom_tool_call_output']) for (const [index, output] of outputs.entries()) await t.test(type + '/' + index, async t => {
    const suffix = '{"ordinal":4,"type":"response_item","timestamp":"' + new Date(at).toISOString() + '","payload":{"type":"' + type + '","call_id":"duplicate-output",' + output + '}}\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, index < 2 ? 'unsupported-tool-result-shape' : 'record-malformed-or-over-limit')
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.callId === 'duplicate-output'), false)
  })
})

test('one fresh projection cannot splice array text or identities across payload containers and aliases', async t => {
  const body = '"type":"custom_tool_call_output","output":[{"type":"input_text","text":"One structural body"}]'
  const cases = [
    ['single-normal', '"payload":{' + body + ',"call_id":"own"}', true, 'own', 'turn'],
    ['body-then-id', '"payload":{' + body + '},"payload":{"call_id":"borrowed"}', false],
    ['id-then-body', '"payload":{"call_id":"borrowed"},"payload":{' + body + '}', false],
    ['body-then-empty', '"payload":{' + body + '},"payload":{}', false],
    ['empty-then-body', '"payload":{},"payload":{' + body + '}', false],
    ['body-then-false', '"payload":{' + body + '},"payload":false', false],
    ['false-then-body', '"payload":false,"payload":{' + body + '}', false],
    ['body-then-null', '"payload":{' + body + '},"payload":null', false],
    ['null-then-body', '"payload":null,"payload":{' + body + '}', false],
    ['body-then-array', '"payload":{' + body + '},"payload":[]', false],
    ['root-call-alias', '"payload":{' + body + '},"payload.call_id":"borrowed"', true, undefined, 'turn'],
    ['root-call-alias-after-own', '"payload":{' + body + ',"call_id":"own"},"payload.call_id":"borrowed"', true, 'own', 'turn'],
    ['root-call-alias-before-own', '"payload.call_id":"borrowed","payload":{' + body + ',"call_id":"own"}', true, 'own', 'turn'],
    ['turn-alias', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough.turn_id":"borrowed-turn"}', true, undefined, 'turn'],
    ['duplicate-turn-container', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough":{"turn_id":"stale-turn"},"internal_chat_message_metadata_passthrough":{}}', false],
    ['duplicate-call-id', '"payload":{' + body + ',"call_id":"first","call_id":"second"}', false],
  ]
  for (const [name, properties, accepted, callId, runId] of cases) await t.test(name, () => {
    const input = '{"type":"response_item",' + properties + '}', bytes = Buffer.from(input)
    for (const chunk of [bytes.length, 7]) {
      const projection = new MaterialProjection()
      for (let offset = 0; offset < bytes.length; offset += chunk) projection.write(bytes.subarray(offset, offset + chunk))
      let event
      try { event = materialRecord(projection.finish(), 'turn', 0, bytes.length) }
      catch (error) { assert.equal(accepted, false); assert.equal(error.message, 'ambiguous material structure'); continue }
      if (!accepted) assert.ok(!event?.text || event.kind === 'gap')
      else { assert.equal(event.text, 'One structural body'); assert.equal(event.data.callId, callId); assert.equal(event.data.runId, runId) }
    }
  })
})

test('ambiguous payload structures stay gray through the production chain after known completion and recovery', async t => {
  const body = '"type":"custom_tool_call_output","output":[{"type":"input_text","text":"Do not publish a spliced body"}]'
  const cases = [
    ['body-then-id', '"payload":{' + body + '},"payload":{"call_id":"borrowed"}'],
    ['id-then-body', '"payload":{"call_id":"borrowed"},"payload":{' + body + '}'],
    ['body-then-empty', '"payload":{' + body + '},"payload":{}'],
    ['body-then-false', '"payload":{' + body + '},"payload":false'],
    ['null-then-body', '"payload":null,"payload":{' + body + '}'],
    ['duplicate-call', '"payload":{' + body + ',"call_id":"first","call_id":"second"}'],
    ['duplicate-context', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough":{"turn_id":"turn"},"internal_chat_message_metadata_passthrough":{}}'],
    ['foreign-context-then-empty', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough":{"turn_id":"foreign-turn"},"internal_chat_message_metadata_passthrough":{}}'],
  ]
  for (const [name, properties] of cases) await t.test(name, async t => {
    const f = await fixture(t); await f.caughtUp()
    const before = (await f.core.snapshot()).tasks[0], original = before.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), retained = (await f.core.diagnostics()).retained.nativeCaptureProgress.records
    f.advance()
    await fs.appendFile(f.path, '{"ordinal":4,"type":"response_item","timestamp":"' + new Date(at).toISOString() + '",' + properties + '}\n' + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'record-malformed-or-over-limit')
    assert.equal(after.status, 'unknown'); assert.equal(after.coverage.complete, false); assert.equal(after.lastActivity, before.lastActivity)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), original)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, retained)
    await f.reopen(); view = await f.read(); after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), original)
    assert.equal(after.events.some(event => event.data.nativeType === 'task_complete'), false)
  })
})

test('completed FileChange results keep a bounded main-chat result without retaining patches or paths', async t => {
  const item = { type: 'FileChange', id: 'file-change', status: 'completed', stdout: 'Patch operation completed', stderr: '', changes: { '/private/not-retained': { diff: 'private-patch-body', kind: 'update' } } }
  const f = await fixture(t, { suffix: row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item }) })
  const view = await f.caughtUp(), snapshot = await f.core.snapshot(), result = snapshot.tasks[0]?.events.find(event => event.data?.nativeType === 'item_completed/FileChange')
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(result?.kind, 'tool-result')
  assert.equal(result.text, 'Patch operation completed')
  assert.equal(result.data.callId, 'native-item:file-change')
  assert.equal(result.data.exitCode, undefined, 'a native patch status is not a test exit code')
  assert.doesNotMatch(JSON.stringify(snapshot), /private-patch-body|not-retained/)
})

test('FileChange result excerpts remain bounded and unknown statuses stay sealed', async t => {
  for (const status of ['completed', 'future']) await t.test(status, async t => {
    const item = { type: 'FileChange', id: 'bounded-file-change', status, stdout: 'x'.repeat(600000), stderr: '' }
    const f = await fixture(t, { suffix: row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item }) })
    const view = await f.caughtUp(), events = (await f.core.snapshot()).tasks.flatMap(task => task.events), result = events.find(event => event.data?.nativeType === 'item_completed/FileChange')
    if (status === 'future') { assert.equal(view.observation.gap, 'unsupported-file-change-shape'); assert.equal(result, undefined) }
    else { assert.equal(result.text.length, 240); assert.equal(result.data.truncated, true) }
  })
})

test('known subagent activity does not block or become main-chat evidence', async t => {
  for (const kind of ['started', 'completed']) await t.test(kind, async t => {
    const item = { type: 'SubAgentActivity', id: 'private-child-item', kind, agent_thread_id: 'private-child-thread', agent_path: 'private-child-path', model: 'private-child-model', reasoning_effort: 'high', content: [{ type: 'text', text: 'Do not adopt private-child instructions' }] }
    const following = agent(owner, 'turn', 'Main chat continues'); following.item.id = 'following-parent-output'
    const f = await fixture(t, { suffix: row(4, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item }) + row(5, 'event_msg', following) })
    const view = await f.caughtUp(), snapshot = await f.core.snapshot(), task = snapshot.tasks.find(value => value.id === owner)
    assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
    assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.ok(task.events.some(event => event.text === 'Main chat continues'))
    assert.equal(snapshot.tasks.length, 1)
    assert.equal(task.events.some(event => event.data?.nativeType?.includes('SubAgentActivity')), false)
    assert.doesNotMatch(JSON.stringify(snapshot), /private-child/)
  })
})

test('unverified subagent activity shapes mark gaps while known main-chat material continues', async t => {
  for (const [type, kind] of [['item_completed', 'future'], ['item_completed', undefined], ['item_started', 'started']]) await t.test(type + '/' + kind, async t => {
    const following = agent(owner, 'turn', 'Must remain sealed'); following.item.id = 'sealed-parent-output'
    const f = await fixture(t, { suffix: row(4, 'event_msg', { type, thread_id: owner, turn_id: 'turn', item: { type: 'SubAgentActivity', ...(kind ? { kind } : {}) } }) + row(5, 'event_msg', following) })
    const view = await f.caughtUp(), snapshot = await f.core.snapshot()
    assert.match(view.observation.gap, /^unsupported-item-type:[a-f0-9]{64}$/)
    assert.equal(view.signal, 'gray')
    assert.ok(snapshot.tasks.some(task => task.events.some(event => event.text === 'Must remain sealed')))
    assert.ok(snapshot.tasks.every(task => !task.coverage.complete))
  })
})

test('unknown item gaps survive a later known end and adapter recovery without discarding verified material', async t => {
  for (const type of ['item_started', 'item_completed']) await t.test(type, async t => {
    const f = await fixture(t); await f.caughtUp()
    const first = (await f.core.snapshot()).tasks[0], captures = first.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
    await fs.appendFile(f.path, row(4, 'event_msg', { type, thread_id: owner, turn_id: 'turn', item: { type: 'SyntheticOpaqueItem', query: 'unknown-body' } }) + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view = await f.caughtUp()
    assert.match(view.observation.gap, /^unsupported-item-type:[a-f0-9]{64}$/); assert.equal(view.signal, 'gray')
    await f.reopen(); view = await f.read()
    const after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false); assert.equal(after.status, 'unknown')
    assert.deepEqual(after.events.slice(0, captures.length).map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), captures)
    assert.ok(after.events.some(event => event.kind === 'gap'))
    assert.equal(after.events.some(event => event.data?.nativeType === 'task_complete'), true)
  })
})

test('current-profile completion and cancellation preserve their distinct outcomes in plain and synthetic zstd regression sources', async t => {
  for (const compressed of [false, true]) for (const end of ['task_complete', 'turn_aborted']) await t.test([compressed, end].join(':'), async t => {
    const f = await fixture(t, { compressed, suffix: row(4, 'event_msg', { type: end, turn_id: 'turn' }) }), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0]
    assert.equal(view.observation.phase, end === 'task_complete' ? 'completed' : 'aborted')
    assert.equal(task.status, 'idle'); assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.ok(task.events.some(event => event.data?.nativeType === end))
    assert.equal(task.events.some(event => event.data?.nativeType === (end === 'task_complete' ? 'turn_aborted' : 'task_complete')), false)
    await f.reopen(); await f.read()
    assert.equal((await f.core.snapshot()).tasks[0].events.filter(event => event.data?.nativeType === end).length, 1)
  })
})

test('current-profile composer and nonhuman triggers retain separate authorization semantics', async t => {
  for (const trigger of ['composer', 'goal']) await t.test(trigger, async t => {
    const f = await fixture(t, { records: row(1, 'event_msg', start('turn', trigger)) + row(2, 'event_msg', user(owner, 'turn')) + row(3, 'event_msg', agent(owner, 'turn')) }), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0]
    assert.equal(view.observation.coverage, 'caught-up')
    if (trigger === 'composer') { assert.equal(task.currentRequest, 'Inspect only the synthetic log'); assert.equal(task.events.filter(event => event.kind === 'input' && event.authorizationEligible).length, 1) }
    else { assert.equal(task.currentRequest, ''); assert.equal(task.events.some(event => event.kind === 'input'), false); assert.equal(view.signal, 'gray') }
  })
})

test('an already running native chat publishes activity and summary without any user message', async t => {
  for (const trigger of ['composer', 'queue', 'goal']) await t.test(trigger, async t => {
    const f = await fixture(t, { records: row(1, 'event_msg', start('turn', trigger)) + row(2, 'event_msg', agent(owner, 'turn')), cooldownMs: 60_000 })
    await f.caughtUp()
    let task
    for (let i = 0; i < 100; i++) {
      task = (await f.core.snapshot()).tasks.find(task => task.id === owner)
      if (task.analysis.summary?.partial) break
      await new Promise(resolve => setTimeout(resolve, 5))
    }
    assert.equal(task.currentRequest, '')
    assert.equal(task.events.some(event => event.kind === 'input'), false)
    assert.ok(task.analysis.summary?.partial.evidenceRefs.length)
    assert.match(task.activity.summary, /synthetic log has been inspected/)
    const view = await f.read(), panel = await panelHarness(t)
    assert.equal(view.currentFacts.request, undefined)
    assert.match(view.currentFacts.activity.summary, /synthetic log has been inspected/)
    assert.ok(view.recentModelActivity.summary.text)
    await panel.deliver(view)
    assert.equal(panel.element('#current-facts').hidden, false)
    assert.match(panel.element('#current-activity').textContent, /synthetic log has been inspected/)
    assert.equal(panel.element('#recent-summary-state').textContent, '已总结')
  })
})

test('admitting another supported Host permits verified recovery without refreshing already captured evidence', async t => {
  const f = await fixture(t, { profile: supportedProfile }); await f.caughtUp()
  const before = (await f.core.snapshot()).tasks[0], captures = before.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence])
  f.state.profile = candidateHostProfile; f.state.currentRecordReads = 0
  await f.reopen()
  const view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
  assert.equal(view.signal, 'gray'); assert.notEqual(view.observation.gap, 'cursor-invalid')
  assert.ok(f.state.currentRecordReads > 0)
  assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), captures)
})
