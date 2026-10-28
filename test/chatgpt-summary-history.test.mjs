import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { resourceMaterial } from './fixtures/chatgpt-plugin-lazy/resource-material.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'
import { withModelSummary } from './helpers/model-summary.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

test('real native material and model HTTP keep a reused summary visible and a later failure readable only as history through MCP', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-summary-history-')), path = join(dir, 'resource-main.jsonl'), token = 'c'.repeat(64), calls = []
  let clock = Date.now() - 10000, failJudgment = false, adapter, core, server, models
  t.after(async () => {
    const failures = []
    for (const close of [() => adapter?.close(), () => core?.close(), () => server?.close(), async () => { if (models) { models.closeAllConnections(); await new Promise(resolve => models.close(resolve)) } }, () => rm(dir, { recursive: true, force: true })]) {
      try { await close() } catch (error) { failures.push(error) }
    }
    if (failures.length) throw new AggregateError(failures, 'summary history fixture cleanup failed')
  })
  models = createServer(async (request, response) => {
    if (request.method === 'GET') return response.writeHead(404, { 'content-type': 'application/json' }).end('{}');
    let raw = ''; for await (const chunk of request) raw += chunk
    const wire = JSON.parse(raw)
    calls.push({ path: request.url, model: wire.model })
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/chat/completions') response.end(JSON.stringify({ model: wire.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(wire, { selectedOptionIds: [], gaps: [] })) } }] }))
    else if (failJudgment) response.writeHead(503).end('{"error":{"code":"controlled-judge-failure"}}')
    else {
      const body = decodeJevRequest(wire)
      response.end(JSON.stringify({ model: wire.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }))
    }
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const baseUrl = 'http://127.0.0.1:' + models.address().port, dataDir = join(dir, 'core')
  server = await startServer({ dataDir, now: () => clock, cooldownMs: 10, leaseMs: 86400000 })
  core = await connectCore({ dataDir })
  const config = { llm: { baseUrl, model: 'summary-first' }, jev: { baseUrl, model: 'judge' } }
  await configureInstallationModels(core, 'chatgpt:summary-history', config)
  await writeFile(path, resourceMaterial({ at: clock - 1000, cwd: dir, producerVersion: supportedProfile.cliVersion }) + JSON.stringify({ type: 'event_msg', timestamp: new Date(clock - 500).toISOString(), payload: { type: 'item_completed', thread_id: 'resource-main', turn_id: 'resource-turn', item: { type: 'AgentMessage', id: 'owned-output', content: [{ type: 'Text', text: 'Read the synthetic log and found a result.' }] } } }) + '\n')
  const thread = { id: 'resource-main', path, cwd: dir, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop' }
  const hostStartedAt = clock - 20000
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'owned-history-host', hostStartedAt, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async listThreads({ archived }) { return { data: archived ? [] : [thread], nextCursor: null } },
    async readThread() { return { thread, anchorTurnId: 'resource-turn' } },
    async close() {},
  }
  adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'summary-history', token, pollMs: 0, collectMaterials: true, now: () => clock })
  const read = async () => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: thread.id } } }) })
    assert.equal(response.status, 200)
    const packet = await response.json(); assert.equal(packet.result.isError, undefined)
    return packet.result.structuredContent
  }
  const until = async predicate => { const deadline = performance.now() + 3000; let view; while (!(await predicate(view = await read()))) { assert.ok(performance.now() < deadline, JSON.stringify(view.analysis)); await delay(2) }; return view }
  const first = await until(view => Boolean(view.analysis.generated)), original = first.analysis.lastSuccessfulSummary
  assert.match(first.analysis.generated.evidence[0].text, /synthetic log/)
  assert.equal(original.configScope.registrationId, 'chatgpt:summary-history')
  clock += 10
  const reused = await until(view => view.analysis.resultDisposition === 'reused')
  assert.deepEqual(reused.analysis.lastSuccessfulSummary, original)
  assert.deepEqual(calls.map(call => call.path), ['/v1/chat/completions', '/v1/systemone'])
  const page = await panelHarness(t)
  await page.deliver(reused)
  assert.equal(page.element('#model-summary').hidden, false)
  assert.match(page.element('#model-status').textContent, /复用/)
  failJudgment = true
  await configureInstallationModels(core, 'chatgpt:summary-history', { ...config, llm: { ...config.llm, model: 'summary-second' } })
  const failed = await until(view => view.analysis.state === 'failed')
  assert.equal(failed.analysis.generated, null)
  assert.deepEqual(failed.analysis.lastSuccessfulSummary, original)
  assert.notEqual(failed.analysis.currentConfigScope.revision, original.configScope.revision)
  await page.refresh(); await page.deliver(failed)
  assert.equal(page.element('#model-summary').hidden, true)
  assert.equal(page.element('#last-summary').hidden, false)
  assert.equal(page.element('#last-summary').open, false)
  assert.match(page.element('#last-summary-evidence').textContent, /synthetic log/)
  assert.deepEqual(calls.map(call => call.path), ['/v1/chat/completions', '/v1/systemone', '/v1/chat/completions', '/v1/systemone'])
})
