import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { createAnalyzer } from '../src/analysis/index.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

async function inference(t) {
  const requests = [], results = []
  let activeRequests = 0
  const server = createServer(async (request, response) => {
    activeRequests++
    try {
      let raw = ''; for await (const chunk of request) raw += chunk
      const body = JSON.parse(raw); requests.push({ path: request.url, body })
      response.setHeader('content-type', 'application/json')
      if (request.url === '/v1/chat/completions') {
        const evidence = JSON.parse(body.messages[1].content).evidence
        const selectedOptionIds = evidence.flatMap(group => {
          const kind = group.contextRole === 'ancestor-limit' ? 'authorization' : group.origin === 'tool-result' ? 'result' : ['tool-start', 'output'].includes(group.origin) ? 'action' : 'context'
          return group.options.filter(option => option.kind === kind).slice(0, 1).map(option => option.optionId)
        })
        response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds, gaps: [] }) } }] }))
      } else response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }))
    } catch (error) { response.writeHead(500); response.end(JSON.stringify({ error: error.message })) }
    finally { activeRequests-- }
  })
  t.after(async () => {
    await new Promise(resolve => server.close(resolve))
    const connections = await new Promise((resolve, reject) => server.getConnections((error, count) => error ? reject(error) : resolve(count)))
    assert.equal(server.listening, false); assert.equal(activeRequests, 0); assert.equal(connections, 0)
    if (process.env.SENTINEL_NATIVE_WAIT_RECEIPTS) await writeFile(join(process.env.SENTINEL_NATIVE_WAIT_RECEIPTS, 'rendered-time-' + randomUUID() + '.json'), JSON.stringify({ scope: 'production createAnalyzer with controlled local HTTP; hand-written input; no actual model/Host', requests, results, cleanup: { serverListening: server.listening, activeRequests, connections } }, null, 2))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${server.address().port}`
  return { requests, async analyze(input, now) {
    const before = structuredClone(input)
    const result = await createAnalyzer({ llm: { baseUrl, model: 'controlled-extractor' }, jev: { baseUrl, model: 'controlled-judge' }, now: () => now }).analyze(input)
    assert.deepEqual(input, before, 'analysis must not rewrite observation or source references')
    const request = requests.filter(item => item.path === '/v1/systemone').at(-1)
    assert.ok(request, 'actual production JEV HTTP request must occur')
    results.push({ input: before, now, result })
    return { result, request: decodeJevRequest(request.body) }
  } }
}

const epoch = Date.parse('2027-04-19T12:00:00.000Z')
function observations({ inherited = false } = {}) {
  const own = (id, kind, text, captured, occurred, data = {}) => ({ id, taskId: 'worker', taskLifecycleId: 'worker-life', kind, text, capturedAt: epoch + captured, occurredAt: occurred === null ? null : epoch + occurred, data })
  const input = { task: { id: 'worker', lifecycleId: 'worker-life', status: 'running', currentRequestId: 'request' }, source: { descriptor: { name: 'Controlled fixture', gaps: [] } }, coverage: { complete: true, gaps: [] },
    events: [own('request', 'input', 'Inspect the assigned report only.', 0, -2000), own('read-start', 'tool-start', 'read report', 1000, -500, { callId: 'read-call' }), own('read-result', 'tool-result', 'Found the requested entry.', 9000, 8000, { callId: 'read-call', isError: false, exitCode: 0 }), own('progress', 'output', 'Checked the requested entry.', 12000, null)] }
  if (inherited) {
    const ref = { taskId: 'parent', lifecycleId: 'parent-life', eventId: 'parent-policy' }
    input.instructionContext = { binding: { taskId: 'worker', lifecycleId: 'worker-life', inputRef: { taskId: 'worker', lifecycleId: 'worker-life', eventId: 'request' }, phase: 'consumed', origin: 'delegated', originRefs: [], ancestorLimitRefs: [ref] }, gaps: [], inheritedEvents: [{ id: ref.eventId, taskId: ref.taskId, taskLifecycleId: ref.lifecycleId, kind: 'input', text: 'Read only. Do not publish before 2027-04-20T14:00:00Z.', capturedAt: epoch - 30000, occurredAt: epoch - 60000, data: {}, contextRole: 'ancestor-limit', authorizationEligible: true, sourceRef: ref }] }
  }
  return input
}

function assertLossless(input, request, result) {
  const basis = request.state.timeBasis
  assert.equal(basis.originEpochMs, input.events[0].capturedAt)
  assert.equal(Date.parse(basis.originUtc), basis.originEpochMs)
  assert.deepEqual(basis.originRef, { taskId: 'worker', lifecycleId: 'worker-life', eventId: 'request' })
  const original = [...input.events, ...(input.instructionContext?.inheritedEvents ?? [])]
  assert.equal(request.state.events.length, original.length)
  for (const event of request.state.events) {
    const raw = original.find(item => item.id === event.id && item.taskId === event.taskId && item.taskLifecycleId === event.taskLifecycleId)
    assert.ok(raw, 'every rendered event must retain its exact original identity')
    assert.equal(event.capturedOffsetMs + basis.originEpochMs, raw.capturedAt)
    if (raw.occurredAt === null) assert.equal(event.occurredAt, null)
    else assert.equal(event.occurredOffsetMs + basis.originEpochMs, raw.occurredAt)
    assert.deepEqual(event.data, raw.data)
    if (raw.sourceRef) assert.deepEqual(event.sourceRef, raw.sourceRef)
  }
  assert.equal(result.coverage.through, Math.max(...original.map(event => event.capturedAt)))
  assert.equal(request.state.coverage.throughOffsetMs + basis.originEpochMs, result.coverage.through)
  for (const check of result.checks) for (const id of check.evidenceIds) assert.ok(original.some(event => event.id === id))
}

test('rendered chronology losslessly preserves raw times, negative offsets, UTC calendar constraints and source references', async t => {
  const http = await inference(t), input = observations({ inherited: true }), { result, request } = await http.analyze(input, epoch + 20000)
  assertLossless(input, request, result)
  const policy = request.state.events.find(event => event.id === 'parent-policy')
  assert.ok(policy.capturedOffsetMs < 0 && policy.occurredOffsetMs < 0)
  assert.equal(policy.text, input.instructionContext.inheritedEvents[0].text)
  assert.deepEqual(request.state.instruction, input.instructionContext.binding)
  assert.ok(request.state.facts.some(fact => fact.eventId === policy.id && JSON.stringify(fact.sourceRef) === JSON.stringify(policy.sourceRef)))
  assert.equal(result.coverage.complete, true)
  assert.equal(http.requests.length, 2)
})

test('the same relative process renders identically after clock translation while original absolute records remain recoverable', async t => {
  const http = await inference(t), first = observations(), shifted = structuredClone(first), shift = 172_800_065
  for (const event of shifted.events) { event.capturedAt += shift; if (event.occurredAt !== null) event.occurredAt += shift }
  const a = await http.analyze(first, epoch + 20000), b = await http.analyze(shifted, epoch + shift + 20000)
  assertLossless(first, a.request, a.result); assertLossless(shifted, b.request, b.result)
  assert.deepEqual(a.request.state.events, b.request.state.events)
  assert.deepEqual(a.request.state.candidates, b.request.state.candidates)
  assert.deepEqual(a.request.questions, b.request.questions)
  assert.equal(b.request.state.timeBasis.originEpochMs - a.request.state.timeBasis.originEpochMs, shift)
  assert.equal(Date.parse(b.request.state.timeBasis.originUtc) - Date.parse(a.request.state.timeBasis.originUtc), shift)
  assert.equal(b.result.coverage.through - a.result.coverage.through, shift)
  assert.deepEqual(a.result.checks, b.result.checks)
  assert.equal(http.requests.length, 4)
})
