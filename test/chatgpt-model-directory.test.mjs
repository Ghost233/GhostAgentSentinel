import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { readModelDirectory } from '../chatgpt/model-directory.mjs'

test('GMD native models retain both explicitly declared text and decision capabilities', async t => {
  const calls = [], dual = 'tiel-coder-35b-a3b-apex-i-miniplus-v2-1'
  const server = createServer((request, response) => {
    calls.push([request.method, request.url])
    const rows = [dual, 'judge-only', 'unloaded-text'].map(id => ({ id, source: 'native', owned_by: 'ghostmodeldeck' }))
    response.end(JSON.stringify(request.url === '/v1/models' ? { data: [...rows, { id: 'text', type: 'llm' }, { id: 'embedding', type: 'embedding' }, { id: 'unknown' }] } : {
      source: 'ghost-model-deck', models: [
        { key: dual, type: 'llm', source: 'ghost-model-deck', loaded_instances: [{ id: dual }] },
        { key: dual, type: 'decision', source: 'ghost-model-deck', loaded_instances: [{ id: dual }] },
        { key: 'judge-only', type: 'decision', source: 'ghost-model-deck', loaded_instances: [{ id: 'judge-only' }] },
        { key: 'unloaded-text', type: 'llm', source: 'ghost-model-deck', loaded_instances: [] },
        { key: 'unlisted', type: 'llm', source: 'ghost-model-deck', loaded_instances: [{ id: 'unlisted' }] },
      ],
    }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const result = await readModelDirectory(`http://127.0.0.1:${server.address().port}`)
  assert.equal(result.state, 'online')
  assert.equal(result.inferenceVerified, false)
  assert.deepEqual(result.models, [
    { id: dual, role: 'multiple', roles: ['summary', 'jev'] },
    { id: 'judge-only', role: 'jev' }, { id: 'unloaded-text', role: 'jev' },
    { id: 'text', role: 'summary' }, { id: 'embedding', role: 'embedding' }, { id: 'unknown', role: 'unknown' },
  ])
  assert.deepEqual(calls, [['GET', '/v1/models'], ['GET', '/api/v1/models']])
})

for (const kind of ['missing', 'invalid', 'foreign', 'over-budget']) test('unconfirmed GMD capacity metadata never guesses text capability: ' + kind, async t => {
  const server = createServer((request, response) => {
    if (request.url === '/v1/models') return response.end(JSON.stringify({ data: [{ id: 'native-only', source: 'native', owned_by: 'ghostmodeldeck' }], ...(kind === 'over-budget' ? { padding: 'x'.repeat(130000) } : {}) }))
    if (kind === 'missing') return response.writeHead(404).end('{}')
    if (kind === 'invalid') return response.end('{')
    response.end(JSON.stringify({ source: kind === 'foreign' ? 'other' : 'ghost-model-deck', models: [{ key: 'native-only', type: 'llm', source: 'ghost-model-deck', loaded_instances: [{ id: 'native-only' }] }], ...(kind === 'over-budget' ? { padding: 'x'.repeat(140000) } : {}) }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const result = await readModelDirectory(`http://127.0.0.1:${server.address().port}`)
  assert.equal(result.state, 'online')
  assert.equal(result.inferenceVerified, false)
  assert.deepEqual(result.models, [{ id: 'native-only', role: 'jev' }])
})

test('non-GMD declarations can be multi-capable without probing a provider-specific endpoint', async t => {
  const calls = [], server = createServer((request, response) => {
    calls.push([request.method, request.url])
    response.end(JSON.stringify({ data: [
      { id: 'dual', type: 'llm' }, { id: 'dual', type: 'decision' },
      { id: 'judge', type: 'decision' }, { id: 'text', type: 'llm' },
      { id: 'embedding', type: 'embedding' }, { id: 'embedding', type: 'llm' }, { id: 'unknown' },
    ] }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const result = await readModelDirectory(`http://127.0.0.1:${server.address().port}`)
  assert.deepEqual(result.models, [
    { id: 'dual', role: 'multiple', roles: ['summary', 'jev'] }, { id: 'judge', role: 'jev' },
    { id: 'text', role: 'summary' }, { id: 'embedding', role: 'embedding' }, { id: 'unknown', role: 'unknown' },
  ])
  assert.deepEqual(calls, [['GET', '/v1/models']])
})

test('model selection reads only metadata and classifies declared GMD roles without inference', async t => {
  const calls = [], server = createServer((request, response) => {
    calls.push([request.method, request.url])
    response.setHeader('content-type', 'application/json')
    response.end(JSON.stringify({ data: [
      { id: 'summary', owned_by: 'ghostmodeldeck' }, { id: 'judge', source: 'native', owned_by: 'ghostmodeldeck' },
      { id: 'embedding', type: 'embedding' }, { id: '<unknown-model>' },
    ] }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const result = await readModelDirectory('http://127.0.0.1:' + server.address().port)
  assert.equal(result.state, 'online')
  assert.equal(result.inferenceVerified, false)
  assert.deepEqual(result.models, [{ id: 'summary', role: 'summary' }, { id: 'judge', role: 'jev' }, { id: 'embedding', role: 'embedding' }, { id: '<unknown-model>', role: 'unknown' }])
  assert.deepEqual(calls, [['GET', '/v1/models'], ['GET', '/api/v1/models']])
})

test('unsupported model directories remain unconfirmed and nonlocal addresses are rejected', async t => {
  const server = createServer((_request, response) => { response.writeHead(404); response.end('{}') })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const result = await readModelDirectory('http://127.0.0.1:' + server.address().port)
  assert.equal(result.state, 'unconfirmed')
  assert.equal(result.online, true)
  assert.deepEqual(result.models, [])
  await assert.rejects(readModelDirectory('https://example.com'), /本机回环/)
})

test('oversized metadata responses do not become a successful model check', async t => {
  const server = createServer((_request, response) => response.end(JSON.stringify({ data: [], padding: 'x'.repeat(300_000) })))
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  const result = await readModelDirectory('http://127.0.0.1:' + server.address().port)
  assert.equal(result.state, 'unconfirmed')
  assert.equal(result.error.code, 'model_directory_limit')
})
