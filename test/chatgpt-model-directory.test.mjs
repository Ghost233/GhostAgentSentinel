import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { readModelDirectory } from '../chatgpt/model-directory.mjs'

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
  assert.deepEqual(calls, [['GET', '/v1/models']])
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
