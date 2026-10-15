import test from 'node:test'
import assert from 'node:assert/strict'
import { normalizeSource, normalizeBatch, normalizeAnalysisConfig } from '../src/protocol.mjs'

test('ingress preserves event identity and first capture time, bounds excerpts and drops unrelated data', () => {
  const source = normalizeSource({ registrationId: 'profile', hostRunId: 'run', name: 'DSH', token: 'secret' })
  assert.deepEqual(source, { registrationId: 'profile', hostRunId: 'run', name: 'DSH' })
  const batch = normalizeBatch({ events: [{ id: 'e1', taskId: 'a', taskLifecycleId: 'life', kind: 'output', occurredAt: null, capturedAt: 123,
    text: 'a'.repeat(300), data: { exitCode: 1, authorization: 'secret' } }], tasks: [] })
  assert.equal(batch.events[0].capturedAt, 123)
  assert.equal(batch.events[0].text.length, 240)
  assert.deepEqual(batch.events[0].data, { exitCode: 1, truncated: true })
  assert.throws(() => normalizeBatch({ events: [{ id: 'e', taskId: 'a', kind: 'input', text: '', occurredAt: null, capturedAt: 123 }] }), /taskLifecycleId/)
})

test('model configuration accepts public loopback endpoints and rejects network or credential URLs', () => {
  assert.deepEqual(normalizeAnalysisConfig({ llm: { baseUrl: 'http://127.0.0.1:54841', model: 'qwen' }, jev: {} }),
    { llm: { baseUrl: 'http://127.0.0.1:54841', model: 'qwen' }, jev: {} })
  for (const baseUrl of ['https://example.org', 'http://user:secret@localhost:1234', 'file:///tmp/a', 'http://127.0.0.2:1234', 'http://127.0.0.1:1234/custom']) {
    assert.throws(() => normalizeAnalysisConfig({ llm: { baseUrl, model: 'qwen' } }), /loopback/)
  }
  assert.throws(() => normalizeAnalysisConfig({ llm: { baseUrl: 'http://localhost:1234', model: 'x', apiKey: 'secret' } }), /unsupported/)
})
