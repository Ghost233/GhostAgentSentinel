import test from 'node:test'
import assert from 'node:assert/strict'
import { sanitizeObservation } from '../src/privacy.mjs'
import { normalizeBatch } from '../src/protocol.mjs'

test('explicit authentication material is omitted before excerpting or persistence', () => {
  for (const text of [
    "curl -H 'Authorization: Bearer test-private-value' http://localhost",
    '{"Authorization":"Basic dGVzdA=="}',
    "curl -H 'Authorization: Digest test-private-value' http://localhost",
    'llama-server --api-key test-private-value',
    'curl -u user:test-private-value http://localhost',
    'curl http://user:test-private-value@localhost/',
    'export OPENAI_API_KEY=test-private-value',
    '{"access_token": "test-private-value"}',
    '{"password": "test-private-value"}',
    'AWS_SECRET_ACCESS_KEY=test-private-value',
    'GET /?token=test-private-value',
    'cat ~/.codex/auth.json',
    'cat ~/.aws/credentials',
    'cat .env',
    '-----BEGIN OPENSSH PRIVATE KEY-----',
  ]) {
    const result = sanitizeObservation(text)
    assert.equal(result.redacted, true)
    assert.equal(result.text.includes('test-private-value'), false)
  }
  assert.deepEqual(sanitizeObservation('Add an authentication error log'), { text: 'Add an authentication error log', redacted: false })
  const batch = normalizeBatch({ events: [{ id: 'e', taskId: 'a', taskLifecycleId: 'l', occurredAt: null, capturedAt: 100, kind: 'tool-result',
    text: 'unknown credential file content', data: { command: 'cat ~/.codex/auth.json' } }] })
  assert.equal(batch.events[0].data.redacted, true)
  assert.equal(batch.events[0].text.includes('credential file content'), false)
})

test('omission flags combine across fields and caller false cannot claim complete evidence', () => {
  for (const fields of [
    { evidence: 'Authorization: Bearer test-private-value', data: { redacted: false } },
    { data: { errorCode: 'password=test-private-value', redacted: false } },
  ]) {
    const batch = normalizeBatch({ events: [{ id: 'e', taskId: 'a', taskLifecycleId: 'l', occurredAt: null, capturedAt: 100, kind: 'tool-result', text: 'Failed', ...fields }] })
    assert.equal(batch.events[0].data.redacted, true)
    assert.equal(JSON.stringify(batch).includes('test-private-value'), false)
  }
  assert.deepEqual(sanitizeObservation('The password reset test passed'), { text: 'The password reset test passed', redacted: false })
})
