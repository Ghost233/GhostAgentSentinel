import test from 'node:test'
import assert from 'node:assert/strict'
import { createDiscovery } from '../chatgpt/discovery.mjs'

test('directory failures expose only safe classification and a later successful refresh clears it', async () => {
  let failing = true
  const native = { async listThreads() { if (failing) throw Object.assign(new Error('private path or RPC body must not escape'), { code: 'ECONNREFUSED' }); return { data: [], nextCursor: null } } }
  const directory = createDiscovery({ native, sessionsRoot: '/approved/sessions' })
  try {
    const failure = await directory.refresh()
    assert.equal(failure.state, 'unavailable')
    assert.deepEqual(failure.error, { code: 'ECONNREFUSED', stage: 'metadata', category: 'transport' })
    assert.equal(JSON.stringify(failure).includes('private path'), false)
    assert.equal(JSON.stringify(failure).includes('RPC body'), false)
    failing = false
    const recovered = await directory.refresh()
    assert.equal(recovered.state, 'ready')
    assert.equal(recovered.error, undefined)
  } finally { await directory.close() }
})

test('unknown metadata error codes are classified without free text, while bounded pagination is not a failure', async () => {
  const unavailable = createDiscovery({ native: { async listThreads() { throw Object.assign(new Error('untrusted message'), { code: 'untrusted-code' }) } }, sessionsRoot: '/approved/sessions' })
  let cursor = 0
  const limited = createDiscovery({ native: { async listThreads() { return { data: [], nextCursor: 'page-' + ++cursor } } }, sessionsRoot: '/approved/sessions' })
  try {
    const failure = await unavailable.refresh()
    assert.deepEqual(Object.keys(failure.error).sort(), ['category', 'code', 'codeHash', 'stage'])
    assert.equal(failure.error.code, 'unrecognized_error')
    assert.equal(failure.error.stage, 'metadata')
    assert.equal(failure.error.category, 'other')
    assert.match(failure.error.codeHash, /^[a-f0-9]{64}$/)
    assert.equal((await unavailable.refresh()).error.codeHash, failure.error.codeHash)
    for (const secret of ['untrusted-code', 'untrusted message']) assert.equal(JSON.stringify(failure).includes(secret), false)
    const budget = await limited.refresh()
    assert.ok(budget.gaps.includes('directory-page-budget'))
    assert.equal(budget.error, undefined)
    assert.equal(budget.state, 'ready')
  } finally { await unavailable.close(); await limited.close() }
})
