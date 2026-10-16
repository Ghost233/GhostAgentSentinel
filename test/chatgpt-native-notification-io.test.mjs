import test from 'node:test'
import assert from 'node:assert/strict'
import { createNativeNotificationIO, notificationPrefix } from '../chatgpt/native-notifications.mjs'

const identity = 'com.ghostsentinel.notifications'
const packet = (operation, result) => ({ stdout: JSON.stringify({ schemaVersion: 1, bundleId: identity, operation, ok: true, result }) })
test('the native bridge sends bounded JSON on stdin under GhostSentinel identity and never requests permission during settings or submission', async () => {
  const calls = [], owner = 'isolated-installation', prefix = notificationPrefix(owner)
  const io = createNativeNotificationIO({ bundlePath: '/private/tmp/Isolated GhostSentinel.app', installationId: owner, run: async (file, args, input, options) => {
    const request = JSON.parse(input); calls.push({ file, args, request, options })
    return packet(request.operation, request.operation === 'status' ? { authorization: 'denied', alertSetting: 'disabled', notificationCenterSetting: 'disabled', observedAt: 1000 } : { requestId: request.requestId, accepted: true, authorization: 'authorized', observedAt: 1001 })
  } })
  assert.equal((await io.settings()).authorization, 'denied')
  const body = 'Literal $(touch /tmp/not-created) ; `no-shell`'
  const submitted = await io.submit({ requestId: prefix + 'attempt', title: 'GhostSentinel', body, metadata: { notificationId: 'notice', attemptId: 'attempt', sourceKey: 'source', hostRunId: 'host', mainId: null, lifecycleId: null } })
  assert.equal(submitted.accepted, true)
  assert.deepEqual(calls.map(call => call.request.operation), ['status', 'submit'])
  assert.equal(calls[1].request.body, body)
  assert.ok(calls.every(call => call.file.endsWith('.app/Contents/MacOS/GhostSentinelNotifications') && !call.args.some(arg => arg.includes(body))))
  assert.ok(calls.every(call => call.options.maxBuffer <= 262144 && call.options.timeoutMs <= 60000 && call.request.ownerPrefix === prefix))
})

test('the native bridge rejects foreign installation cancellation, wrong bundle identity and oversized protocol output', async () => {
  let calls = 0
  const io = createNativeNotificationIO({ bundlePath: '/private/tmp/GhostSentinel.app', installationId: 'own', run: async () => { calls++; return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.openai.chat', operation: 'status', ok: true, result: {} }) } } })
  await assert.rejects(io.cancel([notificationPrefix('other') + 'attempt']), /installation/)
  assert.equal(calls, 0)
  await assert.rejects(io.settings(), error => error.code === 'native_protocol')
  const big = createNativeNotificationIO({ bundlePath: '/private/tmp/GhostSentinel.app', installationId: 'own', run: async () => ({ stdout: 'x'.repeat(262145) }) })
  await assert.rejects(big.settings(), error => error.code === 'native_protocol')
})
