import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
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

test('uninstall clears only its installation and preserves delivered OS history without requesting permission', async () => {
  const calls = [], prefix = notificationPrefix('own-uninstall')
  const io = createNativeNotificationIO({ bundlePath: '/private/tmp/Owned Notifications.app', installationId: 'own-uninstall', run: async (file, args, input) => {
    const value = JSON.parse(input); calls.push(value)
    return packet('purge-installation', { complete: false, requestedCancellations: 100, remainingPending: 2, clearedActions: 3, deliveredPreserved: true, observedAt: 1200 })
  } })
  const result = await io.purgeInstallation()
  assert.deepEqual(calls, [{ schemaVersion: 1, operation: 'purge-installation', ownerPrefix: prefix }])
  assert.equal(result.complete, false)
  assert.equal(result.remainingPending, 2)
  assert.equal(result.clearedActions, 3)
  assert.equal(result.deliveredPreserved, true)
})

test('uninstall never treats contradictory or unbounded cancellation results as completed cleanup', async () => {
  for (const change of [{ complete: true }, { requestedCancellations: 101 }, { remainingPending: -1 }, { clearedActions: 257 }, { deliveredPreserved: false }]) {
    const io = createNativeNotificationIO({ bundlePath: '/private/tmp/Owned Notifications.app', installationId: 'own-uninstall', run: async () => packet('purge-installation', { complete: false, requestedCancellations: 100, remainingPending: 2, clearedActions: 3, deliveredPreserved: true, observedAt: 1200, ...change }) })
    await assert.rejects(io.purgeInstallation(), error => error.code === 'native_protocol')
    io.close()
  }
})

test('the compiled Swift authorize operation returns known denial without another system request and preserves other outcomes', { skip: process.platform !== 'darwin' && 'requires the macOS Swift toolchain' }, async t => {
  const run = promisify(execFile), dir = await mkdtemp(join(tmpdir(), 'sentinel-swift-authorization-')), cache = join(dir, 'cache'), binary = join(dir, 'authorization'), fixture = join(dir, 'authorization.swift')
  t.after(() => rm(dir, { recursive: true, force: true })); await mkdir(cache)
  const source = await readFile(new URL('../chatgpt/native/GhostSentinelNotifications.swift', import.meta.url), 'utf8'), start = source.indexOf('case "authorize":'), end = source.indexOf('case "snapshot":', start)
  assert.ok(start >= 0 && end > start)
  const body = source.slice(start + 'case "authorize":'.length, end)
  await writeFile(fixture, `import Foundation
private enum PermissionOption { case alert, sound }
private final class FakeCenter {
    var requests = 0
    var error: Error? = nil
    func requestAuthorization(options: [PermissionOption], completionHandler: (Bool, Error?) -> Void) {
        requests += 1; completionHandler(false, error)
    }
}
private let center = FakeCenter()
private var samples: [[String: Any]] = [], reads = 0, replies = 0
private var result: [String: Any]? = nil, failure: String? = nil
private func settings(_ completion: @escaping ([String: Any]) -> Void) { reads += 1; completion(samples.removeFirst()) }
private func reply(_ operation: String, result value: [String: Any]? = nil, error: String? = nil) { replies += 1; result = value; failure = error }
private func executeAuthorization() { let operation = "authorize"
${body}
}
private func prepare(_ before: String, _ after: String, error: Error? = nil) {
    samples = [before, after].enumerated().map { ["authorization": $0.element, "alertSetting": "enabled", "notificationCenterSetting": "disabled", "observedAt": $0.offset + 1000] }
    center.requests = 0; center.error = error; reads = 0; replies = 0; result = nil; failure = nil
}
private func check(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() { throw NSError(domain: "authorization-fixture", code: 1, userInfo: [NSLocalizedDescriptionKey: message]) }
}
@main private struct AuthorizationChecks {
    static func main() {
        do { try run(); print("PASS native authorization boundaries") }
        catch { fputs("FAIL \\(error)\\n", stderr); exit(1) }
    }
    static func run() throws {
        prepare("denied", "authorized", error: NSError(domain: "synthetic-system-error", code: 1)); executeAuthorization()
        try check(center.requests == 0 && reads == 1 && replies == 1, "known denial must not request system authorization again")
        try check(failure == nil && result?["authorization"] as? String == "denied" && result?["observedAt"] as? Int == 1000 && result?["alertSetting"] as? String == "enabled" && result?["notificationCenterSetting"] as? String == "disabled", "return the observed denied settings unchanged")
        for initial in ["not-determined", "authorized", "provisional", "unknown"] {
            prepare(initial, "authorized"); executeAuthorization()
            try check(center.requests == 1 && reads == 2 && replies == 1 && failure == nil && result?["authorization"] as? String == "authorized" && result?["observedAt"] as? Int == 1001, "other states retain the request and fresh settings path")
        }
        for domain in ["UNErrorDomain", "NSCocoaErrorDomain", "unknown-error"] {
            prepare("not-determined", "denied", error: NSError(domain: domain, code: 1)); executeAuthorization()
            try check(center.requests == 1 && reads == 1 && replies == 1 && failure == "native_unavailable" && result == nil, "a callback error must stay unavailable")
        }
        prepare("unknown", "unknown"); executeAuthorization()
        try check(center.requests == 1 && reads == 2 && failure == nil && result?["authorization"] as? String == "unknown", "false grant and unknown settings must not become denied")
    }
}
`)
  await run('/usr/bin/xcrun', ['swiftc', '-swift-version', '5', '-parse-as-library', '-module-cache-path', cache, fixture, '-o', binary], { timeout: 60000, maxBuffer: 1048576 })
  const result = await run(binary, [], { timeout: 5000, maxBuffer: 65536 })
  assert.match(result.stdout, /PASS native authorization boundaries/)
})

test('authorization remains one bounded invocation while a pending precheck is cancelled', async () => {
  let began, invocation, calls = 0
  const started = new Promise(resolve => { began = resolve })
  const io = createNativeNotificationIO({ bundlePath: '/private/tmp/Owned Notifications.app', installationId: 'permission-check', run: (file, args, input, options) => new Promise((resolve, reject) => {
    calls++; invocation = { args, request: JSON.parse(input), options }; began()
    options.signal.addEventListener('abort', () => reject(Object.assign(new Error('synthetic cancelled operation'), { code: 'native_unavailable' })), { once: true })
  }) })
  const pending = io.requestAuthorization(); await started
  try {
    await assert.rejects(io.settings(), { code: 'native_unavailable' })
    assert.equal(calls, 1); assert.equal(invocation.request.operation, 'authorize'); assert.deepEqual(invocation.args, ['--stdio'])
    assert.equal(invocation.options.timeoutMs, 60000); assert.equal(invocation.options.maxBuffer, 262144)
  } finally { io.close(); await assert.rejects(pending, { code: 'native_unavailable' }) }
  assert.equal(invocation.options.signal.aborted, true)
})
