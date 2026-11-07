import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

test('the compiled notification response opens only its own chat on a default click and settles before exit', { skip: process.platform !== 'darwin' && 'requires the macOS Swift toolchain' }, async t => {
  const run = promisify(execFile), dir = await mkdtemp(join(tmpdir(), 'sentinel-notification-navigation-')), cache = join(dir, 'cache'), binary = join(dir, 'navigation'), fixture = join(dir, 'navigation.swift')
  t.after(() => rm(dir, { recursive: true, force: true })); await mkdir(cache)
  const source = await readFile(new URL('../chatgpt/native/GhostSentinelNotifications.swift', import.meta.url), 'utf8')
  const metadata = source.slice(source.indexOf('private let metadataKeys ='), source.indexOf('private func authorization('))
  const delegate = source.slice(source.indexOf('private final class Delegate:'), source.indexOf('private func reply('))
  assert.ok(metadata.includes('private func metadata(') && delegate.includes('didReceive response:'))
  await writeFile(fixture, `import Foundation
import Darwin
private let UNNotificationDefaultActionIdentifier = "default-action"
private let UNNotificationDismissActionIdentifier = "dismiss-action"
private struct UNNotificationPresentationOptions: OptionSet {
    let rawValue: Int
    static let banner = Self(rawValue: 1), list = Self(rawValue: 2), sound = Self(rawValue: 4)
}
private protocol NSApplicationDelegate {}
private protocol UNUserNotificationCenterDelegate {}
private final class UNUserNotificationCenter {}
private struct Content { let userInfo: [String: Any] }
private struct Request { let identifier: String; let content: Content }
private struct UNNotification { let request: Request }
private struct UNNotificationResponse { let notification: UNNotification; let actionIdentifier: String }
private var timeline: [String] = []
private final class Application { func terminate(_ sender: Any?) { timeline.append("terminate") } }
private let NSApp = Application()
private final class NSWorkspace {
    static let shared = NSWorkspace()
    var urls: [URL] = [], accepted = true
    func open(_ url: URL) -> Bool { timeline.append("open"); urls.append(url); return accepted }
}
private final class Journal {
    var events: [[String: Any]] = [], unavailable = false
    func append(_ event: [String: Any]) throws {
        if unavailable { throw NSError(domain: "journal-fixture", code: 1) }
        events.append(event)
    }
}
private let actionJournal = Journal(), clock: () -> Int64 = { 1000 }
${metadata}
${delegate}
private func check(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() { throw NSError(domain: "navigation-fixture", code: 1, userInfo: [NSLocalizedDescriptionKey: message]) }
}
@main private struct NavigationChecks {
    static func main() {
        do { try run(); print("PASS native notification navigation") }
        catch { fputs("FAIL \\(error)\\n", stderr); exit(1) }
    }
    static func run() throws {
        let thread = "01a1b639-5934-7381-b647-59316cb2e305", other = "01a1b639-5934-7381-b647-59316cb2e999"
        let facts: [String: Any] = ["notificationId": "notice", "attemptId": "attempt", "sourceKey": "source", "hostRunId": "host", "mainId": thread, "lifecycleId": "run"]
        let owned = "ghostsentinel:0123456789abcdef01234567:attempt", delegate = Delegate(), center = UNUserNotificationCenter()
        func receive(_ action: String, _ metadata: [String: Any], id: String = owned) {
            timeline = []; NSWorkspace.shared.urls = []
            delegate.userNotificationCenter(center, didReceive: UNNotificationResponse(notification: UNNotification(request: Request(identifier: id, content: Content(userInfo: metadata))), actionIdentifier: action)) { timeline.append("completion") }
            RunLoop.main.run(until: Date().addingTimeInterval(0.02))
        }
        receive(UNNotificationDefaultActionIdentifier, facts)
        try check(NSWorkspace.shared.urls.map(\\.absoluteString) == ["codex://threads/" + thread], "click must open the notification's original chat")
        try check(timeline.filter { $0 == "completion" }.count == 1 && timeline.last == "terminate" && timeline.firstIndex(of: "open")! < timeline.firstIndex(of: "terminate")!, "open and complete before the short-lived component exits")
        try check(actionJournal.events.last?["action"] as? String == "default", "preserve click facts")
        var another = facts; another["mainId"] = other
        receive(UNNotificationDefaultActionIdentifier, another)
        try check(NSWorkspace.shared.urls.map(\\.absoluteString) == ["codex://threads/" + other], "another notification must use its own chat instead of the previous one")
        receive(UNNotificationDismissActionIdentifier, facts)
        try check(NSWorkspace.shared.urls.isEmpty && actionJournal.events.last?["action"] as? String == "dismiss", "dismiss records an action without navigating")
        for invalid: Any in [NSNull(), "", "new", "../settings", thread + "?prompt=unexpected", "https://example.test", thread + "/other", 7] {
            var value = facts; value["mainId"] = invalid; value["lifecycleId"] = NSNull()
            receive(UNNotificationDefaultActionIdentifier, value)
            try check(NSWorkspace.shared.urls.isEmpty && timeline.filter { $0 == "completion" }.count == 1, "unconfirmed or invalid identity must not navigate or create a chat")
        }
        for action in ["unknown-action", UNNotificationDefaultActionIdentifier] {
            receive(action, facts, id: "foreign:attempt")
            try check(NSWorkspace.shared.urls.isEmpty, "foreign request must not open a chat")
        }
        var extra = facts; extra["url"] = "https://example.test"
        receive(UNNotificationDefaultActionIdentifier, extra)
        try check(NSWorkspace.shared.urls.isEmpty, "additional metadata must not introduce a navigation target")
        receive("unknown-action", facts)
        try check(NSWorkspace.shared.urls.isEmpty, "unknown action must not navigate")
        actionJournal.unavailable = true
        receive(UNNotificationDefaultActionIdentifier, facts)
        try check(NSWorkspace.shared.urls.count == 1, "journal IO failure must not block an explicit chat click")
        NSWorkspace.shared.accepted = false
        receive(UNNotificationDefaultActionIdentifier, facts)
        try check(NSWorkspace.shared.urls.count == 1 && timeline.last == "terminate", "an unavailable URL handler must not retry or leak the notification component")
    }
}
`)
  await run('/usr/bin/xcrun', ['swiftc', '-swift-version', '5', '-parse-as-library', '-module-cache-path', cache, fixture, '-o', binary], { timeout: 60000, maxBuffer: 1048576 })
  const result = await run(binary, [], { timeout: 5000, maxBuffer: 65536 })
  assert.match(result.stdout, /PASS native notification navigation/)
})
