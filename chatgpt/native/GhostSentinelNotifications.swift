import AppKit
import Foundation
import UserNotifications
import Darwin

private let identity = "com.ghostsentinel.notifications"
private let inputLimit = 131072
private let outputLimit = 262144
private let clock: () -> Int64 = { Int64(Date().timeIntervalSince1970 * 1000) }
private let center = UNUserNotificationCenter.current()
private let metadataKeys = ["notificationId", "attemptId", "sourceKey", "hostRunId", "mainId", "lifecycleId"]

private func identifier(_ value: Any?, maximum: Int = 256) -> String? {
    guard let text = value as? String, !text.isEmpty, text.utf8.count <= maximum,
          text.unicodeScalars.allSatisfy({ !CharacterSet.controlCharacters.contains($0) }) else { return nil }
    return text
}
private func metadata(_ value: Any?) -> [String: Any]? {
    guard let value = value as? [String: Any], Set(value.keys) == Set(metadataKeys) else { return nil }
    for key in metadataKeys {
        if ["mainId", "lifecycleId"].contains(key), value[key] is NSNull { continue }
        guard identifier(value[key]) != nil else { return nil }
    }
    if value["mainId"] is NSNull && !(value["lifecycleId"] is NSNull) { return nil }
    return value
}
private func authorization(_ status: UNAuthorizationStatus) -> String {
    switch status {
    case .notDetermined: return "not-determined"
    case .denied: return "denied"
    case .authorized: return "authorized"
    case .provisional: return "provisional"
    @unknown default: return "unknown"
    }
}
private func setting(_ value: UNNotificationSetting) -> String {
    switch value { case .notSupported: return "not-supported"; case .disabled: return "disabled"; case .enabled: return "enabled"; @unknown default: return "unknown" }
}
private func settings(_ completion: @escaping ([String: Any]) -> Void) {
    center.getNotificationSettings { value in completion(["authorization": authorization(value.authorizationStatus), "alertSetting": setting(value.alertSetting), "notificationCenterSetting": setting(value.notificationCenterSetting), "observedAt": clock()]) }
}
private func owned(_ id: String, _ prefix: String) -> Bool { id.hasPrefix(prefix) && id.utf8.count <= 200 && id.count > prefix.count }
private func prefixValid(_ value: String) -> Bool { value.range(of: "^ghostsentinel:[a-f0-9]{24}:$", options: .regularExpression) != nil }

// User actions are safe identity facts only. No title, body, or transport token is stored.
private let journalDirectory = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("Library/Application Support/GhostSentinel/NotificationFacts", isDirectory: true)
private let journalURL = journalDirectory.appendingPathComponent("actions.json")
private let journalLock = NSLock()
private func journalRead() throws -> (events: [[String: Any]], gap: Bool) {
    guard FileManager.default.fileExists(atPath: journalURL.path) else { return ([], false) }
    var info = stat()
    guard lstat(journalURL.path, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG, info.st_uid == getuid(), info.st_mode & 0o077 == 0 else { throw NSError(domain: identity, code: 1) }
    let handle = try FileHandle(forReadingFrom: journalURL); defer { try? handle.close() }
    let bytes = try handle.read(upToCount: 1048577) ?? Data()
    guard bytes.count <= 1048576, let value = try JSONSerialization.jsonObject(with: bytes) as? [String: Any], let events = value["events"] as? [[String: Any]], events.count <= 256 else { throw NSError(domain: identity, code: 2) }
    let cutoff = clock() - 86_400_000
    return (events.filter { ($0["observedAt"] as? NSNumber)?.int64Value ?? 0 > cutoff }, value["historyGap"] as? Bool ?? false)
}
private func appendAction(_ value: [String: Any]) throws {
    journalLock.lock(); defer { journalLock.unlock() }
    var stored = try journalRead(); stored.events.append(value)
    if stored.events.count > 256 { stored.events = Array(stored.events.suffix(256)); stored.gap = true }
    try FileManager.default.createDirectory(at: journalDirectory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    let bytes = try JSONSerialization.data(withJSONObject: ["schemaVersion": 1, "events": stored.events, "historyGap": stored.gap])
    guard bytes.count <= 1048576 else { throw NSError(domain: identity, code: 3) }
    let temporary = journalDirectory.appendingPathComponent(UUID().uuidString + ".tmp")
    try bytes.write(to: temporary, options: .withoutOverwriting)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: temporary.path)
    if rename(temporary.path, journalURL.path) != 0 { try? FileManager.default.removeItem(at: temporary); throw NSError(domain: identity, code: 4) }
}

private final class Delegate: NSObject, NSApplicationDelegate, UNUserNotificationCenterDelegate {
    func userNotificationCenter(_ center: UNUserNotificationCenter, willPresent notification: UNNotification, withCompletionHandler completion: @escaping (UNNotificationPresentationOptions) -> Void) { completion([.banner, .list, .sound]) }
    func userNotificationCenter(_ center: UNUserNotificationCenter, didReceive response: UNNotificationResponse, withCompletionHandler completion: @escaping () -> Void) {
        defer { completion(); if !CommandLine.arguments.contains("--stdio") { DispatchQueue.main.async { NSApp.terminate(nil) } } }
        let request = response.notification.request
        guard request.identifier.range(of: "^ghostsentinel:[a-f0-9]{24}:.+", options: .regularExpression) != nil,
              let facts = metadata(request.content.userInfo) else { return }
        let action: String
        if response.actionIdentifier == UNNotificationDefaultActionIdentifier { action = "default" }
        else if response.actionIdentifier == UNNotificationDismissActionIdentifier { action = "dismiss" }
        else { return }
        try? appendAction(["observationId": UUID().uuidString, "requestId": request.identifier, "action": action, "observedAt": clock(), "metadata": facts])
    }
}

private func reply(_ operation: String, result: [String: Any]? = nil, error: String? = nil) {
    var packet: [String: Any] = ["schemaVersion": 1, "bundleId": identity, "operation": operation, "ok": error == nil]
    if let error = error { packet["error"] = ["code": error] } else { packet["result"] = result ?? [:] }
    guard let bytes = try? JSONSerialization.data(withJSONObject: packet), bytes.count <= outputLimit else { exit(2) }
    FileHandle.standardOutput.write(bytes); FileHandle.standardOutput.write(Data([10]))
    DispatchQueue.main.async { NSApp.terminate(nil) }
}
private func snapshot(_ prefix: String, completion: @escaping ([String: Any]) -> Void) {
    center.getPendingNotificationRequests { requests in
        center.getDeliveredNotifications { notifications in
            let pending = requests.filter { owned($0.identifier, prefix) }
            let delivered = notifications.filter { owned($0.request.identifier, prefix) }
            journalLock.lock(); let journal = try? journalRead(); journalLock.unlock()
            let actions = journal?.events.filter { owned($0["requestId"] as? String ?? "", prefix) } ?? []
            let deliveredFacts: [[String: Any]] = delivered.prefix(32).compactMap { item in
                guard let facts = metadata(item.request.content.userInfo) else { return nil }
                return ["requestId": item.request.identifier, "deliveryDate": Int64(item.date.timeIntervalSince1970 * 1000), "metadata": facts]
            }
            completion(["pending": pending.prefix(100).map { ["requestId": $0.identifier] }, "pendingHasMore": pending.count > 100, "delivered": deliveredFacts, "deliveredHasMore": delivered.count > 32, "actions": Array(actions.suffix(32)), "historyGap": (journal?.gap ?? false) || journal == nil || actions.count > 32 || delivered.count > 32, "observedAt": clock()])
        }
    }
}
private func execute(_ request: [String: Any]) {
    let operation = request["operation"] as? String ?? "invalid"
    guard request["schemaVersion"] as? Int == 1, let prefix = request["ownerPrefix"] as? String, prefixValid(prefix), Bundle.main.bundleIdentifier == identity else { reply(operation, error: "native_protocol"); return }
    let fields: [String: Set<String>] = ["status": [], "authorize": [], "snapshot": [], "submit": ["requestId", "title", "body", "metadata"], "cancel": ["requestIds"]]
    guard let allowed = fields[operation], Set(request.keys).isSubset(of: allowed.union(["schemaVersion", "operation", "ownerPrefix"])) else { reply(operation, error: "native_protocol"); return }
    switch operation {
    case "status": settings { reply(operation, result: $0) }
    case "authorize": center.requestAuthorization(options: [.alert, .sound]) { _, error in if error != nil { reply(operation, error: "native_unavailable") } else { settings { reply(operation, result: $0) } } }
    case "snapshot": snapshot(prefix) { reply(operation, result: $0) }
    case "cancel":
        guard let ids = request["requestIds"] as? [String], ids.count <= 100, ids.allSatisfy({ owned($0, prefix) }) else { reply(operation, error: "native_protocol"); return }
        center.removePendingNotificationRequests(withIdentifiers: ids)
        center.getPendingNotificationRequests { requests in reply(operation, result: ["pendingIds": requests.filter { ids.contains($0.identifier) }.map { $0.identifier }, "observedAt": clock()]) }
    case "submit":
        guard let id = identifier(request["requestId"], maximum: 200), owned(id, prefix), let title = request["title"] as? String, !title.isEmpty, title.count <= 200, let body = request["body"] as? String, body.count <= 1200, let facts = metadata(request["metadata"]) else { reply(operation, error: "native_protocol"); return }
        settings { permission in
            guard ["authorized", "provisional"].contains(permission["authorization"] as? String ?? "") else { reply(operation, result: ["requestId": id, "accepted": false, "authorization": permission["authorization"] ?? "unknown", "observedAt": clock()]); return }
            snapshot(prefix) { observed in
                let pending = observed["pending"] as? [[String: Any]] ?? [], delivered = observed["delivered"] as? [[String: Any]] ?? []
                if (pending + delivered).contains(where: { $0["requestId"] as? String == id }) { reply(operation, result: ["requestId": id, "accepted": true, "alreadyPresent": true, "authorization": permission["authorization"] ?? "unknown", "observedAt": clock()]); return }
                guard observed["pendingHasMore"] as? Bool != true && observed["deliveredHasMore"] as? Bool != true else { reply(operation, error: "native_protocol"); return }
                let category = UNNotificationCategory(identifier: "GhostSentinelObservation", actions: [], intentIdentifiers: [], options: [.customDismissAction])
                center.setNotificationCategories([category])
                let content = UNMutableNotificationContent(); content.title = title; content.body = body; content.userInfo = facts; content.categoryIdentifier = category.identifier
                center.add(UNNotificationRequest(identifier: id, content: content, trigger: nil)) { error in
                    if error != nil { reply(operation, error: "native_submission") }
                    else { reply(operation, result: ["requestId": id, "accepted": true, "authorization": permission["authorization"] ?? "unknown", "observedAt": clock()]) }
                }
            }
        }
    default: reply(operation, error: "native_protocol")
    }
}

@main private struct Main {
    static func main() {
        let app = NSApplication.shared; app.setActivationPolicy(.prohibited)
        let delegate = Delegate(); app.delegate = delegate; center.delegate = delegate
        if CommandLine.arguments.contains("--stdio") {
            do {
                var bytes = Data()
                while true {
                    let part = try FileHandle.standardInput.read(upToCount: min(4096, inputLimit + 1 - bytes.count)) ?? Data()
                    if part.isEmpty { break }; bytes.append(part)
                    if bytes.count > inputLimit { throw NSError(domain: identity, code: 5) }
                }
                guard let request = try JSONSerialization.jsonObject(with: bytes) as? [String: Any] else { throw NSError(domain: identity, code: 6) }
                DispatchQueue.main.async { execute(request) }
            } catch { DispatchQueue.main.async { reply("invalid", error: "native_protocol") } }
        } else { DispatchQueue.main.asyncAfter(deadline: .now() + 5) { NSApp.terminate(nil) } }
        app.run(); withExtendedLifetime(delegate) {}
    }
}
