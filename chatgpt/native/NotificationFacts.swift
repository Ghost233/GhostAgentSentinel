import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

enum NotificationInventory {
    static func alreadyPresent(_ id: String, prefix: String, pending: [String], delivered: [String]) throws -> Bool {
        guard id.hasPrefix(prefix), id.utf8.count <= 200, id.count > prefix.count else { throw NSError(domain: "com.ghostsentinel.notifications", code: 1) }
        let pending = pending.filter { $0.hasPrefix(prefix) }, delivered = delivered.filter { $0.hasPrefix(prefix) }
        return pending.contains(id) || delivered.contains(id)
    }
}

// The fixed action journal owns its lock and atomic replacement. It stores only
// safe action identity facts; it never reads or changes OS notification history.
final class NotificationActionJournal {
    private let url: URL
    private let clock: () -> Int64
    private let lock = NSLock()
    init(url: URL, clock: @escaping () -> Int64) { self.url = url; self.clock = clock }
    private func withJournalLock<T>(_ operation: () throws -> T) throws -> T {
        lock.lock(); defer { lock.unlock() }
        let directory = url.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        var info = stat()
        guard lstat(directory.path, &info) == 0, (info.st_mode & S_IFMT) == S_IFDIR, info.st_uid == getuid(), info.st_mode & 0o077 == 0 else { throw NSError(domain: "com.ghostsentinel.notifications", code: 5) }
        let descriptor = open(directory.appendingPathComponent(".actions.lock").path, O_CREAT | O_RDWR | O_NOFOLLOW, 0o600)
        guard descriptor >= 0 else { throw NSError(domain: "com.ghostsentinel.notifications", code: 6) }
        defer { close(descriptor) }
        guard fstat(descriptor, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG, info.st_uid == getuid(), info.st_mode & 0o077 == 0,
              flock(descriptor, LOCK_EX) == 0 else { throw NSError(domain: "com.ghostsentinel.notifications", code: 7) }
        defer { flock(descriptor, LOCK_UN) }
        return try operation()
    }
    private func readUnlocked() throws -> (events: [[String: Any]], gap: Bool) {
        guard FileManager.default.fileExists(atPath: url.path) else { return ([], false) }
        var info = stat()
        guard lstat(url.path, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG, info.st_uid == getuid(), info.st_mode & 0o077 == 0 else { throw NSError(domain: "com.ghostsentinel.notifications", code: 1) }
        let handle = try FileHandle(forReadingFrom: url); defer { try? handle.close() }
        let bytes = try handle.read(upToCount: 1048577) ?? Data()
        guard bytes.count <= 1048576, let value = try JSONSerialization.jsonObject(with: bytes) as? [String: Any], let events = value["events"] as? [[String: Any]], events.count <= 256 else { throw NSError(domain: "com.ghostsentinel.notifications", code: 2) }
        let cutoff = clock() - 86_400_000
        let retained = events.filter { ($0["observedAt"] as? NSNumber)?.int64Value ?? 0 > cutoff }, gap = value["historyGap"] as? Bool ?? false
        if retained.count != events.count { try write((retained, gap)) }
        return (retained, gap)
    }
    func read() throws -> (events: [[String: Any]], gap: Bool) {
        return try withJournalLock { try readUnlocked() }
    }
    private func write(_ value: (events: [[String: Any]], gap: Bool)) throws {
        let directory = url.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let bytes = try JSONSerialization.data(withJSONObject: ["schemaVersion": 1, "events": value.events, "historyGap": value.gap])
        guard bytes.count <= 1048576 else { throw NSError(domain: "com.ghostsentinel.notifications", code: 3) }
        let temporary = directory.appendingPathComponent(UUID().uuidString + ".tmp")
        do {
            try bytes.write(to: temporary, options: .withoutOverwriting)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: temporary.path)
            if rename(temporary.path, url.path) != 0 { throw NSError(domain: "com.ghostsentinel.notifications", code: 4) }
        } catch { try? FileManager.default.removeItem(at: temporary); throw error }
    }
    func append(_ value: [String: Any]) throws {
        try withJournalLock {
            var stored = try readUnlocked(); stored.events.append(value)
            if stored.events.count > 256 { stored.events = Array(stored.events.suffix(256)); stored.gap = true }
            try write(stored)
        }
    }
    func purge(prefix: String) throws -> Int {
        guard prefix.range(of: "^ghostsentinel:[a-f0-9]{24}:$", options: .regularExpression) != nil else { throw NSError(domain: "com.ghostsentinel.notifications", code: 8) }
        return try withJournalLock {
            var stored = try readUnlocked()
            let before = stored.events.count
            stored.events.removeAll { ($0["requestId"] as? String)?.hasPrefix(prefix) == true }
            if stored.events.count != before { try write(stored) }
            return before - stored.events.count
        }
    }
}
