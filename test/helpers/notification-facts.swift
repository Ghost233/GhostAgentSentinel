import Foundation
import Darwin

enum CheckFailure: Error { case mismatch(String) }
func check(_ condition: @autoclosure () throws -> Bool, _ message: String) throws { if try !condition() { throw CheckFailure.mismatch(message) } }
let prefix = "ghostsentinel:0123456789abcdef01234567:"

@main struct FactsChecks {
    static func main() {
        do { try run() } catch { fputs("FAIL \(error)\n", stderr); exit(1) }
    }
    static func run() throws {
        let mode = CommandLine.arguments[1]
        if mode == "inventory" {
            let old = (0..<33).map { prefix + String($0) }
            try check(try NotificationInventory.alreadyPresent(prefix + "new", prefix: prefix, pending: [], delivered: old) == false, "33 delivered records must not block a new ID")
            try check(try NotificationInventory.alreadyPresent(prefix + "32", prefix: prefix, pending: [], delivered: old), "a delivered ID outside the DTO window remains idempotent")
            try check(try NotificationInventory.alreadyPresent(prefix + "100", prefix: prefix, pending: (0..<101).map { prefix + String($0) }, delivered: []), "pending DTO limits do not weaken idempotence")
            do { _ = try NotificationInventory.alreadyPresent("foreign:new", prefix: prefix, pending: [], delivered: []); throw CheckFailure.mismatch("foreign owner accepted") } catch is CheckFailure { throw CheckFailure.mismatch("foreign owner accepted") } catch {}
        } else if mode == "journal" {
            let url = URL(fileURLWithPath: CommandLine.arguments[2]).appendingPathComponent("actions.json")
            var clock: Int64 = 100_000_000
            let journal = NotificationActionJournal(url: url, clock: { clock })
            try journal.append(["observedAt": clock, "requestId": prefix + "expired", "metadata": ["mainId": "old-chat", "lifecycleId": "old-run"]])
            clock += 86_400_001
            let result = try journal.read()
            try check(result.events.isEmpty, "the returned snapshot expires actions")
            let bytes = try Data(contentsOf: url), stored = try JSONSerialization.jsonObject(with: bytes) as! [String: Any]
            try check((stored["events"] as! [[String: Any]]).isEmpty, "status/snapshot maintenance must persist expiry without a later action")
            try check(!String(decoding: bytes, as: UTF8.self).contains("old-chat"), "expired execution identity remains on disk")
            try journal.append(["observedAt": clock, "requestId": prefix + "fresh"])
            try check(try journal.read().events.count == 1, "maintenance must retain a fresh action")
        } else if mode == "journal-boundaries" {
            let url = URL(fileURLWithPath: CommandLine.arguments[2]).appendingPathComponent("actions.json"), clock: Int64 = 200_000_000
            let journal = NotificationActionJournal(url: url, clock: { clock })
            let bytes = try JSONSerialization.data(withJSONObject: ["schemaVersion": 1, "historyGap": true, "events": [["observedAt": clock - 86_400_000, "requestId": prefix + "old"], ["observedAt": clock - 86_399_999, "requestId": prefix + "fresh"]]])
            try bytes.write(to: url); try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
            let result = try journal.read()
            try check(result.gap && result.events.count == 1 && result.events[0]["requestId"] as? String == prefix + "fresh", "expiry boundary and history gap must be preserved")
            let persisted = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
            try check((persisted["events"] as! [[String: Any]]).count == 1 && persisted["historyGap"] as? Bool == true, "atomic maintenance must preserve live facts and the gap")
            for bad in [Data("not-json".utf8), Data(repeating: 120, count: 1_048_577)] {
                try bad.write(to: url)
                do { _ = try journal.read(); throw CheckFailure.mismatch("invalid journal accepted") } catch is CheckFailure { throw CheckFailure.mismatch("invalid journal accepted") } catch {}
                try check(try Data(contentsOf: url) == bad, "a rejected journal must not be rewritten")
            }
            try bytes.write(to: url); try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: url.path)
            do { _ = try journal.read(); throw CheckFailure.mismatch("public journal permissions accepted") } catch is CheckFailure { throw CheckFailure.mismatch("public journal permissions accepted") } catch {}
            try check(try Data(contentsOf: url) == bytes, "unsafe journal remains an explicit error")
            try check(try FileManager.default.contentsOfDirectory(atPath: url.deletingLastPathComponent().path).allSatisfy { ["actions.json", ".actions.lock"].contains($0) }, "atomic maintenance leaked a temporary file")
        } else if mode == "journal-concurrency" {
            let url = URL(fileURLWithPath: CommandLine.arguments[2]).appendingPathComponent("actions.json")
            var clock: Int64 = 100_000_000
            let journal = NotificationActionJournal(url: url, clock: { clock }), errors = NSLock()
            var failures = 0
            try journal.append(["observedAt": clock, "requestId": prefix + "old"]); clock += 86_400_001
            DispatchQueue.concurrentPerform(iterations: 32) { index in
                do { if index % 2 == 0 { _ = try journal.read() }; try journal.append(["observedAt": clock, "requestId": prefix + String(index)]); _ = try journal.read() }
                catch { errors.lock(); failures += 1; errors.unlock() }
            }
            let events = try journal.read().events, disk = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
            try check(failures == 0 && events.count == 32 && Set(events.compactMap { $0["requestId"] as? String }).count == 32, "maintenance and append must not lose fresh actions")
            try check((disk["events"] as! [[String: Any]]).count == 32, "concurrent journal facts were not persisted")
        } else if mode == "journal-errors" {
            let directory = URL(fileURLWithPath: CommandLine.arguments[2]), url = directory.appendingPathComponent("actions.json"), lock = directory.appendingPathComponent(".actions.lock")
            var clock: Int64 = 100_000_000
            let journal = NotificationActionJournal(url: url, clock: { clock })
            try journal.append(["observedAt": clock, "requestId": prefix + "old"]); clock += 86_400_001
            let before = try Data(contentsOf: url)
            try FileManager.default.setAttributes([.posixPermissions: 0o500], ofItemAtPath: directory.path)
            do {
                defer { try? FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: directory.path) }
                do { _ = try journal.read(); throw CheckFailure.mismatch("failed expiry write was silently accepted") } catch is CheckFailure { throw CheckFailure.mismatch("failed expiry write was silently accepted") } catch {}
                try check(try Data(contentsOf: url) == before, "failed replacement must preserve its original disk facts")
            }
            try check(try journal.read().events.isEmpty, "a failed maintenance must release its locks for the next operation")
            try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: lock.path)
            do { _ = try journal.read(); throw CheckFailure.mismatch("unsafe lock permissions accepted") } catch is CheckFailure { throw CheckFailure.mismatch("unsafe lock permissions accepted") } catch {}
            try FileManager.default.removeItem(at: lock)
            try FileManager.default.createSymbolicLink(at: lock, withDestinationURL: url)
            do { _ = try journal.read(); throw CheckFailure.mismatch("symlink lock accepted") } catch is CheckFailure { throw CheckFailure.mismatch("symlink lock accepted") } catch {}
            try FileManager.default.removeItem(at: lock)
            try check(try journal.read().events.isEmpty, "a rejected lock must not poison a later safe operation")
        } else if mode == "hold-journal-lock" {
            let url = URL(fileURLWithPath: CommandLine.arguments[2]).appendingPathComponent(".actions.lock")
            let fd = open(url.path, O_CREAT | O_RDWR | O_NOFOLLOW, 0o600)
            guard fd >= 0 else { throw CheckFailure.mismatch("owned lock could not open") }
            guard flock(fd, LOCK_EX) == 0 else { close(fd); throw CheckFailure.mismatch("owned lock could not acquire") }
            FileHandle.standardOutput.write(Data("LOCKED\n".utf8))
            _ = try FileHandle.standardInput.read(upToCount: 1)
            flock(fd, LOCK_UN); close(fd)
            _ = try NotificationActionJournal(url: url.deletingLastPathComponent().appendingPathComponent("actions.json"), clock: { 100_000_000 }).read()
        } else if mode == "journal-writer" {
            let url = URL(fileURLWithPath: CommandLine.arguments[2]).appendingPathComponent("actions.json")
            FileHandle.standardOutput.write(Data("WRITING\n".utf8))
            try NotificationActionJournal(url: url, clock: { 100_000_000 }).append(["observedAt": 100_000_000, "requestId": prefix + "concurrent"])
        } else { throw CheckFailure.mismatch("unknown test case") }
        print("PASS " + mode)
    }
}
