import Foundation
import ServiceManagement
import Darwin

@main
struct GhostSentinelHelper {
    static func emit(_ value: [String: Any], error: Bool = false) {
        let data = (try? JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])) ?? Data("{}".utf8)
        let file = error ? FileHandle.standardError : FileHandle.standardOutput
        file.write(data); file.write(Data("\n".utf8))
    }

    static func main() async {
        do {
            let command = try HelperCommand.parse(Array(CommandLine.arguments.dropFirst()))
            try HelperLaunch.identity(Bundle.main.bundleIdentifier)
            if command == .agent {
                let launch = try HelperLaunch(bundleURL: Bundle.main.bundleURL, identifier: Bundle.main.bundleIdentifier)
                exit(try runHelperChild(executable: launch.executable, arguments: launch.arguments, workingDirectory: launch.workingDirectory))
            }
            let service = SMAppService.agent(plistName: helperAgentPlistName)
            switch command {
            case .register:
                _ = try HelperLaunch(bundleURL: Bundle.main.bundleURL, identifier: Bundle.main.bundleIdentifier)
                try service.register()
            case .unregister:
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    service.unregister { error in
                        if let error { continuation.resume(throwing: error) }
                        else { continuation.resume(returning: ()) }
                    }
                }
            case .status: break
            case .agent: break
            }
            let status: String
            switch service.status {
            case .notRegistered: status = "notRegistered"
            case .enabled: status = "enabled"
            case .requiresApproval: status = "requiresApproval"
            case .notFound: status = "notFound"
            @unknown default: status = "unknown"
            }
            emit(["ok": true, "bundleId": helperBundleIdentifier, "command": command.rawValue, "serviceStatus": status, "unregisterCompleted": command == .unregister])
        } catch {
            let native = error as NSError
            emit(["ok": false, "bundleId": helperBundleIdentifier, "error": (error as? HelperFailure)?.rawValue ?? native.domain, "code": native.code], error: true)
            exit(1)
        }
    }
}
