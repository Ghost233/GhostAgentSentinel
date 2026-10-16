import Foundation
import Darwin

let helperBundleIdentifier = "com.ghostsentinel.helper"
let helperAgentPlistName = "com.ghostsentinel.helper.agent.plist"

enum HelperFailure: String, Error {
    case invalidArguments, wrongBundleIdentity, invalidBundleResource, invalidConfigPointer, privateConfigRequired
}

enum HelperCommand: String {
    case register = "--register", unregister = "--unregister", status = "--status", agent = "--agent"
    static func parse(_ arguments: [String]) throws -> HelperCommand {
        if arguments.isEmpty { return .status }
        guard arguments.count == 1, let command = HelperCommand(rawValue: arguments[0]) else { throw HelperFailure.invalidArguments }
        return command
    }
}

struct HelperLaunch {
    let executable: URL
    let entry: URL
    let config: URL
    let workingDirectory: URL
    var arguments: [String] { [entry.path, "--config", config.path] }

    static func identity(_ identifier: String?) throws {
        guard identifier == helperBundleIdentifier else { throw HelperFailure.wrongBundleIdentity }
    }

    init(bundleURL: URL, identifier: String?) throws {
        try Self.identity(identifier)
        let manager = FileManager.default, root = bundleURL.resolvingSymlinksInPath()
        func resource(_ path: String, executable: Bool = false) throws -> URL {
            let url = root.appendingPathComponent(path).resolvingSymlinksInPath()
            guard url.path.hasPrefix(root.path + "/"),
                  let attributes = try? manager.attributesOfItem(atPath: url.path),
                  attributes[.type] as? FileAttributeType == .typeRegular,
                  !executable || manager.isExecutableFile(atPath: url.path) else { throw HelperFailure.invalidBundleResource }
            return url
        }
        executable = try resource("Contents/Resources/node/bin/node", executable: true)
        entry = try resource("Contents/Resources/observer/chatgpt/helper-main.mjs")
        _ = try resource("Contents/Library/LaunchAgents/" + helperAgentPlistName)
        let pointer = try resource("Contents/Resources/helper-config.json")
        let pointerSize = (try manager.attributesOfItem(atPath: pointer.path)[.size] as? NSNumber)?.intValue ?? 0
        guard pointerSize > 0, pointerSize <= 4096,
              let value = try JSONSerialization.jsonObject(with: Data(contentsOf: pointer)) as? [String: Any],
              value.count == 1, let path = value["installationConfigPath"] as? String,
              path.hasPrefix("/"), path.utf8.count <= 2048, !path.contains("\n"), !path.contains("\0") else { throw HelperFailure.invalidConfigPointer }
        let file = URL(fileURLWithPath: path)
        guard file.resolvingSymlinksInPath().path == file.standardizedFileURL.path,
              let attributes = try? manager.attributesOfItem(atPath: file.path),
              attributes[.type] as? FileAttributeType == .typeRegular,
              (attributes[.ownerAccountID] as? NSNumber)?.uint32Value == getuid(),
              let mode = attributes[.posixPermissions] as? NSNumber, mode.intValue & 0o077 == 0,
              let bytes = attributes[.size] as? NSNumber, bytes.intValue > 0, bytes.intValue <= 16384 else { throw HelperFailure.privateConfigRequired }
        config = file
        workingDirectory = root.appendingPathComponent("Contents/Resources/observer")
    }
}

// Only the agent calls this. Tests compile the same function in an isolated
// harness, without ServiceManagement or the real observer entry.
func runHelperChild(executable: URL, arguments: [String], workingDirectory: URL, graceSeconds: Double = 10) throws -> Int32 {
    let child = Process(), queue = DispatchQueue(label: "com.ghostsentinel.helper.signals")
    child.executableURL = executable
    child.arguments = arguments
    child.currentDirectoryURL = workingDirectory
    child.environment = ["HOME": NSHomeDirectory(), "USER": NSUserName(), "LOGNAME": NSUserName(), "TMPDIR": NSTemporaryDirectory(), "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"]
    child.standardInput = FileHandle.nullDevice
    let oldTerm = signal(SIGTERM, SIG_IGN), oldInt = signal(SIGINT, SIG_IGN)
    let term = DispatchSource.makeSignalSource(signal: SIGTERM, queue: queue)
    let interrupt = DispatchSource.makeSignalSource(signal: SIGINT, queue: queue)
    var requested: Int32?, force: DispatchWorkItem?
    func forward(_ value: Int32) {
        requested = requested ?? value
        guard child.isRunning else { return }
        kill(child.processIdentifier, value)
        if force == nil {
            let work = DispatchWorkItem { if child.isRunning { kill(child.processIdentifier, SIGKILL) } }
            force = work
            queue.asyncAfter(deadline: .now() + graceSeconds, execute: work)
        }
    }
    term.setEventHandler { forward(SIGTERM) }
    interrupt.setEventHandler { forward(SIGINT) }
    term.resume(); interrupt.resume()
    defer {
        queue.sync { force?.cancel(); term.cancel(); interrupt.cancel() }
        signal(SIGTERM, oldTerm); signal(SIGINT, oldInt)
    }
    try child.run()
    queue.sync { if let value = requested { forward(value) } }
    child.waitUntilExit()
    return child.terminationReason == .exit ? child.terminationStatus : 128 + child.terminationStatus
}
