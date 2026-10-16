import Foundation
import Darwin

@main
struct HelperHarness {
    static func main() {
        do {
            let args = Array(CommandLine.arguments.dropFirst())
            switch args.first {
            case "parse":
                let command = try HelperCommand.parse(Array(args.dropFirst()))
                print(command.rawValue)
            case "contract":
                guard args.count == 3 else { exit(2) }
                let launch = try HelperLaunch(bundleURL: URL(fileURLWithPath: args[1]), identifier: args[2])
                let data = try JSONSerialization.data(withJSONObject: ["node": launch.executable.path, "arguments": launch.arguments, "workingDirectory": launch.workingDirectory.path])
                print(String(decoding: data, as: UTF8.self))
            case "supervise":
                guard args.count >= 3 else { exit(2) }
                exit(try runHelperChild(executable: URL(fileURLWithPath: args[1]), arguments: Array(args.dropFirst(2)), workingDirectory: URL(fileURLWithPath: FileManager.default.currentDirectoryPath), graceSeconds: 0.4))
            default: exit(2)
            }
        } catch {
            fputs("isolated-helper-harness: \(error)\n", stderr)
            exit(1)
        }
    }
}
