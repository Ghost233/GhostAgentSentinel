import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { mkdir, writeFile, readFile } from 'node:fs/promises'
import { resolve, join, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'

const execute = promisify(execFile)
export async function buildNotificationApp({ outputDir = resolve('.scratch/native-notifier-build') } = {}) {
  outputDir = resolve(outputDir)
  const roots = [resolve('.scratch'), resolve(tmpdir()), '/private/tmp']
  if (!roots.some(root => outputDir === root || outputDir.startsWith(root + sep))) throw new Error('notification development bundle must be built in isolated scratch or temporary storage')
  const bundle = join(outputDir, 'GhostSentinel Notifications.app'), contents = join(bundle, 'Contents'), executable = join(contents, 'MacOS/GhostSentinelNotifications'), cache = join(outputDir, 'module-cache')
  await mkdir(join(contents, 'MacOS'), { recursive: true, mode: 0o700 }); await mkdir(cache, { recursive: true, mode: 0o700 })
  const plist = '<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>CFBundleIdentifier</key><string>com.ghostsentinel.notifications</string><key>CFBundleExecutable</key><string>GhostSentinelNotifications</string><key>CFBundleDisplayName</key><string>GhostSentinel</string><key>CFBundleName</key><string>GhostSentinel</string><key>CFBundleVersion</key><string>1</string><key>CFBundleShortVersionString</key><string>0.1.0</string><key>CFBundlePackageType</key><string>APPL</string><key>NSPrincipalClass</key><string>NSApplication</string><key>LSUIElement</key><true/></dict></plist>'
  await writeFile(join(contents, 'Info.plist'), plist, { mode: 0o600 })
  const sdk = (await execute('/usr/bin/xcrun', ['--show-sdk-path'], { timeout: 10000, maxBuffer: 65536 })).stdout.trim()
  const source = fileURLToPath(new URL('../chatgpt/native/GhostSentinelNotifications.swift', import.meta.url))
  const factsSource = fileURLToPath(new URL('../chatgpt/native/NotificationFacts.swift', import.meta.url))
  await execute('/usr/bin/xcrun', ['swiftc', '-swift-version', '5', '-O', '-parse-as-library', '-sdk', sdk, '-module-cache-path', cache, '-framework', 'AppKit', '-framework', 'UserNotifications', source, factsSource, '-o', executable], { timeout: 120000, maxBuffer: 1048576, env: { ...process.env, CLANG_MODULE_CACHE_PATH: cache, SWIFT_MODULE_CACHE_PATH: cache } })
  await execute('/usr/bin/codesign', ['--force', '--sign', '-', '--identifier', 'com.ghostsentinel.notifications', bundle], { timeout: 10000, maxBuffer: 65536 })
  await execute('/usr/bin/codesign', ['--verify', '--strict', bundle], { timeout: 10000, maxBuffer: 65536 })
  return { bundle, executable, bundleId: 'com.ghostsentinel.notifications', sourceSha256: createHash('sha256').update(await readFile(source)).digest('hex'), factsSourceSha256: createHash('sha256').update(await readFile(factsSource)).digest('hex'), executableSha256: createHash('sha256').update(await readFile(executable)).digest('hex'), signed: 'adhoc-development', installed: false }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const output = process.argv[2]
  process.stdout.write(JSON.stringify(await buildNotificationApp(output ? { outputDir: output } : {})) + '\n')
}
