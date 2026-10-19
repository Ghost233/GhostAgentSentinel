import { isAbsolute, join } from 'node:path'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { readFile, lstat } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { loadInstallationConfig, startFromInstallation } from './runtime.mjs'
import { createNativeIO } from './native-io.mjs'
import { createStdioMetadataClient } from './stdio-metadata.mjs'
import { createNativeNotificationIO } from './native-notifications.mjs'

// The metadata subprocess is lazy and belongs to this observation instance.
// Losing the plugin/App qualification closes it with the observation runtime.
export async function startGitHubOwner({ configPath, channel, metadataClient, native, notificationIO, releaseRoot = fileURLToPath(new URL('../', import.meta.url)) } = {}) {
  if (!['stable', 'test'].includes(channel) || !isAbsolute(configPath ?? '')) throw new Error('invalid GitHub owner arguments')
  const config = await loadInstallationConfig(configPath)
  if (config.pluginId !== (channel === 'test' ? 'ghost-sentinel-test' : 'ghost-sentinel') + '@ghostsentinel' || !config.releaseId) throw new Error('GitHub installation identity mismatch')
  const manifestPath = join(releaseRoot, 'plugin-package.json'), info = await lstat(manifestPath)
  if (!info.isFile() || info.isSymbolicLink() || info.size > 8 * 1024 * 1024) throw new Error('GitHub release manifest invalid')
  const releaseId = createHash('sha256').update(await readFile(manifestPath)).digest('hex')
  if (releaseId !== config.releaseId) throw new Error('GitHub owner release identity mismatch')
  const metadata = metadataClient ?? createStdioMetadataClient()
  const host = native ?? createNativeIO({ pluginId: config.pluginId, metadataClient: metadata })
  try {
    const runtime = await startFromInstallation({ configPath, ownership: 'plugin', native: host, expectedReleaseId: releaseId,
      displayName: channel === 'test' ? 'GhostSentinel test' : 'GhostSentinel',
      notificationIO: notificationIO ?? (config.notificationBundlePath ? createNativeNotificationIO({ bundlePath: config.notificationBundlePath, installationId: config.installationId, testMode: channel === 'test' }) : undefined),
    })
    if (runtime.reused) { await metadata.close(); return runtime }
    return { ...runtime, closed: runtime.closed.then(async value => { await metadata.close(); return value }) }
  } catch (error) {
    try { await metadata.close() }
    catch (cleanup) { throw new AggregateError([error, cleanup], 'GitHub observation startup and cleanup failed', { cause: error }) }
    throw error
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  let runtime, deadline, stopping = false
  const stop = () => {
    stopping = true
    deadline ??= setTimeout(() => process.exit(1), 5000)
    if (runtime && !runtime.reused) void runtime.close().catch(() => {})
  }
  process.once('SIGINT', stop); process.once('SIGTERM', stop)
  try {
    const args = process.argv.slice(2)
    if (args.length !== 4 || args[0] !== '--config' || args[2] !== '--channel') throw new Error('invalid GitHub owner arguments')
    runtime = await startGitHubOwner({ configPath: args[1], channel: args[3] })
    if (!runtime.reused) { if (stopping) stop(); await runtime.closed }
  } catch { process.stderr.write('{"status":"failed","code":"github_owner_failed"}\n'); process.exitCode = 1 }
  finally { clearTimeout(deadline); process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop) }
}
