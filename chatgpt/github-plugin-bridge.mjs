import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { isAbsolute } from 'node:path'
import { pathToFileURL } from 'node:url'
import { prepareGitHubInstallation } from './github-installation.mjs'
import { loadInstallationConfig } from './runtime.mjs'
import { startPluginBridge } from './plugin-bridge.mjs'

export async function startGitHubBridge({ channel, releaseRoot, archiveSHA256, stateRoot, input, output, signal } = {}) {
  const prepared = await prepareGitHubInstallation({ channel, releaseRoot, archiveSHA256, stateRoot })
  const check = async configPath => {
    if (signal?.aborted) throw new Error('GitHub connection stopped')
    if ((await loadInstallationConfig(configPath)).releaseId !== prepared.releaseId) throw new Error('GitHub release superseded')
  }
  await check(prepared.configPath)
  return startPluginBridge({ configPath: prepared.configPath, input, output, async startOwner(configPath) {
    await check(configPath)
    const child = spawn(prepared.nodePath, [prepared.ownerPath, '--config', configPath, '--channel', channel], { detached: true, stdio: 'ignore' })
    await once(child, 'spawn'); child.unref()
  } })
}

export function parseGitHubBridgeArguments(args) {
  const keys = new Map([['--channel', 'channel'], ['--release', 'releaseRoot'], ['--archive-sha', 'archiveSHA256'], ['--state-root', 'stateRoot']]), options = {}
  for (let index = 0; index < args.length; index += 2) {
    const key = keys.get(args[index]), value = args[index + 1]
    if (!key || Object.hasOwn(options, key) || typeof value !== 'string' || !value || value.startsWith('--')) throw new Error('invalid GitHub bridge arguments')
    options[key] = value
  }
  if (Object.keys(options).length !== 4 || !['stable', 'test'].includes(options.channel) || !/^[a-f0-9]{64}$/.test(options.archiveSHA256) || !isAbsolute(options.releaseRoot) || !isAbsolute(options.stateRoot)) throw new Error('invalid GitHub bridge arguments')
  return options
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const controller = new AbortController(); let bridge
  const stop = () => { controller.abort(); void bridge?.close('process-stopped') }
  process.once('SIGTERM', stop); process.once('SIGINT', stop)
  try {
    bridge = await startGitHubBridge({ ...parseGitHubBridgeArguments(process.argv.slice(2)), signal: controller.signal })
    if (controller.signal.aborted) await bridge.close('process-stopped')
    const { reason } = await bridge.closed
    process.stderr.write(JSON.stringify({ status: 'closed', code: 'github_bridge_closed', reason }) + '\n')
  } catch { process.stderr.write('{"status":"failed","code":"github_bridge_failed"}\n'); process.exitCode = 1 }
  finally { process.removeListener('SIGTERM', stop); process.removeListener('SIGINT', stop) }
}
