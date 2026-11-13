import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { isAbsolute } from 'node:path'
import { pathToFileURL } from 'node:url'
import { GITHUB_CHANNELS, inspectGitHubInstallation, prepareGitHubInstallation } from './github-installation.mjs'
import { loadInstallationConfig } from './runtime.mjs'
import { startResilientPluginBridge } from './plugin-bridge.mjs'

async function launchGitHubOwner(prepared, channel) {
  const child = spawn(prepared.nodePath, [prepared.ownerPath, '--config', prepared.configPath, '--channel', channel], { detached: true, stdio: 'ignore' })
  await once(child, 'spawn'); child.unref()
}

export async function startGitHubBridge(options = {}, { prepareInstallation = prepareGitHubInstallation, launchOwner = launchGitHubOwner } = {}) {
  const { channel, input, output, signal } = options
  if (!Object.hasOwn(GITHUB_CHANNELS, channel)) throw new Error('invalid GitHub bridge channel')
  let prepared, preparing, nextPrepare = 0
  let superseded = false
  // The launcher already verified the HTML and executable. Installation
  // preparation may wait for an old observer, so keep it off the MCP read path.
  async function installation() {
    if (signal?.aborted) return null
    if (prepared === undefined && !superseded && !preparing && Date.now() >= nextPrepare) {
      preparing = prepareInstallation(options).then(value => { prepared = value }, error => {
        if (['github_prepare_busy', 'github_owner_stop_unconfirmed'].includes(error.code)) nextPrepare = Date.now() + 1000
        else if (error.code === 'github_release_superseded') superseded = true
        else prepared = false
      }).finally(() => { preparing = null })
    }
    if (!superseded && !prepared) return prepared
    if (prepared && (await loadInstallationConfig(prepared.configPath)).releaseId === prepared.releaseId) return prepared
    const active = await inspectGitHubInstallation(options)
    if (!active?.receipt.releases.some(release => release.archiveSHA256 === options.archiveSHA256)) return null
    return { configPath: active.configPath, releaseId: active.receipt.activeReleaseId, canLaunch: false }
  }
  return startResilientPluginBridge({ displayName: GITHUB_CHANNELS[channel].displayName, installation, input, output, async startOwner() {
    if (!signal?.aborted && (await loadInstallationConfig(prepared.configPath)).releaseId === prepared.releaseId) await launchOwner(prepared, channel)
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
