import { isAbsolute, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export function parsePluginManagementArguments(args) {
  if (args.length === 1 && args[0] === '--help') return { help: true }
  const [operation, ...flags] = args
  if (!['install', 'upgrade', 'uninstall'].includes(operation)) throw new Error('operation must be install, upgrade or uninstall')
  const options = { operation, apply: false }, names = { '--root': 'installRoot', '--package': 'packageRoot', '--plugin-id': 'pluginId', '--plugin-name': 'pluginName', '--marketplace-name': 'marketplaceName', '--core-data-dir': 'coreDataDir', '--sessions-root': 'sessionsRoot', '--metadata-socket': 'metadataSocket', '--resume': 'resumeOperationId' }
  for (let at = 0; at < flags.length; at++) {
    const flag = flags[at]
    if (flag === '--apply' && options.apply === false) { options.apply = true; continue }
    const name = names[flag], value = flags[++at]
    if (!name || Object.hasOwn(options, name) || !value || value.startsWith('--')) throw new Error('invalid or duplicate plugin management argument')
    options[name] = value
  }
  if (!isAbsolute(options.installRoot ?? '')) throw new Error('--root requires an absolute installation path')
  return options
}

export async function managePlugin(argumentsList) {
  const options = parsePluginManagementArguments(argumentsList)
  if (options.help) return { help: 'manage-chatgpt-plugin <install|upgrade|uninstall> --root <absolute-path> [--package <built-payload>] [--plugin-id <id> --plugin-name <name> --marketplace-name <name> --core-data-dir <path> --sessions-root <path> --metadata-socket <path>] [--apply] [--resume <operation-id>]', exitCode: 0 }
  const { pluginManagementDefaults, previewPluginChange, applyPluginChange } = await import('../chatgpt/plugin-installation.mjs')
  const defaults = await pluginManagementDefaults(options.installRoot)
  options.installRoot = defaults.installRoot
  for (const [key, value] of Object.entries(defaults)) options[key] ??= value
  const { createPluginHost } = await import('../chatgpt/plugin-host.mjs')
  const host = await createPluginHost({ socketPath: options.metadataSocket, marketplacePath: join(options.installRoot, 'state/.agents/plugins/marketplace.json'), marketplaceName: options.marketplaceName, pluginName: options.pluginName, pluginId: options.pluginId, pluginPath: join(options.installRoot, 'state/plugin') })
  try {
    const plan = await previewPluginChange(options, { host })
    if (!options.apply) return { plan, exitCode: plan.canApply ? 0 : 2 }
    const result = await applyPluginChange(plan, { host })
    return { result, exitCode: result.status === 'complete' ? 0 : ['needs-user-action', 'recovery-required'].includes(result.status) ? 2 : 1 }
  } finally { await host.close?.() }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { const result = await managePlugin(process.argv.slice(2)); process.stdout.write(JSON.stringify(result) + '\n'); process.exitCode = result.exitCode }
  catch (error) { process.stderr.write(JSON.stringify({ status: 'failed', code: /^[a-z][a-z0-9_-]{0,80}$/i.test(error.code ?? '') ? error.code : 'plugin_management_failed' }) + '\n'); process.exitCode = 1 }
}
