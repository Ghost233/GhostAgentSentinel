import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { previewHelperChange, applyHelperChange } from '../chatgpt/helper-installation.mjs'

export function parseHelperManagementArguments(args) {
  const operation = args[0], values = {}, flags = new Set()
  if (!['install', 'upgrade', 'uninstall'].includes(operation)) throw new Error('usage: install|upgrade|uninstall --config <absolute installation.json> --destination <absolute GhostSentinel.app> [--bundle <signed candidate.app>] [--apply] [--resume <operationId>]')
  for (let index = 1; index < args.length; index++) {
    const key = args[index]
    if (key === '--apply') { if (flags.has(key)) throw new Error('duplicate apply flag'); flags.add(key); continue }
    if (!['--config', '--destination', '--bundle', '--resume'].includes(key) || !args[index + 1] || args[index + 1].startsWith('--') || Object.hasOwn(values, key)) throw new Error('unknown, duplicate or incomplete management argument')
    values[key] = args[++index]
  }
  if (!values['--config'] || !values['--destination'] || operation !== 'uninstall' && !values['--resume'] && !values['--bundle'] || operation === 'uninstall' && values['--bundle']) throw new Error('explicit configuration, destination and applicable signed candidate required')
  return { apply: flags.has('--apply'), options: { operation, configPath: values['--config'], destinationBundle: values['--destination'], ...(values['--bundle'] ? { candidateBundle: values['--bundle'] } : {}), ...(values['--resume'] ? { resume: values['--resume'] } : {}) } }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { apply, options } = parseHelperManagementArguments(process.argv.slice(2))
    const result = await (apply ? applyHelperChange : previewHelperChange)(options)
    process.stdout.write(JSON.stringify(result) + '\n')
    if (result.status === 'failed') process.exitCode = 1
    else if (result.status?.startsWith('pending-') || result.status === 'incomplete') process.exitCode = 2
  } catch (error) {
    process.stderr.write(JSON.stringify({ status: 'failed', code: typeof error.code === 'string' ? error.code : 'helper-management-validation-failed' }) + '\n')
    process.exitCode = 1
  }
}
