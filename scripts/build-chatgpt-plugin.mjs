import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { buildPluginPackage } from '../chatgpt/plugin-package.mjs'

export function parsePluginBuildArguments(args) {
  const values = {}
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index], value = args[index + 1]
    if (!['--out', '--notification-app', '--version'].includes(key) || typeof value !== 'string' || !value || Object.hasOwn(values, key)) throw new Error('usage: --out <new absolute directory> [--version <semver>] [--notification-app <existing signed.app>]')
    values[key] = value
  }
  if (!values['--out']) throw new Error('explicit new output required')
  return { outputDir: values['--out'], notificationApp: values['--notification-app'], ...(values['--version'] === undefined ? {} : { version: values['--version'] }) }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) process.stdout.write(JSON.stringify(await buildPluginPackage(parsePluginBuildArguments(process.argv.slice(2)))) + '\n')
