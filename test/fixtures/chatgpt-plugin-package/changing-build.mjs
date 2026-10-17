import { readFile, writeFile, chmod, readdir } from 'node:fs/promises'
import { join, dirname, basename } from 'node:path'
import { pathToFileURL } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'

const [source, outputDir] = process.argv.slice(2)
const { buildPluginPackage } = await import(pathToFileURL(join(source, 'chatgpt/plugin-package.mjs')).href)
const runtimePath = join(source, 'chatgpt/runtime.mjs'), before = await readFile(runtimePath)
let changed = false, finished = false, monitorError
const mutation = (async () => {
  while (!finished) {
    if ((await readdir(dirname(outputDir))).some(name => name.startsWith('.' + basename(outputDir) + '.partial-'))) {
      changed = true
      await Promise.all([
        writeFile(runtimePath, Buffer.concat([before, Buffer.from('\n// changed in isolated source fixture\n')])),
        chmod(runtimePath, 0o700), writeFile(join(source, 'chatgpt/concurrently-added.mjs'), 'export const changed = true\n'),
      ])
      return
    }
    await delay(5)
  }
})().catch(error => { monitorError = error })
try {
  try { await buildPluginPackage({ outputDir }); throw new Error('source mutation was accepted') }
  catch (error) {
    finished = true
    await mutation
    if (monitorError) throw monitorError
    if (!changed || !/source changed|changed during/.test(error.message)) throw error
    process.stdout.write(JSON.stringify({ changed, rejected: true }) + '\n')
  }
} finally { finished = true; await mutation }
