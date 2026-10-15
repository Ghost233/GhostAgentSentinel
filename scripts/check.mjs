import { readdir } from 'node:fs/promises'
import { spawnSync } from 'node:child_process'

async function files(path) {
  const entries = await readdir(path, { withFileTypes: true })
  const result = []
  for (const entry of entries) {
    if (entry.name === 'node_modules') continue
    const name = `${path}/${entry.name}`
    if (entry.isDirectory()) result.push(...await files(name))
    else if (/\.(mjs|js|cjs)$/.test(name)) result.push(name)
  }
  return result
}
for (const directory of ['src', 'dsh', 'chatgpt', 'test']) {
  for (const file of await files(directory)) {
    const result = spawnSync(process.execPath, ['--check', file], { stdio: 'inherit' })
    if (result.status !== 0) process.exit(result.status ?? 1)
  }
}
console.log('JavaScript syntax checks passed')
