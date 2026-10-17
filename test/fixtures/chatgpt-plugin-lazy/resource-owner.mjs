import { readFile } from 'node:fs/promises'
import { join, isAbsolute } from 'node:path'
import { pathToFileURL } from 'node:url'

const [variantRoot, configPath, statePath] = process.argv.slice(2)
if (![variantRoot, configPath, statePath].every(isAbsolute)) throw new Error('explicit owned fixture paths required')
const { startFromInstallation } = await import(pathToFileURL(join(variantRoot, 'chatgpt/runtime.mjs')).href)
const { supportedProfile } = await import(pathToFileURL(join(variantRoot, 'chatgpt/bounded-reader.mjs')).href)
const config = JSON.parse(await readFile(configPath, 'utf8')), hostStartedAt = Date.now() - 10000
const thread = { id: 'resource-main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: join(config.sessionsRoot, 'resource-main.jsonl'), cwd: config.sessionsRoot }
let runtime, stopRequested = false
const stop = () => { stopRequested = true; void runtime?.close().catch(() => {}) }
process.once('SIGTERM', stop); process.once('SIGINT', stop)
try {
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'owned-resource-host', hostStartedAt, profile: supportedProfile } },
    async readEnablement() { const enabled = JSON.parse(await readFile(statePath, 'utf8')); return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads({ archived }) { return { data: archived ? [] : [thread], nextCursor: null } },
    async readThread(id) { return { thread: id === thread.id ? thread : null, anchorTurnId: id === thread.id ? 'resource-turn' : null } }, async close() {},
  }
  runtime = await startFromInstallation({ configPath, native, pollMs: 150, ownership: 'plugin' })
  process.send?.({ pid: runtime.pid, runId: runtime.runId, reused: runtime.reused })
  if (stopRequested) await runtime.close()
  if (!runtime.reused) await runtime.closed
} finally { process.removeListener('SIGTERM', stop); process.removeListener('SIGINT', stop) }
