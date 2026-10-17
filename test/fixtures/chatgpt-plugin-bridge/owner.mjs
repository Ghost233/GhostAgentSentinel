import { readFile, appendFile, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { startFromInstallation } from '../../../chatgpt/runtime.mjs'
import { supportedProfile } from '../../../chatgpt/bounded-reader.mjs'

const [configPath, statePath, callsPath] = process.argv.slice(2)
const config = JSON.parse(await readFile(configPath, 'utf8'))
let closed = false
const native = {
  async sampleHost() { return { available: true, hostRunId: 'bridge-test-host', hostStartedAt: 1, profile: supportedProfile } },
  async readEnablement() { const enabled = JSON.parse(await readFile(statePath, 'utf8')); return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
  async listThreads() { return { data: [], nextCursor: null } },
  async readThread(id) {
    await appendFile(callsPath, id + '\n')
    const finite = await readFile(join(config.sessionsRoot, 'finite-native-ms'), 'utf8').then(Number, () => 0)
    if (finite) await delay(finite)
    while (!closed && await stat(join(config.sessionsRoot, 'hold-native')).then(() => true, () => false)) await delay(10)
    await appendFile(callsPath, 'completed:' + id + '\n')
    return { thread: { id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: join(config.sessionsRoot, id + '.jsonl'), cwd: config.sessionsRoot }, anchorTurnId: id === 'resource-main' ? 'resource-turn' : null }
  },
  async close() {
    closed = true
    await appendFile(callsPath, 'native-close-start\n')
    while (await stat(join(config.sessionsRoot, 'hold-close')).then(() => true, () => false)) await delay(10)
  },
}
const runtime = await startFromInstallation({ configPath, native, pollMs: 20, ownership: 'plugin' })
if (process.send) process.send({ runId: runtime.runId, pid: runtime.pid, reused: runtime.reused })
if (!runtime.reused) await runtime.closed
