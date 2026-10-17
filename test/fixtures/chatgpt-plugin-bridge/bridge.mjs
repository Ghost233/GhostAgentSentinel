import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { appendFile } from 'node:fs/promises'
import { startPluginBridge } from '../../../chatgpt/plugin-bridge.mjs'

const [configPath, statePath, callsPath, pidsPath] = process.argv.slice(2)
const bridge = await startPluginBridge({ configPath, async startOwner(path) {
  const child = spawn(process.execPath, [new URL('./owner.mjs', import.meta.url).pathname, path, statePath, callsPath], { detached: true, stdio: 'ignore' })
  await once(child, 'spawn'); await appendFile(pidsPath, child.pid + '\n'); child.unref()
} })
await bridge.closed
