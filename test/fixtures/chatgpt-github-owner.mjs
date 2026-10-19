import { startFromInstallation, loadInstallationConfig } from '../../chatgpt/runtime.mjs'
import { supportedProfile } from '../../chatgpt/bounded-reader.mjs'

const [configPath, hold] = process.argv.slice(2), config = await loadInstallationConfig(configPath), hostStartedAt = Date.now() - 1000
let enabled = true, released = false, releaseClose, runtime
const closeGate = new Promise(resolve => { releaseClose = resolve })
process.on('message', value => {
  if (value === 'disable') enabled = false
  if (value === 'release-close') { released = true; releaseClose() }
})
try {
  runtime = await startFromInstallation({ configPath, ownership: 'plugin', pollMs: 20, native: {
    async sampleHost() { return { available: true, hostRunId: config.installationId, hostStartedAt, profile: supportedProfile } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads() { return { data: [], nextCursor: null } },
    async close() { if (hold === 'hold-close' && !released) await closeGate },
  } })
  process.send({ url: runtime.url, runId: runtime.runId, pid: process.pid, releaseId: runtime.releaseId })
  await runtime.closed
} finally {
  await runtime?.close()
  if (process.connected) process.disconnect()
}
