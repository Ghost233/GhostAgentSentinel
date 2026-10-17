import { isAbsolute } from 'node:path'
import { startFromInstallation } from './runtime.mjs'

// Controlled process entry only. This is not an MCP stdio server and does not
// establish that the host starts the plugin before any panel or tool is used.
let runtime, deadline, stopRequested = false
const report = value => process.stderr.write(JSON.stringify(value) + '\n')
function stop() {
  stopRequested = true
  deadline ??= setTimeout(() => { report({ status: 'failed', code: 'plugin_close_timeout' }); process.exit(1) }, 5000)
  if (runtime && !runtime.reused) void runtime.close().catch(() => {})
}
process.once('SIGINT', stop); process.once('SIGTERM', stop)
try {
  const args = process.argv.slice(2)
  if (args.length !== 2 || args[0] !== '--config' || !isAbsolute(args[1])) throw new Error('invalid plugin arguments')
  runtime = await startFromInstallation({ configPath: args[1], ownership: 'plugin' })
  report({ status: runtime.reused ? 'reused' : 'running', ownership: 'plugin', runId: runtime.runId, pid: runtime.pid })
  if (!runtime.reused) {
    if (stopRequested) stop()
    const result = await runtime.closed
    report({ status: 'stopped', runId: runtime.runId, reason: result.reason })
  }
} catch {
  report({ status: 'failed', code: runtime ? 'plugin_close_failed' : 'plugin_start_failed' })
  process.exitCode = 1
} finally {
  process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop)
  clearTimeout(deadline)
}
