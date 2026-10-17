import { isAbsolute } from 'node:path'
import { startFromInstallation } from './runtime.mjs'

// The native launcher owns registration. Signals are installed before startup
// so a ready record never races its own shutdown handler.
let runtime, deadline, closing, stopRequested = false
function stop() {
  stopRequested = true
  deadline ??= setTimeout(() => { console.error(JSON.stringify({ status: 'failed', code: 'helper_close_timeout' })); process.exit(1) }, 5000)
  if (!runtime || closing) return
  closing = (async () => {
    try { await runtime.close(); console.log(JSON.stringify({ status: 'stopped', runId: runtime.runId })); process.exitCode = 0 }
    catch { console.error(JSON.stringify({ status: 'failed', code: 'helper_close_failed' })); process.exit(1) }
    finally { clearTimeout(deadline) }
  })()
}
process.once('SIGINT', stop); process.once('SIGTERM', stop)
try {
  const args = process.argv.slice(2)
  if (args.length !== 2 || args[0] !== '--config' || !isAbsolute(args[1])) throw new Error('invalid helper arguments')
  runtime = await startFromInstallation({ configPath: args[1] })
  if (runtime.reused) { process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop); clearTimeout(deadline) }
  if (stopRequested && !runtime.reused) stop()
  else console.log(JSON.stringify({ status: runtime.reused ? 'reused' : 'running', protocol: runtime.protocol, installationId: runtime.installationId, runId: runtime.runId, pid: runtime.pid, version: runtime.version, url: runtime.url, policy: 'chatgpt-main/1' }))
} catch {
  process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop); clearTimeout(deadline)
  console.error(JSON.stringify({ status: 'failed', code: 'helper_start_failed' }))
  process.exitCode = 1
}
