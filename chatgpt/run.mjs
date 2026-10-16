// Internal auxiliary-process entry. Installation and App registration are
// separate lifecycle gates; this never opens the App or controls a chat.
import { startFromInstallation } from './runtime.mjs'

const args = process.argv.slice(2)
if (args.length !== 2 || args[0] !== '--config') throw new Error('usage: node chatgpt/run.mjs --config <private installation.json>')
const runtime = await startFromInstallation({ configPath: args[1] })
console.log(JSON.stringify({ adapter: 'ghost-sentinel-chatgpt', url: runtime.url, pid: process.pid, policy: 'chatgpt-main/1' }))
let stopping = false
async function stop() { if (stopping) return; stopping = true; await runtime.close() }
process.once('SIGINT', () => { void stop() }); process.once('SIGTERM', () => { void stop() })
