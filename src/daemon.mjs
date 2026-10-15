#!/usr/bin/env node
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { startServer } from './server.mjs'
import { connectCore, defaultDataDir } from './core-client.mjs'

export async function main(args = process.argv.slice(2)) {
  const value = flag => { const index = args.indexOf(flag); return index < 0 ? undefined : args[index + 1] }
  const dataDir = resolve(value('--data-dir') || defaultDataDir)
  if (args.includes('--serve')) {
    let server
    const shutdown = async () => { await server?.close() }
    server = await startServer({ dataDir, idleGraceMs: 10_000, onIdle: shutdown })
    process.once('SIGTERM', shutdown)
    process.once('SIGINT', shutdown)
    return
  }
  const command = args[0]
  if (!['start', 'status', 'pause', 'resume', 'configure'].includes(command)) {
    console.log('GhostAgentSentinel\n  start | status | pause | resume [--data-dir PATH]\n  configure --llm-url URL --llm-model ID --jev-url URL --jev-model ID\n  --serve [--data-dir PATH]   foreground diagnostics\nDSH adapters start the core automatically. Only loopback model URLs are supported.')
    return
  }
  const client = await connectCore({ dataDir })
  try {
    if (command === 'start') console.log('Core ready; it exits when no source remains.')
    else if (command === 'status') console.log(JSON.stringify(await client.snapshot(), null, 2))
    else if (command === 'configure') {
      const config = { llm: { baseUrl: value('--llm-url'), model: value('--llm-model') }, jev: { baseUrl: value('--jev-url'), model: value('--jev-model') } }
      console.log(JSON.stringify(await client.configureAnalysis(config), null, 2))
    } else await client[command]()
  } finally { await client.close() }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(`Sentinel: ${error.message}`); process.exitCode = 1 })
}
