import { writeFileSync } from 'node:fs'
const [mode, output] = process.argv.slice(2)
process.on('SIGTERM', () => {
  if (mode === 'ignore') return
  writeFileSync(output, JSON.stringify({ signal: 'SIGTERM', nodeOptionsPresent: Object.hasOwn(process.env, 'NODE_OPTIONS'), nodePathPresent: Object.hasOwn(process.env, 'NODE_PATH') }) + '\n')
  process.exit(0)
})
process.on('SIGINT', () => { writeFileSync(output, JSON.stringify({ signal: 'SIGINT' }) + '\n'); process.exit(0) })
process.stdout.write(JSON.stringify({ ready: true, pid: process.pid }) + '\n')
setInterval(() => {}, 1000)
