import { execFile } from 'node:child_process'
import { promisify, isDeepStrictEqual } from 'node:util'
import { realpath, lstat } from 'node:fs/promises'
import { isAbsolute, resolve } from 'node:path'
import { createNativeIO } from './native-io.mjs'

const execute = promisify(execFile)
const nativePath = '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'
const absolute = value => typeof value === 'string' && isAbsolute(value) && resolve(value) === value && !/[\x00-\x1f\x7f]/.test(value)
const appPid = host => Number(host?.hostRunId?.match(/^([1-9]\d*):/)?.[1])
const qualified = host => host?.available === true && Number.isSafeInteger(appPid(host)) && appPid(host) > 0 && Number.isSafeInteger(host.hostStartedAt) && Date.parse(host.hostRunId.slice(host.hostRunId.indexOf(':') + 1) + ' UTC') === host.hostStartedAt && ['appVersion', 'appBuild', 'cliVersion'].every(key => typeof host.profile?.[key] === 'string' && host.profile[key].length > 0)
const generation = host => ({ appHostRunId: host.hostRunId, appHostStartedAt: host.hostStartedAt, profile: host.profile })
const identity = info => [info.dev, info.ino, info.mode, info.uid, info.ctimeMs]
const unknown = reason => ({ scope: 'unknown', reason })

export async function readSocketBinding(socketPath) {
  const path = await realpath(socketPath), info = await lstat(path)
  if (!info.isSocket() || info.uid !== process.getuid()) throw new Error('socket-unconfirmed')
  return { path, identity: identity(info) }
}

// These are local process facts only. No private App IPC, launch or MCP call
// participates in proving which native App generation owns a connection.
export function createAppActivation({ pluginId, socketPath, sampleHost, execute: run = execute } = {}) {
  const native = sampleHost ? null : createNativeIO({ pluginId, socketPath })
  sampleHost ??= () => native.sampleHost()
  let closed = false
  async function socketOwner() {
    const before = await readSocketBinding(socketPath)
    const { stdout } = await run('/usr/sbin/lsof', ['-nP', '-a', '-U', '-Fpu', '--', before.path], { timeout: 3000, maxBuffer: 65536 })
    const owners = []
    let owner = null
    for (const line of stdout.split('\n')) {
      if (/^p[1-9]\d*$/.test(line)) { owner = { pid: Number(line.slice(1)), uid: null }; owners.push(owner) }
      else if (/^u\d+$/.test(line) && owner) owner.uid = Number(line.slice(1))
    }
    const after = await readSocketBinding(socketPath)
    if (!isDeepStrictEqual(before, after) || owners.length !== 1 || owners[0].uid !== process.getuid()) return null
    return { ...owners[0], socketBinding: after }
  }
  async function connectionContext() {
    if (closed) return unknown('activation-closed')
    try {
      const before = await sampleHost()
      if (!qualified(before)) return unknown('app-source-unconfirmed')
      const previous = structuredClone(generation(before))
      const first = await socketOwner(), second = await socketOwner(), after = await sampleHost()
      if (closed) return unknown('activation-closed')
      if (!qualified(after) || !isDeepStrictEqual(previous, generation(after))) return unknown('app-generation-changed')
      if (!first || !isDeepStrictEqual(first, second)) return unknown('connection-owner-unconfirmed')
      return first.pid === appPid(after) ? { scope: 'app', ...generation(after), socketBinding: first.socketBinding } : { scope: 'configuration-only', reason: 'different-process' }
    } catch { return unknown('connection-verification-unavailable') }
  }
  async function pathFacts(paths) {
    return Promise.all(['nodePath', 'bridgePath', 'configPath'].map(async key => {
      const path = paths?.[key]
      if (!absolute(path) || await realpath(path) !== path) throw new Error('path-unconfirmed')
      const info = await lstat(path)
      if (!info.isFile() || info.uid !== process.getuid() || (key === 'configPath' ? (info.mode & 0o7777) !== 0o600 : Boolean(info.mode & 0o022))) throw new Error('path-unconfirmed')
      return [path, ...identity(info)]
    }))
  }
  async function bridgeProcess(host, paths) {
    const { stdout } = await run('/bin/ps', ['-ww', '-axo', 'uid=,pid=,ppid=,lstart=,comm='], { env: { ...process.env, TZ: 'UTC', LC_ALL: 'C' }, timeout: 3000, maxBuffer: 4194304 })
    const processes = stdout.split('\n').flatMap(line => {
      const match = line.trim().match(/^(\d+)\s+(\d+)\s+(\d+)\s+(.{24})\s+(.+)$/)
      return match ? [{ uid: Number(match[1]), pid: Number(match[2]), ppid: Number(match[3]), started: match[4], executable: match[5] }] : []
    })
    const parents = processes.filter(row => row.pid === appPid(host))
    if (parents.length !== 1 || parents[0].uid !== process.getuid() || parents[0].executable !== nativePath || parents[0].pid + ':' + parents[0].started !== host.hostRunId || Date.parse(parents[0].started + ' UTC') !== host.hostStartedAt) return null
    if (new Set(processes.map(row => row.pid)).size !== processes.length) return null
    const children = processes.filter(row => row.uid === parents[0].uid && row.ppid === parents[0].pid && row.pid > 0 && row.pid !== parents[0].pid && row.executable === paths.nodePath && Date.parse(row.started + ' UTC') >= host.hostStartedAt)
    if (!children.length) return null
    const args = await run('/bin/ps', ['-ww', '-p', children.map(row => row.pid).join(','), '-o', 'pid=,args='], { timeout: 3000, maxBuffer: 65536 })
    const expected = paths.nodePath + ' ' + paths.bridgePath + ' --config ' + paths.configPath
    const commands = args.stdout.split('\n').flatMap(line => {
      const match = line.trim().match(/^(\d+)\s+(.+)$/)
      return match ? [{ pid: Number(match[1]), command: match[2] }] : []
    })
    if (new Set(commands.map(row => row.pid)).size !== commands.length) return null
    const matches = children.filter(child => commands.some(row => row.pid === child.pid && row.command === expected)).sort((left, right) => left.pid - right.pid)
    return matches[0] ?? null
  }
  async function appActivationWitness(paths) {
    if (closed) return null
    try {
      const files = await pathFacts(paths), before = await sampleHost()
      if (!qualified(before)) return null
      const previous = structuredClone(generation(before))
      const first = await bridgeProcess(before, paths), second = await bridgeProcess(before, paths), after = await sampleHost()
      const currentFiles = await pathFacts(paths)
      if (closed || !first || !isDeepStrictEqual(first, second) || !qualified(after) || !isDeepStrictEqual(previous, generation(after)) || !isDeepStrictEqual(files, currentFiles)) return null
      return { appHostRunId: after.hostRunId, appHostStartedAt: after.hostStartedAt, bridgePid: second.pid, bridgeEntry: paths.bridgePath }
    } catch { return null }
  }
  return { connectionContext, appActivationWitness, async close() { closed = true; await native?.close() } }
}
