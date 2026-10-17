import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:net'
import { mkdtemp, realpath, writeFile, rm, symlink, chmod, lstat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { createAppActivation } from '../chatgpt/app-activation.mjs'

const started = 'Sat Oct 17 01:00:00 2026'
const nativePath = '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'
const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const host = () => ({ available: true, hostRunId: '12:' + started, hostStartedAt: Date.parse(started + ' UTC'), profile: { ...profile } })

async function fixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-app-')))
  const socketPath = join(await realpath(tmpdir()), 'gs-aa-' + randomUUID().slice(0, 8) + '.sock')
  const server = createServer()
  await new Promise(resolve => server.listen(socketPath, resolve))
  const paths = { nodePath: join(root, 'node runtime'), bridgePath: join(root, 'plugin-bridge.mjs'), configPath: join(root, 'config.json') }
  for (const path of Object.values(paths)) await writeFile(path, '', { mode: 0o600 })
  const uid = process.getuid(), calls = []
  const state = { hosts: [host()], listenerPids: [12], processRows: [{ uid, pid: 12, ppid: 11, started, executable: nativePath }, { uid, pid: 13, ppid: 12, started, executable: paths.nodePath }], command: paths.nodePath + ' ' + paths.bridgePath + ' --config ' + paths.configPath }
  const activation = createAppActivation({ pluginId: 'sentinel@test', socketPath, sampleHost: async () => state.hosts.length > 1 ? state.hosts.shift() : state.hosts[0], execute: async (file, args, options) => {
    calls.push({ file, args, options })
    await state.onExecute?.(file, args)
    if (state.error) throw new Error(state.error)
    if (file === '/usr/sbin/lsof') return { stdout: state.listenerPids.map(pid => 'p' + pid + '\nu' + (state.listenerUid ?? uid) + '\nf3\nn' + socketPath + '\n').join('') }
    if (file === '/bin/ps' && args.includes('uid=,pid=,ppid=,lstart=,comm=')) return { stdout: state.processRows.map(row => [row.uid, row.pid, row.ppid, row.started, row.executable].join(' ')).join('\n') }
    if (file === '/bin/ps' && args.includes('pid=,args=')) return { stdout: state.argsOutput ?? '13 ' + state.command + '\n' }
    throw new Error('unexpected IO Bearer MUST_NOT_APPEAR')
  } })
  t.after(async () => { await activation.close(); await new Promise(resolve => server.close(resolve)); await rm(root, { recursive: true, force: true }); await rm(socketPath, { force: true }) })
  return { activation, state, paths, calls, socketPath, uid }
}

test('connection scope proves that the canonical listener belongs to the same current App generation', async t => {
  const f = await fixture(t)
  const info = await lstat(f.socketPath)
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'app', appHostRunId: '12:' + started, appHostStartedAt: Date.parse(started + ' UTC'), profile, socketBinding: { path: f.socketPath, identity: [info.dev, info.ino, info.mode, info.uid, info.ctimeMs] } })
  assert.equal(f.calls.filter(call => call.file === '/usr/sbin/lsof').length, 2)
  assert.ok(f.calls.every(call => call.options.timeout <= 3000 && call.options.maxBuffer <= 4194304))
})

test('a separate daemon listener stays configuration-only and absent or ambiguous listeners stay unknown', async t => {
  const f = await fixture(t)
  f.state.listenerPids = [85457]
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'configuration-only', reason: 'different-process' })
  for (const pids of [[], [12, 85457]]) {
    f.state.listenerPids = pids
    assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'connection-owner-unconfirmed' })
  }
})

test('unavailable App facts and a late App or profile change cannot prove connection scope', async t => {
  const f = await fixture(t)
  for (const facts of [{ available: false }, { ...host(), profile: null }, { ...host(), hostStartedAt: null }, { ...host(), hostRunId: '12:unverified' }]) {
    f.state.hosts = [facts]
    assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'app-source-unconfirmed' })
  }
  for (const after of [{ ...host(), hostRunId: '14:' + started }, { ...host(), hostStartedAt: 1 }, { ...host(), profile: { ...profile, appBuild: 'other-build' } }]) {
    f.state.hosts = [host(), after]
    assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'app-generation-changed' })
  }
})

test('only an exact live bridge directly parented by the current App native process is an activation witness', async t => {
  const f = await fixture(t)
  assert.deepEqual(await f.activation.appActivationWitness(f.paths), { appHostRunId: '12:' + started, appHostStartedAt: Date.parse(started + ' UTC'), bridgePid: 13, bridgeEntry: f.paths.bridgePath })
  assert.equal(f.calls.some(call => call.file === '/usr/sbin/lsof'), false, 'the App-origin bridge can prove native activation even when the configuration socket belongs to a daemon')
})

test('foreign UID, missing native facts, daemon children, owner processes and inexact bridge commands are never witnesses', async t => {
  const f = await fixture(t), rows = structuredClone(f.state.processRows), command = f.state.command
  const cases = [
    () => { f.state.processRows[0].uid++ },
    () => { f.state.processRows[1].uid++ },
    () => { f.state.processRows.shift() },
    () => { f.state.processRows[0].started = 'Sat Oct 17 00:59:59 2026' },
    () => { f.state.processRows[0].executable = '/unrelated/codex' },
    () => { f.state.processRows[1].ppid = 85457 },
    () => { f.state.processRows[1].executable = '/unrelated/node' },
    () => { f.state.processRows[1].started = 'Sat Oct 17 00:59:59 2026' },
    () => { f.state.command = command.replace('plugin-bridge.mjs', 'observer-owner.mjs') },
    () => { f.state.command = command.replace('--config ', '--other ') },
    () => { f.state.command = command + ' --extra' },
    () => { f.state.command = command + '-other-installation' },
    () => { f.state.processRows.push({ ...f.state.processRows[1], ppid: 85457 }) },
    () => { f.state.argsOutput = '13 ' + command + '\n13 ' + command + '\n' },
  ]
  for (const change of cases) {
    f.state.processRows = structuredClone(rows); f.state.command = command; delete f.state.argsOutput
    change()
    assert.equal(await f.activation.appActivationWitness(f.paths), null)
  }
})

test('multiple chats may have exact App-origin bridges and a deterministic stable bridge proves activation', async t => {
  const f = await fixture(t)
  f.state.processRows.push({ ...f.state.processRows[1], pid: 14 })
  f.state.argsOutput = '14 ' + f.state.command + '\n13 ' + f.state.command + '\n'
  assert.deepEqual(await f.activation.appActivationWitness(f.paths), { appHostRunId: '12:' + started, appHostStartedAt: Date.parse(started + ' UTC'), bridgePid: 13, bridgeEntry: f.paths.bridgePath })
  let inventories = 0
  f.state.onExecute = (file, args) => {
    if (file === '/bin/ps' && args.includes('uid=,pid=,ppid=,lstart=,comm=') && ++inventories === 2) {
      f.state.processRows = f.state.processRows.filter(row => row.pid !== 13)
      f.state.argsOutput = '14 ' + f.state.command + '\n'
    }
  }
  assert.equal(await f.activation.appActivationWitness(f.paths), null, 'losing the selected bridge during verification stays pending even when another chat bridge exists')
})

test('twenty valid App-origin chat bridges retain a stable activation witness within bounded process IO', async t => {
  const f = await fixture(t), bridge = f.state.processRows[1]
  f.state.processRows = [f.state.processRows[0], ...Array.from({ length: 20 }, (_, index) => ({ ...bridge, pid: 13 + index }))]
  f.state.argsOutput = f.state.processRows.slice(1).reverse().map(row => row.pid + ' ' + f.state.command + '\n').join('')
  assert.deepEqual(await f.activation.appActivationWitness(f.paths), { appHostRunId: '12:' + started, appHostStartedAt: Date.parse(started + ' UTC'), bridgePid: 13, bridgeEntry: f.paths.bridgePath })
  const commands = f.calls.filter(call => call.args.includes('pid=,args='))
  assert.equal(commands.length, 2)
  for (const command of commands) {
    assert.equal(command.args[command.args.indexOf('-p') + 1].split(',').length, 20)
    assert.equal(command.options.maxBuffer, 65536)
    assert.equal(command.options.timeout, 3000)
  }
})

test('activation witness accepts current-user files with protected executable and bridge modes plus a private config', async t => {
  const f = await fixture(t)
  await chmod(f.paths.nodePath, 0o755)
  await chmod(f.paths.bridgePath, 0o644)
  for (const path of Object.values(f.paths)) assert.equal((await lstat(path)).uid, process.getuid())
  assert.deepEqual(await f.activation.appActivationWitness(f.paths), { appHostRunId: '12:' + started, appHostStartedAt: Date.parse(started + ' UTC'), bridgePid: 13, bridgeEntry: f.paths.bridgePath })
})

test('initially writable node or bridge files and any nonprivate config mode fail before process sampling', async t => {
  const f = await fixture(t)
  for (const [key, mode] of [['nodePath', 0o620], ['nodePath', 0o602], ['bridgePath', 0o620], ['bridgePath', 0o602], ['configPath', 0o644], ['configPath', 0o640], ['configPath', 0o400], ['configPath', 0o700]]) {
    await chmod(f.paths[key], mode)
    assert.equal(await f.activation.appActivationWitness(f.paths), null, key + ' mode ' + mode.toString(8))
    assert.equal(f.calls.length, 0, 'unsafe initial artifacts must fail before ps')
    await chmod(f.paths[key], 0o600)
  }
})

test('a canonical system executable owned by another UID cannot become a bridge activation witness', async t => {
  const f = await fixture(t), nodePath = await realpath('/bin/ls'), info = await lstat(nodePath)
  if (info.uid === process.getuid()) { t.skip('the available system executable belongs to the test UID'); return }
  f.state.processRows[1].executable = nodePath
  f.state.command = nodePath + ' ' + f.paths.bridgePath + ' --config ' + f.paths.configPath
  assert.equal(await f.activation.appActivationWitness({ ...f.paths, nodePath }), null)
  assert.equal(f.calls.length, 0, 'foreign ownership must fail before ps even when process and command facts would match')
})

test('App absence, noncanonical paths and unavailable process IO do not leak commands or become witnesses', async t => {
  const f = await fixture(t)
  f.state.hosts = [{ available: false, reason: 'Bearer MUST_NOT_APPEAR' }]
  assert.equal(await f.activation.appActivationWitness(f.paths), null)
  assert.equal(f.calls.length, 0)
  f.state.hosts = [host()]
  await symlink(f.paths.configPath, f.paths.configPath + '.alias')
  for (const paths of [null, { ...f.paths, bridgePath: '/missing/bridge.mjs' }, { ...f.paths, configPath: f.paths.configPath + '.alias' }, { ...f.paths, configPath: f.paths.configPath + '\n' }]) assert.equal(await f.activation.appActivationWitness(paths), null)
  f.state.error = 'Bearer MUST_NOT_APPEAR'
  assert.equal(await f.activation.appActivationWitness(f.paths), null)
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'connection-verification-unavailable' })
})

test('a late bridge restart, native App change or changed activation file invalidates the witness', async t => {
  for (const change of ['bridge', 'native', 'profile', 'file']) {
    const f = await fixture(t)
    if (change === 'native') f.state.hosts = [host(), { ...host(), hostRunId: '14:' + started }]
    if (change === 'profile') f.state.hosts = [host(), { ...host(), profile: { ...profile, cliVersion: 'other-version' } }]
    let inventory = 0
    f.state.onExecute = async (file, args) => {
      if (file === '/bin/ps' && args.includes('uid=,pid=,ppid=,lstart=,comm=') && ++inventory === 2) {
        if (change === 'bridge') f.state.processRows[1].started = 'Sat Oct 17 01:00:01 2026'
        if (change === 'file') await writeFile(f.paths.configPath, 'changed configuration')
      }
    }
    assert.equal(await f.activation.appActivationWitness(f.paths), null, change)
  }
})

test('foreign or changing socket ownership is unknown and closing activation leaves no sampling activity', async t => {
  const f = await fixture(t)
  f.state.listenerUid = f.uid + 1
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'connection-owner-unconfirmed' })
  delete f.state.listenerUid
  let listeners = 0
  f.state.onExecute = file => { if (file === '/usr/sbin/lsof' && ++listeners === 2) f.state.listenerPids = [85457] }
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'connection-owner-unconfirmed' })
  await f.activation.close()
  const count = f.calls.length
  assert.equal(await f.activation.appActivationWitness(f.paths), null)
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'activation-closed' })
  assert.equal(f.calls.length, count)
})

test('mutating sampled App facts or closing during verification cannot produce a late witness', async t => {
  const f = await fixture(t)
  const sampled = host()
  f.state.hosts = [sampled]
  let inventories = 0
  f.state.onExecute = (file, args) => { if (file === '/bin/ps' && args.includes('uid=,pid=,ppid=,lstart=,comm=') && ++inventories === 2) sampled.profile.appBuild = 'changed-during-verification' }
  assert.equal(await f.activation.appActivationWitness(f.paths), null)
  f.state.hosts = [host()]
  f.state.onExecute = () => f.activation.close()
  assert.deepEqual(await f.activation.connectionContext(), { scope: 'unknown', reason: 'activation-closed' })
})
