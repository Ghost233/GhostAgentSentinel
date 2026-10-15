import { req } from './dsh-native-runtime.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, realpath, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { extractVerificationEvidence } from '../dsh/verification-evidence.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { mountAgentLoopTestDependencies } = req('@deepseek-ai/dsh-agent-loop-testkit')

async function nativeShell(t) {
  const cwd = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-verification-')))
  const ctx = new Context(), observations = []
  t.after(async () => { await ctx.fiber.dispose(); await rm(cwd, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx, { workingDirectory: true })
  await ctx.plugin(req('@deepseek-ai/dsh-subprocess-local').default)
  await ctx.plugin(req('@deepseek-ai/dsh-shell-env'))
  await ctx.plugin(req('@deepseek-ai/dsh-bash-local').LocalBashExecutor, { timeoutMs: 2000 })
  await ctx.plugin(req('@deepseek-ai/dsh-tool-bash'))
  ctx.on('tools/result', (exec, result) => observations.push({ type: 'tools/result', exec, result }))
  return { ctx, cwd, observations,
    async run(command, callId = 'native-shell') {
      const result = await ctx.tools.execute({ callId, name: 'bash', arguments: { command, description: 'Fixed local verification test', workdir: cwd }, signal: new AbortController().signal })
      return { result, observed: observations.findLast(item => item.exec.callId === callId) }
    },
  }
}

test('a real native foreground result supplies its actual exit status and resolved cwd without changing the tool outcome', async t => {
  const f = await nativeShell(t)
  const { result, observed } = await f.run("printf 'sentinel-fixed-output'; exit 0")
  assert.equal(result.isError, false)
  assert.equal(result.value.stdout.text, 'sentinel-fixed-output')
  const facts = extractVerificationEvidence(observed)
  assert.equal(facts.data.exitCode, 0)
  assert.equal(facts.data.cwd, f.cwd)
  assert.equal(facts.data.command, "printf 'sentinel-fixed-output'; exit 0")
  assert.equal(facts.data.resultSource, 'native-shell-canonical')
  assert.ok(facts.gaps.some(gap => gap.includes('版本')))
  assert.equal(Object.isFrozen(observed.result), true)
  assert.equal(result.value.exitCode, 0)
})

test('native exit 1 and exit 7 remain process failures even when both tool calls return isError false', async t => {
  const f = await nativeShell(t)
  for (const code of [1, 7]) {
    const { result, observed } = await f.run(`printf 'fixed'; exit ${code}`, `exit-${code}`)
    assert.equal(result.isError, false)
    assert.equal(extractVerificationEvidence(observed).data.exitCode, code)
  }
})

test('real Node run_code yields shell facts from its nested dispatch, while durable replay and the outer return cannot supply an exit code', async t => {
  const f = await nativeShell(t)
  await f.ctx.plugin(req('@deepseek-ai/dsh-agent-loop').default, { agents: [] })
  await f.ctx.plugin(req('@deepseek-ai/dsh-sandbox-local').default)
  await f.ctx.plugin(req('@deepseek-ai/dsh-sandbox-policy').default, { mode: 'danger-full-access', workspaceRoot: f.cwd })
  await f.ctx.plugin(req('@deepseek-ai/dsh-ptc-runtime-node').default, { timeoutMs: 10_000, maxTimeoutMs: 10_000 })
  const agent = await f.ctx.agentLoop.create('verification-ptc', {}, { cwd: f.cwd })
  agent.ctx.tools.presentAs('ptc')
  const command = "printf 'ptc-fixed'; exit 7"
  const outer = await f.ctx.tools.execute({ callId: 'ptc-root', name: 'run_code', agent,
    arguments: { description: 'Fixed nested shell test', code: `return await tools.bash(${JSON.stringify({ command, description: 'Fixed output' })})` }, signal: new AbortController().signal })
  assert.equal(outer.isError, false)
  const inner = f.observations.find(item => item.exec.name === 'bash')
  assert.equal(inner.result.value.exitCode, 7)
  assert.equal(extractVerificationEvidence(inner).data.exitCode, 7)
  const outerFacts = extractVerificationEvidence(f.observations.find(item => item.exec.name === 'run_code'))
  assert.equal(outerFacts.data.exitCode, undefined)
  const dispatch = agent.session.snapshotEvents().find(event => event.type === 'tool/ptc-dispatch')
  assert.ok(dispatch)
  assert.equal(dispatch.data.value, undefined)
  const replay = extractVerificationEvidence({ type: 'session/event', event: dispatch })
  assert.equal(replay.data.exitCode, undefined)
  assert.equal(replay.data.cwd, f.cwd)
  assert.ok(replay.gaps.some(gap => gap.includes('退出码')))
})

test('tool success, textual exit markers and arbitrary returned JSON cannot become process or validation evidence', async t => {
  const f = await nativeShell(t)
  f.ctx.tools.register({ name: 'read_tool', description: 'Fixed JSON, no process execution', parameters: {},
    output: { schema: { type: 'object', properties: { exitCode: { type: 'integer' } }, required: ['exitCode'] },
      render: () => [{ type: 'text', text: '[exit code: 0]' }] }, execute() { return { exitCode: 0 } } })
  const arbitrary = await f.ctx.tools.execute({ callId: 'not-process', name: 'read_tool', arguments: {}, signal: new AbortController().signal })
  assert.equal(arbitrary.isError, false)
  const plain = extractVerificationEvidence(f.observations.find(item => item.exec.callId === 'not-process'))
  assert.equal(plain.data.exitCode, undefined)
  assert.equal(plain.data.validationScope, undefined)
  const partial = extractVerificationEvidence({ type: 'tools/result', exec: { name: 'bash', arguments: { command: 'npm test' } }, result: arbitrary })
  assert.equal(partial.data.exitCode, undefined)
  assert.equal(partial.data.validationScope, undefined)
  assert.ok(partial.gaps.some(gap => gap.includes('结构化')))
  const withoutCommand = extractVerificationEvidence({ type: 'tools/result', exec: { name: 'bash', arguments: {} }, result: arbitrary })
  assert.equal(withoutCommand.data.command, undefined)
})

test('a native command cut short by its deadline retains interruption evidence and cannot stand for complete validation', async t => {
  const f = await nativeShell(t)
  const result = await f.ctx.tools.execute({ callId: 'deadline', name: 'bash',
    arguments: { command: "printf 'before-wait'; sleep 1", description: 'Fixed deadline test', workdir: f.cwd, timeoutMs: 20 }, signal: new AbortController().signal })
  assert.equal(result.isError, false)
  assert.equal(result.value.timedOut, true)
  const facts = extractVerificationEvidence(f.observations.find(item => item.exec.callId === 'deadline'))
  assert.equal(facts.data.timedOut, true)
  assert.equal(facts.data.interrupted, true)
  assert.equal(facts.data.sourceVersion, undefined)
})
