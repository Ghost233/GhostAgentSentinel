import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCore } from '../src/core/index.mjs';

const task = { id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' };
const event = (id, kind, capturedAt, text, data = {}, target = task) => ({ id, kind, capturedAt, occurredAt: null, text, data, taskId: target.id, taskLifecycleId: target.lifecycleId });
async function fixture(t, options = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-activity-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 24 * 60 * 60 * 1000 * 2, ...options });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
  await core.ingest(source, { tasks: [task], events: [event('request', 'input', time, '实现日志')] });
  return { core, source, now: () => time, advance: ms => { time += ms; return time; } };
}

test('observed activity updates during paused analysis without presenting an agent claim as verified progress', async t => {
  const f = await fixture(t);
  await f.core.pause();
  const capturedAt = f.advance(1);
  await f.core.ingest(f.source, { events: [event('output', 'output', capturedAt, '日志已经准备好')] });
  const current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /代理输出.*日志已经准备好/);
  assert.equal(current.activity.observedAt, capturedAt);
  assert.ok(current.activity.evidenceIds.includes('output'));
  assert.equal(current.analysis.state, 'paused');
});

test('new activity stays visible while an external analyzer is still pending', async t => {
  let analysisCalls = 0;
  const f = await fixture(t, { analyzer: { analyze() { analysisCalls++; return new Promise(() => {}); } } });
  await Promise.resolve();
  await f.core.ingest(f.source, { events: [event('working', 'output', f.advance(1), '正在核对参数')] });
  let current = (await f.core.snapshot()).tasks[0];
  assert.equal(current.analysis.state, 'analyzing');
  assert.match(current.activity.summary, /代理输出.*正在核对参数/);
  await f.core.ingest(f.source, { events: [event('reading', 'tool-start', f.advance(1), '读取配置', { callId: 'call', toolName: 'read_file' })] });
  f.advance(3_000);
  current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /read_file.*尚未观察到结束.*至少 3 秒/);
  assert.equal(analysisCalls, 1);
});

test('waiting tools use retained start lower bounds and only their associated completion clears the wait', async t => {
  const f = await fixture(t);
  const start = f.advance(1);
  await f.core.ingest(f.source, { events: [event('start', 'tool-start', start, '执行命令', { callId: 'call', toolName: 'run_code' })] });
  f.advance(5_000);
  await f.core.ingest(f.source, { events: [event('unrelated', 'tool-result', f.now(), '另一调用的结果', { callId: 'other-call', exitCode: 7 })] });
  let current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /尚未观察到结束.*至少 5 秒/);
  assert.ok(current.activity.evidenceIds.includes('start'));
  await f.core.ingest(f.source, { events: [event('start', 'tool-start', start, '重复补传', { callId: 'call', toolName: 'run_code' })] });
  assert.match((await f.core.snapshot()).tasks[0].activity.summary, /至少 5 秒/);
  await f.core.ingest(f.source, { events: [event('result', 'tool-result', f.advance(1), '返回结果', { callId: 'call', exitCode: 0 })] });
  current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /工具结果.*返回结果.*退出码 0/);
  assert.doesNotMatch(current.activity.summary, /尚未观察到结束|已通过|验证通过|测试通过/);
  assert.ok(current.activity.evidenceIds.includes('result'));
});

test('old request activity and another task lifecycle cannot supply or complete current activity', async t => {
  const f = await fixture(t);
  await f.core.ingest(f.source, { events: [event('old-start', 'tool-start', f.advance(1), '旧请求动作', { callId: 'reused' }), event('new-request', 'input', f.advance(1), '新的范围')] });
  let current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /尚无动作/);
  assert.doesNotMatch(current.activity.summary, /旧请求动作|尚未观察到结束/);
  const other = { ...task, lifecycleId: 'other-life' };
  await f.core.ingest(f.source, { tasks: [other], events: [event('current-start', 'tool-start', f.advance(1), '当前执行', { callId: 'reused' }), event('foreign-result', 'tool-result', f.advance(1), '其它代次已结束', { callId: 'reused', exitCode: 0 }, other)] });
  current = (await f.core.snapshot()).tasks.find(value => value.lifecycleId === 'life');
  assert.match(current.activity.summary, /尚未观察到结束/);
  assert.ok(current.activity.evidenceIds.includes('current-start'));
  assert.ok(!current.activity.evidenceIds.includes('foreign-result'));
});

test('source lease loss and observation gaps turn unmatched starts into historical or unknown activity', async t => {
  const f = await fixture(t, { leaseMs: 1_000 });
  await f.core.ingest(f.source, { events: [event('start', 'tool-start', f.advance(1), '可能等待', { callId: 'call' })] });
  await f.core.ingest(f.source, { events: [event('gap', 'gap', f.advance(1), '丢失中间记录')] });
  let current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /覆盖不足|无法确认/);
  assert.doesNotMatch(current.activity.summary, /已观察至少/);
  f.advance(1_001);
  current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /来源失联|历史观察/);
  assert.doesNotMatch(current.activity.summary, /已观察至少/);
});

test('24-hour expiry removes old activity text and evidence rather than renewing an open tool wait', async t => {
  const f = await fixture(t);
  await f.core.ingest(f.source, { events: [event('start', 'tool-start', f.advance(1), '过期动作原文', { callId: 'call' })] });
  await f.core.pause();
  f.advance(24 * 60 * 60 * 1000 + 1);
  const current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /当前请求未知/);
  assert.doesNotMatch(current.activity.summary, /过期动作原文|已观察至少/);
  assert.equal(current.activity.observedAt, null);
  assert.deepEqual(current.activity.evidenceIds, []);
});

test('missing call linkage and interrupted results remain explicit without manufactured execution or validation success', async t => {
  const f = await fixture(t);
  await f.core.ingest(f.source, { events: [event('no-call', 'tool-start', f.advance(1), '缺少调用身份')] });
  let current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /调用关联未知/);
  assert.doesNotMatch(current.activity.summary, /已观察至少/);
  await f.core.ingest(f.source, { events: [event('interrupted', 'tool-result', f.advance(1), '命令被取消', { callId: 'call', interrupted: true, exitCode: 0 })] });
  current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /中断.*命令被取消/);
  assert.doesNotMatch(current.activity.summary, /正常|已通过|验证通过|测试通过/);
  await f.core.ingest(f.source, { tasks: [{ ...task, status: 'idle' }] });
  assert.match((await f.core.snapshot()).tasks[0].activity.summary, /历史观察|不在运行/);
});

test('native refinement enriches its original completion and cannot close a newer invocation reusing the call identity', async t => {
  const f = await fixture(t);
  const originalTime = f.advance(1);
  await f.core.ingest(f.source, { events: [event('fallback', 'tool-result', originalTime, '普通工具结果', { callId: 'reused' }), event('native', 'tool-result', originalTime, '真实原生命令结果', { callId: 'reused', resultSource: 'native-shell-canonical', refinesEventId: 'fallback', exitCode: 7 })] });
  let current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /真实原生命令结果.*退出码 7/);
  assert.equal(current.activity.observedAt, originalTime);
  assert.ok(current.activity.evidenceIds.includes('native'));
  await f.core.ingest(f.source, { events: [event('new-start', 'tool-start', f.advance(1), '新的执行', { callId: 'reused', toolName: 'run_code' }), event('late-refinement', 'tool-result', originalTime, '旧执行晚到补充', { callId: 'reused', resultSource: 'native-shell-canonical', refinesEventId: 'fallback', exitCode: 7 })] });
  current = (await f.core.snapshot()).tasks[0];
  assert.match(current.activity.summary, /尚未观察到结束/);
  assert.ok(current.activity.evidenceIds.includes('new-start'));
});
