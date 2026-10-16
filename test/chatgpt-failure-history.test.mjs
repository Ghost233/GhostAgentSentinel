import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';
import { CHECK_IDS } from '../src/analysis/index.mjs';

test('history and inherited records stay context without updating activity, request, status or new validation input', async t => {
  for (const flag of ['history', 'inherited']) await t.test(flag, async t => {
    const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-history-guard-'));
    let time = 1_000_000;
    const inputs = [];
    const core = await createCore({ dataDir, now: () => time, cooldownMs: 0, analyzer: { async analyze(input) {
      inputs.push(input);
      return { summary: { currentTask: '', currentState: '', recentProgress: '', attention: '' }, checks: CHECK_IDS.map(id => ({ id, status: 'unknown', reason: 'insufficient', evidenceIds: [] })), coverage: { dependencies: [] } };
    } } });
    t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
    const sourceKey = await core.registerSource({ registrationId: 'history-guard', hostRunId: 'host', name: 'DSH control' });
    const event = (id, kind, data = {}) => ({ id, taskId: 'main', taskLifecycleId: 'run', kind, text: id, data, occurredAt: time, capturedAt: time });
    await core.ingest(sourceKey, { tasks: [{ id: 'main', lifecycleId: 'run', title: 'Main', status: 'running' }], events: [event('current-request', 'input'), event('current-output', 'output')] });
    await delay(1);
    time += 100;
    await core.ingest(sourceKey, { events: [event('old-request', 'input', { [flag]: true }), event('old-validation', 'tool-result', { [flag]: true, exitCode: 0, sourceVersion: 'old-source', validationScope: 'old-validation' }), event('old-status', 'status', { [flag]: true, status: 'idle' })] });
    const task = (await core.snapshot()).tasks[0];
    assert.equal(task.status, 'running');
    assert.equal(task.currentRequest, 'current-request');
    assert.equal(task.lastActivity, 1_000_000);
    assert.equal(task.events.some(value => value.id === 'old-validation'), true, 'history remains available as context');
    await delay(1);
    assert.ok(inputs.length >= 2);
    assert.equal(inputs.at(-1).events.some(value => value.id.startsWith('old-')), false, 'old verification cannot become fresh analysis evidence');
  });
});
