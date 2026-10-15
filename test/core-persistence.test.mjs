import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCore } from '../src/core/index.mjs';
import { DatabaseSync } from 'node:sqlite';

test('durable evidence is isolated by source lifecycle and replay does not renew retention', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-core-'));
  let time = 1_000_000;
  let core = await createCore({ dataDir, now: () => time });
  try {
    const first = await core.registerSource({ registrationId: 'profile', hostRunId: 'host-a', name: 'A' });
    const second = await core.registerSource({ registrationId: 'profile', hostRunId: 'host-b', name: 'B' });
    assert.notEqual(first, second);
    const task = { id: 'same-id', lifecycleId: 'life', title: 'Task', status: 'running' };
    const event = { id: 'event', taskId: task.id, taskLifecycleId: task.lifecycleId, kind: 'input', occurredAt: null, capturedAt: time, text: 'first request' };
    assert.equal((await core.ingest(first, { tasks: [task], events: [event] })).accepted, 1);
    time += 10;
    assert.equal((await core.ingest(first, { events: [{ ...event, capturedAt: time }] })).duplicate, 1);
    await core.ingest(second, { tasks: [task], events: [{ ...event, text: 'second request' }] });
    let state = await core.snapshot();
    assert.equal(state.tasks.length, 2);
    assert.deepEqual(state.tasks.map(value => value.currentRequest).sort(), ['first request', 'second request']);
    await core.close();
    core = await createCore({ dataDir, now: () => time });
    state = await core.snapshot();
    assert.equal(state.tasks.every(value => value.status === 'unknown'), true);
    assert.equal(state.tasks.reduce((count, value) => count + value.events.length, 0), 2);
    time += 24 * 60 * 60 * 1000 + 1;
    const restored = await core.registerSource({ registrationId: 'profile', hostRunId: 'host-a', name: 'A' });
    assert.equal(restored, first);
    assert.equal((await core.ingest(restored, { events: [event] })).expired, 1);
    state = await core.snapshot();
    assert.equal(state.tasks.reduce((count, value) => count + value.events.length, 0), 0);
  } finally {
    await core.close();
    await rm(dataDir, { recursive: true, force: true });
  }
});

test('a failed retention delete remains retryable and clears the expired request after storage recovers', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-retention-failure-'));
  const day = 24 * 60 * 60 * 1000;
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: day * 2 });
  let blocker;
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'host', name: 'A' });
    const task = { id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' };
    await core.ingest(source, { tasks: [task], events: [{ id: 'request', taskId: task.id,
      taskLifecycleId: task.lifecycleId, kind: 'input', occurredAt: null, capturedAt: time, text: 'expired request' }] });
    blocker = new DatabaseSync(join(dataDir, 'observations.sqlite'));
    blocker.exec('BEGIN IMMEDIATE');
    time += day + 1;
    try {
      await assert.rejects(core.snapshot(), /database is locked/);
    } finally {
      blocker.exec('ROLLBACK');
    }
    const recovered = await core.snapshot();
    assert.equal(recovered.tasks.length, 1, 'the running task stays observable');
    assert.deepEqual(recovered.tasks[0].events, []);
    assert.equal(recovered.tasks[0].currentRequest, '', 'expired input cannot remain the current request');
    assert.ok(recovered.tasks[0].coverage.gaps.includes('Earlier evidence expired after 24 hours'));
    assert.ok(recovered.tasks[0].analysis.checks.every(check => check.status === 'unknown'));
    await core.close();
    blocker.close(); blocker = undefined;
    const reopened = await createCore({ dataDir, now: () => time });
    try {
      const state = await reopened.snapshot();
      assert.equal(state.tasks.reduce((count, value) => count + value.events.length, 0), 0);
      assert.ok(state.tasks.every(value => value.currentRequest === ''));
    } finally {
      await reopened.close();
    }
  } finally {
    blocker?.close();
    await core.close();
    await rm(dataDir, { recursive: true, force: true });
  }
});

for (const restart of [false, true]) {
  test(`partial retention deletion clears expired request references after ${restart ? 'restart' : 'retry'}`, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-partial-retention-'));
    const day = 24 * 60 * 60 * 1000;
    let time = 1_000_000, fault;
    let core = await createCore({ dataDir, now: () => time, leaseMs: day * 2 });
    try {
      const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'host', name: 'A' });
      const task = { id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' };
      await core.ingest(source, { tasks: [task], events: [
        { id: 'request', taskId: task.id, taskLifecycleId: task.lifecycleId, kind: 'input', occurredAt: null, capturedAt: time, text: 'expired request' },
        { id: 'later', taskId: task.id, taskLifecycleId: task.lifecycleId, kind: 'output', occurredAt: null, capturedAt: time, text: 'expired action' },
      ] });
      fault = new DatabaseSync(join(dataDir, 'observations.sqlite'));
      // External storage fault only; business assertions use public snapshots, never database queries.
      fault.exec(`CREATE TRIGGER fail_later_delete BEFORE DELETE ON records
        WHEN OLD.collection = 'events' AND json_extract(OLD.value, '$.id') = 'later'
        BEGIN SELECT RAISE(ABORT, 'simulated later delete failure'); END`);
      time += day + 1;
      try {
        await assert.rejects(core.snapshot(), /simulated later delete failure/);
      } finally {
        fault.exec('DROP TRIGGER fail_later_delete');
      }
      if (restart) {
        await core.close();
        core = await createCore({ dataDir, now: () => time, leaseMs: day * 2 });
      }
      const recovered = await core.snapshot();
      assert.equal(recovered.tasks.reduce((count, value) => count + value.events.length, 0), 0);
      assert.ok(recovered.tasks.every(value => value.currentRequest === ''), 'successfully deleted input must not leave a dangling request');
      assert.ok(recovered.tasks.every(value => value.analysis.checks.every(check => check.status === 'unknown')));
    } finally {
      fault?.close();
      await core.close();
      await rm(dataDir, { recursive: true, force: true });
    }
  });
}

for (const collection of ['tasks', 'sources']) {
  test(`failed retention ${collection} saves stay visible until durable cleanup succeeds`, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-retention-save-'));
    const day = 24 * 60 * 60 * 1000;
    let time = 1_000_000, fault;
    let core = await createCore({ dataDir, now: () => time, leaseMs: day * 2 });
    try {
      const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'host', name: 'A' });
      const task = { id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' };
      await core.ingest(source, { tasks: [task], gaps: ['old source gap'], events: [{ id: 'request', taskId: task.id,
        taskLifecycleId: task.lifecycleId, kind: 'input', occurredAt: null, capturedAt: time, text: 'expired request' }] });
      fault = new DatabaseSync(join(dataDir, 'observations.sqlite'));
      fault.exec(`CREATE TRIGGER fail_retention_save BEFORE UPDATE ON records
        WHEN OLD.collection = '${collection}'
        BEGIN SELECT RAISE(ABORT, 'simulated retention save failure'); END`);
      time += day + 1;
      try {
        await assert.rejects(core.snapshot(), /simulated retention save failure/);
        await assert.rejects(core.snapshot(), /simulated retention save failure/, 'unfinished durable cleanup must remain visible');
        if (collection === 'tasks') await assert.rejects(core.pause(), /simulated retention save failure/);
        else await core.pause();
        await assert.rejects(core.snapshot(), /simulated retention save failure/, 'pausing must not lose a failed cleanup write');
      } finally {
        fault.exec('DROP TRIGGER fail_retention_save');
      }
      const recovered = await core.snapshot();
      assert.equal(recovered.tasks[0].currentRequest, '');
      assert.deepEqual(recovered.tasks[0].events, []);
      assert.ok(!recovered.sources[0].gaps.includes('old source gap'));
      await core.close();
      core = await createCore({ dataDir, now: () => time });
      const reopened = await core.snapshot();
      assert.ok(reopened.tasks.every(value => value.currentRequest === '' && value.events.length === 0));
      assert.ok(reopened.sources.every(value => !value.gaps.includes('old source gap')));
    } finally {
      fault?.close();
      await core.close();
      await rm(dataDir, { recursive: true, force: true });
    }
  });
}
