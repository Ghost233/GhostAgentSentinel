import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCore } from '../src/core/index.mjs';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';

test('the production server reopens a retained event set larger than the JavaScript argument limit', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-large-startup-'));
  let core, server, client;
  t.after(async () => { await client?.close(); await server?.close(); await core?.close(); await rm(dataDir, { recursive: true, force: true }); });
  core = await createCore({ dataDir });
  await core.pause();
  const source = await core.registerSource({ registrationId: 'large', hostRunId: 'host', name: 'Retained source' });
  await core.ingest(source, { tasks: [{ id: 'task', lifecycleId: 'life', title: 'Retained task', status: 'running' }] });
  const task = (await core.snapshot()).tasks[0], capturedAt = Date.now();
  await core.close(); core = null;
  // Storage fixture preparation only: all rows belong to the public task above.
  // One transaction reproduces the retained production volume without thousands
  // of ingestion requests. Business acceptance remains at HTTP diagnostics.
  const db = new DatabaseSync(join(dataDir, 'observations.sqlite'));
  try {
    const insert = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?)');
    db.exec('BEGIN');
    for (let index = 0; index < 150_000; index++) {
      const event = { key: 'event-' + index, sourceKey: source, taskKey: task.key, id: 'event-' + index, taskId: 'task', taskLifecycleId: 'life', kind: 'output', text: 'Retained evidence', capturedAt, occurredAt: null, sequence: index + 1 };
      insert.run('events', event.key, JSON.stringify(event));
    }
    db.exec('COMMIT');
  } finally { db.close(); }
  server = await startServer({ dataDir }); client = await connectCore({ dataDir });
  const diagnostics = await client.diagnostics();
  assert.equal(diagnostics.retained.events, 150_000);
  assert.equal(diagnostics.retained.tasks, 1);
  assert.equal(diagnostics.runtime.connectedSources, 0);
  assert.equal((await client.health()).instanceId, (await client.diagnostics()).instanceId);
});
