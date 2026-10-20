import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs';
import { createLazyCore } from '../chatgpt/lazy-core.mjs';
import { supportedProfile } from '../chatgpt/storage-compatibility.mjs';
import { req } from './dsh-native-runtime.mjs';
import { createAdapter } from '../dsh/host.mjs';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';

async function until(predicate) { const end = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < end, 'expected public bridge state'); await delay(3); } }

test('app-only installation preference and authorization tools work without a current chat or collection source', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notification-settings-')), token = 'a'.repeat(64);
  const server = await startServer({ dataDir }), control = await connectCore({ dataDir }), lazy = createLazyCore({ dataDir });
  let authorizationCalls = 0, enabled = true;
  const adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile }; },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' }; },
    async readThread() { throw new Error('notification settings must not read a chat'); },
  }, notificationIO: {
    async requestAuthorization() { authorizationCalls++; return { authorization: 'denied', observedAt: Date.now() }; },
    async settings() { return { authorization: 'denied', observedAt: Date.now() }; },
    async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: Date.now() }; },
    async cancel() { return { pendingIds: [], observedAt: Date.now() }; }, close() {},
  }, installationId: 'prefs', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 });
  t.after(async () => { await adapter.close(); await lazy.close(); await control.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const rpc = async (name, args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) });
    return (await response.json()).result;
  };
  const initial = await rpc('sentinel_installation_notification_preferences');
  assert.ok(!initial.isError, JSON.stringify(initial));
  assert.equal(initial.structuredContent.registrationId, 'chatgpt:prefs');
  assert.deepEqual(initial.structuredContent.preferences, { 'task-alert': true, 'monitoring-failure': true });
  assert.equal(initial.structuredContent.permission.authorization, 'unknown');
  assert.equal(initial.structuredContent.permission.requestInFlight, false);
  assert.equal(initial.structuredContent.permission.errorCode, null);
  const changed = await rpc('sentinel_set_installation_notification_preference', { kind: 'task-alert', enabled: false });
  assert.deepEqual((await control.installationNotificationPreferences('chatgpt:prefs')), changed.structuredContent);
  assert.equal(authorizationCalls, 0, 'read and save do not request system permission');
  const granted = await rpc('sentinel_request_installation_notification_permission');
  assert.equal(granted.structuredContent.registrationId, 'chatgpt:prefs');
  assert.equal(granted.structuredContent.permission.authorization, 'denied');
  assert.equal(authorizationCalls, 1);
  assert.equal((await control.snapshot()).sources.length, 0);
  enabled = false;
  assert.equal((await rpc('sentinel_request_installation_notification_permission')).isError, true);
  assert.equal(authorizationCalls, 1);
});

test('Native Remote preferences bind their Host installation and preserve rejected write outcomes without touching another installation', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-pref-bridge-')), server = await startServer({ dataDir }), control = await connectCore({ dataDir });
  const ctx = new (req('@deepseek-ai/cordis').Context)();
  let adapter;
  t.after(async () => { await adapter?.close(); await ctx.fiber.dispose(); await control.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default);
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) await ctx.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] });
  await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default);
  await ctx.plugin(req('@deepseek-ai/dsh-api-remote-access').default);
  await ctx.plugin(req('@deepseek-ai/dsh-api-gateway').default);
  const peer = new (req('@deepseek-ai/dsh-client-connection').Peer)(ctx, { principal: { kind: 'owner' }, interactions: 'none' });
  adapter = createAdapter(ctx, { dataDir, registrationId: 'dsh:own-prefs' }, { flushMs: 5 });
  await until(() => adapter.connected);
  const rpc = (method, args = {}) => ctx.typertGateway.dispatchRpc(`sentinel/${method}`, { args }, undefined, peer);
  const first = await rpc('installationNotificationPreferences');
  assert.equal(first.ok, true);
  assert.equal(first.value.registrationId, 'dsh:own-prefs');
  const other = await control.installationNotificationPreferences('other');
  const changed = await rpc('setInstallationNotificationPreference', { kind: 'task-alert', enabled: false });
  assert.equal(changed.ok, true);
  assert.deepEqual(changed.value, await control.installationNotificationPreferences('dsh:own-prefs'));
  assert.deepEqual(await control.installationNotificationPreferences('other'), other);
  assert.equal((await rpc('beginNotificationSubmission', { id: 'another-installation-notice' })).ok, false);
  const db = new DatabaseSync(join(dataDir, 'observations.sqlite'));
  try {
    db.exec("CREATE TRIGGER reject_pref_bridge BEFORE UPDATE ON records WHEN NEW.collection='installationNotificationSettings' BEGIN SELECT RAISE(FAIL,'own bridge write failure'); END");
    const rejected = await rpc('setInstallationNotificationPreference', { kind: 'task-alert', enabled: true });
    assert.equal(rejected.ok, false);
    assert.equal(rejected.error.details.saveOutcome, 'rejected');
  } finally { db.exec('DROP TRIGGER reject_pref_bridge'); db.close(); }
  assert.deepEqual((await rpc('installationNotificationPreferences')).value, changed.value);
});

test('global authorization leaves cold Core asleep and rejects permission results after plugin qualification changes', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-cold-permission-')), token = 'b'.repeat(64);
  let connects = 0, entered = false, release, enabled = true;
  const lazy = createLazyCore({ dataDir, connect: async () => { connects++; throw new Error('permission must not attach Core'); } });
  const adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile }; },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' }; },
  }, notificationIO: {
    async requestAuthorization() { entered = true; await new Promise(resolve => { release = resolve; }); return { authorization: 'authorized', observedAt: Date.now() }; },
    async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: Date.now() }; },
    async cancel() { return { pendingIds: [], observedAt: Date.now() }; }, close() {},
  }, installationId: 'cold', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 });
  t.after(async () => { release?.(); await adapter.close(); await lazy.close(); await rm(dataDir, { recursive: true, force: true }); });
  const request = fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_request_installation_notification_permission', arguments: {} } }) }).then(response => response.json());
  await until(() => entered);
  enabled = false; release();
  assert.equal((await request).result.isError, true);
  assert.equal(connects, 0);
});
