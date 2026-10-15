import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { createNativeIO } from '../chatgpt/native-io.mjs';
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs';
import { supportedProfile } from '../chatgpt/bounded-reader.mjs';
const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws');

async function fixture(t, initialMode) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-metadata-classification-'));
  const socketPath = join(tmpdir(), 'gs-mf-' + randomUUID().slice(0, 8) + '.sock');
  const metadata = createServer(), websocket = new WebSocketServer({ noServer: true });
  let mode = initialMode, entered;
  const pendingList = new Promise(resolve => { entered = resolve; });
  metadata.on('upgrade', (req, socket, head) => websocket.handleUpgrade(req, socket, head, ws => websocket.emit('connection', ws, req)));
  websocket.on('connection', ws => ws.on('message', bytes => {
    const message = JSON.parse(bytes);
    if (!message.id) return;
    if (message.method === 'thread/list') {
      entered();
      ws.send(JSON.stringify({ method: 'unrelated/native-notice', params: { private: 'PRIVATE_NOTIFICATION' } }));
      if (mode === 'timeout') return;
      if (mode === 'disconnect') { ws.close(1011); return; }
      if (mode === 'rpc') { ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, error: { code: -32601, message: 'PRIVATE_RPC_MESSAGE', data: { path: dir } } })); return; }
      if (mode === 'invalid-json') { ws.send('PRIVATE_INVALID_JSON'); return; }
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, result: { data: [], nextCursor: null } })); return;
    }
    const enabled = { plugins: { 'sentinel@test': { enabled: true } } };
    const result = message.method === 'initialize' ? { serverInfo: { name: 'controlled-metadata' } } : { config: enabled, layers: [{ name: { type: 'user' }, config: enabled }] };
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, result }));
  }));
  await new Promise(resolve => metadata.listen(socketPath, resolve));
  const started = new Date(Date.now() - 10000).toUTCString().replace(',', '').replace(/ GMT$/, '');
  const native = createNativeIO({ pluginId: 'sentinel@test', socketPath, profileSample: async () => supportedProfile, processSample: async () => [
    { pid: 11, ppid: 1, command: '/Applications/ChatGPT.app/Contents/MacOS/ChatGPT', started },
    { pid: 12, ppid: 11, command: '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex app-server', started },
  ] });
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') });
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'metadata-classification', token: 'a'.repeat(64), pollMs: 0, collectMaterials: true });
  t.after(async () => {
    await adapter.close(); await core.close(); await server.close();
    for (const ws of websocket.clients) ws.terminate();
    await new Promise(resolve => websocket.close(resolve)); await new Promise(resolve => metadata.close(resolve));
    await rm(socketPath, { force: true }); await rm(dir, { recursive: true, force: true });
  });
  return { adapter, core, native, pendingList, setMode(value) { mode = value; }, async page() { const sourceKey = (await core.snapshot()).sources[0].key; return core.failureReceipts({ sourceKey }); } };
}

for (const [mode, code, category] of [
  ['timeout', 'deadline', 'timeout'], ['disconnect', 'metadata-unavailable', 'transport'],
  ['rpc', 'metadata-rpc-error', 'protocol'], ['invalid-json', 'metadata-invalid-response', 'protocol'],
]) test(`real metadata ${mode} retains its safe cause and metadata stage through public receipts`, async t => {
  const f = await fixture(t, mode), started = performance.now();
  await f.adapter.refresh();
  if (mode === 'timeout') assert.ok(performance.now() - started >= 4900, 'the fixed production deadline actually elapsed');
  const page = await f.page();
  assert.equal(page.receipts.length, 1);
  assert.deepEqual(page.receipts[0].error, { code, stage: 'metadata', category });
  assert.equal(page.receipts[0].channel, 'directory');
  assert.equal(JSON.stringify(page).includes('PRIVATE_'), false);
  f.setMode('success'); await f.adapter.refresh();
  assert.equal((await f.page()).receipts[0].state, 'recovered');
});

test('closing a real pending metadata request cancels it without publishing an extra failure', async t => {
  const f = await fixture(t, 'timeout'), refreshing = f.adapter.refresh();
  await f.pendingList;
  await f.adapter.close(); await refreshing;
  assert.deepEqual((await f.page()).receipts, []);
  assert.deepEqual(f.native.diagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0, connected: false });
});
