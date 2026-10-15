import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs';
import { supportedProfile } from '../chatgpt/bounded-reader.mjs';

test('two unknown metadata codes keep distinct safe hashes through discovery, adapter FIFO and public receipts', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-unknown-metadata-'));
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') });
  const hostStartedAt = Date.now() - 10000;
  let code = 'PRIVATE_CODE_A';
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile }; },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' }; },
    async listThreads() { throw Object.assign(new Error('PRIVATE_MESSAGE'), { code }); },
    async close() {},
  };
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'unknown-metadata', token: 'a'.repeat(64), collectMaterials: true, pollMs: 0 });
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }); });
  await adapter.refresh();
  code = 'PRIVATE_CODE_B';
  await adapter.refresh();
  const sourceKey = (await core.snapshot()).sources[0].key;
  const page = await core.failureReceipts({ sourceKey });
  assert.equal(page.receipts.length, 2);
  for (const receipt of page.receipts) {
    assert.equal(receipt.error.code, 'unrecognized_error');
    assert.equal(receipt.error.stage, 'metadata');
    assert.match(receipt.error.codeHash, /^[a-f0-9]{64}$/);
  }
  assert.notEqual(page.receipts[0].error.codeHash, page.receipts[1].error.codeHash);
  assert.equal(JSON.stringify(page).includes('PRIVATE_'), false);
  assert.equal(adapter.diagnostics().healthOutbox.pending, 0);
});
