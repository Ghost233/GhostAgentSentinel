// Metadata-only is the default; material mode shares the same bounded physical read.
import { MaterialProjection, materialRecord } from './material-projection.mjs';
import { jsonParser } from 'stream-json/core/parser.js';
import filter from 'stream-json/core/filters/filter.js';
import stringer from 'stream-json/core/stringer.js';
import { none, isMany } from 'stream-chain/defs.js';
import fs from 'node:fs/promises';
import { resolve, sep } from 'node:path';
import { createHash } from 'node:crypto';

const values = value => value === none ? [] : isMany(value) ? value.values : [value];
const fields = /^(?:type|timestamp|payload\.(?:type|turn_id|root_turn_id|thread_id|id))$/;

export class Projection {
  constructor() {
    this.decode = new TextDecoder('utf-8', { fatal: true });
    this.parse = jsonParser({ packValues: false, streamValues: true });
    this.pick = filter({ filter: fields, streamKeys: false });
    this.stringify = stringer({ useKeyValues: true, useStringValues: false, useNumberValues: false });
    this.output = '';
    this.peakProjectionBytes = 0;
    this.depth = 0;
    this.key = null;
    this.keyOverflow = false;
  }

  collect(value) {
    for (const token of values(value)) {
      const text = this.stringify(token);
      if (text === none) continue;
      const bytes = Buffer.byteLength(this.output) + Buffer.byteLength(text);
      if (bytes > 4096) throw new Error('metadata projection limit exceeded');
      this.output += text;
      this.peakProjectionBytes = Math.max(this.peakProjectionBytes, bytes);
    }
  }

  tokens(result) {
    for (const token of values(result)) {
      if (token.name === 'startObject' || token.name === 'startArray') {
        if (++this.depth > 64) throw new Error('JSON depth limit exceeded');
      } else if (token.name === 'endObject' || token.name === 'endArray') this.depth--;
      if (token.name === 'startKey') { this.key = ''; this.keyOverflow = false; continue; }
      if (token.name === 'stringChunk' && this.key !== null) {
        if (!this.keyOverflow) {
          if (this.key.length + token.value.length > 128) { this.key = '\0ignored-long-key'; this.keyOverflow = true; }
          else this.key += token.value;
        }
        continue;
      }
      if (token.name === 'endKey') {
        this.collect(this.pick({ name: 'keyValue', value: this.key }));
        this.key = null; this.keyOverflow = false; continue;
      }
      this.collect(this.pick(token));
    }
  }

  write(bytes) {
    for (let at = 0; at < bytes.length; at += 4096) {
      this.tokens(this.parse(this.decode.decode(bytes.subarray(at, at + 4096), { stream: true })));
    }
  }

  finish() {
    this.tokens(this.parse(this.decode.decode()));
    this.tokens(this.parse(none));
    this.collect(this.pick(none));
    this.stringify(none);
    if (this.depth !== 0) throw new Error('incomplete JSON record');
    return JSON.parse(this.output);
  }
}

export const supportedProfile = Object.freeze({ appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' });
const profileKey = p => [p?.appVersion, p?.appBuild, p?.cliVersion].join('|');
const digest = value => createHash('sha256').update(value).digest('hex');
const emptyState = () => ({ header: false, anchored: false, phase: 'unknown', turnId: null, at: null, orphan: false });
const otherRecords = new Set(['response_item', 'turn_context', 'compacted', 'token_usage_record', 'inter_agent_communication_metadata', 'world_state']);
const otherEvents = new Set(['item_started', 'item_completed', 'token_count', 'agent_message', 'agent_reasoning', 'user_message', 'context_compacted', 'thread_settings_applied']);
const lifecycle = new Set(['task_started', 'task_complete', 'turn_aborted']);

export class BoundedReader {
  constructor(source, cursor = null, { mode = 'metadata' } = {}) {
    if (!['metadata', 'material'].includes(mode)) throw new Error('unsupported reader mode');
    this.mode = mode;
    this.source = source;
    this.scope = digest(JSON.stringify([typeof source.path === 'string' ? resolve(source.path) : '', source.threadId, source.hostRunId, profileKey(source.profile)]));
    this.state = emptyState();
    this.offset = 0; this.committed = 0; this.pendingBytes = 0;
    this.rolling = Buffer.alloc(0); this.committedTail = Buffer.alloc(0);
    this.guardHash = digest(Buffer.alloc(0)); this.needsGuard = Boolean(cursor);
    this.handle = null; this.fileId = null; this.stat = null; this.projection = null;
    this.gap = null; this.attachSize = null; this.knownSize = null; this.excluded = 0;
    this.metrics = { physicalBytes: 0, records: 0, peakProjectionBytes: 0, maximumReadBytes: 0 };
    this.recentLifecycle = [];
    this.materials = [];
    this.turnTrigger = this.mode === 'material' && typeof cursor?.turnTrigger === 'string' && cursor.turnTrigger.length <= 256 ? cursor.turnTrigger : null;
    if (typeof source.threadId !== 'string' || !source.threadId || source.threadId.length > 128 || source.parentId !== null || !source.hostRunId || !Number.isFinite(source.hostStartedAt) || !['vscode', 'cli', 'appServer'].includes(source.sourceKind) || source.originator !== 'Codex Desktop') this.gap = 'main-identity-unverified';
    else if (source.forkedFromId) this.gap = 'fork-provenance-unverified';
    else if (profileKey(source.profile) !== profileKey(supportedProfile)) this.gap = 'unsupported-host-version';
    else if (cursor && this.mode === 'material' && cursor.projectionMode !== 'material') this.gap = 'material-cursor-baseline-missing';
    if (cursor && !this.gap) {
      const s = cursor.state;
      if (cursor.format !== 2 || cursor.scope !== this.scope || !Number.isSafeInteger(cursor.offset) || cursor.offset < 0 || !Number.isSafeInteger(cursor.attachSize) || cursor.attachSize < 0 || !Number.isSafeInteger(cursor.knownSize) || cursor.knownSize < cursor.offset || cursor.knownSize < cursor.attachSize || !/^[0-9a-f]{64}$/.test(cursor.guardHash ?? '') || typeof cursor.fileId !== 'string' || cursor.fileId.length > 100 || typeof s?.header !== 'boolean' || typeof s?.anchored !== 'boolean' || !['unknown', 'started', 'completed', 'aborted'].includes(s?.phase) || s.turnId !== null && (typeof s.turnId !== 'string' || s.turnId.length > 128) || s.at !== null && !Number.isSafeInteger(s.at)) {
        this.gap = 'cursor-invalid';
      } else {
        this.offset = this.committed = cursor.offset; this.fileId = cursor.fileId; this.guardHash = cursor.guardHash; this.attachSize = cursor.attachSize; this.knownSize = cursor.knownSize;
        this.state = { header: s.header, anchored: s.anchored, phase: s.phase, turnId: s.turnId, at: s.at, orphan: s.orphan === true };
        this.gap = cursor.gap === null ? null : 'cursor-retains-gap';
      }
    }
  }

  view(reason = null) {
    reason ??= this.closed ? 'reader-closed' : this.gap ?? (this.available === false ? 'source-unavailable' : null);
    // ps lstart is precise to one second. Recompute this for restored cursors too.
    const startGap = this.state.phase === 'started' && this.state.at < this.source.hostStartedAt + 1000 ? this.state.at < this.source.hostStartedAt ? 'start-predates-host' : 'host-start-time-overlap' : null;
    const coverage = reason || this.gap || startGap ? 'gap' : this.stat && this.offset < this.stat.size ? 'catching-up' : this.pendingBytes ? 'partial-record' : this.stat ? 'caught-up' : 'catching-up';
    const gap = reason ?? this.gap ?? (!this.state.header ? 'main-header-unverified' : !this.state.anchored ? 'current-turn-not-observed' : startGap);
    return { coverage, gap, observed: { ...this.state }, effectivePhase: coverage === 'caught-up' && !gap ? this.state.phase : 'unknown', offset: this.offset, committedOffset: this.committed, pendingBytes: this.pendingBytes, excluded: this.excluded, recentLifecycle: this.recentLifecycle.map(e => ({ ...e })), ...(this.mode === 'material' ? { materials: this.materials.map(e => ({ ...e, data: { ...e.data } })) } : {}), metrics: { ...this.metrics } };
  }

  apply(row, recordStart) {
    if (row.type === 'session_meta') {
      if (row.payload?.id !== this.source.threadId) {
        if (this.state.header) this.gap = 'record-identity-changed';
        return;
      }
      this.state.header = true; return;
    }
    if (!this.state.header) return;
    if (row.payload?.thread_id && row.payload.thread_id !== this.source.threadId) { this.excluded++; return; }
    if (this.mode === 'material' && row.type === 'event_msg' && row.payload?.type === 'item_completed' && row.payload.thread_id !== this.source.threadId) { this.excluded++; return; }
    if (this.mode === 'material' && this.state.anchored && this.state.turnId) {
      const material = materialRecord(row, this.state.turnId, recordStart, this.offset + 1, this.turnTrigger);
      if (material && material.turnId === this.state.turnId) this.materials.push(material);
      else if (material) this.excluded++;
    }
    if (row.type !== 'event_msg') {
      if (this.state.anchored && !otherRecords.has(row.type)) this.gap = 'unsupported-record-type:' + String(row.type).slice(0, 80);
      return;
    }
    const type = row.payload?.type;
    if (!lifecycle.has(type)) {
      if (this.state.anchored && !otherEvents.has(type)) this.gap = 'unsupported-event-type:' + String(type).slice(0, 80);
      return;
    }
    const id = row.payload.turn_id;
    if (!this.state.anchored && id !== this.source.anchorTurnId && !(type === 'task_started' && recordStart >= this.attachSize)) return;
    if (typeof id !== 'string' || !id || id.length > 128 || !Number.isFinite(Date.parse(row.timestamp))) { this.gap = 'invalid-lifecycle-record'; return; }
    this.state.anchored = true;
    const turnTrigger = this.mode === 'material' && row.material.fields['payload.turn_attribution.turn_id']?.text === id ? row.material.turnTrigger ?? null : null;
    this.recentLifecycle.push({ type, turnId: id, at: Date.parse(row.timestamp), recordStart, ...(this.mode === 'material' ? { recordEnd: this.offset + 1, turnTrigger } : {}) });
    if (this.mode === 'metadata' && this.recentLifecycle.length > 8) this.recentLifecycle.shift();
    if (type === 'task_started') {
      if (this.mode === 'material') this.turnTrigger = turnTrigger;
      this.state = { header: true, anchored: true, phase: 'started', turnId: id, at: Date.parse(row.timestamp), orphan: Date.parse(row.timestamp) < this.source.hostStartedAt };
    } else if (!this.state.turnId || this.state.turnId === id) {
      this.state = { header: true, anchored: true, phase: type === 'task_complete' ? 'completed' : 'aborted', turnId: id, at: Date.parse(row.timestamp), orphan: false };
    }
  }

  consume(bytes) {
    let at = 0;
    while (at < bytes.length && !this.gap && (this.mode !== 'material' || this.recentLifecycle.length + this.materials.length < 16)) {
      const newline = bytes.indexOf(10, at);
      const end = newline < 0 ? bytes.length : newline;
      const part = bytes.subarray(at, end);
      const recordStart = this.offset - this.pendingBytes;
      this.projection ??= this.mode === 'material' ? new MaterialProjection() : new Projection();
      try {
        this.projection.write(part);
        this.metrics.peakProjectionBytes = Math.max(this.metrics.peakProjectionBytes, this.projection.peakProjectionBytes);
        this.pendingBytes += part.length; this.offset += part.length;
        this.rolling = Buffer.concat([this.rolling, part.subarray(Math.max(0, part.length - 64))]).subarray(-64);
        if (newline >= 0) {
          const row = this.projection.finish();
          this.metrics.peakProjectionBytes = Math.max(this.metrics.peakProjectionBytes, this.projection.peakProjectionBytes);
          this.apply(row, recordStart);
          this.offset++; this.rolling = Buffer.concat([this.rolling, Buffer.from('\n')]).subarray(-64);
          this.committed = this.offset; this.committedTail = Buffer.from(this.rolling); this.guardHash = digest(this.committedTail);
          this.pendingBytes = 0; this.projection = null; this.metrics.records++;
        }
      } catch { this.gap = 'record-malformed-or-over-limit'; }
      at = newline < 0 ? bytes.length : newline + 1;
    }
  }

  async pump({ budgetBytes = 262144, available = this.available ?? true, profile = this.source.profile, hostRunId = this.source.hostRunId } = {}) {
    if (!Number.isSafeInteger(budgetBytes) || budgetBytes < 1 || budgetBytes > 262144) throw new Error('invalid read budget');
    if (this.mode === 'material' && !this.busy) { this.recentLifecycle = []; this.materials = []; }
    if (this.closed) return { ...this.view('reader-closed'), readBytes: 0 };
    if (this.gap) return { ...this.view(), readBytes: 0 };
    if (profileKey(profile) !== profileKey(supportedProfile)) { this.gap = 'unsupported-host-version'; return { ...this.view(), readBytes: 0 }; }
    if (hostRunId !== this.source.hostRunId) { this.gap = 'source-generation-changed'; return { ...this.view(), readBytes: 0 }; }
    this.available = available;
    if (!available) { this.needsGuard = true; return { ...this.view('source-unavailable'), readBytes: 0 }; }
    if (this.busy) return { ...this.view('reader-busy'), ...(this.mode === 'material' ? { materials: [], recentLifecycle: [] } : {}), readBytes: 0 };
    this.busy = true;
    let finishPump;
    this.pumpComplete = new Promise(resolve => { finishPump = resolve; });
    let readBytes = 0;
    try {
      if (!this.handle) {
        const path = await fs.realpath(this.source.path);
        const allowed = await fs.realpath(this.source.allowedRoot);
        if (!path.startsWith(allowed + sep) || !path.endsWith('.jsonl')) { this.gap = 'record-path-unverified'; return { ...this.view(), readBytes }; }
        this.handle = await fs.open(path, 'r');
      }
      const stat = await fs.stat(this.source.path);
      const opened = await this.handle.stat();
      const id = `${stat.dev}:${stat.ino}`;
      if (!stat.isFile() || id !== `${opened.dev}:${opened.ino}` || this.fileId && this.fileId !== id) { this.gap = 'record-file-replaced'; return { ...this.view(), readBytes }; }
      this.fileId = id; this.attachSize ??= stat.size;
      if (stat.size < Math.max(this.offset, this.knownSize ?? this.attachSize)) { this.gap = 'record-file-truncated'; return { ...this.view(), readBytes }; }
      this.knownSize = stat.size;
      if (this.stat && stat.size === this.stat.size && stat.mtimeMs !== this.stat.mtimeMs) { this.gap = 'record-rewrite-suspected'; return { ...this.view(), readBytes }; }
      const unchanged = this.stat && stat.size === this.stat.size && stat.mtimeMs === this.stat.mtimeMs;
      if (!unchanged || this.needsGuard) {
        const guardSize = Math.min(64, this.committed);
        if (guardSize >= budgetBytes) return { ...this.view('budget-too-small-for-guard'), readBytes };
        if (guardSize) {
          const guard = Buffer.alloc(guardSize);
          const got = await this.handle.read(guard, 0, guardSize, this.committed - guardSize);
          readBytes += got.bytesRead;
          if (got.bytesRead !== guardSize || digest(guard) !== this.guardHash) { this.gap = 'cursor-anchor-changed'; return { ...this.view(), readBytes }; }
          if (!this.pendingBytes) this.rolling = this.committedTail = guard;
        }
        this.needsGuard = false;
      }
      this.stat = { size: stat.size, mtimeMs: stat.mtimeMs };
      const count = Math.min(budgetBytes - readBytes, stat.size - this.offset);
      if (count > 0) {
        this.ioBuffer ??= Buffer.alloc(262144);
        const buffer = this.ioBuffer;
        const got = await this.handle.read(buffer, 0, count, this.offset);
        readBytes += got.bytesRead;
        this.consume(buffer.subarray(0, got.bytesRead));
      }
      const after = await fs.stat(this.source.path);
      if (`${after.dev}:${after.ino}` !== this.fileId) this.gap = 'record-file-replaced';
      else if (after.size < Math.max(this.offset, this.knownSize)) this.gap = 'record-file-truncated';
      else if (after.size === stat.size && after.mtimeMs !== stat.mtimeMs) this.gap = 'record-rewrite-suspected';
      else if (after.size > stat.size) {
        this.stat = { size: after.size, mtimeMs: after.mtimeMs };
        this.knownSize = after.size;
        this.needsGuard = true;
      }
    } catch { this.gap = 'record-source-unavailable'; }
    finally { this.busy = false; this.metrics.physicalBytes += readBytes; this.metrics.maximumReadBytes = Math.max(this.metrics.maximumReadBytes, readBytes); finishPump(); this.pumpComplete = null; }
    return { ...this.view(), readBytes };
  }

  checkpoint() {
    return { format: 2, scope: this.scope, offset: this.committed, attachSize: this.attachSize, knownSize: this.knownSize, fileId: this.fileId, guardHash: this.guardHash, state: { ...this.state }, gap: this.gap, ...(this.mode === 'material' ? { projectionMode: 'material', turnTrigger: this.turnTrigger } : {}) };
  }

  async close() {
    if (this.closing) return this.closing;
    this.closed = true;
    this.closing = (async () => {
      await this.pumpComplete;
      await this.handle?.close(); this.handle = null; this.projection = null;
      this.ioBuffer?.fill(0); this.ioBuffer = null;
    })();
    return this.closing;
  }
}
