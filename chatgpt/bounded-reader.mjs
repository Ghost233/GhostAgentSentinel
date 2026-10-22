// Metadata-only is the default; material mode shares the same bounded physical read.
import { MaterialProjection, materialRecord } from './material-projection.mjs';
import { jsonParser } from 'stream-json/core/parser.js';
import filter from 'stream-json/core/filters/filter.js';
import stringer from 'stream-json/core/stringer.js';
import { none, isMany } from 'stream-chain/defs.js';
import fs from 'node:fs/promises';
import { resolve, sep } from 'node:path';
import { createHash } from 'node:crypto';
import { NativeJsonlSource, nativeStorageKey } from './native-jsonl-source.mjs';
import { nativeRecordIdentity } from '../src/fork-proof.mjs';
import { headerIdentity, normalizeNativePrefix } from '../src/native-origin.mjs';
import { supportedProfile, supportsHostProfile, supportsStorageProducer, hostProfileKey as profileKey } from './storage-compatibility.mjs';

const values = value => value === none ? [] : isMany(value) ? value.values : [value];
const fields = /^(?:type|timestamp|ordinal|payload\.(?:type|turn_id|root_turn_id|thread_id|id|originator|source|cli_version|cwd|history_mode|forked_from_id|forked_from_ordinal_exclusive)|payload\.history_base\.(?:thread_id|end_ordinal_exclusive|end_byte_offset))$/;

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

export { supportedProfile } from './storage-compatibility.mjs';
const digest = value => createHash('sha256').update(value).digest('hex');
const emptyState = () => ({ header: false, anchored: false, phase: 'unknown', turnId: null, at: null, orphan: false });
const otherRecords = new Set(['response_item', 'turn_context', 'compacted', 'token_usage_record', 'inter_agent_communication_metadata', 'world_state']);
const otherEvents = new Set(['item_started', 'item_completed', 'token_count', 'agent_message', 'agent_reasoning', 'user_message', 'context_compacted', 'thread_settings_applied', 'thread_goal_updated']);
const lifecycle = new Set(['task_started', 'task_complete', 'turn_aborted']);
const materialItems = new Set(['UserMessage', 'AgentMessage', 'CommandExecution', 'McpToolCall', 'Reasoning']);
const sourceIoCodes = new Set(['ENOENT', 'EACCES', 'EPERM', 'EIO', 'ENOTDIR', 'ELOOP', 'EMFILE', 'ENFILE', 'EBADF']);

const nativeHeader = row => ({ threadId: row.payload.id, ordinal: row.ordinal, originator: row.payload.originator, sourceKind: row.payload.source, cliVersion: row.payload.cli_version, historyMode: row.payload.history_mode, cwd: row.payload.cwd, creationSourceThreadId: row.payload.forked_from_id ?? null, historyBase: row.payload.history_base ? { sourceThreadId: row.payload.history_base.thread_id, endOrdinalExclusive: row.payload.history_base.end_ordinal_exclusive, endByteOffset: row.payload.history_base.end_byte_offset } : null });
const producerAllowed = (source, version) => supportsStorageProducer(version) || version === undefined && (source.nativeOriginVersion ?? 1) === 1 && profileKey(source.profile) === profileKey(supportedProfile);

export class BoundedReader {
  constructor(source, cursor = null, { mode = 'metadata', forkSuffix = false, origins = null, scanOnly = false, terminalProof = cursor?.terminalProof ?? null, successorProof = cursor?.successorProof ?? null } = {}) {
    if (!['metadata', 'material', 'history', 'history-metadata', 'header'].includes(mode)) throw new Error('unsupported reader mode');
    this.origins = origins; this.mode = mode; this.materialMode = ['material', 'history'].includes(mode); this.historyMode = ['history', 'history-metadata'].includes(mode); this.ordinalMode = this.historyMode || forkSuffix;
    this.historyRange = this.historyMode ? source.historyRange : null; this.nextOrdinal = null; this.forkProvenance = null;
    this.source = source; this.nativeOriginVersion = source.nativeOriginVersion ?? 1;
    this.hostProfileKey = profileKey(source.profile);
    this.scanOnly = scanOnly; this.scanStart = null; this.coldScan = null; this.coldCheckpoint = null; this.terminalProof = terminalProof; this.successorProof = successorProof;
    this.originIdentity = this.nativeOriginVersion === 2 ? 'native-storage/2:' + nativeStorageKey(source.path) : null;
    const binding = [this.nativeOriginVersion === 2 ? this.originIdentity : typeof source.path === 'string' ? resolve(source.path) : '', source.threadId, source.hostRunId, profileKey(source.profile), ...(this.ordinalMode ? [mode, source.forkedFromId ?? null, this.historyRange, this.nativeOriginVersion === 2 ? null : source.expectedFileId ?? null, source.expectedHeader ?? null] : [])];
    this.scope = digest(JSON.stringify(binding));
    this.storageScope = digest(JSON.stringify(binding.filter((_, index) => index !== 2)));
    // Legacy v2 cursors only stored a Host-bound hash. Rebinding requires the
    // retained Core origin and a fresh physical/header/prefix verification.
    const recovering = cursor && this.nativeOriginVersion === 2 && origins && cursor.scope !== this.scope && /^[a-f0-9]{64}$/.test(cursor.scope ?? '') && (cursor.storageScope === undefined || cursor.storageScope === this.storageScope);
    this.recoveryCheckpoint = recovering ? structuredClone(cursor) : null;
    this.recoveryFence = null;
    this.state = emptyState();
    this.offset = 0; this.committed = 0; this.pendingBytes = 0;
    this.rolling = Buffer.alloc(0); this.committedTail = Buffer.alloc(0);
    this.guardHash = digest(Buffer.alloc(0)); this.needsGuard = Boolean(cursor);
    this.handle = null; this.fileId = null; this.stat = null; this.projection = null; this.header = null; this.io = null; this.ioView = null; this.ioCheckpoint = null; this.error = null;
    this.gap = null; this.attachSize = null; this.knownSize = null; this.excluded = 0;
    this.metrics = { physicalBytes: 0, records: 0, peakProjectionBytes: 0, maximumReadBytes: 0 };
    this.recentLifecycle = [];
    this.materials = [];
    this.turnTrigger = this.materialMode && typeof cursor?.turnTrigger === 'string' && cursor.turnTrigger.length <= 256 ? cursor.turnTrigger : null;
    if (typeof source.threadId !== 'string' || !source.threadId || source.threadId.length > 128 || source.parentId !== null || !source.hostRunId || !Number.isFinite(source.hostStartedAt) || !['vscode', 'cli', 'appServer'].includes(source.sourceKind) || source.originator !== 'Codex Desktop') this.gap = 'main-identity-unverified';
    else if (source.forkedFromId && !forkSuffix && !this.historyMode) this.gap = 'fork-provenance-unverified';
    else if (!supportsHostProfile(source.profile)) this.gap = 'unsupported-host-version';
    else if (this.historyMode && (!Number.isSafeInteger(this.historyRange?.endOrdinalExclusive) || this.historyRange.endOrdinalExclusive < 1 || !Number.isSafeInteger(this.historyRange?.endByteOffset) || this.historyRange.endByteOffset < 1)) this.gap = 'history-boundary-unverified';
    else if (cursor && this.materialMode && cursor.projectionMode !== this.mode) this.gap = 'material-cursor-baseline-missing';
    else if (cursor && this.materialMode && cursor.nativeOriginVersion !== this.nativeOriginVersion) this.gap = 'cursor-invalid';
    else if (cursor && this.materialMode && cursor.state?.header && cursor.header?.threadId !== source.threadId) this.gap = 'cursor-invalid';
    else if (cursor?.state?.header && cursor.header && !producerAllowed(source, cursor.header.cliVersion)) this.gap = 'cursor-invalid';
    const pristineV2 = cursor && this.nativeOriginVersion === 2 && cursor.format === 3 && (cursor.scope === this.scope || recovering) && cursor.offset === 0 && cursor.fileId === null && cursor.knownSize === null && cursor.attachSize === null && cursor.state?.header === false && cursor.state?.anchored === false && cursor.state?.phase === 'unknown' && cursor.state?.turnId === null && cursor.gap === null;
    if (cursor && !this.gap && !pristineV2) {
      const s = cursor.state;
      if (cursor.format !== (this.nativeOriginVersion === 2 ? 3 : 2) || cursor.scope !== this.scope && !recovering || !Number.isSafeInteger(cursor.offset) || cursor.offset < 0 || !Number.isSafeInteger(cursor.attachSize) || cursor.attachSize < 0 || !Number.isSafeInteger(cursor.knownSize) || cursor.knownSize < cursor.offset || cursor.knownSize < cursor.attachSize || !/^[0-9a-f]{64}$/.test(cursor.guardHash ?? '') || typeof cursor.fileId !== 'string' || cursor.fileId.length > 100 || this.ordinalMode && !(cursor.nextOrdinal === null && cursor.offset === 0 && s?.header === false) && (!Number.isSafeInteger(cursor.nextOrdinal) || cursor.nextOrdinal < 0) || typeof s?.header !== 'boolean' || typeof s?.anchored !== 'boolean' || !['unknown', 'started', 'completed', 'aborted'].includes(s?.phase) || s.turnId !== null && (typeof s.turnId !== 'string' || s.turnId.length > 128) || s.at !== null && !Number.isSafeInteger(s.at)) {
        this.gap = 'cursor-invalid';
      } else {
        this.offset = this.committed = cursor.offset; this.fileId = cursor.fileId; this.guardHash = cursor.guardHash; this.attachSize = cursor.attachSize; this.knownSize = cursor.knownSize;
        this.nextOrdinal = cursor.nextOrdinal ?? null; this.forkProvenance = cursor.forkProvenance ?? null; this.header = cursor.header ?? null;
        this.ioCheckpoint = cursor.io ?? null;
        this.state = { header: s.header, anchored: s.anchored, phase: s.phase, turnId: s.turnId, at: s.at, orphan: s.orphan === true };
        this.gap = cursor.gap === null ? null : 'cursor-retains-gap';
      }
    }
    if (terminalProof) {
      try {
        if (this.nativeOriginVersion !== 2 || Object.keys(terminalProof).length !== 6 || terminalProof.key !== nativeStorageKey(source.path) || !/^[a-f0-9]{64}$/.test(terminalProof.headerHash) || typeof terminalProof.turnId !== 'string' || !terminalProof.turnId || terminalProof.turnId.length > 128 || !['completed', 'aborted'].includes(terminalProof.phase) || !Number.isSafeInteger(terminalProof.at) || terminalProof.at < 0) throw new Error('invalid terminal proof');
        normalizeNativePrefix(terminalProof.prefix);
      } catch { this.gap = 'cursor-invalid'; }
    }
    if (successorProof) {
      try {
        if (this.nativeOriginVersion !== 2 || Object.keys(successorProof).length !== 7 || successorProof.key !== nativeStorageKey(source.path) || !/^[a-f0-9]{64}$/.test(successorProof.headerHash) || [successorProof.turnId, successorProof.previousTurnId].some(id => typeof id !== 'string' || !id || id.length > 128) || successorProof.turnId === successorProof.previousTurnId || !Number.isSafeInteger(successorProof.at) || successorProof.at < 0 || !recovering && successorProof.at < source.hostStartedAt + 1000 || !Number.isSafeInteger(successorProof.recordStart) || successorProof.recordStart < 0 || successorProof.prefix.offset <= successorProof.recordStart) throw new Error('invalid successor proof');
        normalizeNativePrefix(successorProof.prefix);
        if (successorProof.at < source.hostStartedAt + 1000) this.successorProof = null;
      } catch { this.gap = 'cursor-invalid'; }
    }
  }

  view(reason = null) {
    reason ??= this.closed ? 'reader-closed' : this.gap ?? (this.available === false ? 'source-unavailable' : null);
    const verification = this.nativeOriginVersion === 2 ? { recordRevalidating: Boolean(this.io?.revalidation) } : {};
    if (this.recoveryCheckpoint && !reason) return { ...verification, coverage: 'catching-up', gap: 'cursor-revalidating', observed: emptyState(), effectivePhase: 'unknown', offset: this.committed, committedOffset: this.committed, pendingBytes: 0, recentLifecycle: [], materials: [], metrics: { ...this.metrics } };
    // ps lstart is precise to one second. Recompute this for restored cursors too.
    const terminalVerified = this.io?.terminalVerified && this.io.baseVerified && !this.io.revalidation && (!this.io.compressed || this.io.validated);
    const terminal = terminalVerified && this.terminalProof?.turnId === this.state.turnId ? this.terminalProof : null;
    const unconfirmed = terminalVerified && this.successorProof?.previousTurnId === this.state.turnId && this.state.phase === 'started';
    const startGap = !this.historyMode && !terminal && !unconfirmed && this.state.phase === 'started' && this.state.at < this.source.hostStartedAt + 1000 ? this.state.at < this.source.hostStartedAt ? 'start-predates-host' : 'host-start-time-overlap' : null;
    const coverage = reason || this.gap || startGap ? 'gap' : this.nativeOriginVersion === 2 && this.ioView && (this.ioView.phase === 'validating' || this.ioView.position < this.offset) ? 'catching-up' : this.stat && this.offset < Math.min(this.stat.size, this.historyRange?.endByteOffset ?? Infinity) ? 'catching-up' : this.pendingBytes ? 'partial-record' : this.stat ? 'caught-up' : 'catching-up';
    const gap = reason ?? this.gap ?? (!this.state.header ? 'main-header-unverified' : !this.state.anchored ? 'current-turn-not-observed' : unconfirmed ? 'historical-execution-unconfirmed' : startGap);
    if (this.coldScan && !reason) return { ...verification, coverage: 'catching-up', gap: null, observed: { ...this.state, phase: 'unknown' }, effectivePhase: 'unknown', offset: this.coldCheckpoint.offset, committedOffset: this.coldCheckpoint.offset, pendingBytes: 0, recentLifecycle: [], materials: [], coldRead: { phase: 'scan', offset: this.coldScan.offset, turnId: this.state.turnId }, metrics: { ...this.metrics } };
    if ((this.terminalProof || this.successorProof) && !terminalVerified && !this.gap && !reason) return { ...verification, coverage: 'catching-up', gap: null, observed: { ...this.state, phase: 'unknown' }, effectivePhase: 'unknown', offset: this.committed, committedOffset: this.committed, pendingBytes: 0, recentLifecycle: [], materials: [], coldRead: { phase: 'verify', offset: this.io?.revalidation?.prefix.offset ?? this.io?.position ?? 0, turnId: (this.terminalProof ?? this.successorProof).turnId }, metrics: { ...this.metrics } };
    return { ...verification, coverage, gap, ...(terminal ? { historicalTerminal: { turnId: terminal.turnId, phase: terminal.phase, at: terminal.at } } : {}), ...(terminalVerified && this.successorProof ? { unconfirmedStarts: [this.successorProof.previousTurnId] } : {}), observed: { ...this.state, ...(terminal ? { phase: terminal.phase, at: terminal.at, orphan: false } : {}) }, effectivePhase: !this.historyMode && !gap && (coverage === 'caught-up' || terminal) ? terminal?.phase ?? this.state.phase : 'unknown', offset: this.offset, committedOffset: this.committed, pendingBytes: this.pendingBytes, excluded: this.excluded, recentLifecycle: this.recentLifecycle.map(e => ({ ...e })), ...(this.materialMode ? { materials: this.materials.map(e => ({ ...e, data: { ...e.data } })) } : {}), metrics: { ...this.metrics }, ...(this.ordinalMode ? { captureThroughOffset: this.nextOrdinal === null ? -1 : this.nextOrdinal - 1, forkProvenance: this.forkProvenance, header: this.header ?? null } : {}), ...(this.error ? { error: { ...this.error } } : {}) };
  }

  apply(row, recordStart) {
    if (this.mode === 'header' && !this.state.header && (row.type !== 'session_meta' || row.payload?.id !== this.source.threadId)) { this.gap = 'history-source-unverified'; return; }
    if (this.ordinalMode) {
      if (!Number.isSafeInteger(row.ordinal) || row.ordinal < 0 || this.nextOrdinal !== null && row.ordinal !== this.nextOrdinal) { this.gap = 'record-ordinal-unverified'; return; }
      this.nextOrdinal = row.ordinal + 1;
    }
    if (row.type === 'session_meta') {
      if (row.payload?.id !== this.source.threadId) {
        if (this.state.header) this.gap = 'record-identity-changed';
        return;
      }
      this.header = nativeHeader(row);
      if (!producerAllowed(this.source, this.header.cliVersion)) { this.gap = 'record-producer-unverified'; return; }
      if (this.nativeOriginVersion === 2) this.io?.header(this.header);
      if (this.source.expectedHeader && JSON.stringify(this.header) !== JSON.stringify(this.source.expectedHeader)) { this.gap = 'history-source-generation-changed'; return; }
      if (this.historyMode && row.ordinal >= this.historyRange.endOrdinalExclusive) { this.gap = 'history-generation-range-unverified'; return; }
      if (this.ordinalMode) {
        const payload = row.payload, base = payload.history_base;
        if (payload.history_mode !== 'paginated' || this.source.forkedFromId && payload.forked_from_id !== this.source.forkedFromId || base && (typeof base.thread_id !== 'string' || !base.thread_id || !Number.isSafeInteger(base.end_ordinal_exclusive) || base.end_ordinal_exclusive < 1 || !Number.isSafeInteger(base.end_byte_offset) || base.end_byte_offset < 1 || row.ordinal !== base.end_ordinal_exclusive) || !base && row.ordinal !== 0) { this.gap = 'fork-provenance-unverified'; return; }
        if (base) this.forkProvenance = { forkThreadId: this.source.threadId, creationSourceThreadId: payload.forked_from_id ?? null, historyBase: { sourceThreadId: base.thread_id, endOrdinalExclusive: base.end_ordinal_exclusive, endByteOffset: base.end_byte_offset }, recordOrdinal: row.ordinal, recordStart, recordEnd: this.offset + 1 };
        else if (this.source.forkedFromId) { this.gap = 'fork-provenance-unverified'; return; }
      }
      this.state.header = true; return;
    }
    if (!this.state.header) return;
    if (row.payload?.thread_id && row.payload.thread_id !== this.source.threadId) { this.excluded++; return; }
    if (this.materialMode && row.type === 'event_msg' && row.payload?.type === 'item_completed' && row.payload.thread_id !== this.source.threadId) { this.excluded++; return; }
    const historyProducer = this.materialMode && this.historyMode && row.type === 'event_msg' && row.payload?.type === 'item_completed' && row.material.fields['payload.item.type']?.text === 'UserMessage';
    if (this.materialMode && (this.state.anchored && this.state.turnId || historyProducer)) {
      const turnId = historyProducer ? row.payload.turn_id : this.state.turnId;
      const itemType = row.material.fields['payload.item.type']?.text;
      if (row.type === 'event_msg' && ['item_started', 'item_completed'].includes(row.payload.type) && row.payload.turn_id === turnId && !materialItems.has(itemType)) { this.gap = 'unsupported-item-type:' + digest(JSON.stringify(itemType ?? null)); return; }
      const material = materialRecord(row, turnId, recordStart, this.offset + 1, turnId === this.state.turnId ? this.turnTrigger : null);
      // Do not reserve a native body identity for a temporary omission: later
      // fixed-field support must not conflict with an already captured body.
      if (material?.turnId === turnId && material.kind === 'tool-result' && row.type === 'response_item' && ['function_call_output', 'custom_tool_call_output'].includes(row.payload.type) && !row.material.fields['payload.output']) { this.gap = 'unsupported-tool-result-shape'; return; }
      if (material && (material.turnId === this.state.turnId || historyProducer)) {
        if (this.nativeOriginVersion === 2) Object.assign(material.data, { nativeGenerationKey: nativeStorageKey(this.source.path), nativeRecordStart: recordStart, nativeRecordEnd: this.offset + 1 });
        material.data.nativeRecordId = nativeRecordIdentity(this.originIdentity ?? this.fileId, this.header, recordStart, material.data.nativeType);
        if (this.ordinalMode) { material.captureOffset = row.ordinal; material.data.recordOrdinal = row.ordinal; }
        if (this.historyMode) Object.assign(material.data, { history: true, inherited: true, originRunId: material.turnId, sourceThreadId: this.source.threadId });
        this.materials.push(material);
      }
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
    if (!this.historyMode && !this.state.anchored && id !== this.source.anchorTurnId && !(type === 'task_started' && recordStart >= this.attachSize)) return;
    if (typeof id !== 'string' || !id || id.length > 128 || !Number.isFinite(Date.parse(row.timestamp))) { this.gap = 'invalid-lifecycle-record'; return; }
    this.state.anchored = true;
    const turnTrigger = this.materialMode && row.material.fields['payload.turn_attribution.turn_id']?.text === id ? row.material.turnTrigger ?? null : null;
    this.recentLifecycle.push({ type, turnId: id, at: Date.parse(row.timestamp), recordStart, ...(this.materialMode ? { recordEnd: this.offset + 1, turnTrigger, nativeRecordId: nativeRecordIdentity(this.originIdentity ?? this.fileId, this.header, recordStart, type), ...(this.nativeOriginVersion === 2 ? { nativeGenerationKey: nativeStorageKey(this.source.path), nativeRecordStart: recordStart, nativeRecordEnd: this.offset + 1 } : {}), ...(this.ordinalMode ? { captureOffset: row.ordinal, recordOrdinal: row.ordinal } : {}), ...(this.historyMode ? { history: true, inherited: true, originRunId: id, sourceThreadId: this.source.threadId } : {}) } : {}) });
    if (!this.materialMode && this.recentLifecycle.length > 8) this.recentLifecycle.shift();
    if (type === 'task_started') {
      if (this.terminalProof && this.terminalProof.turnId !== id) this.terminalProof = null;
      if (this.materialMode) this.turnTrigger = turnTrigger;
      this.state = { header: true, anchored: true, phase: 'started', turnId: id, at: Date.parse(row.timestamp), orphan: Date.parse(row.timestamp) < this.source.hostStartedAt };
      if (this.scanOnly) this.scanStart = { turnId: id, at: this.state.at, recordStart };
    } else if (!this.state.turnId || this.state.turnId === id) {
      this.state = { header: true, anchored: true, phase: type === 'task_complete' ? 'completed' : 'aborted', turnId: id, at: Date.parse(row.timestamp), orphan: false };
    }
  }

  consume(bytes) {
    let at = 0;
    while (at < bytes.length && !this.gap && (this.mode !== 'header' || !this.state.header) && (!this.materialMode || this.recentLifecycle.length + this.materials.length < (this.maxEvents ?? 16))) {
      const newline = bytes.indexOf(10, at);
      const end = newline < 0 ? bytes.length : newline;
      const part = bytes.subarray(at, end);
      const recordStart = this.offset - this.pendingBytes;
      this.projection ??= this.materialMode ? new MaterialProjection() : new Projection();
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
      } catch (error) { this.gap = error.code?.startsWith('native_origin_') ? error.code : 'record-malformed-or-over-limit'; }
      at = newline < 0 ? bytes.length : newline + 1;
    }
  }

  async pump({ budgetBytes = 262144, maxEvents = 16, verifyOnly = false, available = this.available ?? true, profile = this.source.profile, hostRunId = this.source.hostRunId, anchorTurnId = this.source.anchorTurnId } = {}) {
    if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > 16) throw new Error('invalid event budget');
    this.maxEvents = maxEvents;
    if (!Number.isSafeInteger(budgetBytes) || budgetBytes < 1 || budgetBytes > 262144) throw new Error('invalid read budget');
    if (this.materialMode && !this.busy) { this.recentLifecycle = []; this.materials = []; }
    if (this.closed) return { ...this.view('reader-closed'), readBytes: 0 };
    if (this.gap) return { ...this.view(), readBytes: 0 };
    if (!supportsHostProfile(profile) || profileKey(profile) !== this.hostProfileKey || profileKey(this.source.profile) !== this.hostProfileKey) { this.gap = 'unsupported-host-version'; return { ...this.view(), readBytes: 0 }; }
    if (hostRunId !== this.source.hostRunId) { this.gap = 'source-generation-changed'; return { ...this.view(), readBytes: 0 }; }
    this.available = available;
    if (!available) { this.needsGuard = true; return { ...this.view('source-unavailable'), readBytes: 0 }; }
    if (this.busy) return { ...this.view('reader-busy'), ...(this.materialMode ? { materials: [], recentLifecycle: [] } : {}), readBytes: 0 };
    if (this.nativeOriginVersion === 2) return this.pumpV2({ budgetBytes, maxEvents, verifyOnly, anchorTurnId, decodedBudgetBytes: arguments[0]?.decodedBudgetBytes ?? 262144 });
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
      if (!stat.isFile() || id !== `${opened.dev}:${opened.ino}` || this.fileId && this.fileId !== id || this.source.expectedFileId && this.source.expectedFileId !== id) { this.gap = 'record-file-replaced'; return { ...this.view(), readBytes }; }
      this.fileId = id; this.attachSize ??= stat.size;
      if (this.historyMode && this.historyRange.endByteOffset > stat.size) { this.gap = 'history-range-unavailable'; return { ...this.view(), readBytes }; }
      if (stat.size < Math.max(this.offset, this.knownSize ?? this.attachSize)) { this.gap = 'record-file-truncated'; return { ...this.view(), readBytes }; }
      this.knownSize = stat.size;
      if (this.stat && stat.size === this.stat.size && stat.mtimeMs !== this.stat.mtimeMs) { this.gap = 'record-rewrite-suspected'; return { ...this.view(), readBytes }; }
      const unchanged = this.stat && stat.size === this.stat.size && stat.mtimeMs === this.stat.mtimeMs;
      if (!unchanged || this.needsGuard) {
        const guardSize = Math.min(64, this.committed);
        if (guardSize > budgetBytes) return { ...this.view('budget-too-small-for-guard'), readBytes };
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
      const count = verifyOnly ? 0 : Math.min(budgetBytes - readBytes, Math.min(stat.size, this.historyRange?.endByteOffset ?? Infinity) - this.offset);
      if (count > 0) {
        this.ioBuffer ??= Buffer.alloc(262144);
        const buffer = this.ioBuffer;
        const got = await this.handle.read(buffer, 0, count, this.offset);
        readBytes += got.bytesRead;
        this.consume(buffer.subarray(0, got.bytesRead));
      }
      if (this.historyMode && this.offset >= this.historyRange.endByteOffset && (this.pendingBytes || this.nextOrdinal !== this.historyRange.endOrdinalExclusive)) this.gap = 'history-boundary-unverified';
      const after = await fs.stat(this.source.path);
      if (`${after.dev}:${after.ino}` !== this.fileId) this.gap = 'record-file-replaced';
      else if (after.size < Math.max(this.offset, this.knownSize)) this.gap = 'record-file-truncated';
      else if (after.size === stat.size && after.mtimeMs !== stat.mtimeMs) this.gap = 'record-rewrite-suspected';
      else if (after.size > stat.size) {
        this.stat = { size: after.size, mtimeMs: after.mtimeMs };
        this.knownSize = after.size;
        this.needsGuard = true;
      }
    } catch (error) { this.gap = 'record-source-unavailable'; if (sourceIoCodes.has(error?.code)) this.error = { code: error.code }; }
    finally { this.busy = false; this.metrics.physicalBytes += readBytes; this.metrics.maximumReadBytes = Math.max(this.metrics.maximumReadBytes, readBytes); this.metrics.peakDecodedBufferBytes = this.io?.peakBuffered ?? 0; finishPump(); this.pumpComplete = null; }
    return { ...this.view(), readBytes };
  }

  async pumpV2({ budgetBytes, maxEvents, verifyOnly, decodedBudgetBytes, anchorTurnId }) {
    this.busy = true;
    let finishPump; this.pumpComplete = new Promise(resolve => { finishPump = resolve; });
    let readBytes = 0, decodedBytes = 0;
    const previous = this.checkpoint();
    try {
      if (this.recoveryCheckpoint && !this.recoveryFence) {
        const saved = this.recoveryCheckpoint, key = nativeStorageKey(this.source.path);
        const unread = saved.offset === 0 && saved.state?.header === false && saved.state?.anchored === false && saved.state?.phase === 'unknown' && saved.state?.turnId === null && saved.header === null && (!saved.io || saved.io.key === key && saved.io.offset === 0);
        if (unread) this.recoveryCheckpoint = null;
        else {
          const fence = await this.origins.read(key);
          if (!saved.io || saved.io.key !== key || saved.io.offset !== saved.offset || !saved.header || !fence || fence.key !== key || fence.headerHash !== headerIdentity(saved.header) || fence.prefix.offset < saved.offset) { this.gap = 'cursor-invalid'; return { ...this.view(), readBytes, decodedBytes, nativeOrigins: [] }; }
          this.recoveryFence = fence;
        }
      }
      if (this.coldScan) {
        const scan = await this.coldScan.pump({ budgetBytes, maxEvents, decodedBudgetBytes, verifyOnly }); readBytes = scan.readBytes; decodedBytes = scan.decodedBytes; this.handle = this.coldScan.handle;
        if (scan.coverage === 'gap' && !['start-predates-host', 'host-start-time-overlap'].includes(scan.gap)) { this.gap = scan.gap; this.error = scan.error; await this.coldScan.close(); this.coldScan = null; }
        else {
          const ending = scan.recentLifecycle.findLast(event => event.turnId === this.state.turnId && ['task_complete', 'turn_aborted'].includes(event.type));
          const start = this.coldScan.scanStart;
          const successor = start?.turnId === anchorTurnId && start.turnId !== this.state.turnId && start.at >= this.source.hostStartedAt + 1000 ? start : null;
          const origin = this.coldScan.io?.proof();
          if ((ending || successor) && origin?.complete) {
            const proof = { key: origin.key, headerHash: origin.headerHash, prefix: origin.next, ...(ending ? { turnId: ending.turnId, phase: ending.type === 'task_complete' ? 'completed' : 'aborted', at: ending.at } : { turnId: successor.turnId, at: successor.at, recordStart: successor.recordStart, previousTurnId: this.state.turnId }) };
            const checkpoint = { ...this.coldCheckpoint, io: this.coldScan.io.checkpoint(this.coldCheckpoint.offset), fileId: this.coldScan.fileId, header: this.coldScan.header, attachSize: this.coldCheckpoint.attachSize ?? this.coldScan.attachSize, knownSize: this.coldScan.knownSize };
            await this.coldScan.close();
            const restored = new BoundedReader(this.source, checkpoint, { mode: this.mode, forkSuffix: this.ordinalMode && !this.historyMode, origins: this.origins, terminalProof: ending ? proof : null, successorProof: ending ? null : proof });
            Object.assign(this, restored, { busy: true, pumpComplete: this.pumpComplete, metrics: this.metrics });
          }
        }
        return { ...this.view(), readBytes, decodedBytes, nativeOrigins: [] };
      }
      const recoveryFence = this.recoveryFence, origins = recoveryFence ? { read: async key => key === recoveryFence.key ? recoveryFence : this.origins.read(key) } : this.origins;
      this.io ??= new NativeJsonlSource({ ...this.source, ...(!this.historyMode && (this.terminalProof || this.successorProof) ? { terminalProof: this.terminalProof ?? this.successorProof } : {}) }, { ...(this.ioCheckpoint ?? {}), offset: this.committed }, { validateFrame: this.mode !== 'header', origins, onHeader: bytes => { const projection = new Projection(); projection.write(bytes); const row = projection.finish(); if (row.type !== 'session_meta' || row.payload?.id !== this.source.threadId) throw Object.assign(new Error('invalid native header'), { code: 'history-source-unverified' }); const header = nativeHeader(row); if (!producerAllowed(this.source, header.cliVersion)) throw Object.assign(new Error('unverified native producer'), { code: 'record-producer-unverified' }); return header; } });
      const value = await this.io.pump({ physicalBudget: budgetBytes, decodedBudget: decodedBudgetBytes, limit: this.historyRange?.endByteOffset ?? Infinity, verifyOnly: verifyOnly && !this.recoveryCheckpoint }, bytes => {
        this.attachSize ??= this.io.decodedSize;
        const before = this.offset; this.consume(bytes); return this.offset - before;
      });
      readBytes = value.physicalBytes; decodedBytes = value.decodedBytes; this.ioView = value;
      if (this.recoveryCheckpoint && this.io.proof()) this.recoveryCheckpoint = null;
      this.handle = this.io.handle; if (!this.handle) return { ...this.view(), coverage: 'catching-up', readBytes, decodedBytes, nativeOrigins: [] }; this.fileId = this.io.physical.fileId;
      this.attachSize ??= value.decodedSize; this.knownSize = value.decodedSize;
      this.stat = value.decodedSize === null ? null : { size: value.decodedSize, mtimeMs: this.io.stat.mtimeMs };
      if (this.source.expectedFileId && this.source.expectedFileId !== this.fileId) this.gap = 'record-file-replaced';
      if (this.historyMode && value.decodedSize !== null && this.historyRange.endByteOffset > value.decodedSize) this.gap = 'history-range-unavailable';
      if (this.historyMode && value.eof && (this.pendingBytes || this.nextOrdinal !== this.historyRange.endOrdinalExclusive)) this.gap = 'history-boundary-unverified';
      if (!this.scanOnly && this.materialMode && !this.historyMode && !this.terminalProof && ['start-predates-host', 'host-start-time-overlap'].includes(this.view().gap)) {
        const initial = this.io.proof(), ioCheckpoint = this.io.checkpoint(0), upstream = this.origins;
        const origins = initial ? { read: async key => {
          const retained = upstream ? await upstream.read(key) : null;
          return key === initial.key && (!retained || retained.prefix.offset < initial.next.offset) ? { key, headerHash: initial.headerHash, prefix: initial.next, revision: retained?.revision ?? initial.revision } : retained;
        } } : upstream;
        const scan = new BoundedReader({ ...this.source, expectedHeader: this.header, expectedFileId: this.fileId }, null, { mode: this.mode, forkSuffix: this.ordinalMode, origins, scanOnly: true });
        scan.ioCheckpoint = ioCheckpoint; scan.fileId = this.fileId; scan.attachSize = this.attachSize; scan.knownSize = this.knownSize;
        await this.io.close(); this.io = null; this.handle = null;
        this.coldCheckpoint = previous;
        this.coldScan = scan;
      }
    } catch (error) { this.handle = this.io?.handle ?? null; readBytes = error.physicalBytes ?? readBytes; decodedBytes = error.decodedBytes ?? decodedBytes; this.gap = ['native_origin_prefix_changed', 'native_origin_header_changed', 'record-path-unverified', 'record-producer-unverified', 'record-file-replaced', 'record-file-truncated', 'record-rewrite-suspected', 'compressed-frame-unverified', 'compressed-window-unverified', 'compressed-data-invalid', 'decoded-budget-exceeded', 'decoded-buffer-exceeded'].includes(error.code) ? error.code : 'record-source-unavailable'; if (sourceIoCodes.has(error?.code)) this.error = { code: error.code }; }
    finally { this.busy = false; this.metrics.physicalBytes += readBytes; this.metrics.decodedBytes = (this.metrics.decodedBytes ?? 0) + decodedBytes; this.metrics.maximumReadBytes = Math.max(this.metrics.maximumReadBytes, readBytes); finishPump(); this.pumpComplete = null; }
    return { ...this.view(), readBytes, decodedBytes, nativeOrigins: this.io?.proof() ? [this.io.proof()] : [], nativeRangeProof: this.io?.rangeProof() ?? null };
  }

  acknowledgeOrigins(fences) { this.io?.acknowledge(fences); }

  checkpoint() {
    if (this.recoveryCheckpoint) return structuredClone(this.recoveryCheckpoint);
    if (this.coldScan) return structuredClone(this.coldCheckpoint);
    return { format: this.nativeOriginVersion === 2 ? 3 : 2, ...(this.nativeOriginVersion === 2 ? { storageScope: this.storageScope, io: this.io?.checkpoint(this.committed) ?? this.ioCheckpoint ?? null } : {}), ...(this.terminalProof ? { terminalProof: this.terminalProof } : {}), ...(this.successorProof ? { successorProof: this.successorProof } : {}), scope: this.scope, offset: this.committed, attachSize: this.attachSize, knownSize: this.knownSize, fileId: this.fileId, guardHash: this.guardHash, state: { ...this.state }, gap: this.gap, ...(this.ordinalMode ? { nextOrdinal: this.nextOrdinal, forkProvenance: this.forkProvenance, header: this.header ?? null } : {}), ...(this.materialMode ? { header: this.header ?? null, nativeOriginVersion: this.nativeOriginVersion, projectionMode: this.mode, turnTrigger: this.turnTrigger } : {}) };
  }

  async close() {
    if (this.closing) return this.closing;
    this.closed = true;
    const coldClosing = this.coldScan?.close();
    const ioClosing = this.io?.close();
    this.closing = (async () => {
      await this.pumpComplete;
      if (coldClosing) await coldClosing; else if (this.io) await ioClosing; else await this.handle?.close(); this.handle = null; this.projection = null;
      this.ioBuffer?.fill(0); this.ioBuffer = null;
      this.rolling.fill(0); this.committedTail.fill(0);
      this.materials = []; this.recentLifecycle = []; this.turnTrigger = null; this.scanStart = null; this.coldScan = null; this.coldCheckpoint = null; this.terminalProof = null; this.successorProof = null; this.recoveryCheckpoint = null; this.recoveryFence = null;
    })();
    return this.closing;
  }
}
