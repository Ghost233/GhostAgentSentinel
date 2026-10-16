import fs from 'node:fs/promises'
import { basename, dirname, isAbsolute, join, resolve, sep } from 'node:path'
import { NativeJsonlSource, nativeStorageKey } from './native-jsonl-source.mjs'
import { BoundedReader } from './bounded-reader.mjs'
import { emptyForkProof, foldForkProof, forkGeneration, receiptIdentity, validForkProof, nativeRecordIdentity, FORK_LINEAGE_FRAMES, FORK_LEGACY_PRODUCER, FORK_LEGACY_MODE, isLegacyForkProducer } from '../src/fork-proof.mjs'
import { failureErrorInfo } from '../src/core/failure-receipts.mjs'

const MAX_DEPTH = FORK_LINEAGE_FRAMES
const MAX_CANDIDATES = 4, MAX_DIRECTORY_ENTRIES = 8192, MAX_DIRECTORIES = 256
const MAX_METADATA_CALLS = 144
const CHANNELS = ['materials', 'recentLifecycle', 'historyMaterials', 'historyLifecycle', 'historyBoundaries']
const HISTORY_FAILURES = new Set(['record-source-unavailable', 'record-identity-changed', 'record-file-replaced', 'record-file-truncated', 'record-rewrite-suspected', 'cursor-anchor-changed', 'record-malformed-or-over-limit', 'invalid-lifecycle-record', 'unsupported-record-type', 'unsupported-event-type', 'cursor-invalid', 'cursor-retains-gap', 'compressed-data-invalid', 'native_origin_prefix_changed', 'native_origin_header_changed'])
const readable = budget => Math.max(0, budget.bytes - 64 * budget.owner.nodes.length)
const signature = (value, path = true) => JSON.stringify([value?.id, ...(path ? [value?.path] : []), value?.cwd, value?.source, value?.originator, value?.parentThreadId, value?.forkedFromId ?? null])
const GAP = Symbol('fork-gap')
const failure = code => Object.assign(new Error(code), { code, [GAP]: true })
const clone = value => structuredClone(value)
const metadataFields = value => Object.fromEntries(['id', 'path', 'cwd', 'source', 'originator', 'parentThreadId', 'forkedFromId'].filter(key => value[key] !== undefined).map(key => [key, clone(value[key])]))
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const plainPath = path => path.replace(/\.jsonl\.zst$/, '.jsonl')
const recordOwnerPath = (metadata, recordId, path = metadata?.path) => typeof path === 'string' && uuid.test(recordId) && uuid.test(metadata?.id) && (basename(path).match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g)?.length === 2) && basename(plainPath(path)).endsWith('-' + metadata.id + '_' + recordId + '.jsonl')

// This fixed reader owns fork history, not another scheduler or host sampler.
// Parent App/user/current-project guards remain the adapter's batch fence.
export class ForkHistoryReader {
  constructor(source, cursor = null, { native, sessionsRoot, mode = 'material', origins = null } = {}) {
    if (mode !== 'material') throw new Error('fork history requires material mode')
    this.origins = origins; this.source = source; this.native = native; this.sessionsRoot = resolve(sessionsRoot)
    this.archiveRoot = basename(this.sessionsRoot) === 'sessions' ? join(dirname(this.sessionsRoot), 'archived_sessions') : null
    this.captureThroughOffset = cursor?.captureThroughOffset ?? -1
    this.deliveryThrough = clone(cursor?.deliveryThrough ?? { current: this.captureThroughOffset, history: this.captureThroughOffset })
    this.historyGap = cursor?.historyGap ?? null
    const hasDelivery = cursor?.deliveryThrough && ['current', 'history'].every(key => Number.isSafeInteger(cursor.deliveryThrough[key]) && cursor.deliveryThrough[key] >= -1 && cursor.deliveryThrough[key] <= cursor.captureThroughOffset)
    const frame = value => value && typeof value === 'object' && value.source && typeof value.source === 'object' && value.reader && typeof value.reader === 'object' && validForkProof(value.proof) && ['resolved', 'boundaryDelivered', 'complete'].every(key => typeof value[key] === 'boolean')
    this.gap = cursor && (!((cursor.format === 4 && hasDelivery) || cursor.format === (source.nativeOriginVersion === 2 ? 3 : 2)) || cursor.kind !== (source.nativeOriginVersion === 2 ? 'fork-lineage/2' : 'fork-lineage/1') || cursor.receiptVersion !== 2 || !Array.isArray(cursor.frames) || cursor.frames.length < 1 || cursor.frames.length > MAX_DEPTH || !cursor.frames.every(frame)) ? 'cursor-invalid' : null
    if (!['current', 'history'].every(key => Number.isSafeInteger(this.deliveryThrough[key]) && this.deliveryThrough[key] >= -1) || this.historyGap !== null && (typeof this.historyGap !== 'string' || this.historyGap.length > 256)) this.gap = 'cursor-invalid'
    this.nodes = []
    this.physicalBytes = 0; this.maximumReadBytes = 0
    this.current = this.node(source, !this.gap ? cursor?.frames[0] ?? null : null, false)
    if (cursor && !this.gap) {
      for (const saved of cursor.frames.slice(1)) {
        const parent = this.node(saved.source, saved, true)
        this.nodes.at(-2).parent = parent
      }
      if (cursor.resolution) this.restoreResolution(cursor.resolution)
      for (const node of this.nodes) {
        const header = node.reader.header
        if (node.reader.gap || node.history && (!node.metadata?.record || !node.metadata?.owner || node.metadata.record.id !== node.source.threadId || node.metadata.owner.id !== node.source.threadId) || node.complete && (!node.history || !header || node.ack.offset !== node.source.historyRange?.endByteOffset || node.ack.nextOrdinal !== node.source.historyRange?.endOrdinalExclusive) || node.resolved && header?.historyBase && !node.parent || node.parent && header && (node.parent.source.historyRecordId !== header.historyBase?.sourceThreadId || node.parent.source.historyRange?.endOrdinalExclusive !== header.historyBase?.endOrdinalExclusive || node.parent.source.historyRange?.endByteOffset !== header.historyBase?.endByteOffset)) this.gap = 'cursor-invalid'
      }
    }
  }

  node(source, saved, history) {
    if (this.nodes.length >= MAX_DEPTH) throw failure('fork-history-depth-limit')
    if (saved && !validForkProof(saved.proof)) this.gap = 'cursor-invalid'
    const options = { mode: history ? 'history' : 'material', forkSuffix: !history, origins: this.origins }
    const fresh = new BoundedReader(source, null, options)
    const reader = saved?.reader && JSON.stringify(saved.reader) !== JSON.stringify(fresh.checkpoint()) ? new BoundedReader(source, saved.reader, options) : fresh
    const node = { source, reader, history, ack: reader.checkpoint(), pending: [], parent: null, resolved: saved?.resolved === true, boundaryDelivered: saved?.boundaryDelivered === true, complete: saved?.complete === true, coverage: saved?.complete ? 'caught-up' : null, metadata: saved?.metadata ?? null, originProof: saved?.originProof ?? null, proof: clone(saved?.proof ?? emptyForkProof()), ackProof: clone(saved?.proof ?? emptyForkProof()) }
    if (history && saved?.originProof?.complete && saved.originProof.next?.offset > reader.committed) node.replayTarget = clone(saved.originProof)
    this.nodes.push(node)
    return node
  }

  async nativeCall(method, argument) {
    const budget = this.budget, key = method + ':' + argument
    if (budget?.phase.has(key)) return budget.phase.get(key)
    if (budget && ++budget.calls > MAX_METADATA_CALLS) throw failure('history-metadata-budget')
    const pending = this.native[method](argument)
    budget?.phase.set(key, pending)
    return pending
  }

  // Closed historical files keep only their original checkpoint. Reopening
  // verifies the generation/tail again, without retaining one handle per frame.
  async readNode(node, options) {
    let view
    try {
      view = await node.reader.pump(options)
      if (view.nativeOrigins?.length) node.originProof = view.nativeOrigins[0]
      node.coverage = view.coverage
      if (!node.history && view.coverage === 'gap') this.currentFailure = true
      return view
    } catch (error) {
      if (!node.history) this.currentFailure = true
      throw error
    } finally {
      const completedGuard = options.verifyOnly && node.complete
      if (node.history && node.reader.handle && node.reader.pendingBytes === 0 && (node.source.nativeOriginVersion !== 2 || view?.coverage === 'caught-up' || completedGuard && !node.reader.io?.revalidation)) {
        const cursor = completedGuard ? this.position(node, node.ack) : node.reader.checkpoint(), stat = node.reader.stat
        await node.reader.close()
        node.reader = new BoundedReader(node.source, cursor, { mode: 'history', origins: this.origins })
        node.reader.stat = stat
      }
    }
  }

  restoreResolution(saved) {
    const node = this.nodes[saved.frame]
    if (!Number.isInteger(saved.frame) || !node || node.resolved || !saved.metadata?.record || !saved.metadata?.owner || !Array.isArray(saved.candidates) || saved.candidates.length > MAX_CANDIDATES || !saved.candidates.every(path => typeof path === 'string' && isAbsolute(path)) || !Number.isInteger(saved.index) || saved.index < 0 || saved.index > saved.candidates.length || !Array.isArray(saved.matches) || saved.matches.length > MAX_CANDIDATES || !Array.isArray(saved.seenFiles) || saved.seenFiles.length > MAX_CANDIDATES || !['header', 'prefix', undefined].includes(saved.phase) || saved.probe && (!saved.probe.source || !saved.probe.reader) || saved.phase === 'prefix' && !saved.expectedHeader) { this.gap = 'cursor-invalid'; return }
    const { frame, probe, ...resolution } = saved
    node.resolution = { ...clone(resolution), seenFiles: new Set(saved.seenFiles), probe: probe ? new BoundedReader(probe.source, probe.reader, { mode: saved.phase === 'header' ? 'header' : 'history-metadata', forkSuffix: saved.phase === 'header', origins: saved.phase === 'header' ? null : this.origins }) : null }
  }

  async qualify(id, expected = null, candidate = null) {
    if (this.closed) throw failure('reader-closed')
    const metadata = candidate ?? await this.nativeCall('readThread', id), thread = metadata?.thread
    if (this.closed) throw failure('reader-closed')
    if (!thread || thread.id !== id || thread.parentThreadId !== null || thread.originator !== 'Codex Desktop' || !['vscode', 'cli', 'appServer'].includes(thread.source) || !isAbsolute(thread.cwd ?? '') || !isAbsolute(thread.path ?? '') || thread.environments != null && (!Array.isArray(thread.environments) || thread.environments.some(item => item?.environmentId !== 'local'))) throw failure('history-source-unverified')
    const path = resolve(thread.path)
    if (![this.sessionsRoot, this.archiveRoot].filter(Boolean).some(root => path.startsWith(root + sep)) || !(path.endsWith('.jsonl') || this.source.nativeOriginVersion === 2 && path.endsWith('.jsonl.zst'))) throw failure('history-source-unverified')
    const enabled = await this.nativeCall('readEnablement', thread.cwd)
    if (enabled?.user !== 'enabled' || enabled?.effective !== 'enabled') throw failure(enabled?.user === 'disabled' || enabled?.effective === 'disabled' ? 'history-project-disabled' : 'history-enablement-unconfirmed')
    if (expected && signature(thread) !== signature(expected)) throw failure('history-source-generation-changed')
    if (this.closed) throw failure('reader-closed')
    return thread
  }

  async historyMetadata(recordId, expected = null) {
    const lookup = await this.nativeCall('readThread', recordId), owner = lookup?.thread
    let record, canonical
    if (owner?.id === recordId) record = canonical = await this.qualify(recordId, null, lookup)
    else {
      // The native storage key and its logical owner can name different
      // physical generations. Each role must remain stable on its own path.
      if (!owner || !recordOwnerPath(owner, recordId)) throw failure('history-source-unverified')
      record = await this.qualify(owner.id, null, lookup)
      canonical = await this.qualify(owner.id)
      if (signature(record, false) !== signature(canonical, false)) throw failure('history-source-unverified')
    }
    const pair = { record: metadataFields(record), owner: metadataFields(canonical) }
    if (expected && (signature(pair.record) !== signature(expected.record) || signature(pair.owner) !== signature(expected.owner))) throw failure('history-source-generation-changed')
    return pair
  }

  async qualifiedHistory(node, budget) {
    const fresh = await this.historyMetadata(node.source.historyRecordId)
    const unchanged = signature(fresh.record) === signature(node.metadata.record) && signature(fresh.owner) === signature(node.metadata.owner)
    let missing = false
    if (this.source.nativeOriginVersion === 2) { try { await fs.lstat(node.source.path) } catch (error) { if (error.code !== 'ENOENT') throw error; missing = true } }
    if (unchanged && !missing) { budget.qualified.set(node, { recordId: node.source.historyRecordId, metadata: fresh }); return true }
    const codecOnly = this.source.nativeOriginVersion === 2 && ['record', 'owner'].every(role => signature(fresh[role], false) === signature(node.metadata[role], false) && nativeStorageKey(fresh[role].path) === nativeStorageKey(node.metadata[role].path))
    if (!codecOnly) throw failure('history-source-generation-changed')
    const child = this.nodes[this.nodes.indexOf(node) - 1]
    if (!child || child.parent !== node) throw failure('cursor-invalid')
    child.resolved = false; child.codecRebind = true; node.codecPending = true
    if (child.resolution) child.resolution.rebind = true
    return false
  }

  async producerProject(cwd) {
    if (!isAbsolute(cwd ?? '') || cwd.length > 1024) throw failure('history-producer-project-unverified')
    const enabled = await this.nativeCall('readEnablement', cwd)
    if (enabled?.user !== 'enabled' || enabled?.effective !== 'enabled') throw failure(enabled?.user === 'disabled' || enabled?.effective === 'disabled' ? 'history-project-disabled' : 'history-enablement-unconfirmed')
    if (this.closed) throw failure('reader-closed')
  }

  async localFile(path) {
    const actual = await fs.realpath(path), stat = await fs.stat(actual)
    const roots = await Promise.all([this.sessionsRoot, this.archiveRoot].filter(Boolean).map(root => fs.realpath(root).catch(error => { if (root === this.archiveRoot && error.code === 'ENOENT') return null; throw error })))
    const allowedRoot = roots.filter(Boolean).find(root => actual.startsWith(root + sep))
    if (!allowedRoot || !(actual.endsWith('.jsonl') || this.source.nativeOriginVersion === 2 && actual.endsWith('.jsonl.zst')) || !stat.isFile() || typeof process.getuid === 'function' && stat.uid !== process.getuid()) throw failure('history-source-unverified')
    return { actual, stat, allowedRoot }
  }

  historySource(source, record, base) {
    if (!source || source.threadId !== record.id || source.parentId !== null || source.originator !== record.originator || source.sourceKind !== record.source || source.forkedFromId !== (record.forkedFromId ?? null) || source.hostRunId !== this.source.hostRunId || JSON.stringify(source.profile) !== JSON.stringify(this.source.profile) || source.historyRecordId !== base.sourceThreadId || JSON.stringify(source.historyRange) !== JSON.stringify({ endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset })) throw failure('cursor-invalid')
  }

  async candidates(metadata, recordId = metadata.id) {
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(metadata.id)) throw failure('history-source-unverified')
    const paths = new Set([resolve(metadata.path)]), queue = [this.sessionsRoot, this.archiveRoot].filter(Boolean).map(path => ({ path, depth: 0 }))
    let entries = 0, directories = 0
    while (queue.length) {
      if (this.closed) throw failure('reader-closed')
      if (++directories > MAX_DIRECTORIES) throw failure('history-directory-budget')
      const { path, depth } = queue.shift()
      let directory
      try { directory = await fs.opendir(path) } catch (error) { if (error.code === 'ENOENT') continue; throw error }
      try {
        for await (const entry of directory) {
          if (++entries > MAX_DIRECTORY_ENTRIES) throw failure('history-directory-budget')
          if (entry.isDirectory()) { if (depth >= 4) throw failure('history-directory-depth-limit'); if (queue.length + directories >= MAX_DIRECTORIES) throw failure('history-directory-budget'); queue.push({ path: join(path, entry.name), depth: depth + 1 }) }
          else if (entry.isFile() && (recordId === metadata.id ? plainPath(entry.name).endsWith('-' + metadata.id + '.jsonl') : recordOwnerPath(metadata, recordId, entry.name))) paths.add(join(path, entry.name))
          if (paths.size > MAX_CANDIDATES) throw failure('history-candidate-budget')
        }
      } finally { await directory.close().catch(error => { if (error.code !== 'ERR_DIR_CLOSED') throw error }) }
    }
    return [...paths]
  }

  async ancestry(node, budget, output) {
    if (node.resolved) return
    // History expectedHeader was obtained by the unique, complete native
    // prefix witness. Follow that metadata before opening its material stream.
    // Every material stream still rechecks the same file/header/range itself.
    const header = node.reader.header ?? (node.history ? node.source.expectedHeader : null)
    if (!header) return
    const base = header.historyBase
    if (!base) { node.resolved = true; return }
    if (!node.resolution) {
      const metadata = await this.historyMetadata(base.sourceThreadId)
      const paths = await this.candidates(metadata.record, base.sourceThreadId)
      node.resolution = { metadata, candidates: paths, index: 0, probe: null, matches: [], seenFiles: new Set(), recordId: base.sourceThreadId, ...(node.codecRebind ? { rebind: true } : {}) }
    }
    const resolution = node.resolution
    await this.historyMetadata(base.sourceThreadId, resolution.metadata)
    budget.qualified.set(resolution, { recordId: base.sourceThreadId, metadata: resolution.metadata })
    if (resolution.probe) {
      this.historySource(resolution.probe.source, resolution.metadata.record, base)
      await this.localFile(resolution.probe.source.path)
    }
    while (resolution.index < resolution.candidates.length && readable(budget) > 64 && budget.decoded >= 32768) {
      const path = resolution.candidates[resolution.index]
      if (!resolution.probe) {
        let local
        try { local = await this.localFile(path) } catch (error) { if (this.source.nativeOriginVersion === 2 && error.code === 'ENOENT') { resolution.index++; continue } throw error }
        const { actual, stat, allowedRoot } = local
        if (!actual.endsWith('.zst') && stat.size < base.endByteOffset) { resolution.index++; continue }
        const fileId = `${stat.dev}:${stat.ino}`
        if (resolution.seenFiles.has(fileId)) { resolution.index++; continue }
        resolution.seenFiles.add(fileId)
        const metadata = resolution.metadata.record
        if (base.sourceThreadId !== metadata.id && (!recordOwnerPath(metadata, base.sourceThreadId) || !recordOwnerPath(metadata, base.sourceThreadId, actual))) throw failure('history-source-unverified')
        const source = { ...this.source, path: actual, allowedRoot, threadId: metadata.id, historyRecordId: base.sourceThreadId, cwd: metadata.cwd, sourceKind: metadata.source, originator: metadata.originator, forkedFromId: metadata.forkedFromId ?? null, anchorTurnId: null, historyRange: { endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset } }
        resolution.probe = new BoundedReader(source, null, { mode: 'header', forkSuffix: true }); resolution.phase = 'header'
      }
      if (resolution.phase === 'prefix') { await this.producerProject(resolution.probe.source.cwd); budget.projects.add(resolution.probe.source.cwd) }
      const probe = resolution.probe, view = await probe.pump({ ...budget.options, budgetBytes: resolution.phase === 'header' ? Math.min(4096, readable(budget)) : readable(budget), decodedBudgetBytes: budget.decoded })
      budget.bytes -= view.readBytes; output.readBytes += view.readBytes; budget.decoded -= view.decodedBytes ?? 0; output.decodedBytes += view.decodedBytes ?? 0
      if (view.coverage === 'gap' && this.source.nativeOriginVersion === 2 && resolution.candidates.filter(candidate => nativeStorageKey(candidate) === nativeStorageKey(path)).length > 1) throw failure('history-generation-equivalence-unverified')
      if (view.error) { this.error = view.error; throw failure(view.gap) }
      if (view.coverage === 'gap' && HISTORY_FAILURES.has(view.gap?.split(':')[0])) {
        this.error = failureErrorInfo({ code: view.gap, stage: 'collection' }); throw failure(view.gap)
      }
      if (resolution.phase === 'header' && view.header) {
        const header = view.header
        const matches = header.threadId === resolution.metadata.record.id && header.originator === 'Codex Desktop' && header.sourceKind === resolution.metadata.record.source && (header.cliVersion === this.source.profile.cliVersion || isLegacyForkProducer(header.cliVersion)) && header.ordinal < base.endOrdinalExclusive
        if (matches) {
          await this.producerProject(header.cwd); budget.projects.add(header.cwd)
          const source = { ...probe.source, cwd: header.cwd }
          resolution.expectedHeader = { header: clone(header), fileId: probe.checkpoint().fileId }
          await probe.close(); resolution.probe = new BoundedReader(source, null, { mode: 'history-metadata', origins: this.origins }); resolution.phase = 'prefix'
          continue
        }
        await probe.close(); resolution.probe = null; resolution.index++; continue
      }
      if (resolution.phase === 'header' && view.metrics.physicalBytes >= 65536) throw failure('history-header-budget')
      if (view.coverage === 'gap' || view.coverage === 'caught-up') {
        const header = view.header
        if (view.coverage === 'caught-up' && resolution.phase === 'prefix') {
          if (JSON.stringify(header) !== JSON.stringify(resolution.expectedHeader.header) || probe.checkpoint().fileId !== resolution.expectedHeader.fileId) throw failure('history-source-generation-changed')
          resolution.matches.push({ source: { ...probe.source, expectedHeader: clone(header), expectedFileId: probe.checkpoint().fileId }, reader: probe.checkpoint(), ...(view.nativeRangeProof ? { rangeProof: view.nativeRangeProof } : {}) })
        }
        await probe.close(); resolution.probe = null; resolution.index++
      } else break
    }
    if (resolution.index < resolution.candidates.length) return
    if (this.source.nativeOriginVersion === 2 && resolution.matches.length > 1 && resolution.matches.every(match => match.rangeProof && JSON.stringify(match.rangeProof) === JSON.stringify(resolution.matches[0].rangeProof))) resolution.matches = [resolution.matches[0]]
    if (resolution.matches.length !== 1) throw failure(resolution.matches.length ? 'history-generation-ambiguous' : 'history-generation-locator-unverified')
    const selected = resolution.matches[0]
    this.historySource(selected.source, resolution.metadata.record, base)
    await this.localFile(selected.source.path)
    if (selected.source.expectedFileId !== selected.reader.fileId || JSON.stringify(selected.source.expectedHeader) !== JSON.stringify(selected.reader.header)) throw failure('cursor-invalid')
    if (resolution.rebind) {
      const parent = node.parent
      if (!parent || nativeStorageKey(parent.source.path) !== nativeStorageKey(selected.source.path) || JSON.stringify(parent.ack.header) !== JSON.stringify(selected.reader.header)) throw failure('history-source-generation-changed')
      await parent.reader.close(); await parent.verifier?.close(); parent.verifier = null
      parent.source = selected.source; parent.metadata = resolution.metadata
      parent.ack = { ...parent.ack, fileId: selected.reader.fileId, io: { ...selected.reader.io, offset: parent.ack.offset } }
      parent.reader = new BoundedReader(parent.source, parent.ack, { mode: 'history', origins: this.origins })
      if (parent.reader.gap) throw failure(parent.reader.gap)
      parent.pending = []; parent.complete = false; parent.codecPending = false
      node.codecRebind = false
    } else {
      if (this.nodes.some(item => item.source.path === selected.source.path && item.source.historyRange?.endOrdinalExclusive === base.endOrdinalExclusive)) throw failure('fork-history-cycle')
      node.parent = this.node(selected.source, null, true); node.parent.metadata = resolution.metadata
    }
    node.resolved = true; node.resolution = null
  }

  generation(node) {
    const reader = node.reader.checkpoint()
    return forkGeneration(node.reader.originIdentity ?? reader.fileId, reader.header)
  }

  receipt(node) {
    const base = node.reader.forkProvenance?.historyBase
    if (!base || !node.parent) return null
    const legacy = isLegacyForkProducer(node.parent.reader.header.cliVersion)
    const receipt = { version: legacy ? 2 : 1, ownerGeneration: this.generation(node), generation: this.generation(node.parent), beginOrdinal: node.parent.reader.header.ordinal, endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset, users: node.parent.proof.users, digest: node.parent.proof.digest, unconfirmed: node.parent.proof.unconfirmed, parentId: this.receipt(node.parent)?.id ?? null, overBudget: node.parent.proof.overBudget, ...(legacy ? { userProofMode: FORK_LEGACY_MODE } : {}) }
    receipt.id = receiptIdentity(receipt, node.source.threadId, base.sourceThreadId, node.parent.source.threadId)
    return receipt
  }

  boundary(node) {
    const proof = node.reader.forkProvenance
    if (!proof || node.boundaryDelivered) return null
    return { captureOffset: proof.recordOrdinal, recordStart: proof.recordStart, recordEnd: proof.recordEnd, turnId: this.current.reader.view().observed.turnId ?? this.source.anchorTurnId, kind: 'status', text: '继承历史边界', at: null,
      data: { nativeType: 'session_meta', sourceKind: 'chatgpt-native', sourceForm: 'session_meta/history_base', evidenceKind: 'fork-boundary', history: true, inherited: true, forkThreadId: this.source.threadId, historyOwnerThreadId: node.source.threadId, sourceThreadId: proof.historyBase.sourceThreadId, sourceOwnerThreadId: node.parent.source.threadId, nativeRecordId: nativeRecordIdentity(node.reader.originIdentity ?? node.reader.fileId, node.reader.header, proof.recordStart, node.source.nativeOriginVersion === 2 ? 'session_meta' : 'session_meta/history_base'), ...(node.source.nativeOriginVersion === 2 ? { nativeGenerationKey: nativeStorageKey(node.source.path), nativeRecordStart: proof.recordStart, nativeRecordEnd: proof.recordEnd } : {}), recordOrdinal: proof.recordOrdinal, endOrdinalExclusive: proof.historyBase.endOrdinalExclusive, endByteOffset: proof.historyBase.endByteOffset, forkReceipt: this.receipt(node) } }
  }

  async advance(node, budget, output) {
    if (this.closed || budget.events === 0) return
    if (node.history && !budget.qualified.has(node)) {
      const before = await this.historyMetadata(node.source.historyRecordId, node.metadata)
      budget.qualified.set(node, { recordId: node.source.historyRecordId, metadata: before })
    }
    if (node.history) { await this.producerProject(node.source.cwd); budget.projects.add(node.source.cwd) }
    if (node.history) {
      await this.ancestry(node, budget, output)
      if (!node.resolved) return
      if (node.parent && !node.parent.complete) await this.advance(node.parent, budget, output)
      if (node.parent && !node.parent.complete || budget.events === 0) return
    }
    if (!node.pending.length && !node.complete && readable(budget) > 64 && budget.decoded >= 32768) {
      const view = await this.readNode(node, { ...budget.options, budgetBytes: readable(budget), decodedBudgetBytes: budget.decoded, maxEvents: Math.max(1, budget.events) })
      budget.bytes -= view.readBytes; output.readBytes += view.readBytes; budget.decoded -= view.decodedBytes ?? 0; output.decodedBytes += view.decodedBytes ?? 0
      if (view.coverage === 'gap') { this.error = view.error; throw failure(view.gap) }
      if (view.coldRead) return
      node.pending = [...view.materials.map(value => ({ channel: node.history ? 'historyMaterials' : 'materials', value })), ...view.recentLifecycle.map(value => ({ channel: node.history ? 'historyLifecycle' : 'recentLifecycle', value }))].sort((a, b) => a.value.captureOffset - b.value.captureOffset)
      if (!node.pending.length) { node.ack = node.reader.checkpoint(); node.ackProof = clone(node.proof) }
    }
    await this.ancestry(node, budget, output)
    if (!node.resolved || node.parent && !node.reader.header) return
    if (node.parent) {
      const base = node.reader.forkProvenance?.historyBase, parent = node.parent
      if (!base || parent.source.historyRecordId !== base.sourceThreadId || JSON.stringify(parent.source.historyRange) !== JSON.stringify({ endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset }) || parent.source.historyRecordId !== parent.source.threadId && (!recordOwnerPath(parent.metadata?.record, base.sourceThreadId) || !recordOwnerPath(parent.metadata?.record, base.sourceThreadId, parent.source.path))) throw failure('cursor-invalid')
    }
    if (!node.history && node.parent && !node.parent.complete) await this.advance(node.parent, budget, output)
    if (node.parent && !node.parent.complete) return
    const boundary = this.boundary(node)
    if (boundary && budget.events) { output.historyBoundaries.push(boundary); node.boundaryDelivered = true; budget.events--; this.deliveryThrough.history = Math.max(this.deliveryThrough.history, boundary.captureOffset); this.captureThroughOffset = Math.max(this.captureThroughOffset, boundary.captureOffset) }
    while (node.pending.length && budget.events) {
      const { channel, value } = node.pending.shift()
      if (node.history) {
        if (value.data) Object.assign(value.data, { forkThreadId: this.source.threadId, historyGeneration: this.generation(node) })
        else Object.assign(value, { forkThreadId: this.source.threadId, historyGeneration: this.generation(node) })
        node.proof = foldForkProof(node.proof, value.data ? value : { data: { nativeType: value.type, originRunId: value.originRunId ?? value.turnId, recordOrdinal: value.recordOrdinal, ...(value.turnTrigger ? { turnTrigger: value.turnTrigger } : {}) } })
      } else if (value.data?.nativeType === 'item_completed/UserMessage') {
        value.data.forkReceiptId = this.receipt(node).id; value.data.forkGeneration = this.generation(node)
      }
      output[channel].push(value); budget.events--; this.deliveryThrough.history = Math.max(this.deliveryThrough.history, value.captureOffset); this.captureThroughOffset = Math.max(this.captureThroughOffset, value.captureOffset)
    }
    if (!node.pending.length && (!boundary || node.boundaryDelivered)) {
      node.ack = node.reader.checkpoint(); node.ackProof = clone(node.proof)
      this.deliveryThrough.history = Math.max(this.deliveryThrough.history, node.ack.nextOrdinal === null ? -1 : node.ack.nextOrdinal - 1)
      this.captureThroughOffset = Math.max(this.captureThroughOffset, this.deliveryThrough.history)
      if (node.history && node.coverage === 'caught-up') node.complete = true
    }
  }

  async advanceCurrent(budget, output) {
    const node = this.current
    for (let pass = 0; pass < 2 && !node.pending.length && budget.events > 0 && readable(budget) > 64 && budget.decoded >= 32768; pass++) {
      const position = node.reader.io?.position ?? 0, bytes = budget.bytes
      const view = await this.readNode(node, { ...budget.options, budgetBytes: readable(budget), decodedBudgetBytes: budget.decoded, maxEvents: budget.events })
      budget.bytes -= view.readBytes; output.readBytes += view.readBytes; budget.decoded -= view.decodedBytes ?? 0; output.decodedBytes += view.decodedBytes ?? 0
      if (view.coverage === 'gap') { this.error = view.error; throw failure(view.gap) }
      if (view.coldRead) return
      node.pending = [...view.materials.map(value => ({ channel: 'materials', value })), ...view.recentLifecycle.map(value => ({ channel: 'recentLifecycle', value }))].sort((a, b) => a.value.captureOffset - b.value.captureOffset)
      const io = node.reader.io
      if (!io || io.compressed || io.closed || io.revalidation || io.phase !== 'replaying' || io.position !== position || budget.bytes === bytes || output.materials.length || output.recentLifecycle.length) break
    }
    while (node.pending.length && budget.events) {
      const { channel, value } = node.pending.shift()
      if (value.data?.nativeType === 'item_completed/UserMessage') Object.assign(value.data, { forkHistoryPending: true, forkGeneration: this.generation(node) })
      output[channel].push(value); budget.events--
      this.deliveryThrough.current = Math.max(this.deliveryThrough.current, value.captureOffset)
      this.captureThroughOffset = Math.max(this.captureThroughOffset, value.captureOffset)
    }
    if (!node.pending.length) {
      node.ack = node.reader.checkpoint(); node.ackProof = clone(node.proof)
      this.deliveryThrough.current = Math.max(this.deliveryThrough.current, node.ack.nextOrdinal === null ? -1 : node.ack.nextOrdinal - 1)
      this.captureThroughOffset = Math.max(this.captureThroughOffset, this.deliveryThrough.current)
    }
  }

  view(output = {}, reason = null) {
    const current = this.current.reader.view(), revalidating = this.nodes.some(node => node.reader.io?.revalidation), waiting = revalidating || this.budgetWaiting
    const currentBatch = Boolean(output.materials?.length || output.recentLifecycle?.length)
    const complete = this.current.resolved && (!this.current.parent || this.current.parent.complete && this.current.boundaryDelivered) && !waiting && !this.historyGap && (!currentBatch || this.historyVerified)
    const currentWaiting = this.current.reader.io?.revalidation || this.current.pending.length || this.currentGuardedOutput
    const gap = reason ?? (this.closed ? 'reader-closed' : this.gap)
    const keys = new Set(CHANNELS.flatMap(channel => output[channel] ?? []).map(event => event.data?.nativeGenerationKey ?? event.nativeGenerationKey).filter(Boolean))
    const nodes = currentBatch ? this.nodes.filter(node => node === this.current || keys.has(nativeStorageKey(node.source.path))) : this.nodes
    return { ...current, ...output, materials: output.materials ?? [], recentLifecycle: output.recentLifecycle ?? [], historyMaterials: output.historyMaterials ?? [], historyLifecycle: output.historyLifecycle ?? [], historyBoundaries: output.historyBoundaries ?? [], readBytes: output.readBytes ?? 0, decodedBytes: output.decodedBytes ?? 0,
      nativeOrigins: !currentBatch && waiting ? [] : [...new Map(nodes.map(node => node.reader.io?.proof() ?? node.originProof).filter(proof => proof?.complete).map(proof => [proof.key, proof])).values()],
      metrics: { ...current.metrics, physicalBytes: this.physicalBytes + (output.readBytes ?? 0), maximumReadBytes: Math.max(this.maximumReadBytes, output.readBytes ?? 0) },
      coverage: gap ? 'gap' : complete && !this.current.pending.length ? current.coverage : 'catching-up', gap: gap ?? current.gap,
      currentCoverage: gap ? 'gap' : currentWaiting ? 'catching-up' : current.coverage,
      effectivePhase: !gap && !currentWaiting ? current.effectivePhase : 'unknown', captureThroughOffset: Math.max(this.currentGuardedCheckpoint?.deliveryThrough.current ?? this.deliveryThrough.current, this.guardedCheckpoint?.deliveryThrough.history ?? this.deliveryThrough.history),
      deliveryThrough: { current: this.currentGuardedCheckpoint?.deliveryThrough.current ?? this.deliveryThrough.current, history: this.guardedCheckpoint?.deliveryThrough.history ?? this.deliveryThrough.history },
      forkProvenance: { ...(current.forkProvenance ?? {}), ...(complete && this.current.parent ? { receiptId: this.receipt(this.current).id, generation: this.generation(this.current) } : {}), complete: Boolean(complete && !gap), gap: this.historyGap ?? gap ?? null, ...(this.historyError ? { error: this.historyError } : {}) }, ...(this.error ? { error: this.error } : {}) }
  }

  async guardHeld(node, target, budget, output) {
    if (!node.verifier) {
      const cursor = node.reader.checkpoint(), header = node.reader.header
      await node.reader.close()
      node.reader = new BoundedReader(node.source, cursor, { mode: 'history', origins: this.origins })
      node.verifier = new NativeJsonlSource(node.source, cursor.io, { origins: this.origins, onHeader: () => header })
    }
    const pump = async verifyOnly => {
      let value
      try { value = await node.verifier.pump({ physicalBudget: budget.bytes, decodedBudget: budget.decoded, limit: target.prefix.offset, verifyOnly }) }
      catch (error) {
        budget.bytes -= error.physicalBytes ?? 0; output.readBytes += error.physicalBytes ?? 0
        budget.decoded -= error.decodedBytes ?? 0; output.decodedBytes += error.decodedBytes ?? 0
        if (['ENOENT', 'EACCES', 'EPERM', 'EIO', 'EPIPE', 'ENOTDIR', 'ELOOP', 'EMFILE', 'ENFILE', 'EBADF'].includes(error.code)) {
          this.error = failureErrorInfo({ code: error.code, stage: 'collection' })
          throw failure('history-source-unavailable')
        }
        throw failure(error.code ?? 'record-source-unavailable')
      }
      budget.bytes -= value.physicalBytes; output.readBytes += value.physicalBytes
      budget.decoded -= value.decodedBytes; output.decodedBytes += value.decodedBytes
    }
    await pump(false)
    const range = node.verifier.rangeProof(), proof = node.verifier.proof()
    if (!range || !proof?.complete) return false
    if (JSON.stringify(range) !== JSON.stringify(target)) throw failure('native_origin_prefix_changed')
    await pump(true)
    if (!node.verifier.proof()?.complete) return false
    node.originProof = node.verifier.proof()
    const cursor = node.reader.checkpoint()
    node.reader.ioCheckpoint = node.verifier.checkpoint(cursor.offset)
    await node.verifier.close(); node.verifier = null
    return true
  }

  async pump({ budgetBytes = 262144, ...options } = {}) {
    if (!Number.isSafeInteger(budgetBytes) || budgetBytes < 1 || budgetBytes > 262144) throw new Error('invalid read budget')
    if (this.closed || this.gap) return this.view()
    if (options.available === false) { this.temporarilyUnavailable = true; return this.view({}, 'source-unavailable') }
    if (options.hostRunId !== undefined && options.hostRunId !== this.source.hostRunId) { this.gap = 'source-generation-changed'; return this.view() }
    if (options.profile && ['appVersion', 'appBuild', 'cliVersion'].some(key => options.profile[key] !== this.source.profile[key])) { this.gap = 'unsupported-host-version'; return this.view() }
    if (this.busy) return { ...this.view(), coverage: 'catching-up', effectivePhase: 'unknown', gap: 'reader-busy' }
    this.busy = (async () => {
      const previous = this.checkpoint()
      const output = { materials: [], recentLifecycle: [], historyMaterials: [], historyLifecycle: [], historyBoundaries: [], readBytes: 0, decodedBytes: 0 }
      const budget = { bytes: budgetBytes, decoded: 262144, events: 16, options, qualified: new Map(), projects: new Set(), phase: new Map(), calls: 0, owner: this }
      this.budgetWaiting = false; this.currentFailure = false; this.historyVerified = false
      const holdOutput = () => {
        this.guardedCheckpoint ??= this.replayCheckpoint(previous)
        const history = ['historyMaterials', 'historyLifecycle', 'historyBoundaries']
        if (history.some(channel => output[channel].length)) {
          this.guardedOutput = Object.fromEntries(CHANNELS.map(channel => [channel, history.includes(channel) ? output[channel] : []]))
          for (const channel of history) output[channel] = []
        }
      }
      const holdCurrent = () => {
        this.currentGuardedCheckpoint ??= previous
        if (output.materials.length || output.recentLifecycle.length || !this.currentGuardedOutput) this.currentGuardedOutput = { materials: output.materials, recentLifecycle: output.recentLifecycle }
        output.materials = []; output.recentLifecycle = []
        return this.view(output)
      }
      const finishCurrent = async () => {
        if (budget.bytes < 64 || budget.decoded < 1) return holdCurrent()
        const checked = await this.readNode(this.current, { ...options, budgetBytes: budget.bytes, decodedBudgetBytes: budget.decoded, verifyOnly: true })
        budget.bytes -= checked.readBytes; output.readBytes += checked.readBytes; budget.decoded -= checked.decodedBytes ?? 0; output.decodedBytes += checked.decodedBytes ?? 0
        if (checked.coverage === 'gap') { this.error = checked.error; throw failure(checked.gap) }
        if (this.current.reader.io?.revalidation) return holdCurrent()
        return this.view(output)
      }
      const waitForBudget = async () => { holdOutput(); this.budgetWaiting = true; return await finishCurrent() }
      this.budget = budget
      try {
        if (this.historyRetry) {
          const cursor = this.checkpoint(), replacement = new ForkHistoryReader(this.source, cursor, { native: this.native, sessionsRoot: this.sessionsRoot, origins: this.origins })
          if (replacement.gap) throw failure(replacement.gap)
          for (const node of this.nodes) { if (node.history) await node.reader.close(); await node.verifier?.close(); await node.resolution?.probe?.close() }
          Object.assign(this.current, { parent: replacement.current.parent, resolved: replacement.current.resolved, boundaryDelivered: replacement.current.boundaryDelivered, resolution: replacement.current.resolution })
          this.nodes = [this.current, ...replacement.nodes.slice(1)]
          this.deliveryThrough.history = cursor.deliveryThrough.history
          this.historyGap = null; this.historyError = null; this.historyRetry = false; this.guardedCheckpoint = null; this.guardedOutput = null
        }
        if (this.temporarilyUnavailable) {
          for (const node of this.nodes) {
            const position = this.position(node, node.ack)
            await node.reader.close(); await node.verifier?.close(); node.verifier = null; await node.resolution?.probe?.close()
            node.reader = new BoundedReader(node.source, position.fileId ? position : null, { mode: node.history ? 'history' : 'material', forkSuffix: !node.history, origins: this.origins })
            node.pending = []; node.proof = clone(node.ackProof); node.resolution = null
          }
          this.temporarilyUnavailable = false
        }
        if (!this.guardedOutput) this.guardedTargets = new Map()
        const historyCount = this.guardedOutput ? ['historyMaterials', 'historyLifecycle', 'historyBoundaries'].reduce((sum, channel) => sum + this.guardedOutput[channel].length, 0) : 0
        budget.events -= historyCount
        let restoredCurrent = false
        if (this.currentGuardedOutput) {
          const count = this.currentGuardedOutput.materials.length + this.currentGuardedOutput.recentLifecycle.length
          if (count <= budget.events) {
            output.materials = this.currentGuardedOutput.materials; output.recentLifecycle = this.currentGuardedOutput.recentLifecycle
            budget.events -= count; restoredCurrent = true
          }
        } else await this.advanceCurrent(budget, output)
        if (!output.materials.length && !output.recentLifecycle.length && this.current.reader.io?.revalidation && readable(budget) >= 64 && budget.decoded > 0) {
          const checked = await this.readNode(this.current, { ...options, budgetBytes: readable(budget), decodedBudgetBytes: budget.decoded, verifyOnly: true })
          budget.bytes -= checked.readBytes; output.readBytes += checked.readBytes; budget.decoded -= checked.decodedBytes ?? 0; output.decodedBytes += checked.decodedBytes ?? 0
          if (checked.coverage === 'gap') { this.error = checked.error; throw failure(checked.gap) }
          if (!this.current.reader.io?.revalidation) await this.advanceCurrent(budget, output)
        }
        if (output.materials.length || output.recentLifecycle.length || restoredCurrent) {
          if (readable(budget) < 64 || budget.decoded < 1) return holdCurrent()
          const checked = await this.readNode(this.current, { ...options, budgetBytes: readable(budget), decodedBudgetBytes: budget.decoded, verifyOnly: true })
          budget.bytes -= checked.readBytes; output.readBytes += checked.readBytes; budget.decoded -= checked.decodedBytes ?? 0; output.decodedBytes += checked.decodedBytes ?? 0
          if (checked.coverage === 'gap') { this.error = checked.error; throw failure(checked.gap) }
          if (this.current.reader.io?.revalidation) return holdCurrent()
          this.currentGuardedOutput = null; this.currentGuardedCheckpoint = null
        }
        if (this.current.reader.io?.revalidation || this.current.reader.view().coldRead || this.historyGap) return this.view(output)
        this.historyPhase = true
        for (const node of this.nodes.filter(node => node.history)) {
          if (!await this.qualifiedHistory(node, budget)) continue
          await this.localFile(node.source.path)
          await this.producerProject(node.source.cwd); budget.projects.add(node.source.cwd)
          if ((node.complete || node.reader.io?.revalidation) && !node.verifier && (!this.guardedTargets?.has(node) || node.reader.io?.revalidation) && budget.bytes > 64) {
            const checked = await this.readNode(node, { ...options, budgetBytes: budget.bytes, decodedBudgetBytes: budget.decoded, verifyOnly: true })
            budget.bytes -= checked.readBytes; output.readBytes += checked.readBytes; budget.decoded -= checked.decodedBytes ?? 0; output.decodedBytes += checked.decodedBytes ?? 0
            if (checked.coverage === 'gap') { this.error = checked.error; throw failure(checked.gap) }
          }
        }
        if (readable(budget) < 1 || budget.decoded < 1) return await waitForBudget()
        if (this.current.reader.io?.revalidation) {
          const checked = await this.readNode(this.current, { ...options, budgetBytes: readable(budget), decodedBudgetBytes: budget.decoded, verifyOnly: true })
          budget.bytes -= checked.readBytes; output.readBytes += checked.readBytes; budget.decoded -= checked.decodedBytes ?? 0; output.decodedBytes += checked.decodedBytes ?? 0
          if (checked.coverage === 'gap') { this.error = checked.error; throw failure(checked.gap) }
        }
        if (this.nodes.some(node => node.reader.io?.revalidation)) return await finishCurrent()
        if (this.guardedOutput) {
          const history = ['historyMaterials', 'historyLifecycle', 'historyBoundaries']
          for (const channel of history) output[channel] = this.guardedOutput[channel]
          this.guardedOutput = null
        }
        else {
          await this.ancestry(this.current, budget, output)
          if (this.current.resolved && this.current.parent && !this.current.parent.complete) await this.advance(this.current.parent, budget, output)
          if (this.current.resolved && (!this.current.parent || this.current.parent.complete)) {
            const boundary = this.boundary(this.current)
            if (boundary && budget.events) {
              output.historyBoundaries.push(boundary); this.current.boundaryDelivered = true; budget.events--
              this.deliveryThrough.history = Math.max(this.deliveryThrough.history, boundary.captureOffset)
              this.captureThroughOffset = Math.max(this.captureThroughOffset, boundary.captureOffset)
            }
          }
        }
        for (const channel of ['historyMaterials', 'historyLifecycle', 'historyBoundaries']) for (const event of output[channel]) {
          const key = event.data?.nativeGenerationKey ?? event.nativeGenerationKey
          const node = this.nodes.find(value => value.history && nativeStorageKey(value.source.path) === key)
          const origin = node?.replayTarget ?? node?.originProof
          if (node && !this.guardedTargets.has(node) && origin?.complete) this.guardedTargets.set(node, { key, headerHash: origin.headerHash, prefix: clone(origin.next) })
        }
        budget.phase = new Map()
        for (const value of budget.qualified.values()) await this.historyMetadata(value.recordId, value.metadata)
        for (const cwd of budget.projects) await this.producerProject(cwd)
        const guards = this.nodes.filter(node => node.history && !node.codecPending && (node.reader.handle || node.reader.header))
        for (const [index, node] of guards.entries()) {
          const guardBudget = budget.bytes - 64 * (guards.length - index)
          if (guardBudget < 64 || budget.decoded < 1) return await waitForBudget()
          if (this.guardedTargets.has(node)) {
            if (!await this.guardHeld(node, this.guardedTargets.get(node), budget, output)) return await waitForBudget()
            continue
          }
          const guarded = await this.readNode(node, { ...options, budgetBytes: guardBudget, decodedBudgetBytes: budget.decoded, verifyOnly: true })
          budget.bytes -= guarded.readBytes; output.readBytes += guarded.readBytes; budget.decoded -= guarded.decodedBytes ?? 0; output.decodedBytes += guarded.decodedBytes ?? 0
          if (guarded.coverage === 'gap') { this.error = guarded.error; throw failure(guarded.gap) }
        }
        if (this.nodes.some(node => node.reader.io?.revalidation)) holdOutput()
        else if (!this.guardedOutput) { this.guardedCheckpoint = null; this.historyVerified = true }
        if (this.closed) return this.view({ readBytes: output.readBytes })
        return await finishCurrent()
      } catch (error) {
        if (this.closed) return this.view({ readBytes: output.readBytes, decodedBytes: output.decodedBytes })
        const reason = error?.[GAP] ? error.code.split(':')[0] : 'history-source-unavailable'
        if (this.historyPhase && !this.currentFailure) {
          this.historyGap = reason; this.historyError = this.error ?? (!error?.[GAP] || HISTORY_FAILURES.has(reason) ? failureErrorInfo({ code: error?.[GAP] ? reason : error?.code ?? 'metadata-unavailable', stage: 'collection' }) : null); this.error = null
          this.historyRetry = ['history-source-unavailable', 'history-enablement-unconfirmed', 'history-project-disabled'].includes(reason)
          this.guardedCheckpoint ??= this.replayCheckpoint(previous); this.guardedOutput = null
          if (!this.historyRetry) {
            for (const node of this.nodes) {
              if (node.history) { await node.reader.close(); await node.verifier?.close(); node.verifier = null; node.pending = [] }
              await node.resolution?.probe?.close()
            }
          }
          output.historyMaterials = []; output.historyLifecycle = []; output.historyBoundaries = []
          try { return await finishCurrent() }
          catch (currentError) {
            this.gap = currentError?.[GAP] ? currentError.code.split(':')[0] : 'record-source-unavailable'
            this.error = failureErrorInfo({ code: currentError?.code ?? 'record-source-unavailable', stage: 'collection' })
            return this.view({ readBytes: output.readBytes, decodedBytes: output.decodedBytes })
          }
        }
        this.gap = reason
        if (!error?.[GAP]) this.error = failureErrorInfo({ code: error?.code ?? 'metadata-unavailable', stage: error?.stage ?? 'collection' })
        return this.view({ readBytes: output.readBytes, decodedBytes: output.decodedBytes })
      } finally { this.metadataCalls = budget.calls; this.maximumMetadataCalls = Math.max(this.maximumMetadataCalls ?? 0, budget.calls); this.budget = null; this.historyPhase = false; this.physicalBytes += output.readBytes; this.maximumReadBytes = Math.max(this.maximumReadBytes, output.readBytes) }
    })().finally(() => { this.busy = null })
    return this.busy
  }

  acknowledgeOrigins(fences) { for (const node of this.nodes) { node.reader.acknowledgeOrigins(fences); const fence = fences?.find(value => value.key === node.originProof?.key); if (fence && node.replayTarget && fence.prefix.offset >= node.replayTarget.next.offset) node.replayTarget = null; if (fence) node.originProof = { key: fence.key, headerHash: fence.headerHash, revision: fence.revision, base: fence.prefix, verifiedBase: fence.prefix, next: fence.prefix, complete: true } } }

  checkpoint() {
    const frames = this.nodes.map(node => ({ source: node.source, reader: this.position(node, node.ack), resolved: node.resolved, boundaryDelivered: node.boundaryDelivered, complete: node.complete, metadata: node.metadata, ...(node.originProof ? { originProof: node.replayTarget ?? node.originProof } : {}), proof: clone(node.ackProof) }))
    const resolving = this.nodes.findIndex(node => node.resolution)
    const resolution = resolving < 0 ? null : (() => { const { probe, seenFiles, ...value } = this.nodes[resolving].resolution; return { ...clone(value), frame: resolving, seenFiles: [...seenFiles], probe: probe ? { source: probe.source, reader: probe.checkpoint() } : null } })()
    const value = { format: 4, kind: this.source.nativeOriginVersion === 2 ? 'fork-lineage/2' : 'fork-lineage/1', receiptVersion: 2, captureThroughOffset: this.captureThroughOffset, deliveryThrough: clone(this.deliveryThrough), ...(this.historyGap && !this.historyRetry ? { historyGap: this.historyGap } : {}), frames, ...(resolution ? { resolution } : {}) }
    if (!this.guardedCheckpoint && !this.currentGuardedCheckpoint) return value
    const guarded = clone(this.guardedCheckpoint ?? value), current = this.currentGuardedCheckpoint ?? value
    guarded.frames[0] = { ...guarded.frames[0], reader: clone(current.frames[0].reader), proof: clone(current.frames[0].proof) }
    guarded.deliveryThrough.current = current.deliveryThrough.current
    guarded.captureThroughOffset = Math.max(guarded.deliveryThrough.current, guarded.deliveryThrough.history)
    if (this.historyGap && !this.historyRetry) guarded.historyGap = this.historyGap
    else delete guarded.historyGap
    return guarded
  }

  replayCheckpoint(previous) {
    const learned = this.checkpoint()
    const merge = (old, next, node) => {
      const cursor = old?.reader ?? new BoundedReader(next.source, null, { mode: node.history ? 'history' : 'material', forkSuffix: !node.history, origins: this.origins }).checkpoint()
      return { ...next, proof: clone(old?.proof ?? emptyForkProof()), complete: old?.complete ?? false, boundaryDelivered: old?.boundaryDelivered ?? false, reader: this.position(node, cursor) }
    }
    return { ...learned, captureThroughOffset: previous?.captureThroughOffset ?? -1, deliveryThrough: clone(previous?.deliveryThrough ?? { current: previous?.captureThroughOffset ?? -1, history: previous?.captureThroughOffset ?? -1 }), frames: learned.frames.map((frame, index) => merge(previous?.frames[index], frame, this.nodes[index])) }
  }

  position(node, cursor) {
    const learned = node.reader.checkpoint()
    return clone({ ...cursor, ...(node.source.nativeOriginVersion === 2 ? { io: learned.io ?? cursor.io } : {}), fileId: cursor.fileId ?? learned.fileId, attachSize: cursor.attachSize ?? learned.attachSize, knownSize: cursor.knownSize == null && learned.knownSize == null ? null : Math.max(cursor.knownSize ?? 0, learned.knownSize ?? 0) })
  }

  diagnostics() { return { readers: this.nodes.reduce((sum, node) => sum + Number(Boolean(node.reader.handle)) + Number(Boolean(node.resolution?.probe?.handle)) + Number(Boolean(node.verifier?.handle)), 0), pendingEvents: this.nodes.reduce((sum, node) => sum + node.pending.length, 0), ancestryNodes: this.nodes.length, metadataCalls: this.metadataCalls ?? 0, maximumMetadataCalls: this.maximumMetadataCalls ?? 0, busy: Boolean(this.busy) } }

  async close() {
    if (this.closing) return this.closing
    this.closed = true
    this.closing = (async () => { await this.busy; await Promise.all(this.nodes.flatMap(node => [node.reader.close(), node.resolution?.probe?.close(), node.verifier?.close()])); for (const node of this.nodes) node.pending = []; this.nodes = []; this.guardedOutput = null; this.guardedCheckpoint = null; this.currentGuardedOutput = null; this.currentGuardedCheckpoint = null; this.guardedTargets = null })()
    return this.closing
  }
}
