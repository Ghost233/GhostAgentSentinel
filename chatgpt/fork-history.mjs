import fs from 'node:fs/promises'
import { basename, dirname, isAbsolute, join, resolve, sep } from 'node:path'
import { BoundedReader } from './bounded-reader.mjs'
import { emptyForkProof, foldForkProof, forkGeneration, receiptIdentity, validForkProof, nativeRecordIdentity, FORK_LINEAGE_FRAMES, FORK_LEGACY_PRODUCER, FORK_LEGACY_MODE } from '../src/fork-proof.mjs'
import { failureErrorInfo } from '../src/core/failure-receipts.mjs'

const MAX_DEPTH = FORK_LINEAGE_FRAMES
const MAX_CANDIDATES = 4, MAX_DIRECTORY_ENTRIES = 8192, MAX_DIRECTORIES = 256
const MAX_METADATA_CALLS = 144
const readable = budget => Math.max(0, budget.bytes - 64 * budget.owner.nodes.length)
const signature = (value, path = true) => JSON.stringify([value?.id, ...(path ? [value?.path] : []), value?.cwd, value?.source, value?.originator, value?.parentThreadId, value?.forkedFromId ?? null])
const GAP = Symbol('fork-gap')
const failure = code => Object.assign(new Error(code), { code, [GAP]: true })
const clone = value => structuredClone(value)
const metadataFields = value => Object.fromEntries(['id', 'path', 'cwd', 'source', 'originator', 'parentThreadId', 'forkedFromId'].filter(key => value[key] !== undefined).map(key => [key, clone(value[key])]))
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const recordOwnerPath = (metadata, recordId, path = metadata?.path) => typeof path === 'string' && uuid.test(recordId) && uuid.test(metadata?.id) && (basename(path).match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g)?.length === 2) && basename(path).endsWith('-' + metadata.id + '_' + recordId + '.jsonl')

// This fixed reader owns fork history, not another scheduler or host sampler.
// Parent App/user/current-project guards remain the adapter's batch fence.
export class ForkHistoryReader {
  constructor(source, cursor = null, { native, sessionsRoot, mode = 'material' } = {}) {
    if (mode !== 'material') throw new Error('fork history requires material mode')
    this.source = source; this.native = native; this.sessionsRoot = resolve(sessionsRoot)
    this.archiveRoot = basename(this.sessionsRoot) === 'sessions' ? join(dirname(this.sessionsRoot), 'archived_sessions') : null
    this.captureThroughOffset = cursor?.captureThroughOffset ?? -1
    const frame = value => value && typeof value === 'object' && value.source && typeof value.source === 'object' && value.reader && typeof value.reader === 'object' && validForkProof(value.proof) && ['resolved', 'boundaryDelivered', 'complete'].every(key => typeof value[key] === 'boolean')
    this.gap = cursor && (cursor.format !== 2 || cursor.kind !== 'fork-lineage/1' || cursor.receiptVersion !== 2 || !Array.isArray(cursor.frames) || cursor.frames.length < 1 || cursor.frames.length > MAX_DEPTH || !cursor.frames.every(frame)) ? 'cursor-invalid' : null
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
    const options = { mode: history ? 'history' : 'material', forkSuffix: !history }
    const fresh = new BoundedReader(source, null, options)
    const reader = saved?.reader && JSON.stringify(saved.reader) !== JSON.stringify(fresh.checkpoint()) ? new BoundedReader(source, saved.reader, options) : fresh
    const node = { source, reader, history, ack: reader.checkpoint(), pending: [], parent: null, resolved: saved?.resolved === true, boundaryDelivered: saved?.boundaryDelivered === true, complete: saved?.complete === true, coverage: saved?.complete ? 'caught-up' : null, metadata: saved?.metadata ?? null, proof: clone(saved?.proof ?? emptyForkProof()), ackProof: clone(saved?.proof ?? emptyForkProof()) }
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
    const view = await node.reader.pump(options)
    node.coverage = view.coverage
    if (node.history && node.reader.handle && node.reader.pendingBytes === 0) {
      const cursor = node.reader.checkpoint(), stat = node.reader.stat
      await node.reader.close()
      node.reader = new BoundedReader(node.source, cursor, { mode: 'history' })
      node.reader.stat = stat
    }
    return view
  }

  restoreResolution(saved) {
    const node = this.nodes[saved.frame]
    if (!Number.isInteger(saved.frame) || !node || node.resolved || !saved.metadata?.record || !saved.metadata?.owner || !Array.isArray(saved.candidates) || saved.candidates.length > MAX_CANDIDATES || !saved.candidates.every(path => typeof path === 'string' && isAbsolute(path)) || !Number.isInteger(saved.index) || saved.index < 0 || saved.index > saved.candidates.length || !Array.isArray(saved.matches) || saved.matches.length > MAX_CANDIDATES || !Array.isArray(saved.seenFiles) || saved.seenFiles.length > MAX_CANDIDATES || !['header', 'prefix', undefined].includes(saved.phase) || saved.probe && (!saved.probe.source || !saved.probe.reader) || saved.phase === 'prefix' && !saved.expectedHeader) { this.gap = 'cursor-invalid'; return }
    const { frame, probe, ...resolution } = saved
    node.resolution = { ...clone(resolution), seenFiles: new Set(saved.seenFiles), probe: probe ? new BoundedReader(probe.source, probe.reader, { mode: saved.phase === 'header' ? 'header' : 'history-metadata', forkSuffix: saved.phase === 'header' }) : null }
  }

  async qualify(id, expected = null, candidate = null) {
    if (this.closed) throw failure('reader-closed')
    const metadata = candidate ?? await this.nativeCall('readThread', id), thread = metadata?.thread
    if (this.closed) throw failure('reader-closed')
    if (!thread || thread.id !== id || thread.parentThreadId !== null || thread.originator !== 'Codex Desktop' || !['vscode', 'cli', 'appServer'].includes(thread.source) || !isAbsolute(thread.cwd ?? '') || !isAbsolute(thread.path ?? '') || thread.environments != null && (!Array.isArray(thread.environments) || thread.environments.some(item => item?.environmentId !== 'local'))) throw failure('history-source-unverified')
    const path = resolve(thread.path)
    if (![this.sessionsRoot, this.archiveRoot].filter(Boolean).some(root => path.startsWith(root + sep)) || !path.endsWith('.jsonl')) throw failure('history-source-unverified')
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
    if (!allowedRoot || !actual.endsWith('.jsonl') || !stat.isFile() || typeof process.getuid === 'function' && stat.uid !== process.getuid()) throw failure('history-source-unverified')
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
          else if (entry.isFile() && (recordId === metadata.id ? entry.name.endsWith('-' + metadata.id + '.jsonl') : recordOwnerPath(metadata, recordId, entry.name))) paths.add(join(path, entry.name))
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
      node.resolution = { metadata, candidates: paths, index: 0, probe: null, matches: [], seenFiles: new Set(), recordId: base.sourceThreadId }
    }
    const resolution = node.resolution
    await this.historyMetadata(base.sourceThreadId, resolution.metadata)
    budget.qualified.set(resolution, { recordId: base.sourceThreadId, metadata: resolution.metadata })
    if (resolution.probe) {
      this.historySource(resolution.probe.source, resolution.metadata.record, base)
      await this.localFile(resolution.probe.source.path)
    }
    while (resolution.index < resolution.candidates.length && readable(budget) > 64) {
      const path = resolution.candidates[resolution.index]
      if (!resolution.probe) {
        const { actual, stat, allowedRoot } = await this.localFile(path)
        if (stat.size < base.endByteOffset) { resolution.index++; continue }
        const fileId = `${stat.dev}:${stat.ino}`
        if (resolution.seenFiles.has(fileId)) { resolution.index++; continue }
        resolution.seenFiles.add(fileId)
        const metadata = resolution.metadata.record
        if (base.sourceThreadId !== metadata.id && (!recordOwnerPath(metadata, base.sourceThreadId) || !recordOwnerPath(metadata, base.sourceThreadId, actual))) throw failure('history-source-unverified')
        const source = { ...this.source, path: actual, allowedRoot, threadId: metadata.id, historyRecordId: base.sourceThreadId, cwd: metadata.cwd, sourceKind: metadata.source, originator: metadata.originator, forkedFromId: metadata.forkedFromId ?? null, anchorTurnId: null, historyRange: { endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset } }
        resolution.probe = new BoundedReader(source, null, { mode: 'header', forkSuffix: true }); resolution.phase = 'header'
      }
      if (resolution.phase === 'prefix') { await this.producerProject(resolution.probe.source.cwd); budget.projects.add(resolution.probe.source.cwd) }
      const probe = resolution.probe, view = await probe.pump({ ...budget.options, budgetBytes: resolution.phase === 'header' ? Math.min(4096, readable(budget)) : readable(budget) })
      budget.bytes -= view.readBytes; output.readBytes += view.readBytes
      if (view.error) { this.error = view.error; throw failure(view.gap) }
      if (resolution.phase === 'prefix' && view.coverage === 'gap' && ['record-malformed-or-over-limit', 'record-file-replaced', 'record-file-truncated', 'record-rewrite-suspected', 'cursor-anchor-changed', 'invalid-lifecycle-record'].includes(view.gap)) {
        this.error = failureErrorInfo({ code: view.gap, stage: 'collection' }); throw failure(view.gap)
      }
      if (resolution.phase === 'header' && view.header) {
        const header = view.header
        const matches = header.threadId === resolution.metadata.record.id && header.originator === 'Codex Desktop' && header.sourceKind === resolution.metadata.record.source && [this.source.profile.cliVersion, FORK_LEGACY_PRODUCER].includes(header.cliVersion) && header.ordinal < base.endOrdinalExclusive
        if (matches) {
          await this.producerProject(header.cwd); budget.projects.add(header.cwd)
          const source = { ...probe.source, cwd: header.cwd }
          resolution.expectedHeader = { header: clone(header), fileId: probe.checkpoint().fileId }
          await probe.close(); resolution.probe = new BoundedReader(source, null, { mode: 'history-metadata' }); resolution.phase = 'prefix'
          continue
        }
        await probe.close(); resolution.probe = null; resolution.index++; continue
      }
      if (resolution.phase === 'header' && view.metrics.physicalBytes >= 65536) throw failure('history-header-budget')
      if (view.coverage === 'gap' || view.coverage === 'caught-up') {
        const header = view.header
        if (view.coverage === 'caught-up' && resolution.phase === 'prefix') {
          if (JSON.stringify(header) !== JSON.stringify(resolution.expectedHeader.header) || probe.checkpoint().fileId !== resolution.expectedHeader.fileId) throw failure('history-source-generation-changed')
          resolution.matches.push({ source: { ...probe.source, expectedHeader: clone(header), expectedFileId: probe.checkpoint().fileId }, reader: probe.checkpoint() })
        }
        await probe.close(); resolution.probe = null; resolution.index++
      } else break
    }
    if (resolution.index < resolution.candidates.length) return
    if (resolution.matches.length !== 1) throw failure(resolution.matches.length ? 'history-generation-ambiguous' : 'history-generation-locator-unverified')
    const selected = resolution.matches[0]
    this.historySource(selected.source, resolution.metadata.record, base)
    await this.localFile(selected.source.path)
    if (selected.source.expectedFileId !== selected.reader.fileId || JSON.stringify(selected.source.expectedHeader) !== JSON.stringify(selected.reader.header)) throw failure('cursor-invalid')
    if (this.nodes.some(item => item.source.path === selected.source.path && item.source.historyRange?.endOrdinalExclusive === base.endOrdinalExclusive)) throw failure('fork-history-cycle')
    node.parent = this.node(selected.source, null, true); node.parent.metadata = resolution.metadata; node.resolved = true; node.resolution = null
  }

  generation(node) {
    const reader = node.reader.checkpoint()
    return forkGeneration(reader.fileId, reader.header)
  }

  receipt(node) {
    const base = node.reader.view().forkProvenance?.historyBase
    if (!base || !node.parent) return null
    const legacy = node.parent.reader.header.cliVersion === FORK_LEGACY_PRODUCER
    const receipt = { version: legacy ? 2 : 1, ownerGeneration: this.generation(node), generation: this.generation(node.parent), beginOrdinal: node.parent.reader.header.ordinal, endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset, users: node.parent.proof.users, digest: node.parent.proof.digest, unconfirmed: node.parent.proof.unconfirmed, parentId: this.receipt(node.parent)?.id ?? null, overBudget: node.parent.proof.overBudget, ...(legacy ? { userProofMode: FORK_LEGACY_MODE } : {}) }
    receipt.id = receiptIdentity(receipt, node.source.threadId, base.sourceThreadId, node.parent.source.threadId)
    return receipt
  }

  boundary(node) {
    const proof = node.reader.view().forkProvenance
    if (!proof || node.boundaryDelivered) return null
    return { captureOffset: proof.recordOrdinal, recordStart: proof.recordStart, recordEnd: proof.recordEnd, turnId: this.current.reader.view().observed.turnId ?? this.source.anchorTurnId, kind: 'status', text: '继承历史边界', at: null,
      data: { nativeType: 'session_meta', sourceKind: 'chatgpt-native', sourceForm: 'session_meta/history_base', evidenceKind: 'fork-boundary', history: true, inherited: true, forkThreadId: this.source.threadId, historyOwnerThreadId: node.source.threadId, sourceThreadId: proof.historyBase.sourceThreadId, sourceOwnerThreadId: node.parent.source.threadId, nativeRecordId: nativeRecordIdentity(node.reader.fileId, node.reader.header, proof.recordStart, 'session_meta/history_base'), recordOrdinal: proof.recordOrdinal, endOrdinalExclusive: proof.historyBase.endOrdinalExclusive, endByteOffset: proof.historyBase.endByteOffset, forkReceipt: this.receipt(node) } }
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
    if (!node.pending.length && !node.complete && readable(budget) > 64) {
      const view = await this.readNode(node, { ...budget.options, budgetBytes: readable(budget), maxEvents: Math.max(1, budget.events) })
      budget.bytes -= view.readBytes; output.readBytes += view.readBytes
      if (view.coverage === 'gap') { this.error = view.error; throw failure(view.gap) }
      node.pending = [...view.materials.map(value => ({ channel: node.history ? 'historyMaterials' : 'materials', value })), ...view.recentLifecycle.map(value => ({ channel: node.history ? 'historyLifecycle' : 'recentLifecycle', value }))].sort((a, b) => a.value.captureOffset - b.value.captureOffset)
      if (!node.pending.length) { node.ack = node.reader.checkpoint(); node.ackProof = clone(node.proof) }
    }
    await this.ancestry(node, budget, output)
    if (!node.resolved) return
    if (node.parent) {
      const base = node.reader.view().forkProvenance?.historyBase, parent = node.parent
      if (!base || parent.source.historyRecordId !== base.sourceThreadId || JSON.stringify(parent.source.historyRange) !== JSON.stringify({ endOrdinalExclusive: base.endOrdinalExclusive, endByteOffset: base.endByteOffset }) || parent.source.historyRecordId !== parent.source.threadId && (!recordOwnerPath(parent.metadata?.record, base.sourceThreadId) || !recordOwnerPath(parent.metadata?.record, base.sourceThreadId, parent.source.path))) throw failure('cursor-invalid')
    }
    if (!node.history && node.parent && !node.parent.complete) await this.advance(node.parent, budget, output)
    if (node.parent && !node.parent.complete) return
    const boundary = this.boundary(node)
    if (boundary && budget.events) { output.historyBoundaries.push(boundary); node.boundaryDelivered = true; budget.events--; this.captureThroughOffset = Math.max(this.captureThroughOffset, boundary.captureOffset) }
    while (node.pending.length && budget.events) {
      const { channel, value } = node.pending.shift()
      if (node.history) {
        if (value.data) Object.assign(value.data, { forkThreadId: this.source.threadId, historyGeneration: this.generation(node) })
        else Object.assign(value, { forkThreadId: this.source.threadId, historyGeneration: this.generation(node) })
        node.proof = foldForkProof(node.proof, value.data ? value : { data: { nativeType: value.type, originRunId: value.originRunId ?? value.turnId, recordOrdinal: value.recordOrdinal, ...(value.turnTrigger ? { turnTrigger: value.turnTrigger } : {}) } })
      } else if (value.data?.nativeType === 'item_completed/UserMessage') {
        value.data.forkReceiptId = this.receipt(node).id; value.data.forkGeneration = this.generation(node)
      }
      output[channel].push(value); budget.events--; this.captureThroughOffset = Math.max(this.captureThroughOffset, value.captureOffset)
    }
    if (!node.pending.length && (!boundary || node.boundaryDelivered)) {
      node.ack = node.reader.checkpoint(); node.ackProof = clone(node.proof)
      this.captureThroughOffset = Math.max(this.captureThroughOffset, node.ack.nextOrdinal === null ? -1 : node.ack.nextOrdinal - 1)
      if (node.history && node.coverage === 'caught-up') node.complete = true
    }
  }

  view(output = {}, reason = null) {
    const current = this.current.reader.view(), complete = this.current.resolved && (!this.current.parent || this.current.parent.complete)
    const gap = reason ?? (this.closed ? 'reader-closed' : this.gap)
    return { ...current, ...output, materials: output.materials ?? [], recentLifecycle: output.recentLifecycle ?? [], historyMaterials: output.historyMaterials ?? [], historyLifecycle: output.historyLifecycle ?? [], historyBoundaries: output.historyBoundaries ?? [], readBytes: output.readBytes ?? 0,
      metrics: { ...current.metrics, physicalBytes: this.physicalBytes + (output.readBytes ?? 0), maximumReadBytes: Math.max(this.maximumReadBytes, output.readBytes ?? 0) },
      coverage: gap ? 'gap' : complete && !this.current.pending.length ? current.coverage : 'catching-up', gap: gap ?? (complete ? current.gap : null), effectivePhase: !gap && complete && !this.current.pending.length ? current.effectivePhase : 'unknown', captureThroughOffset: this.captureThroughOffset,
      forkProvenance: { ...(current.forkProvenance ?? {}), complete: Boolean(complete && !gap), gap: gap ?? null }, ...(this.error ? { error: this.error } : {}) }
  }

  async pump({ budgetBytes = 262144, ...options } = {}) {
    if (!Number.isSafeInteger(budgetBytes) || budgetBytes < 1 || budgetBytes > 262144) throw new Error('invalid read budget')
    if (this.closed || this.gap) return this.view()
    if (options.available === false) { this.temporarilyUnavailable = true; return this.view({}, 'source-unavailable') }
    if (options.hostRunId !== undefined && options.hostRunId !== this.source.hostRunId) { this.gap = 'source-generation-changed'; return this.view() }
    if (options.profile && ['appVersion', 'appBuild', 'cliVersion'].some(key => options.profile[key] !== this.source.profile[key])) { this.gap = 'unsupported-host-version'; return this.view() }
    if (this.busy) return { ...this.view(), coverage: 'catching-up', effectivePhase: 'unknown', gap: 'reader-busy' }
    this.busy = (async () => {
      const output = { materials: [], recentLifecycle: [], historyMaterials: [], historyLifecycle: [], historyBoundaries: [], readBytes: 0 }
      const budget = { bytes: budgetBytes, events: 16, options, qualified: new Map(), projects: new Set(), phase: new Map(), calls: 0, owner: this }
      this.budget = budget
      try {
        if (this.temporarilyUnavailable) {
          for (const node of this.nodes) {
            const position = this.position(node, node.ack)
            await node.reader.close(); await node.resolution?.probe?.close()
            node.reader = new BoundedReader(node.source, position.fileId ? position : null, { mode: node.history ? 'history' : 'material', forkSuffix: !node.history })
            node.pending = []; node.proof = clone(node.ackProof); node.resolution = null
          }
          this.temporarilyUnavailable = false
        }
        for (const node of this.nodes.filter(node => node.history)) {
          budget.qualified.set(node, { recordId: node.source.historyRecordId, metadata: await this.historyMetadata(node.source.historyRecordId, node.metadata) })
          await this.localFile(node.source.path)
          await this.producerProject(node.source.cwd); budget.projects.add(node.source.cwd)
          if (node.complete && budget.bytes > 64) {
            const checked = await this.readNode(node, { ...options, budgetBytes: budget.bytes, verifyOnly: true })
            budget.bytes -= checked.readBytes; output.readBytes += checked.readBytes
            if (checked.coverage === 'gap') { this.error = checked.error; throw failure(checked.gap) }
          }
        }
        await this.advance(this.current, budget, output)
        budget.phase = new Map()
        for (const value of budget.qualified.values()) await this.historyMetadata(value.recordId, value.metadata)
        for (const cwd of budget.projects) await this.producerProject(cwd)
        for (const node of this.nodes.filter(node => node.reader.handle || node.history && node.reader.fileId)) {
          if (budget.bytes < 64) throw failure('history-guard-budget')
          const guarded = await this.readNode(node, { ...options, budgetBytes: budget.bytes, verifyOnly: true })
          budget.bytes -= guarded.readBytes; output.readBytes += guarded.readBytes
          if (guarded.coverage === 'gap') { this.error = guarded.error; throw failure(guarded.gap) }
        }
        if (this.closed) return this.view({ readBytes: output.readBytes })
        return this.view(output)
      } catch (error) {
        this.gap = error?.[GAP] ? error.code.split(':')[0] : 'history-source-unavailable'
        if (!error?.[GAP]) this.error = failureErrorInfo({ code: error?.code ?? 'metadata-unavailable', stage: error?.stage ?? 'collection' })
        return this.view({ readBytes: output.readBytes })
      } finally { this.metadataCalls = budget.calls; this.maximumMetadataCalls = Math.max(this.maximumMetadataCalls ?? 0, budget.calls); this.budget = null; this.physicalBytes += output.readBytes; this.maximumReadBytes = Math.max(this.maximumReadBytes, output.readBytes) }
    })().finally(() => { this.busy = null })
    return this.busy
  }

  checkpoint() {
    const frames = this.nodes.map(node => ({ source: node.source, reader: this.position(node, node.ack), resolved: node.resolved, boundaryDelivered: node.boundaryDelivered, complete: node.complete, metadata: node.metadata, proof: clone(node.ackProof) }))
    const resolving = this.nodes.findIndex(node => node.resolution)
    const resolution = resolving < 0 ? null : (() => { const { probe, seenFiles, ...value } = this.nodes[resolving].resolution; return { ...clone(value), frame: resolving, seenFiles: [...seenFiles], probe: probe ? { source: probe.source, reader: probe.checkpoint() } : null } })()
    return { format: 2, kind: 'fork-lineage/1', receiptVersion: 2, captureThroughOffset: this.captureThroughOffset, frames, ...(resolution ? { resolution } : {}) }
  }

  replayCheckpoint(previous) {
    const learned = this.checkpoint()
    const merge = (old, next, node) => {
      const cursor = old?.reader ?? new BoundedReader(next.source, null, { mode: node.history ? 'history' : 'material', forkSuffix: !node.history }).checkpoint()
      return { ...next, proof: clone(old?.proof ?? emptyForkProof()), complete: old?.complete ?? false, boundaryDelivered: old?.boundaryDelivered ?? false, reader: this.position(node, cursor) }
    }
    return { ...learned, captureThroughOffset: previous?.captureThroughOffset ?? -1, frames: learned.frames.map((frame, index) => merge(previous?.frames[index], frame, this.nodes[index])) }
  }

  position(node, cursor) {
    const learned = node.reader.checkpoint()
    return clone({ ...cursor, fileId: cursor.fileId ?? learned.fileId, attachSize: cursor.attachSize ?? learned.attachSize, knownSize: cursor.knownSize == null && learned.knownSize == null ? null : Math.max(cursor.knownSize ?? 0, learned.knownSize ?? 0) })
  }

  diagnostics() { return { readers: this.nodes.reduce((sum, node) => sum + Number(Boolean(node.reader.handle)) + Number(Boolean(node.resolution?.probe?.handle)), 0), pendingEvents: this.nodes.reduce((sum, node) => sum + node.pending.length, 0), ancestryNodes: this.nodes.length, metadataCalls: this.metadataCalls ?? 0, maximumMetadataCalls: this.maximumMetadataCalls ?? 0, busy: Boolean(this.busy) } }

  async close() {
    if (this.closing) return this.closing
    this.closed = true
    this.closing = (async () => { await this.busy; await Promise.all(this.nodes.flatMap(node => [node.reader.close(), node.resolution?.probe?.close()])); for (const node of this.nodes) node.pending = []; this.nodes = [] })()
    return this.closing
  }
}
