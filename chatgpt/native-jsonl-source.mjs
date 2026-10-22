import fs from 'node:fs/promises'
import { constants as fileConstants } from 'node:fs'
import { basename, resolve, sep } from 'node:path'
import { createHash } from 'node:crypto'
import { createZstdDecompress, constants } from 'node:zlib'
import { NativePrefix, headerIdentity } from '../src/native-origin.mjs'

const leases = new Set()
const CHUNK = 16384, RESERVE = CHUNK * 2, LIMIT = 262144
const fail = code => Object.assign(new Error(code), { code })
const hash = text => createHash('sha256').update(text).digest('hex')
export const nativeStorageKey = path => hash(JSON.stringify(['native-storage/2', basename(path).replace(/\.jsonl\.zst$/, '.jsonl')]))
const signature = stat => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`

function frame(bytes) {
  if (bytes.length < 6 || bytes.readUInt32LE(0) !== 0xfd2fb528) throw fail('compressed-frame-unverified')
  const descriptor = bytes[4], single = Boolean(descriptor & 32), flag = descriptor >>> 6
  if (descriptor & 24 || descriptor & 3) throw fail('compressed-frame-unverified')
  let at = 5, window = null
  if (!single) { const value = bytes[at++], base = 2 ** (10 + (value >>> 3)); window = base + base / 8 * (value & 7) }
  const length = flag === 0 ? single ? 1 : 0 : flag === 1 ? 2 : flag === 2 ? 4 : 8
  if (!length || at + length > bytes.length) throw fail('compressed-frame-unverified')
  const size = length === 8 ? Number(bytes.readBigUInt64LE(at)) : bytes.readUIntLE(at, length) + (flag === 1 ? 256 : 0)
  if (!Number.isSafeInteger(size) || size < 1 || (window ?? size) > 8388608) throw fail('compressed-window-unverified')
  return { size, window: window ?? size }
}

// One fixed IO seam. JSONL projection, ancestry and observation policy stay in
// their existing owners; no plaintext file or decoder state is persisted.
export class NativeJsonlSource {
  constructor(source, checkpoint = null, { validateFrame = true, origins = null, onHeader = null } = {}) {
    this.source = source; this.origins = origins; this.onHeader = onHeader; this.headerBytes = Buffer.alloc(0); this.prefix = new NativePrefix();
    this.compressed = source.path.endsWith('.jsonl.zst')
    this.terminal = source.terminalProof ?? null; this.terminalVerified = !this.terminal
    this.key = nativeStorageKey(source.path); this.validateFrame = validateFrame
    this.resume = checkpoint?.offset ?? 0; this.position = 0; this.phase = this.compressed && validateFrame ? 'validating' : 'replaying'
    this.saved = checkpoint; this.handle = null; this.decoder = null; this.pending = null; this.deviceRemapped = false
    this.rawOffset = 0; this.removed = 0; this.accounted = 0; this.pendingWrite = false; this.inputEnded = false
    this.totalPhysical = 0; this.totalDecoded = 0; this.peakBuffered = 0; this.closed = false
    this.waiters = new Set()
  }
  wake() { for (const resolve of this.waiters) resolve(); this.waiters.clear() }
  async wait() { if (this.closed || this.error || this.decoder?.readableLength || !this.pendingWrite && !this.inputEnded || this.decoder?.readableEnded) return; await new Promise(resolve => this.waiters.add(resolve)) }
  decoderOpen() {
    if (!leases.has(this) && leases.size >= 4) return false
    leases.add(this)
    this.decoder = createZstdDecompress({ chunkSize: CHUNK, readableHighWaterMark: CHUNK, writableHighWaterMark: CHUNK, rejectGarbageAfterEnd: true, params: { [constants.ZSTD_d_windowLogMax]: 23 } })
    this.decoder.on('error', error => { this.error = fail('compressed-data-invalid'); this.error.cause = error; this.wake() })
    this.decoder.on('readable', () => this.wake()); this.decoder.on('end', () => this.wake()); this.decoder.on('close', () => this.wake())
    this.rawOffset = 0; this.removed = 0; this.accounted = 0; this.pendingWrite = false; this.inputEnded = false
  }
  async open() {
    let path = this.source.path
    try { await fs.lstat(path) }
    catch (error) {
      if (error.code !== 'ENOENT' || !path.endsWith('.jsonl')) throw error
      path += '.zst'
    }
    const actual = await fs.realpath(path), root = await fs.realpath(this.source.allowedRoot)
    if (!actual.startsWith(root + sep) || !/\.jsonl(?:\.zst)?$/.test(actual)) throw fail('record-path-unverified')
    this.path = path; this.compressed = path.endsWith('.jsonl.zst')
    if (this.compressed && !leases.has(this) && leases.size >= 4) { this.phase = 'queued'; return false }
    this.expected = this.origins ? await this.origins.read(this.key) : null
    if (this.closed) throw fail('reader-closed')
    this.phase = this.compressed && this.validateFrame || !this.compressed && this.expected ? 'validating' : 'replaying'
    this.baseVerified = !this.expected
    if (!this.compressed && this.expected) this.phase = 'validating'
    const stat = await fs.lstat(actual)
    if (!stat.isFile() || typeof process.getuid === 'function' && stat.uid !== process.getuid()) throw fail('record-path-unverified')
    this.handle = await fs.open(actual, fileConstants.O_RDONLY | fileConstants.O_NOFOLLOW)
    const opened = await this.handle.stat()
    if (signature(opened) !== signature(stat)) throw fail('record-file-replaced')
    this.physical = { signature: signature(stat), fileId: `${stat.dev}:${stat.ino}`, pathHash: hash(resolve(path)) }
    if (this.saved?.io?.pathHash === this.physical.pathHash) {
      const prior = this.saved.io, previous = prior.fileId?.split(':') ?? []
      this.deviceRemapped = previous.length === 2 && previous[0] !== String(stat.dev) && previous[1] === String(stat.ino) && prior.signature?.startsWith(prior.fileId + ':') && this.saved.key === this.key && Boolean(this.onHeader && this.expected && this.expected.prefix.offset >= this.resume)
      if (prior.fileId !== this.physical.fileId && !this.deviceRemapped) throw fail('record-file-replaced')
      const priorSignature = this.deviceRemapped ? stat.dev + ':' + prior.signature.split(':').slice(1).join(':') : prior.signature
      const priorSize = Number(priorSignature?.split(':')[2])
      if (!Number.isSafeInteger(priorSize) || stat.size < priorSize) throw fail('record-file-truncated')
      if ((this.compressed || stat.size === priorSize) && priorSignature !== this.physical.signature) throw fail('record-rewrite-suspected')
      if (!this.compressed && this.expected && (stat.size > priorSize || this.deviceRemapped)) { this.physical.signature = priorSignature; this.revalidation = { target: this.expected.prefix, prefix: new NativePrefix() } }
    }
    this.stat = stat; this.decodedSize = this.compressed ? null : stat.size
    if (!this.compressed && this.expected?.prefix.offset > stat.size) throw fail('native_origin_prefix_changed')
    if (!this.compressed && this.terminal) {
      if (this.terminal.prefix.offset > stat.size) throw fail('native_origin_prefix_changed')
      const target = [this.terminal.prefix, this.expected?.prefix].filter(Boolean).sort((a, b) => b.offset - a.offset)[0]
      this.revalidation = { target, prefix: new NativePrefix() }
    }
    if (this.compressed) this.decoderOpen()
    else this.position = 0
    return true
  }
  header(header) {
    const value = headerIdentity(header)
    if (this.expected && value !== this.expected.headerHash) throw fail('native_origin_header_changed')
    if (this.terminal && value !== this.terminal.headerHash) throw fail('native_origin_header_changed')
    this.headerHash = value
  }
  observe(bytes) {
    if (!this.headerHash && this.onHeader) {
      const lf = bytes.indexOf(10), part = lf < 0 ? bytes : bytes.subarray(0, lf)
      if (this.headerBytes.length + part.length > 65536) throw fail('history-header-budget')
      this.headerBytes = Buffer.concat([this.headerBytes, part])
      if (lf >= 0) { this.header(this.onHeader(this.headerBytes)); this.headerBytes.fill(0); this.headerBytes = Buffer.alloc(0) }
    }
    const cuts = [!this.baseVerified ? this.expected?.prefix.offset : null, !this.terminalVerified ? this.terminal.prefix.offset : null, !this.cutPrefix && Number.isSafeInteger(this.cut) ? this.cut : null].filter(value => value !== null && value !== undefined && value >= this.prefix.offset && value <= this.prefix.offset + bytes.length).sort((a, b) => a - b)
    let at = 0
    for (const boundary of cuts) {
      const count = boundary - this.prefix.offset
      this.prefix.update(bytes.subarray(at, at + count)); at += count
      const actual = this.prefix.snapshot()
      if (!this.baseVerified && boundary === this.expected?.prefix.offset) {
        if (JSON.stringify(actual) !== JSON.stringify(this.expected.prefix)) throw fail('native_origin_prefix_changed')
        this.baseVerified = true; this.verifiedBase = actual
      }
      if (boundary === this.cut) this.cutPrefix = actual
      if (!this.terminalVerified && boundary === this.terminal.prefix.offset) { if (JSON.stringify(actual) !== JSON.stringify(this.terminal.prefix)) throw fail('native_origin_prefix_changed'); this.terminalVerified = true }
    }
    this.prefix.update(bytes.subarray(at))
  }

  proof() {
    if (this.revalidation || !this.terminalVerified || !this.headerHash || !this.baseVerified || this.compressed && !this.validated || !this.prefix.offset && !this.validatedPrefix) return null
    const measured = this.prefix.snapshot(), actual = this.validatedPrefix && this.validatedPrefix.offset > measured.offset ? this.validatedPrefix : measured, next = this.expected && actual.offset < this.expected.prefix.offset ? this.expected.prefix : actual
    return { key: this.key, headerHash: this.headerHash, revision: this.expected?.revision ?? 0, base: this.expected?.prefix ?? null, verifiedBase: this.expected ? this.verifiedBase ?? this.expected.prefix : null, next, complete: !this.compressed || this.validated === true }
  }
  async guard() {
    const [path, opened] = await Promise.all([fs.lstat(this.path), this.handle.stat()])
    if (!path.isFile() || `${path.dev}:${path.ino}` !== this.physical.fileId || `${opened.dev}:${opened.ino}` !== this.physical.fileId) throw fail('record-file-replaced')
    if (this.compressed && signature(opened) !== this.physical.signature) throw fail('record-rewrite-suspected')
    if (!this.compressed) {
      if (opened.size < this.stat.size) throw fail('record-file-truncated')
      if (opened.size === this.stat.size && (opened.mtimeMs !== this.stat.mtimeMs || opened.ctimeMs !== this.stat.ctimeMs)) throw fail('record-rewrite-suspected')
      if (opened.size > this.stat.size) {
        const target = [this.prefix.snapshot(), this.expected?.prefix, this.validatedPrefix, this.terminal?.prefix].filter(Boolean).sort((a, b) => b.offset - a.offset)[0]
        this.revalidation = { target, prefix: new NativePrefix() }
      }
      this.stat = opened; this.decodedSize = opened.size
    }
  }
  async pump({ physicalBudget = LIMIT, decodedBudget = LIMIT, limit = Infinity, verifyOnly = false } = {}, accept = bytes => bytes.length) {
    for (const value of [physicalBudget, decodedBudget]) if (!Number.isSafeInteger(value) || value < 0 || value > LIMIT) throw new Error('invalid source budget')
    if (this.closed) throw fail('reader-closed')
    if (this.busy) throw fail('reader-busy')
    let physicalBytes = 0, decodedBytes = 0
    const account = () => {
      const produced = this.removed + (this.decoder?.readableLength ?? 0), fresh = produced - this.accounted
      if (fresh < 0 || decodedBytes + fresh > decodedBudget) throw fail('decoded-budget-exceeded')
      decodedBytes += fresh; this.accounted = produced
      this.peakBuffered = Math.max(this.peakBuffered, (this.decoder?.readableLength ?? 0) + (this.pending?.length ?? 0))
      if (this.peakBuffered > 65536) throw fail('decoded-buffer-exceeded')
    }
    this.busy = (async () => {
      try {
        if (!this.handle && !await this.open()) return
        await this.guard(); this.cut = limit
        if (!this.compressed && this.revalidation) {
          const checking = this.revalidation, count = Math.min(physicalBudget, decodedBudget, checking.target.offset - checking.prefix.offset)
          if (count > 0) {
            const bytes = Buffer.alloc(count), got = await this.handle.read(bytes, 0, count, checking.prefix.offset)
            physicalBytes += got.bytesRead; decodedBytes += got.bytesRead
            if (this.closed) throw fail('reader-closed')
            await this.guard()
            if (checking !== this.revalidation) return
            const actual = bytes.subarray(0, got.bytesRead), cut = !this.terminalVerified ? this.terminal.prefix.offset - checking.prefix.offset : -1
            if (cut >= 0 && cut <= actual.length) {
              checking.prefix.update(actual.subarray(0, cut))
              if (JSON.stringify(checking.prefix.snapshot()) !== JSON.stringify(this.terminal.prefix)) throw fail('native_origin_prefix_changed')
              this.terminalVerified = true; checking.prefix.update(actual.subarray(cut))
            } else checking.prefix.update(actual)
          }
          if (checking.prefix.offset === checking.target.offset) {
            if (JSON.stringify(checking.prefix.snapshot()) !== JSON.stringify(checking.target)) throw fail('native_origin_prefix_changed')
            this.physical.signature = signature(this.stat); this.revalidation = null
            if (verifyOnly && this.expected) { this.headerHash = this.expected.headerHash; this.baseVerified = true; this.verifiedBase = this.expected.prefix; this.validatedPrefix = checking.target }
          }
          return
        }
        if (verifyOnly) return
        if (!this.compressed) {
          const end = this.phase === 'validating' ? this.expected.prefix.offset : Math.min(this.decodedSize, limit)
          const count = Math.min(physicalBudget, decodedBudget, end - this.position)
          if (count > 0) {
            const bytes = Buffer.alloc(count), got = await this.handle.read(bytes, 0, count, this.position)
            physicalBytes += got.bytesRead; decodedBytes += got.bytesRead
            if (this.closed) throw fail('reader-closed')
            await this.guard()
            if (this.revalidation) return
            const actual = bytes.subarray(0, got.bytesRead)
            const skipped = this.phase === 'validating' ? actual.length : Math.min(actual.length, Math.max(0, this.resume - this.position))
            const consumed = skipped + (this.phase === 'validating' ? 0 : accept(actual.subarray(skipped)))
            this.observe(actual.subarray(0, consumed)); this.position += consumed
          }
          if (this.phase === 'validating' && this.position >= end) {
            if (!this.baseVerified) throw fail('native_origin_prefix_changed')
            this.validatedPrefix = this.expected.prefix; this.phase = 'replaying'; this.position = 0; this.resume = Math.max(this.resume, 0); this.prefix = new NativePrefix()
          }
          return
        }
        while (!this.closed && !this.error) {
          account()
          if (this.phase !== 'validating' && this.phase !== 'queued' && this.position >= limit) break
          if (this.pending) {
            let consumed
            if (this.phase === 'validating' || this.position < this.resume) consumed = Math.min(this.pending.length, this.phase === 'validating' ? Infinity : this.resume - this.position)
            else consumed = accept(this.pending.subarray(0, Math.min(this.pending.length, limit - this.position)))
            if (!Number.isSafeInteger(consumed) || consumed < 0 || consumed > this.pending.length) throw fail('decoded-consumer-invalid')
            if (this.phase === 'validating') this.observe(this.pending.subarray(0, consumed))
            this.position += consumed; this.pending = consumed === this.pending.length ? null : this.pending.subarray(consumed)
            if (!consumed || this.pending && this.position >= this.resume && this.phase !== 'validating') break
            continue
          }
          if (this.decoder.readableEnded) {
            if (this.position !== this.frame.size) throw fail('compressed-data-invalid')
            if (this.phase === 'validating') {
              if (!this.baseVerified) throw fail('native_origin_prefix_changed')
              this.validated = true; this.validatedPrefix = this.prefix.snapshot()
              const previous = this.decoder; if (!previous.closed) await new Promise(resolve => { previous.once('close', resolve); previous.destroy() }); this.decoder = null; this.phase = 'replaying'; this.position = 0; this.decoderOpen(); continue
            }
            break
          }
          if (decodedBudget - decodedBytes < RESERVE) break
          const chunk = this.decoder.read(Math.min(CHUNK, this.decoder.readableLength) || CHUNK)
          if (chunk) { this.removed += chunk.length; this.pending = chunk; account(); continue }
          if (!this.pendingWrite && !this.inputEnded) {
            const count = Math.min(CHUNK, physicalBudget - physicalBytes, this.stat.size - this.rawOffset)
            if (count > 0) {
              const bytes = Buffer.alloc(count), got = await this.handle.read(bytes, 0, count, this.rawOffset)
              physicalBytes += got.bytesRead; this.rawOffset += got.bytesRead
              if (this.closed) throw fail('reader-closed')
              if (!this.frame) { this.frame = frame(bytes.subarray(0, got.bytesRead)); this.decodedSize = this.frame.size; if (this.expected?.prefix.offset > this.decodedSize) throw fail('native_origin_prefix_changed') }
              this.pendingWrite = true
              this.decoder.write(bytes.subarray(0, got.bytesRead), error => { this.pendingWrite = false; if (error) this.error = fail('compressed-data-invalid'); this.wake() })
            } else if (this.rawOffset === this.stat.size) { this.inputEnded = true; this.decoder.end() }
            else break
          }
          await this.wait()
        }
        if (this.closed) throw fail('reader-closed')
        if (this.error) throw this.error
        await new Promise(resolve => setImmediate(resolve)); account()
        await this.guard()
      } catch (error) { error.physicalBytes = physicalBytes; error.decodedBytes = decodedBytes; this.closed = true; this.decoder?.destroy(); this.wake(); await this.handle?.close(); this.handle = null; this.pending?.fill(0); this.pending = null; leases.delete(this); throw error }
      finally { this.totalPhysical += physicalBytes; this.totalDecoded += decodedBytes }
    })()
    try {
      await this.busy
      const phase = this.revalidation ? 'validating' : this.phase, position = this.position
      return { physicalBytes, decodedBytes, position, decodedSize: this.decodedSize, phase, eof: phase !== 'validating' && position >= Math.min(limit, this.decodedSize), bufferedBytes: (this.decoder?.readableLength ?? 0) + (this.pending?.length ?? 0) }
    } finally { this.busy = null }
  }
  acknowledge(fences) { const fence = fences?.find(value => value.key === this.key); if (fence && this.headerHash === fence.headerHash && this.proof()?.next.offset <= fence.prefix.offset) { this.expected = structuredClone(fence); this.verifiedBase = structuredClone(fence.prefix); this.baseVerified = true } }
  rangeProof() { return this.headerHash && this.cutPrefix ? { key: this.key, headerHash: this.headerHash, prefix: this.cutPrefix } : null }
  checkpoint(offset = this.position) { return { offset, key: this.key, io: this.physical ? { ...this.physical } : null } }
  async close() {
    if (this.closing) return this.closing
    this.closed = true; this.decoder?.destroy(); this.wake()
    this.closing = (async () => { try { await this.busy } catch {} await this.handle?.close(); this.handle = null; this.decoder = null; this.pending?.fill(0); this.pending = null; this.headerBytes.fill(0); this.headerBytes = Buffer.alloc(0); this.revalidation = null; leases.delete(this) })()
    return this.closing
  }
}
