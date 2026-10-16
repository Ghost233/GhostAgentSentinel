import { createHash } from 'node:crypto'

const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const hex = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
export const headerIdentity = header => hash(['native-header/2', header])
export class NativePrefix {
  constructor() { this.offset = 0; this.blocks = 0; this.root = hash(['native-prefix/2']); this.tail = createHash('sha256'); this.tailLength = 0 }
  update(bytes) {
    for (let at = 0; at < bytes.length;) {
      const count = Math.min(bytes.length - at, 65536 - this.tailLength)
      this.tail.update(bytes.subarray(at, at + count)); this.offset += count; this.tailLength += count; at += count
      if (this.tailLength === 65536) { this.root = hash([this.root, this.blocks++, this.tail.digest('hex')]); this.tail = createHash('sha256'); this.tailLength = 0 }
    }
  }
  snapshot() { return { offset: this.offset, blocks: this.blocks, root: this.root, tailLength: this.tailLength, tailHash: this.tail.copy().digest('hex') } }
}
export function normalizeNativePrefix(value) {
  if (!value || typeof value !== 'object' || Object.keys(value).length !== 5 || !Number.isSafeInteger(value.offset) || value.offset < 0 || !Number.isSafeInteger(value.blocks) || value.blocks < 0 || !Number.isInteger(value.tailLength) || value.tailLength < 0 || value.tailLength >= 65536 || value.offset !== value.blocks * 65536 + value.tailLength || !hex(value.root) || !hex(value.tailHash)) throw Object.assign(new Error('invalid native prefix'), { code: 'native_origin_invalid' })
  return { offset: value.offset, blocks: value.blocks, root: value.root, tailLength: value.tailLength, tailHash: value.tailHash }
}
export function normalizeNativeOrigins(values) {
  if (!Array.isArray(values) || values.length > 16) throw Object.assign(new Error('invalid native origins'), { code: 'native_origin_invalid' })
  const seen = new Set()
  return values.map(value => {
    if (!value || Object.keys(value).length !== 7 || !hex(value.key) || !hex(value.headerHash) || !Number.isSafeInteger(value.revision) || value.revision < 0 || typeof value.complete !== 'boolean' || seen.has(value.key)) throw Object.assign(new Error('invalid native origin'), { code: 'native_origin_invalid' })
    seen.add(value.key)
    return { key: value.key, headerHash: value.headerHash, revision: value.revision, base: value.base === null ? null : normalizeNativePrefix(value.base), verifiedBase: value.verifiedBase === null ? null : normalizeNativePrefix(value.verifiedBase), next: normalizeNativePrefix(value.next), complete: value.complete }
  })
}
