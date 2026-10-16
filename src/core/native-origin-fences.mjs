import { normalizeNativePrefix } from '../native-origin.mjs'
import { nativeRecordIdentity } from '../fork-proof.mjs'

const COLLECTION = 'native-origin-fences', VERSIONS = 'native-origin-versions', LIMIT = 10000
const keyOf = (registrationId, key) => JSON.stringify([registrationId, key])
const fail = code => Object.assign(new Error(code), { code })
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)

export function createNativeOriginFences({ db }) {
  const entries = new Map(db.prepare('SELECT key,value FROM records WHERE collection=?').all(COLLECTION).map(row => [row.key, JSON.parse(row.value)]))
  for (const [key, value] of entries) { const scope = JSON.parse(key); if (!Array.isArray(scope) || scope.length !== 2 || typeof scope[0] !== 'string' || value.key !== scope[1] || !/^[a-f0-9]{64}$/.test(value.key ?? '') || !/^[a-f0-9]{64}$/.test(value.headerHash ?? '') || !Number.isSafeInteger(value.revision) || value.revision < 1 || Object.keys(value).length !== 4 || Buffer.byteLength(JSON.stringify(value)) > 1024) throw fail('native_origin_state_invalid'); normalizeNativePrefix(value.prefix) }
  if (entries.size > LIMIT) throw fail('native_origin_capacity')
  const versions = new Map(db.prepare('SELECT key,value FROM records WHERE collection=?').all(VERSIONS).map(row => [row.key, JSON.parse(row.value)]))
  if ([...versions.values()].some(value => ![1, 2].includes(value))) throw fail('native_origin_state_invalid')
  const legacy = new Set(db.prepare('SELECT value FROM records WHERE collection=?').all('native-capture-progress').map(row => JSON.parse(row.value).registrationId))
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value')
  const remove = db.prepare('DELETE FROM records WHERE collection=? AND key=?')
  function version(registrationId, requested) {
    const known = versions.get(registrationId) ?? (legacy.has(registrationId) ? 1 : null)
    if (known !== null && known !== requested) throw fail('native_origin_upgrade_unverified')
  }
  function prepareClearRegistration(registrationId) {
    const selected = [...entries.keys()].filter(key => JSON.parse(key)[0] === registrationId)
    return { removed: { nativeOriginFences: selected.length, nativeOriginVersions: Number(versions.has(registrationId)) }, stage() {
      for (const key of selected) remove.run(COLLECTION, key)
      remove.run(VERSIONS, registrationId)
      return () => { for (const key of selected) entries.delete(key); versions.delete(registrationId); legacy.delete(registrationId) }
    } }
  }
  return {
    prepareClearRegistration,
    checkVersion: version,
    read(registrationId, key) { version(registrationId, 2); return structuredClone(entries.get(keyOf(registrationId, key)) ?? null) },
    project(registrationId, requested, origins = [], events = []) {
      version(registrationId, requested)
      const changes = new Map(), proofs = new Map(origins.map(proof => [proof.key, proof]))
      if (requested !== 2 && origins.length) throw fail('native_origin_invalid')
      for (const proof of origins) {
        if (!proof.complete) throw fail('native_origin_invalid')
        const key = keyOf(registrationId, proof.key), known = entries.get(key)
        if (known && known.headerHash !== proof.headerHash) throw fail('native_origin_header_changed')
        if (known && same(known.prefix, proof.next)) continue
        if (known ? proof.revision !== known.revision || !same(proof.base, known.prefix) || !same(proof.verifiedBase, known.prefix) : proof.revision !== 0 || proof.base !== null || proof.verifiedBase !== null) throw fail('native_origin_revision_conflict')
        if (known && proof.next.offset <= known.prefix.offset || proof.next.offset === 0) throw fail('native_origin_prefix_changed')
        changes.set(key, { key: proof.key, headerHash: proof.headerHash, prefix: proof.next, revision: (known?.revision ?? 0) + 1 })
      }
      if (entries.size + [...changes.keys()].filter(key => !entries.has(key)).length > LIMIT) throw fail('native_origin_capacity')
      if (requested === 2) for (const event of events) {
        const data = event.data ?? {}, proof = proofs.get(data.nativeGenerationKey)
        if (data?.sourceKind !== 'chatgpt-native' || !data.nativeRecordId || !proof?.complete || !Number.isSafeInteger(data.nativeRecordStart) || data.nativeRecordStart < 0 || !Number.isSafeInteger(data.nativeRecordEnd) || data.nativeRecordEnd <= data.nativeRecordStart || data.nativeRecordEnd > proof.next.offset || data.nativeRecordId !== nativeRecordIdentity('native-storage/2:' + data.nativeGenerationKey, null, data.nativeRecordStart, data.nativeType)) throw fail('native_origin_invalid')
      }
      return { stage(hasCapturedProgress = false) {
        const determinesVersion = origins.length > 0 || hasCapturedProgress
        if (determinesVersion) write.run(VERSIONS, registrationId, JSON.stringify(requested))
        for (const [key, value] of changes) write.run(COLLECTION, key, JSON.stringify(value))
        return () => { if (hasCapturedProgress) legacy.add(registrationId); if (determinesVersion) versions.set(registrationId, requested); for (const [key, value] of changes) entries.set(key, structuredClone(value)) }
      } }
    },
    diagnostics() { const backed = new Set([...legacy, ...[...entries.keys()].map(key => JSON.parse(key)[0])]); return { generations: entries.size, limit: LIMIT, registrations: versions.size, newRegistrationLimit: LIMIT * 2, legacyModeOnly: [...versions.keys()].filter(key => !backed.has(key)).length } },
    clearRegistration(registrationId) { prepareClearRegistration(registrationId).stage()() },
  }
}
