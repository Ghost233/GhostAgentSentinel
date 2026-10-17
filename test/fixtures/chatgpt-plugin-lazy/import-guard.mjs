import { registerHooks } from 'node:module'
import { readFileSync, appendFileSync } from 'node:fs'

const root = process.env.SENTINEL_LAZY_GUARD_ROOT
const blocked = new Set(['lazy-core.mjs', 'native-io.mjs', 'adapter.mjs', 'native-notifications.mjs'].map(name => new URL(name, root).href))
registerHooks({ resolve(specifier, context, nextResolve) {
  const resolved = nextResolve(specifier, context)
  if (!blocked.has(resolved.url)) return resolved
  let owner
  try { owner = JSON.parse(readFileSync(process.env.SENTINEL_LAZY_GUARD_OWNER, 'utf8')) } catch {}
  appendFileSync(process.env.SENTINEL_LAZY_GUARD_EVENTS, JSON.stringify({ ownedLock: owner?.pid === process.pid, module: resolved.url }) + '\n')
  throw Object.assign(new Error('Owned test collection import is unavailable'), { code: 'owned_import_failure' })
} })
