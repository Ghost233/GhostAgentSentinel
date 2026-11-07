import { DatabaseSync } from 'node:sqlite'
import { lstat, realpath, mkdir, mkdtemp, readFile, writeFile, rename, rm, chmod } from 'node:fs/promises'
import { resolve, join, dirname } from 'node:path'
import { normalizeAnalysisConfig } from './protocol.mjs'

const fail = code => { throw Object.assign(new Error(code), { code }) }
const optional = async path => { try { return await lstat(path) } catch (error) { if (error.code === 'ENOENT') return null; throw error } }
async function owned(path, directory = false) {
  const info = await optional(path)
  if (info && (info.isSymbolicLink() || (directory ? !info.isDirectory() : !info.isFile()) || info.uid !== process.getuid() || info.mode & 0o077 || await realpath(path) !== path)) fail('core_profile_unconfirmed')
  return info
}
const scoped = new Set(['conversationSettings', 'installationAnalysisSettings', 'installationMonitoringSettings', 'installationNotificationSettings', 'installationNotificationHistory', 'native-capture-progress', 'native-initial-baselines', 'native-initial-monitored', 'native-initial-rollups'])
const sourceScoped = new Set(['tasks', 'notifications', 'failure-receipts', 'failure-states'])
const settings = new Set(['monitoring', 'enrollment', 'notification-record-cursor-key', 'failure-receipts', 'native-capture-progress', 'native-notification-metadata'])

// Bootstrap-only migration: a consistent, read-only snapshot of one installation.
// No runtime endpoint, process identity or another installation's data is copied.
export async function migrateCoreProfile({ from, to, registrationId }) {
  from = resolve(from); to = resolve(to)
  if (from === to || typeof registrationId !== 'string' || !registrationId.startsWith('chatgpt:') || registrationId.length > 256) fail('core_profile_unconfirmed')
  const marker = { format: 1, kind: 'ghost-sentinel-core-profile/1', from, registrationId }
  if (await optional(to)) {
    await owned(to, true); await owned(join(to, 'core-profile.json'))
    let saved
    try { saved = JSON.parse(await readFile(join(to, 'core-profile.json'), 'utf8')) } catch { fail('core_profile_unconfirmed') }
    if (JSON.stringify(saved) !== JSON.stringify(marker)) fail('core_profile_unconfirmed')
    return { migrated: false }
  }
  await owned(from, true)
  const inputPath = join(from, 'observations.sqlite'), before = await owned(inputPath)
  await mkdir(dirname(to), { recursive: true, mode: 0o700 }); await owned(dirname(to), true)
  const temporary = await mkdtemp(join(dirname(to), '.core-migration-'))
  let source, target, copied = 0
  try {
    if (before) {
      source = new DatabaseSync(inputPath, { readOnly: true }); source.exec('BEGIN')
      const select = source.prepare('SELECT key,value FROM records WHERE collection=?')
      const sourceKeys = new Set([...select.iterate('sources')].filter(row => JSON.parse(row.value).descriptor?.registrationId === registrationId).map(row => row.key))
      const taskKeys = new Set([...select.iterate('tasks')].filter(row => sourceKeys.has(JSON.parse(row.value).sourceKey)).map(row => row.key))
      const noticeIds = new Set([...select.iterate('notifications')].filter(row => sourceKeys.has(JSON.parse(row.value).sourceKey)).map(row => row.key))
      target = new DatabaseSync(join(temporary, 'observations.sqlite'))
      target.exec('CREATE TABLE records(collection TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(collection,key)); BEGIN')
      const write = target.prepare('INSERT INTO records VALUES(?,?,?)')
      for (const row of source.prepare('SELECT collection,key,value FROM records').iterate()) {
        const value = JSON.parse(row.value); let keep = false, selected = value
        if (row.collection === 'sources') keep = sourceKeys.has(row.key)
        else if (sourceScoped.has(row.collection)) keep = sourceKeys.has(value.sourceKey)
        else if (row.collection === 'events') keep = taskKeys.has(value.taskKey)
        else if (scoped.has(row.collection)) keep = value.registrationId === registrationId || row.key === registrationId
        else if (row.collection === 'native-origin-fences') keep = JSON.parse(row.key)[0] === registrationId
        else if (row.collection === 'native-origin-versions') keep = row.key === registrationId
        else if (row.collection === 'native-notification-links') keep = noticeIds.has(value.notificationId)
        else if (row.collection === 'settings') {
          if (!settings.has(row.key)) fail('core_profile_schema_unverified')
          keep = true
          if (row.key === 'enrollment') selected = { ...value, entries: value.entries.filter(entry => entry.registrationId === registrationId) }
        } else fail('core_profile_schema_unverified')
        if (keep) { write.run(row.collection, row.key, JSON.stringify(selected)); copied++ }
      }
      target.exec('COMMIT'); target.close(); target = null
      source.exec('COMMIT'); source.close(); source = null
      const after = await owned(inputPath)
      if (after.dev !== before.dev || after.ino !== before.ino) fail('core_profile_source_changed')
      await chmod(join(temporary, 'observations.sqlite'), 0o600)
      const configPath = join(from, 'analysis-config.json')
      if (await owned(configPath)) {
        const config = await readFile(configPath, 'utf8'); normalizeAnalysisConfig(JSON.parse(config))
        await writeFile(join(temporary, 'analysis-config.json'), config, { mode: 0o600 })
      }
    }
    await writeFile(join(temporary, 'core-profile.json'), JSON.stringify(marker), { mode: 0o600 })
    await rename(temporary, to)
    return { migrated: Boolean(before), copied }
  } finally {
    target?.close(); source?.close()
    await rm(temporary, { recursive: true, force: true })
  }
}
