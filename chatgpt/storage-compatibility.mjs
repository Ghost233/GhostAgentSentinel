import { isAbsolute } from 'node:path'

// Historical profiles only recognize old cursor hashes during verified recovery.
// They do not admit or reject a running App or a stored-record producer.
export const supportedProfile = Object.freeze({ appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' })
export const candidateHostProfile = Object.freeze({ appVersion: '26.1013.31256', appBuild: '20586', cliVersion: '0.163.0-alpha.10.2' })
export const legacyHostProfiles = Object.freeze([supportedProfile, candidateHostProfile, Object.freeze({ appVersion: '26.1013.40934', appBuild: '20770', cliVersion: '0.163.0-alpha.10.2' }), Object.freeze({ appVersion: '26.1021.31345', appBuild: '21828', cliVersion: '0.164.0-alpha.11.1' })])
export const hostProfileKey = profile => [profile?.appVersion, profile?.appBuild, profile?.cliVersion].join('|')
export function supportsRecordHeader(source, header) {
  if (header?.threadId !== source.threadId) return false
  if ((source.nativeOriginVersion ?? 1) !== 2) return true
  if (header.originator !== source.originator || header.sourceKind !== source.sourceKind || typeof header.cwd !== 'string' || !isAbsolute(header.cwd)) return false
  if (header.historyMode !== undefined && header.historyMode !== 'paginated' || header.ordinal !== undefined && (!Number.isSafeInteger(header.ordinal) || header.ordinal < 0)) return false
  return !source.forkedFromId && !source.historyRange || header.historyMode === 'paginated' && Number.isSafeInteger(header.ordinal)
}
