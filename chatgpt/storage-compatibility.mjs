// Host qualification and stored-record producers are separate, finite policies.
// The candidate Host is not enabled by the production adapter by this module.
export const supportedProfile = Object.freeze({ appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' })
export const candidateHostProfile = Object.freeze({ appVersion: '26.1013.31256', appBuild: '20586', cliVersion: '0.163.0-alpha.10.2' })
export const hostProfiles = Object.freeze([supportedProfile, candidateHostProfile])
export const storageProducers = Object.freeze(['0.162.0-alpha.17.2', '0.163.0-alpha.10.2', '0.162.0-alpha.2', '0.160.1'])
export const hostProfileKey = profile => [profile?.appVersion, profile?.appBuild, profile?.cliVersion].join('|')
export const supportsHostProfile = profile => hostProfiles.some(value => ['appVersion', 'appBuild', 'cliVersion'].every(key => typeof profile?.[key] === 'string' && profile[key] === value[key]))
export const supportsStorageProducer = version => storageProducers.includes(version)
