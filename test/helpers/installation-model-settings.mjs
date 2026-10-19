export async function configureInstallationModels(core, registrationId, config) {
  const current = await core.installationAnalysisConfig(registrationId);
  return core.configureInstallationAnalysis(registrationId, config, current.revision);
}
