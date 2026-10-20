function parameterCodec(name) {
  return { mode: 'strict', typeSymbol: `Sentinel.${name}`, create: () => ({ parse(value) {
    if (name === 'entries') {
      if (!Array.isArray(value) || value.length > 100) throw new Error('后台清单最多 100 项')
      const seen = new Set()
      for (const entry of value) {
        if (!entry || typeof entry !== 'object' || Array.isArray(entry) || Object.keys(entry).some(key => !['registrationId', 'name', 'profile', 'enabled'].includes(key)) || typeof entry.enabled !== 'boolean') throw new Error('后台清单格式无效')
        for (const field of ['registrationId', 'name', ...(entry.profile === undefined ? [] : ['profile'])]) {
          if (typeof entry[field] !== 'string' || !entry[field].trim() || entry[field].length > (field === 'profile' ? 2000 : 500) || /[\u0000-\u001f]/.test(entry[field])) throw new Error('后台清单标识和名称无效')
        }
        if (seen.has(entry.registrationId)) throw new Error('后台标识不可重复')
        seen.add(entry.registrationId)
      }
      return value
    }
    if (name === 'revision') {
      if (typeof value !== 'string' || !value || value.length > 2000) throw new Error('分析配置修订令牌无效')
      return value
    }
    if (name === 'notificationClaimSelection') {
              if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 1 || value.submissionProtocol !== 1) throw new Error('通知客户端不支持撤销确认；未领取通知')
              return value
            }
          if (name === 'kind') {
      if (!['task-alert', 'monitoring-failure'].includes(value)) throw new Error('通知类别无效')
      return value
    }
    if (name === 'enabled') {
      if (typeof value !== 'boolean') throw new Error('通知偏好必须为布尔值')
      return value
    }
    if (name === 'notificationRecordsSelection') {
      if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => key !== 'cursor') || value.cursor !== undefined && (typeof value.cursor !== 'string' || !value.cursor || value.cursor.length > 2000)) throw new Error('通知记录游标无效')
      return value
    }
    if (name === 'directorySelection') {
      if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== 1 || typeof value.baseUrl !== 'string' || !value.baseUrl || value.baseUrl.length > 2000 || /[\u0000-\u001f\u007f]/.test(value.baseUrl)) throw new Error('模型目录地址无效')
      return value
    }
    if (name === 'id' || name === 'attemptToken') {
      if (typeof value !== 'string' || !value || value.length > 2000) throw new Error('通知 ID 无效')
      return value
    }
    if (name === 'sessionId') {
      if (typeof value !== 'string' || !value || value.length > 2000 || /[\u0000-\u001f]/.test(value)) throw new Error('会话标识无效')
      return value
    }
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('观察操作参数必须是对象')
    if (name === 'result') {
      if (!['submitted', 'failed', 'invalidated'].includes(value.status) || Object.keys(value).some(key => !['status', 'error', 'attemptToken'].includes(key))
        || (value.attemptToken !== undefined && (typeof value.attemptToken !== 'string' || !value.attemptToken || value.attemptToken.length > 2000)) || (value.error !== undefined && (typeof value.error !== 'string' || value.error.length > 500))) throw new Error('通知投递结果无效')
    } else {
      for (const key of Object.keys(value)) {
        const model = value[key]
        if (!['llm', 'jev'].includes(key) || !model || typeof model !== 'object' || Array.isArray(model)) throw new Error('本地分析配置无效')
        for (const field of Object.keys(model)) if (!['baseUrl', 'model'].includes(field) || typeof model[field] !== 'string' || model[field].length > 2000) throw new Error('模型连接字段无效')
      }
    }
    return value
  } }) }
}

module.exports = function descriptors() {
  const methods = {
    snapshot: [], gsSummary: ['sessionId'], diagnostics: [], installationDiagnostics: [], sourcePlan: [], pause: [], resume: [], analysisConfig: [], analysisConfigState: [], claimNotifications: ['notificationClaimSelection'], configureSources: ['entries'],
    configureAnalysisIfCurrent: ['config', 'revision'], modelDirectory: ['directorySelection'], ackNotification: ['id', 'result'],
    installationNotificationPreferences: [], setInstallationNotificationPreference: ['kind', 'enabled'], beginNotificationSubmission: ['id', 'attemptToken'],
    installationNotificationRecords: ['notificationRecordsSelection'], waitNotificationSubmissionRevocation: ['id', 'attemptToken'],
  }
  return Object.entries(methods).map(([method, parameters]) => ({
    id: `ghost-agent-sentinel#sentinel/${method}`, service: 'sentinel', namespace: 'sentinel', method,
    invocation: { kind: 'direct' }, cancellation: { parameter: 'signal' }, parameters: parameters.map(name => ({ name, wire: name, source: 'json', codec: parameterCodec(name) })), result: { mode: 'src-json' },
  }))
}
