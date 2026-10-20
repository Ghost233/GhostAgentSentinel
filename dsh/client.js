(() => {
  window.__ModuleLoader__.load({
    id: 'ghost-agent-sentinel',
    factory(require) {
      const React = require('react')
      const { createElement: h, useEffect, useLayoutEffect, useRef, useState } = React
      const id = 'ghost-agent-sentinel'
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
          if (name === 'id') {
            if (typeof value !== 'string' || !value || value.length > 2000) throw new Error('通知 ID 无效')
            return value
          }
          if (name === 'sessionId') {
            if (typeof value !== 'string' || !value || value.length > 2000 || /[\u0000-\u001f]/.test(value)) throw new Error('会话标识无效')
            return value
          }
          if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('观察操作参数必须是对象')
          if (name === 'directorySelection') {
            if (Object.keys(value).length !== 1 || typeof value.baseUrl !== 'string' || !value.baseUrl || value.baseUrl.length > 2000 || /[\u0000-\u001f\u007f]/.test(value.baseUrl)) throw new Error('模型目录地址无效')
            return value
          }
          if (name === 'result') {
            if (!['submitted', 'failed'].includes(value.status) || Object.keys(value).some(key => !['status', 'error'].includes(key))
              || (value.error !== undefined && (typeof value.error !== 'string' || value.error.length > 500))) throw new Error('通知投递结果无效')
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
      const methods = { snapshot: [], gsSummary: ['sessionId'], diagnostics: [], installationDiagnostics: [], sourcePlan: [], pause: [], resume: [], analysisConfig: [], analysisConfigState: [], claimNotifications: [], configureSources: ['entries'], configureAnalysisIfCurrent: ['config', 'revision'], modelDirectory: ['directorySelection'], ackNotification: ['id', 'result'] }
      const descriptors = Object.entries(methods).map(([method, parameters]) => ({
        id: `${id}#sentinel/${method}`, service: 'sentinel', namespace: 'sentinel', method,
        invocation: { kind: 'direct' }, cancellation: { parameter: 'signal' }, parameters: parameters.map(name => ({ name, wire: name, source: 'json', codec: parameterCodec(name) })), result: { mode: 'src-json' },
      }))
      const checks = {
        stalled_progress: '活跃但推进不足', request_drift: '偏离当前请求', failure_loop: '同类失败反复返工',
        validation_cost: '完整验证重复成本', validation_gap: '本地验证证据缺口', long_wait: '长等待或活动中断',
      }
      const checkStatus = { attention: '需要关注', clear: '本轮未发现疑点', unknown: '不可判断' }
      const checkLabel = check => check?.judgmentState === 'failed' ? '调用失败' : check?.applicability?.state === 'not-triggered' ? '未触发' : check?.applicability?.state === 'triggered' && check?.status === 'unknown' ? '材料不足' : checkStatus[check?.status] || '不可判断'
      const gsLabel = value => value?.label ? `${value.label}${Number.isFinite(value.waiting?.durationMs) ? ` · 已等待 ${Math.floor(value.waiting.durationMs / 1000)} 秒` : ''}` : '状态未提供'
      const analysisStatus = { queued: '排队', analyzing: '分析中', cooldown: '冷却中', idle: '等待新材料', paused: '观察分析已暂停',
        unknown: '分析状态未知', error: '分析失败', failed: '分析失败', unavailable: '分析依赖不可用', completed: '本轮已完成', ready: '本轮已完成',
        interrupted: '分析已取消；底层计算可能延迟结束', stale: '分析结论已过期' }
      const taskStatus = { running: '运行中', idle: '空闲', disposed: '已结束', unknown: '状态未知' }
      const alertStatus = { attention: '疑点持续', recovered: '已恢复（有新证据）', unknown: '当前不可判断', superseded: '请求已变更，原疑点失效',
        ended: '任务已结束，原疑点失效', 'source-disconnected': '来源失联，原疑点失效', 'evidence-expired': '证据已到期，原疑点失效' }
      let connectionScope, snapshotRequest, notificationRequest, snapshotTimer, notificationTimer, refreshRequested = false
      let summaryTimer, summaryRequest, summaryRefreshRequested = false, entryObservers = 0, footerObservers = 0
      let state = { snapshot: null, snapshotVersion: 0, error: '正在连接共享核心', notification: '', notificationError: '', config: null, sourcePlan: null }
      const subscribers = new Set(), viewers = new Map(), viewBindings = new Map(), notificationCallbacks = new Map()
      const settingsBindings = new Set()
      const unfinishedCalls = new Set()
      const summaryBindings = new Map()
      let viewListeners = 0
      const text = value => typeof value === 'string' ? value : ''
      const timeText = value => Number.isFinite(value) ? new Date(value).toLocaleString() : '时间未知'
      const errorText = error => String(error?.message || error || '操作失败').slice(0, 200)
      const visibleViewers = () => [...viewers.values()].filter(Boolean).length
      function diagnostics() {
        return { viewInstances: viewers.size, visibleViewers: visibleViewers(), snapshotTimer: Number(snapshotTimer !== undefined),
          backgroundTimer: Number(notificationTimer !== undefined), listeners: subscribers.size + viewListeners + settingsBindings.size,
          pendingSnapshot: Number(snapshotRequest !== undefined), pendingNotification: Number(notificationRequest !== undefined),
          callbacks: notificationCallbacks.size, activeRequests: connectionScope?.pending.size || 0, unfinishedRequests: unfinishedCalls.size,
          summaryViewers: visibleSummaries().length, summaryTimer: Number(summaryTimer !== undefined), pendingSummary: Number(summaryRequest !== undefined), entryObservers: entryObservers + footerObservers }
      }
      function notifyState(patch) { state = { ...state, ...patch }; for (const listener of subscribers) listener(state) }
      function notificationStatus() {
        if (typeof Notification === 'undefined') return '此客户端没有桌面通知能力'
        return Notification.permission === 'granted' ? '桌面通知已获授权；投递受系统设置影响' : Notification.permission === 'denied' ? '桌面通知被拒绝；请在系统或浏览器设置中启用' : '桌面通知未授权'
      }
      async function scopedCall(scope, method, args = [], signal) {
        if (!scope || !scope.active) throw new Error('观察连接不可用')
        if (['snapshot', 'gsSummary'].includes(method) && [...unfinishedCalls].filter(call => call.method === method).length >= 2) throw new Error('先前读取尚未收尾；暂不开始新的读取')
        const abort = new AbortController(), lifetime = scope.abort.signal
        const cancel = () => abort.abort(new Error('观察请求已取消'))
        lifetime.addEventListener('abort', cancel, { once: true }); signal?.addEventListener('abort', cancel, { once: true })
        if (lifetime.aborted || signal?.aborted) cancel()
        const deadline = setTimeout(() => abort.abort(new Error('观察请求超过 10 秒')), 10000)
        const cancelled = new Promise((_, reject) => {
          const rejectAbort = () => reject(abort.signal.reason || new Error('观察请求已取消'))
          abort.signal.addEventListener('abort', rejectAbort, { once: true })
          if (abort.signal.aborted) rejectAbort()
        })
        scope.pending.add(abort)
        try {
          const invocation = { method }
          unfinishedCalls.add(invocation)
          const completion = Promise.resolve().then(() => { abort.signal.throwIfAborted(); return scope.remote[method](...args, abort.signal) }).finally(() => unfinishedCalls.delete(invocation))
          const response = await Promise.race([completion, cancelled])
          if (!scope.active || abort.signal.aborted) throw new Error('观察请求已取消')
          if (!response?.ok) {
            const error = new Error(response?.error?.message || `观察操作失败：${response?.error?.code || method}`)
            if (method === 'configureAnalysisIfCurrent') error.saveOutcome = response?.error?.details?.saveOutcome === 'rejected' ? 'rejected' : 'unknown'
            throw error
          }
          return response.value
        } finally {
          clearTimeout(deadline); lifetime.removeEventListener('abort', cancel); signal?.removeEventListener('abort', cancel)
          scope.pending.delete(abort)
        }
      }
      const call = (method, ...args) => scopedCall(connectionScope, method, args)
      function reconcileSnapshot() {
        if (connectionScope?.active && visibleViewers() > 0) {
          if (snapshotTimer === undefined) {
            snapshotTimer = setInterval(() => { void pollSnapshot() }, 1000)
            void pollSnapshot()
          }
        } else {
          if (snapshotTimer !== undefined) clearInterval(snapshotTimer)
          snapshotTimer = undefined; refreshRequested = false
          snapshotRequest?.abort.abort()
        }
      }
      async function pollSnapshot() {
        if (!connectionScope?.active || visibleViewers() === 0) return
        if (snapshotRequest) { refreshRequested = true; return }
        const scope = connectionScope, request = { scope, abort: new AbortController() }
        snapshotRequest = request; refreshRequested = false
        const read = method => scopedCall(scope, method, [], request.abort.signal)
        try {
          const snapshot = await read('snapshot')
          if (connectionScope !== scope || request.abort.signal.aborted || visibleViewers() === 0) return
          if (snapshot?.protocol !== 1 || !Array.isArray(snapshot.tasks) || !Array.isArray(snapshot.sources)) throw new Error('观察协议不兼容')
          notifyState({ snapshot, snapshotVersion: state.snapshotVersion + 1, sourcePlan: snapshot.enrollment || state.sourcePlan, error: snapshot.adapter?.error || '', notification: state.notification || notificationStatus() })
        } catch (error) { if (connectionScope === scope && !request.abort.signal.aborted && visibleViewers() > 0) notifyState({ error: errorText(error) }) }
        finally {
          if (snapshotRequest === request) snapshotRequest = undefined
          if (refreshRequested && connectionScope?.active && visibleViewers() > 0) void pollSnapshot()
        }
      }
      function releaseNotification(notification) {
        const timer = notificationCallbacks.get(notification)?.timer
        if (timer !== undefined) clearTimeout(timer)
        notificationCallbacks.delete(notification); notification.onerror = null
      }
      async function pollNotifications() {
        if (!connectionScope?.active || notificationRequest || typeof Notification === 'undefined' || Notification.permission !== 'granted') return
        const scope = connectionScope, request = { scope }; notificationRequest = request
        try {
          const notices = await scopedCall(scope, 'claimNotifications')
          if (connectionScope !== scope || !scope.active) return
          if (state.notificationError) notifyState({ notificationError: '' })
          for (const notice of Array.isArray(notices) ? notices : []) {
            if (!scope.active) return
            try {
              const notification = new Notification(text(notice.title) || 'Sentinel · 执行疑点', {
                body: (text(notice.body) || text(notice.message) || '请打开 Sentinel 查看依据和分析覆盖').slice(0, 1200), tag: `sentinel:${notice.id}`,
              })
              notification.onerror = () => {
                releaseNotification(notification)
                if (!scope.active || connectionScope !== scope) return
                notifyState({ notification: '桌面通知投递失败；面板仍显示疑点' })
                void scopedCall(scope, 'ackNotification', [notice.id, { status: 'failed', error: '浏览器报告通知投递失败' }]).catch(error => {
                  if (scope.active && connectionScope === scope) notifyState({ error: errorText(error) })
                })
              }
              notificationCallbacks.set(notification, { scope, timer: setTimeout(() => releaseNotification(notification), 10000) })
              while (notificationCallbacks.size > 100) releaseNotification(notificationCallbacks.keys().next().value)
              await scopedCall(scope, 'ackNotification', [notice.id, { status: 'submitted' }])
              if (scope.active && connectionScope === scope) notifyState({ notification: '通知已提交给系统；未确认用户看见' })
            } catch (error) {
              if (!scope.active || connectionScope !== scope) return
              notifyState({ notification: `桌面通知投递失败：${errorText(error)}` })
              await scopedCall(scope, 'ackNotification', [notice.id, { status: 'failed', error: errorText(error) }])
            }
          }
        } catch (error) { if (scope.active && connectionScope === scope) notifyState({ notificationError: errorText(error) }) }
        finally { if (notificationRequest === request) notificationRequest = undefined }
      }
      function attachView(binding) {
        if (!binding.visible || binding.signal?.aborted || binding.attached || !connectionScope?.active) return
        binding.attached = true; subscribers.add(binding.update)
        document.addEventListener('visibilitychange', binding.change); binding.signal?.addEventListener('abort', binding.change)
        viewListeners += 1 + Number(Boolean(binding.signal)); binding.update(state); binding.change()
      }
      function detachView(binding) {
        if (!binding?.attached) return
        binding.attached = false; subscribers.delete(binding.update)
        document.removeEventListener('visibilitychange', binding.change); binding.signal?.removeEventListener('abort', binding.change)
        viewListeners -= 1 + Number(Boolean(binding.signal))
        if (viewers.has(binding.key)) viewers.set(binding.key, false)
        reconcileSnapshot()
      }
      function useMonitor(visible, tabSignal) {
        const [current, update] = useState(state), viewer = useRef({})
        useEffect(() => {
          const key = viewer.current
          viewers.set(key, false); update(state)
          return () => { detachView(viewBindings.get(key)); viewBindings.delete(key); viewers.delete(key); reconcileSnapshot() }
        }, [])
        useEffect(() => {
          const key = viewer.current, binding = { key, visible: Boolean(visible), signal: tabSignal, update, attached: false,
            change() { if (tabSignal?.aborted) { binding.update(previous => ({ ...previous, error: '查看区已关闭' })); detachView(binding); return }; if (viewers.has(key)) { viewers.set(key, Boolean(visible) && document.visibilityState !== 'hidden'); reconcileSnapshot() } },
          }
          viewBindings.set(key, binding); attachView(binding)
          return () => { detachView(binding); if (viewBindings.get(key) === binding) viewBindings.delete(key) }
        }, [visible, tabSignal])
        return current
      }
      const visibleSummaries = () => [...summaryBindings.values()].filter(binding => binding.attached && document.visibilityState !== 'hidden' && !binding.signal?.aborted)
      function attachSummary(binding) {
        if (binding.attached || binding.signal?.aborted || !connectionScope?.active) return
        binding.attached = true
        document.addEventListener('visibilitychange', binding.changed); binding.signal?.addEventListener('abort', binding.changed)
        viewListeners += 1 + Number(Boolean(binding.signal)); reconcileSummaries()
      }
      function detachSummary(binding) {
        if (!binding.attached) return
        binding.attached = false
        document.removeEventListener('visibilitychange', binding.changed); binding.signal?.removeEventListener('abort', binding.changed)
        viewListeners -= 1 + Number(Boolean(binding.signal)); reconcileSummaries()
      }
      function reconcileSummaries() {
        const bindings = visibleSummaries()
        if (connectionScope?.active && bindings.length) {
          if (summaryTimer === undefined) { summaryTimer = setInterval(() => { void pollSummaries() }, 1000); void pollSummaries() }
          if (summaryRequest?.sessionId && !bindings.some(binding => binding.sessionId === summaryRequest.sessionId)) { summaryRefreshRequested = true; summaryRequest.abort.abort() }
        } else {
          if (summaryTimer !== undefined) clearInterval(summaryTimer)
          summaryTimer = undefined; summaryRefreshRequested = false; summaryRequest?.abort.abort()
        }
      }
      async function pollSummaries() {
        if (!connectionScope?.active || !visibleSummaries().length) return
        if (summaryRequest) { summaryRefreshRequested = true; return }
        const scope = connectionScope, request = { scope, abort: new AbortController() }
        summaryRequest = request; summaryRefreshRequested = false
        try {
          for (const sessionId of [...new Set(visibleSummaries().map(binding => binding.sessionId))]) {
            if (request.abort.signal.aborted || !scope.active || connectionScope !== scope) break
            if (!visibleSummaries().some(binding => binding.sessionId === sessionId)) continue
            request.sessionId = sessionId
            try {
              const value = await scopedCall(scope, 'gsSummary', [sessionId], request.abort.signal)
              if (request.abort.signal.aborted || !scope.active || connectionScope !== scope) break
              if (value?.protocol !== 1 || value.sessionId !== sessionId || !value.counts || ['green', 'yellow', 'red', 'gray'].some(color => !Number.isSafeInteger(value.counts[color]) || value.counts[color] < 0)) throw new Error('GS 摘要协议不兼容')
              for (const binding of visibleSummaries()) if (binding.sessionId === sessionId) binding.update({ value, error: '' })
            } catch (error) {
              if (!request.abort.signal.aborted && scope.active && connectionScope === scope) for (const binding of visibleSummaries()) if (binding.sessionId === sessionId) binding.update(previous => ({ ...previous, error: errorText(error) }))
            }
          }
        } finally {
          if (summaryRequest === request) summaryRequest = undefined
          if (summaryRefreshRequested && connectionScope?.active && visibleSummaries().length) void pollSummaries()
        }
      }
      function useGSSummary(sessionId, visible, signal) {
        const [summary, update] = useState({ value: null, error: '正在读取 GS 摘要' }), key = useRef({})
        useEffect(() => {
          if (!visible || !sessionId || signal?.aborted) return
          const binding = { sessionId, update, signal, attached: false, changed() {
            if (signal?.aborted) { update(previous => ({ ...previous, error: '查看区已关闭' })); detachSummary(binding); summaryBindings.delete(key.current) }
            else reconcileSummaries()
          } }
          summaryBindings.set(key.current, binding); attachSummary(binding)
          return () => { detachSummary(binding); summaryBindings.delete(key.current); reconcileSummaries() }
        }, [sessionId, visible, signal])
        return summary
      }
      function AnalysisConfig({ config, request = call, onConfig = config => notifyState({ config }), expanded = false, signal, onCheck = () => {} }) {
        const [draft, setDraft] = useState(() => Object.fromEntries(['llm', 'jev'].map(role => [role, {
          baseUrl: config.config?.[role]?.baseUrl || '', model: config.config?.[role]?.model || '',
        }])))
        const [results, setResults] = useState({})
        const [directories, setDirectories] = useState({}), [checks, setChecks] = useState({}), actions = useRef({})
        const edits = useRef({ llm: 0, jev: 0 }), values = useRef(draft), confirmed = useRef(config)
        const savedEdits = useRef({ llm: 0, jev: 0 }), resultScopes = useRef({}), confirmedScope = useRef(connectionScope)
        const pending = useRef(new Map()), saving = useRef(false), active = useRef(true), uncertain = useRef(false)
        useEffect(() => () => { active.current = false; pending.current.clear(); for (const action of Object.values(actions.current)) action.abort.abort(); actions.current = {} }, [])
        const current = () => active.current && !signal?.aborted
        function change(role, field, value) {
          edits.current[role]++; values.current = { ...values.current, [role]: { ...values.current[role], [field]: value } }
          setDraft(values.current); setResults(previous => ({ ...previous, [role]: '草稿尚未保存' }))
          for (const kind of field === 'baseUrl' ? ['list', 'check'] : ['check']) {
            const key = `${role}:${kind}`; actions.current[key]?.abort.abort(); delete actions.current[key]
          }
          if (field === 'baseUrl') setDirectories(previous => ({ ...previous, [role]: undefined }))
          setChecks(previous => ({ ...previous, [role]: undefined })); onCheck(role, null)
        }
        async function readModels(role, kind) {
          const key = `${role}:${kind}`, scope = connectionScope
          if (!current() || !scope?.active || actions.current[key]) return
          const action = { scope, baseUrl: values.current[role].baseUrl.trim(), model: values.current[role].model.trim(), version: edits.current[role], abort: new AbortController() }
          actions.current[key] = action
          const valid = () => current() && scope.active && scope === connectionScope && actions.current[key] === action
            && action.baseUrl === values.current[role].baseUrl.trim() && (kind === 'list' || action.version === edits.current[role])
          const update = value => {
            if (!valid()) return
            if (kind === 'list') setDirectories(previous => ({ ...previous, [role]: { ...value, scope, baseUrl: action.baseUrl } }))
            else { setChecks(previous => ({ ...previous, [role]: { ...value, scope } })); onCheck(role, { ...value, scope }) }
          }
          update({ loading: true, message: kind === 'list' ? '正在读取列表' : '正在检查连接' })
          try {
            const value = await scopedCall(scope, 'modelDirectory', [{ baseUrl: action.baseUrl }], action.abort.signal)
            if (!valid()) return
            if (!Array.isArray(value?.models) || !['online', 'offline', 'unconfirmed'].includes(value.state)) throw new Error('模型目录响应未确认')
            if (kind === 'list') {
              const models = value.state === 'online' ? value.models.filter(model => ['unknown', role === 'llm' ? 'summary' : 'jev'].includes(model.role)) : []
              update({ models, message: value.state === 'online' ? `已读取 ${models.length} 个候选模型；尚未检查连接` : `读取列表失败：${value.error?.code || value.state}` })
            } else {
              const message = value.state !== 'online' ? `${value.state === 'offline' ? '服务离线' : '服务响应未确认'}；模型可见性未确认${value.error?.code ? ' · ' + value.error.code : ''}`
                : !action.model ? '服务在线；尚未填写模型 ID' : value.models.some(model => model.id === action.model) ? '服务在线；模型可见' : value.complete ? '服务在线；所填模型未列出，仍可手填' : '服务在线；模型可见性未确认'
              update({ message: `${message}；实际分析未验证` })
            }
          } catch (error) { update({ models: [], message: `${kind === 'list' ? '读取列表失败' : '检查连接失败'}：${errorText(error)}` }) }
          finally { if (actions.current[key] === action) delete actions.current[key] }
        }
        async function flush() {
          if (saving.current) return
          const scope = connectionScope, valid = () => current() && scope?.active && connectionScope === scope
          saving.current = true
          try {
            while (pending.current.size && valid()) {
              const [role, edit] = pending.current.entries().next().value; pending.current.delete(role)
              if (edit.scope !== scope) continue
              const report = message => { if (valid() && edits.current[role] === edit.version) { resultScopes.current[role] = scope; setResults(previous => ({ ...previous, [role]: message })) } }
              report('正在保存')
              try {
                if (confirmedScope.current !== scope) {
                  const latest = await request('analysisConfigState')
                  if (!valid()) break
                  if (latest?.registrationId !== config.registrationId) throw new Error('安装响应不匹配')
                  confirmed.current = latest; confirmedScope.current = scope; uncertain.current = false
                }
                if (!uncertain.current && savedEdits.current[role] === edit.version && JSON.stringify(confirmed.current.config?.[role]) === JSON.stringify(edit.model)) { report('已保存；下一轮采用新配置'); continue }
                const next = { ...confirmed.current.config }
                if (edit.model) next[role] = edit.model; else delete next[role]
                const saved = await request('configureAnalysisIfCurrent', next, confirmed.current.revision)
                if (!valid()) break
                if (saved?.registrationId !== config.registrationId || JSON.stringify(saved.config?.[role]) !== JSON.stringify(edit.model)) throw new Error('安装或模型响应不匹配')
                confirmed.current = saved; savedEdits.current[role] = edit.version; uncertain.current = false; onConfig(saved); report('已保存；下一轮采用新配置')
              } catch (error) {
                if (!valid()) break
                if (error.saveOutcome !== 'rejected') uncertain.current = true
                report(`${error.saveOutcome === 'rejected' ? '保存失败' : '保存结果未知'}：${errorText(error)}`)
              }
            }
          } finally {
            saving.current = false
            if (!valid()) for (const [role, edit] of pending.current) if (edit.scope === scope) pending.current.delete(role)
            if (pending.current.size && current() && connectionScope?.active) void flush()
          }
        }
        function save(role) {
          if (!current()) return
          const scope = connectionScope
          if (!scope?.active) { setResults(previous => ({ ...previous, [role]: '保存结果未知：观察连接已关闭' })); return }
          let model = { baseUrl: values.current[role].baseUrl.trim(), model: values.current[role].model.trim() }
          let url
          try { url = new URL(model.baseUrl) } catch {}
          if ((model.baseUrl || model.model) && (!url || !['http:', 'https:'].includes(url.protocol) || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)
            || url.username || url.password || url.search || url.hash || !['', '/'].includes(url.pathname) || model.baseUrl.length > 2000 || !model.model || model.model.length > 500)) {
            pending.current.delete(role); setResults(previous => ({ ...previous, [role]: '草稿未保存：请填写有效的本机服务地址和模型 ID' })); return
          }
          if (!model.baseUrl && !model.model) model = undefined
          else model.baseUrl = url.href.replace(/\/$/, '')
          pending.current.delete(role); pending.current.set(role, { model, version: edits.current[role], scope }); void flush()
        }
        const field = (role, name, label, placeholder) => h('label', null, label,
          h('input', { value: draft[role][name], placeholder, onChange: event => change(role, name, event.target.value), autoComplete: 'off' }))
        return h('details', { open: expanded || undefined }, h('summary', null, '本地分析连接'), h('form', { onSubmit: event => event.preventDefault() },
          h('p', null, '填写本机服务地址和模型 ID，完成一组后离开该分组即可自动保存。保存不要求服务在线。'),
          [['llm', '总结模型'], ['jev', 'JEV']].map(([role, label]) => {
            const directory = directories[role], options = directory?.scope === connectionScope && connectionScope?.active ? directory.models || [] : []
            const check = checks[role]
            return h('fieldset', { key: role, 'aria-label': label,
              onBlur: event => { if (!event.currentTarget.contains(event.relatedTarget) && !event.target.closest('button')) save(role) },
              onKeyDown: event => { if (event.key === 'Enter' && !['BUTTON', 'SELECT'].includes(event.target.tagName)) { event.preventDefault(); save(role) } } },
            h('legend', null, label), field(role, 'baseUrl', `${label}服务`, 'http://127.0.0.1:端口'),
            h('label', null, '可选模型', h('select', { 'aria-label': `${label}可选模型`, value: options.some(model => model.id === draft[role].model) ? draft[role].model : '',
              onChange: event => { change(role, 'model', event.target.value); save(role) } },
            h('option', { value: '', disabled: true }, '选择模型，或在下方手填'), options.map(model => h('option', { key: model.id, value: model.id }, model.id)))),
            field(role, 'model', `${label}模型 ID（可手填）`, '模型 ID'),
            h('button', { type: 'button', disabled: Boolean(directory?.loading && directory.scope === connectionScope), onMouseDown: event => event.preventDefault(), onClick: () => { void readModels(role, 'list') } }, '读取列表'),
            h('button', { type: 'button', disabled: Boolean(check?.loading && check.scope === connectionScope), onMouseDown: event => event.preventDefault(), onClick: () => { void readModels(role, 'check') } }, '检查连接'),
            h('p', { 'data-sentinel-model-list': true }, directory ? directory.scope === connectionScope && connectionScope?.active ? directory.message : '列表已过期，请重新读取' : '尚未读取列表'),
            h('p', { 'data-sentinel-model-check': true }, check ? check.scope === connectionScope && connectionScope?.active ? check.message : '检查已过期，请重新检查连接' : '尚未检查连接；实际分析未验证'),
            results[role] && h('p', { role: 'status' }, results[role] === '正在保存' && (resultScopes.current[role] !== connectionScope || !connectionScope?.active) ? '保存结果未知：观察连接已关闭' : results[role]))
          })))
      }
      function InstallationDiagnostics({ config, visible, signal, checks }) {
        const [result, setResult] = useState({}), pending = useRef(null), active = useRef(true)
        useEffect(() => () => { active.current = false; pending.current?.abort.abort(); pending.current = null }, [])
        async function refresh() {
          const scope = connectionScope
          if (!visible || !config || !scope?.active || pending.current || signal?.aborted) return
          const action = { scope, abort: new AbortController() }; pending.current = action
          const current = () => active.current && !signal?.aborted && scope.active && scope === connectionScope && pending.current === action
          setResult(previous => ({ ...previous, scope, loading: true, error: '' }))
          try {
            const value = await scopedCall(scope, 'installationDiagnostics', [], action.abort.signal)
            if (!current()) return
            if (value?.protocol !== 1 || value.registrationId !== config.registrationId) throw new Error('安装诊断响应归属未确认')
            setResult({ scope, value, loading: false, error: '' })
          } catch (error) { if (current()) setResult({ scope, value: null, loading: false, error: errorText(error) }) }
          finally { if (pending.current === action) pending.current = null }
        }
        useEffect(() => {
          if (visible && config) void refresh()
          return () => { pending.current?.abort.abort(); pending.current = null }
        }, [visible, config?.registrationId])
        const stale = result.scope && (result.scope !== connectionScope || !connectionScope?.active)
        const value = stale ? null : result.value, collection = value?.collection, analysis = value?.analysis, latest = analysis?.latest
        const failure = error => `${error?.stage || '环节未提供'} · ${error?.code || '原因代码未提供'}`
        return h('section', { 'data-sentinel-diagnostics': true, 'aria-label': '本安装连接诊断' },
          h('button', { type: 'button', disabled: !config || !connectionScope?.active || Boolean(result.loading && !stale), onClick: () => { void refresh() } }, '刷新诊断'),
          stale ? h('p', { role: 'status' }, '诊断结果已过期：观察连接已关闭，请重新打开或刷新。')
            : result.loading ? h('p', { role: 'status' }, '正在读取本安装诊断…') : result.error ? h('p', { role: 'alert' }, `读取诊断失败：${result.error}`) : null,
          value && h('div', null,
            h('p', null, `诊断快照：${timeText(value.generatedAt)}${value.configRevision !== config?.revision ? '；配置已更新，请刷新诊断' : ''}`),
            h('h3', null, '采集'),
            h('p', null, ({ connected: '来源已连接，不代表采集完整', disconnected: '来源已断开', unconfirmed: '来源或采集范围未确认', failed: '采集失败' })[collection?.state] || '采集状态未确认'),
            h('p', null, `${collection?.connectedSources ?? 0}/${collection?.sourceCount ?? 0} 个来源已连接；最近联系：${timeText(collection?.lastSeenAt)}`),
            collection?.error && h('p', null, `采集失败环节：${failure(collection.error)}`),
            h('h3', null, '已保存模型准备状态'),
            ['llm', 'jev'].map(role => h('div', { key: role }, h('p', null, `${role === 'llm' ? '总结模型' : 'JEV'}：${value.models?.[role]?.configured ? '配置完整；在线与实际推理未确认' : '尚未配置完整'}`),
              h('p', null, `当前编辑内容的连接检查：${checks[role] ? checks[role].scope === connectionScope && connectionScope?.active ? checks[role].message : '检查已过期，请重新检查' : '尚未检查当前编辑内容'}`))),
            h('h3', null, '实际分析'),
            h('p', null, `当前活动观察对象：${analysis?.counts?.total ?? 0}；${Object.entries(analysis?.counts?.states || {}).map(([state, count]) => `${analysisStatus[state] || state} ${count}`).join(' · ')}`),
            latest ? h('div', null, h('p', null, `最近保留轮次：${analysisStatus[latest.state] || latest.state || '状态未知'}；结果：${latest.resultDisposition || '未确认'}`),
              h('p', null, `该轮配置：${latest.configRevision || '未确认'}；开始：${timeText(latest.startedAt)}；完成：${timeText(latest.completedAt)}；下次资格：${timeText(latest.nextEligibleAt)}`),
              latest.error && h('p', null, `最近保留轮次失败环节：${failure(latest.error)}`)) : h('p', null, '尚无保留分析轮次'),
            analysis?.latestFailure && h('p', null, `最近保留失败：${failure(analysis.latestFailure)}；${timeText(analysis.latestFailure.at)}`)))
      }
      function SettingsPanel() {
        const [tab, setTab] = useState('settings'), [config, setConfig] = useState(null), [error, setError] = useState('')
        const [permissionMessage, setPermissionMessage] = useState(notificationStatus), [requestingPermission, setRequestingPermission] = useState(false)
        const [modelChecks, setModelChecks] = useState({})
        const savedConfig = useRef(null), lifetime = useRef(null), permissionRequest = useRef(null)
        if (!lifetime.current) lifetime.current = new AbortController()
        const acceptConfig = current => { savedConfig.current = current; setConfig(current) }
        useEffect(() => {
          const abort = lifetime.current
          let loading
          const connect = scope => {
            permissionRequest.current = null; setRequestingPermission(false); setPermissionMessage(notificationStatus())
            if (!scope?.active) { loading = undefined; setError('观察连接已关闭'); return }
            setError('')
            if (savedConfig.current || loading?.scope === scope) return
            const request = { scope }; loading = request
            scopedCall(scope, 'analysisConfigState', [], abort.signal).then(current => {
              if (!abort.signal.aborted && loading === request) acceptConfig(current)
            }, failure => { if (!abort.signal.aborted && loading === request) setError(errorText(failure)) }).finally(() => { if (loading === request) loading = undefined })
          }
          settingsBindings.add(connect); connect(connectionScope)
          return () => { settingsBindings.delete(connect); abort.abort() }
        }, [])
        async function permission() {
          const scope = connectionScope
          if (typeof Notification === 'undefined' || !scope?.active || permissionRequest.current) return
          const request = { scope }
          permissionRequest.current = request; setRequestingPermission(true); setPermissionMessage('正在请求桌面通知许可…')
          const current = () => !lifetime.current.signal.aborted && scope.active && connectionScope === scope && permissionRequest.current === request
          try {
            await Notification.requestPermission()
            if (!current()) return
            const message = notificationStatus()
            setPermissionMessage(message); notifyState({ notification: message }); await pollNotifications()
          } catch (error) {
            if (current()) {
              const message = `${notificationStatus()}；${errorText(error)}`
              setPermissionMessage(message); notifyState({ notification: message })
            }
          } finally { if (current()) { permissionRequest.current = null; setRequestingPermission(false) } }
        }
        const request = (method, ...args) => scopedCall(connectionScope, method, args, lifetime.current.signal)
        return h('section', { className: 'sentinel-overview', 'data-sentinel-installation-settings': true, 'aria-label': 'Sentinel 安装设置' },
          h('h2', null, 'Sentinel'),
          h('div', { role: 'tablist', 'aria-label': 'Sentinel 安装设置页面' },
            [['settings', '设置'], ['notifications', '通知记录'], ['diagnostics', '连接诊断']].map(([value, label]) =>
              h('button', { key: value, type: 'button', role: 'tab', id: `${id}-${value}-tab`, 'aria-controls': `${id}-${value}-panel`, 'aria-selected': tab === value, onClick: () => setTab(value) }, label))),
          error && h('p', { role: 'alert' }, error),
          h('div', { role: 'tabpanel', id: `${id}-settings-panel`, 'aria-labelledby': `${id}-settings-tab`, hidden: tab !== 'settings' },
            config ? h(AnalysisConfig, { key: config.registrationId, config, request, onConfig: acceptConfig, expanded: true, signal: lifetime.current.signal, onCheck: (role, value) => setModelChecks(previous => ({ ...previous, [role]: value })) }) : h('p', null, error ? '尚未读取本安装配置。' : '正在读取本安装配置…'),
            h('section', { 'aria-label': '桌面通知许可' }, h('h3', null, '桌面通知许可'),
              h('p', { role: 'status', 'data-sentinel-notification-permission': true }, permissionMessage),
              typeof Notification !== 'undefined' && Notification.permission !== 'granted' && h('button', { type: 'button',
                disabled: requestingPermission || !connectionScope?.active, onClick: permission }, '启用桌面通知'))),
          h('div', { role: 'tabpanel', id: `${id}-notifications-panel`, 'aria-labelledby': `${id}-notifications-tab`, hidden: tab !== 'notifications' }, h('p', null, '本安装通知记录尚未接入。')),
          h('div', { role: 'tabpanel', id: `${id}-diagnostics-panel`, 'aria-labelledby': `${id}-diagnostics-tab`, hidden: tab !== 'diagnostics' },
            h(InstallationDiagnostics, { key: config?.registrationId || 'loading', config, visible: tab === 'diagnostics', signal: lifetime.current.signal, checks: modelChecks })))
      }
      const enrollmentStatus = { pending: '等待首次连接', connecting: '已连接，等待基线', connected: '已连接', disconnected: '已断开', disabled: '清单项已停用' }
      function SourcePlan({ plan, sources }) {
        const entries = plan?.entries || []
        const initial = entries.map(({ registrationId, name, profile, enabled }) => ({ registrationId, name, ...(profile ? { profile } : {}), enabled }))
        const serialized = JSON.stringify(initial, null, 2)
        const [draft, setDraft] = useState(serialized), [result, setResult] = useState('')
        const previousConfiguration = useRef(serialized)
        useEffect(() => {
          if (previousConfiguration.current === serialized) return
          previousConfiguration.current = serialized; setDraft(serialized)
        }, [serialized])
        async function save(event) {
          event.preventDefault(); setResult('正在保存')
          try {
            const entries = parameterCodec('entries').create().parse(JSON.parse(draft))
            const saved = await call('configureSources', entries)
            notifyState({ sourcePlan: saved }); setResult('已保存；清单不会自动安装或停用后台插件')
            await pollSnapshot()
          } catch (error) { setResult(errorText(error)) }
        }
        return h('details', null, h('summary', null, '预期后台清单'),
          h('p', null, '逐后台启用一次适配器后自动连接。清单记录预期配置，未列出的后台仍可能存在；不代表整机发现完整。'),
          h('p', null, '启用配置 ', plan?.configured ?? entries.filter(entry => entry.enabled).length, ' · 已连接配置 ', plan?.connected ?? '未知',
            ' · 已连接 Host ', sources.filter(source => source.status === 'connected').length),
          entries.map(entry => h('p', { key: entry.registrationId }, entry.name, ' · ', enrollmentStatus[entry.status] || '状态未知',
            ' · Host ', entry.connectedSources ?? 0, ' · 标识 ', entry.registrationId, entry.profile ? ` · 配置 ${entry.profile}` : '')),
          (plan?.unlisted || []).length > 0 && h('p', null, '有已接入 Host 不在预期清单中。'),
          h('form', { onSubmit: save }, h('label', null, '后台配置 JSON（最多 100 项）', h('textarea', {
            rows: 7, value: draft, onChange: event => setDraft(event.target.value), autoComplete: 'off', spellCheck: false,
          })), h('p', null, '每项填写 registrationId、name、enabled；profile 可选。标识必须与该后台适配器设置一致。'),
          h('button', { type: 'submit' }, '保存后台清单'), result && h('p', { role: 'status' }, result)))
      }
      const historicalGaps = {
        'Task baseline is unconfirmed': '任务基线未确认',
        'Source is disconnected': '来源已断开',
        'Task baseline is not yet available': '任务基线尚未取得',
      }
      const gapText = value => historicalGaps[value] || value
      function EvidenceCitations({ record, task, snapshot, recovery = false, compact = false }) {
        const references = recovery ? record?.recoveryEvidenceRefs : record?.evidenceRefs
        const ids = recovery ? record?.recoveryEvidenceIds : record?.evidenceIds
        const scoped = Array.isArray(references)
        const selected = scoped ? references : (ids || []).map(eventId => ({ taskId: task?.id, lifecycleId: task?.lifecycleId, eventId }))
        const now = Number.isFinite(snapshot?.generatedAt) ? snapshot.generatedAt : Date.now()
        return h('div', null, selected.map((reference, index) => {
          const owner = scoped ? snapshot?.tasks?.find(item => item.sourceKey === task?.sourceKey && item.id === reference.taskId && item.lifecycleId === reference.lifecycleId) : task
          const event = owner?.events?.find(event => event.id === reference.eventId && (!scoped || event.taskId === reference.taskId && event.taskLifecycleId === reference.lifecycleId)
            && (!Number.isFinite(event.capturedAt) || event.capturedAt <= now && event.capturedAt > now - 86400000))
          if (!event) return h(compact ? 'small' : 'p', { key: index, 'data-sentinel-citation-gap': true }, '原拥有节点的部分引用已到期或不在当前读取范围，不能作为当前完整依据。')
          const source = snapshot?.sources?.find(source => source.key === owner.sourceKey)
          return h(compact ? 'small' : 'p', { key: index, 'data-sentinel-citation': reference.eventId, 'data-source-key': owner.sourceKey, 'data-task-id': owner.id, 'data-lifecycle-id': owner.lifecycleId },
            scoped && h('span', null, '原拥有节点：', text(owner.title) || owner.id, ' · 来源：', text(source?.descriptor?.name) || owner.sourceKey, ' · '), event.text || '仅执行元数据')
        }))
      }
      function AlertHistory({ alerts, task, snapshot }) {
        return h('details', null, h('summary', null, `查看告警记录（${alerts.length}）`), alerts.map(alert => {
          const notification = alert.notification
          const delivery = notification?.status === 'submitted' ? '系统已受理提交' : notification?.status === 'failed' ? '投递失败'
            : notification?.status === 'expired' ? '领取超时，未确认提交' : notification?.status === 'claimed' ? '已领取，等待提交确认' : '尚未确认通知提交'
          return h('details', { key: alert.id }, h('summary', null, checks[alert.checkId] || '执行疑点', ' · ', alertStatus[alert.status] || '状态未知'),
            h('p', null, '首次发现：', timeText(alert.firstAt)),
            h('p', null, '疑点记录：', text(alert.reason) || '原判断依据不可用'),
            h('p', null, '原判断证据：'), h(EvidenceCitations, { record: alert, task, snapshot }),
            (alert.recoveryEvidenceIds?.length > 0 || alert.recoveryEvidenceRefs?.length > 0) && h('div', null, h('p', null, '恢复依据：'), h(EvidenceCitations, { record: alert, task, snapshot, recovery: true })),
            h('p', null, '通知：', delivery, '；用户是否看见未知'),
            notification?.error && h('p', { role: 'alert' }, notification.error),
            Number.isFinite(notification?.at) && h('small', null, '通知状态时间：', timeText(notification.at)))
        }))
      }
      const visibleFooters = new Map(), footerListeners = new Set()
      function footerChanged() { for (const update of footerListeners) update() }
      function GSCounts({ counts = {} }) {
        const labels = { green: '正常', yellow: '观察', red: '关注', gray: '不可判断' }
        return h('span', { className: 'sentinel-gs-counts' }, Object.entries(labels).map(([color, label]) => h('span', { key: color, className: `sentinel-gs-${color}`, 'aria-label': `${label} ${counts[color] ?? 0}` },
          h('i', { 'aria-hidden': true }), h('span', { 'data-gs-count': color }, counts[color] ?? 0))))
      }
      function HideNormal({ value, change }) {
        return h('label', { className: 'sentinel-hide-normal' }, h('input', { type: 'checkbox', 'data-gs-hide-normal': true,
          checked: value, onChange: event => change(event.target.checked) }), '隐藏正常')
      }
      function MonitorHeader({ scope, tab, change, counts, children, close, overview = false }) {
        return h('header', { className: 'sentinel-monitor-header', 'data-gs-monitor-header': true },
          h('div', { className: 'sentinel-monitor-title' }, h('strong', null, scope), close && h('button', { type: 'button', onClick: close, 'aria-label': '关闭 GS' }, '关闭')),
          h('div', { className: 'sentinel-monitor-lamps', ...(overview ? { 'data-gs-overview-counts': true } : {}) }, h('strong', null, 'GS'), h(GSCounts, { counts }), overview && children), !overview && children,
          h('nav', { role: 'tablist', 'aria-label': '监控内容', className: 'sentinel-monitor-tabs' },
            [['issues', '当前问题'], ['details', '详情']].map(([value, label]) => h('button', { key: value, type: 'button', role: 'tab',
              'data-gs-tab': value, 'aria-selected': tab === value, tabIndex: tab === value ? 0 : -1,
              onClick: () => change(value), onKeyDown: event => {
                if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
                event.preventDefault()
                const next = event.key === 'Home' ? 'issues' : event.key === 'End' ? 'details' : tab === 'issues' ? 'details' : 'issues'
                change(next); event.currentTarget.parentElement.querySelector(`[data-gs-tab=${next}]`)?.focus()
              } }, label))))
      }
      function IssueEvidence({ task, snapshot, gs = task.gs }) {
        const attention = gs?.light === 'red' ? (task.analysis?.checks || []).filter(check => check.status === 'attention') : []
        const notification = (task.alerts || []).find(alert => alert.status === 'attention' && alert.notification)?.notification
        return h('div', { className: 'sentinel-issue-evidence' },
          attention.map(check => h('div', { key: check.id }, h('small', null, checks[check.id] || '执行疑点', '：', text(check.reason)),
            h(EvidenceCitations, { record: { ...check, ...(Array.isArray(check.evidenceRefs) ? { evidenceRefs: check.evidenceRefs.slice(0, 2) } : { evidenceIds: check.evidenceIds?.slice(0, 2) }) }, task, snapshot, compact: true }))),
          notification && h('small', { 'data-gs-notification': true }, notification.status === 'submitted' ? '系统已受理 · 是否看见未知'
            : notification.status === 'failed' ? '通知投递失败' : notification.status === 'claimed' ? '通知待确认' : '通知尚未确认'))
      }
      function ObservationDetail({ task, snapshot, current, gs = task.gs }) {
        const analysis = task.analysis || {}, source = snapshot.sources.find(item => item.key === task.sourceKey)
        const observed = connectionScope?.active && !current.error && !snapshot.adapter?.error
        const generated = analysis.summary?.generated
        const publishableSummary = observed && source?.status === 'connected' && ['current', 'reused'].includes(analysis.resultDisposition) && !['failed', 'stale', 'unavailable', 'interrupted', 'unknown'].includes(analysis.state) && text(generated?.text)
        const previous = analysis.lastSuccessfulSummary, now = Date.now()
        const observedThrough = Number.isFinite(snapshot.generatedAt) ? Math.min(now, snapshot.generatedAt) : now
        const completedThrough = Number.isFinite(analysis.completedAt) ? Math.min(observedThrough, analysis.completedAt) : observedThrough
        const historicalSummary = !publishableSummary && source?.descriptor?.registrationId && previous?.configScope?.registrationId === source.descriptor.registrationId
          && Number.isSafeInteger(previous.completedAt) && previous.completedAt <= completedThrough
          && Number.isSafeInteger(previous.expiresAt) && previous.expiresAt > now && previous.expiresAt <= previous.completedAt + 86400000 && text(previous.generated?.text)
        const analyzing = observed && snapshot.monitoring?.state !== 'paused' && task.status === 'running' && source?.status === 'connected'
          && !['unknown', 'interrupted', 'stale', 'failed', 'unavailable', 'paused'].includes(analysis.state)
        return h('article', { 'data-gs-detail': task.key, 'aria-live': 'polite' },
          h('div', { 'data-gs-focus-summary': true, className: 'sentinel-focus-summary' },
            h('small', null, text(task.title) || task.id),
            h('strong', { 'data-gs-focus-status': true }, observed ? gs ? gsLabel(gs) : taskStatus[task.status] || '当前状态不可判断' : '当前状态不可判断'),
            gs?.reason && h('small', null, observed ? '' : '上次观察依据：', text(gs.reason)),
            h('p', null, observed ? '最近活动：' : '历史活动：', text(task.activity?.summary) || '活动摘要未提供')),
          h('p', null, '当前任务：', text(analysis.summary?.currentTask) || text(task.currentRequest) || '未采集当前请求'),
          h('p', null, '当前状态：', observed ? taskStatus[task.status] || '状态未知' : '当前无法确认', ' · ',
            observed ? '' : '历史活动：', text(task.activity?.summary) || '活动摘要未提供'),
          publishableSummary && h('div', { 'data-sentinel-model-summary': true }, h('p', null, analysis.resultDisposition === 'reused' ? '复用有效总结：' : '模型总结：', generated.text), h(EvidenceCitations, { record: generated, task, snapshot, compact: true })),
          historicalSummary && h('details', { 'data-sentinel-summary-history': true }, h('summary', null, '上次成功总结'),
            h('small', null, '历史总结 · 原完成时间：', timeText(previous.completedAt)), h('p', null, previous.generated.text), h(EvidenceCitations, { record: previous.generated, task, snapshot, compact: true })),
          task.instructionContext?.gaps?.length > 0 && h('p', null, '当前指令上下文缺口：', task.instructionContext.gaps.join('；')),
          h('details', { 'data-gs-checks': true }, h('summary', null, '完整检查与依据'),
            h('p', null, '最近推进：', text(analysis.summary?.recentProgress) || '尚未判断'),
            h('p', null, '需要关注：', analyzing ? text(analysis.summary?.attention) || '尚未判断' : '当前判断已失效或不可判断；已有疑点见告警记录'),
            h(EvidenceCitations, { record: analysis.summary, task, snapshot }),
            Object.entries(checks).map(([checkId, label]) => {
              const check = (analysis.checks || []).find(item => item.id === checkId) || { status: 'unknown', reason: '尚未完成此项判断' }
              return h('details', { key: checkId }, h('summary', null, label, ' · ',
                check.judgmentState && observed && task.status === 'running' && source?.status === 'connected' || analyzing ? checkLabel(check) : '当前不可判断'),
                h('p', null, text(check.reason) || '缺少判断依据'), h(EvidenceCitations, { record: check, task, snapshot }))
            }), h(AlertHistory, { alerts: task.alerts || [], task, snapshot })),
          h('details', { 'data-gs-diagnostics': true }, h('summary', null, '身份、时间与覆盖'),
            h('p', null, '观察身份：', h('code', null, task.key)), h('p', null, '来源身份：', h('code', null, task.sourceKey)),
            h('p', null, '分析状态：', analysisStatus[analysis.state] || '当前不可判断'),
            h('p', null, '分析覆盖截止：', timeText(analysis.through)),
            h('p', null, '最近完成：', timeText(analysis.completedAt)), h('p', null, '最近实际活动：', timeText(task.lastActivity)),
            h('p', null, '任务资料更新时间：', timeText(task.updatedAt)),
            Number.isFinite(analysis.waitMs) && analysis.waitMs > 0 && h('p', null, '排队等待约 ', Math.round(analysis.waitMs / 1000), ' 秒。'),
            Number.isFinite(analysis.nextEligibleAt) && analysis.nextEligibleAt > Date.now() && h('p', null, '冷却剩余约 ', Math.ceil((analysis.nextEligibleAt - Date.now()) / 1000), ' 秒；取得资格后仍可能排队。'),
            Number.isFinite(analysis.lagMs) && analysis.lagMs > 0 && h('p', null, '当前结论尚未覆盖新活动；滞后约 ', Math.round(analysis.lagMs / 1000), ' 秒。'),
            analysis.resultDisposition && h('p', null, '结果覆盖状态：', { none: '尚无结果', current: '已覆盖当前材料', pending: '等待覆盖新材料', reused: '已复用有效结果', invalid: '原结果已失效' }[analysis.resultDisposition] || '未知'),
            analysis.error && h('p', { role: 'alert' }, errorText(analysis.error)),
            h('p', null, '采集缺口：', [...new Set([...(source?.gaps || []), ...(task.coverage?.gaps || task.gaps || [])])].map(gapText).join('；') || '已知材料范围外仍不可判断'),
            h('details', null, h('summary', null, `查看已采集证据（${task.events?.length || 0}）`), (task.events || []).slice(-50).map(event => h('p', { key: event.id },
              timeText(event.capturedAt), ' · ', event.kind, ' · ', event.text || '仅元数据')))))
      }
      // Depth and light are public core projections; this tree only controls display.
      function displayTree(nodes, tasks, hideNormal, issuesOnly = false) {
        const roots = [], stack = [], byKey = new Map(tasks.map(task => [task.key, task]))
        for (const node of nodes) {
          const task = byKey.get(node.taskKey), light = node.light ?? node.gs?.light ?? task?.gs?.light
          const ended = node.state === 'ended' || node.gs?.light === null || task?.gs?.light === null
          const entry = { node, task, light, ended, children: [], rank: ended ? 4 : ({ red: 0, yellow: 1, gray: 2, green: 3 }[light] ?? 2) }
          while (stack.length && (stack.at(-1).node.depth || 0) >= (node.depth || 0)) stack.pop()
          const siblings = stack.at(-1)?.children || roots
          siblings.push(entry); stack.push(entry)
        }
        const prune = entries => entries.flatMap(entry => {
          entry.children = prune(entry.children)
          entry.rank = Math.min(entry.rank, ...entry.children.map(child => child.rank))
          entry.active = hideNormal && entry.light === 'green' ? entry.children.some(child => child.active) : !entry.ended || entry.children.some(child => child.active)
          if (issuesOnly && (entry.ended || !['red', 'yellow'].includes(entry.light)) && !entry.children.length) return []
          if (hideNormal && entry.light === 'green' && !entry.children.length) return []
          entry.pathOnly = issuesOnly && !['red', 'yellow'].includes(entry.light) || hideNormal && entry.light === 'green' || entry.ended && entry.active
          return [entry]
        }).sort((a, b) => a.rank - b.rank)
        return prune(roots)
      }
      function ObservationTree({ nodes, snapshot, current, selected, select, hideNormal, overview = false, issuesOnly = false }) {
        const tree = displayTree(nodes, snapshot.tasks, hideNormal, issuesOnly)
        const observed = connectionScope?.active && !current.error && !snapshot.adapter?.error
        const render = entry => {
          const { node, task, light, children, pathOnly } = entry
          const gs = node.gs || task?.gs, label = gs?.label ? gsLabel(gs) : ({ green: '正常', yellow: '观察', red: '需要关注', gray: '不可判断' }[light] || (entry.ended ? '已结束' : '状态未提供'))
          return h('div', { key: node.taskKey, className: 'sentinel-tree-node' },
            h('div', { 'data-gs-row': node.taskKey, style: { paddingInlineStart: `${Math.min(node.depth || 0, 8) * 8}px` }, ...(pathOnly ? { 'data-gs-path': node.taskKey } : {}) },
              pathOnly ? h('span', { className: 'sentinel-node-name' }, text(task?.title) || node.sessionId || '外部执行', ' · 所属路径') :
                h('button', { type: 'button', 'data-gs-node': node.taskKey, 'data-task-key': node.taskKey, 'data-gs-depth': node.depth,
                  'aria-pressed': selected === node.taskKey, onClick: () => select(selected === node.taskKey ? null : node.taskKey) },
                  h('span', { className: 'sentinel-node-name' }, text(task?.title) || node.sessionId || '外部执行'),
                  h('small', { 'data-gs-light': light, 'data-sentinel-light': light }, node.external ? '外部执行 · ' : '', observed ? '' : '当前无法确认 · 上次观察：', label)),
              !pathOnly && (light !== 'green' || selected === node.taskKey) && gs?.reason && !(snapshot.sources.find(source => source.key === task?.sourceKey)?.status === 'disconnected' && gs.reason.startsWith('来源失联')) && h('small', { 'data-gs-basis': node.taskKey, className: 'sentinel-node-basis' }, observed ? '' : '上次观察依据：', text(gs.reason).slice(0, 160), gs.reason.length > 160 ? '…' : ''),
              issuesOnly && !pathOnly && task && h(IssueEvidence, { task, gs, snapshot }),
              !issuesOnly && pathOnly && entry.ended && task && h('details', { 'data-gs-history': true }, h('summary', null, '已结束节点'),
                h('button', { type: 'button', 'data-gs-node': node.taskKey, 'data-task-key': node.taskKey, 'aria-pressed': selected === node.taskKey,
                  onClick: () => select(selected === node.taskKey ? null : node.taskKey) }, h('span', { className: 'sentinel-node-name' }, text(task.title) || node.sessionId), ' · 已结束'))),
            children.length > 0 && h('details', { 'data-gs-branch': node.taskKey, open: children.some(child => child.rank < 3) },
              h('summary', null, '委派后代'), children.filter(child => child.active).map(render),
              children.some(child => !child.active) && h('details', { 'data-gs-history': true }, h('summary', null, '已结束历史'), children.filter(child => !child.active).map(render))))
        }
        return h('nav', { 'aria-label': overview ? '选择观察任务' : '选择检查节点', className: 'sentinel-tree' },
          tree.filter(entry => entry.active).map(render),
          tree.some(entry => !entry.active) && h('details', { 'data-gs-history': true }, h('summary', null, '已结束历史'), tree.filter(entry => !entry.active).map(render)),
          !issuesOnly && !tree.some(entry => entry.active) && h('p', { 'data-gs-empty': true }, observed ? hideNormal ? '没有需要关注的节点。' : '当前范围没有活动节点。' : hideNormal ? '上次观察中没有需要关注的节点；当前无法确认。' : '上次观察中没有活动节点；当前无法确认。'))
      }
      function GSView({ sessionId, placement = 'above', visible = true, signal, useTabInfo }) {
        const [open, setOpen] = useState(false), [selected, select] = useState(null), [wholeTree, setWholeTree] = useState(false), [hideNormal, setHideNormal] = useState(false), [monitorTab, setMonitorTab] = useState('issues')
        const [intersects, setIntersects] = useState(!document.defaultView?.IntersectionObserver)
        const tab = useTabInfo ? useTabInfo().tab : { visible, signal }, tabSignal = signal || tab.signal, entryVisible = Boolean(visible && tab.visible && intersects && !tabSignal?.aborted)
        const current = useMonitor(open && entryVisible, tabSignal), snapshot = current.snapshot
        const summary = useGSSummary(sessionId, entryVisible && (!open || Boolean(current.error)), tabSignal)
        const anchor = useRef(null), openedVersion = useRef(0)
        const scope = snapshot?.hierarchyContext?.scopes?.find(item => item.sessionId === sessionId && item.sourceKey === snapshot.adapter?.sourceKey)
        const group = snapshot?.hierarchyContext?.groups?.find(item => item.key === scope?.groupKey)
        const nodes = group ? wholeTree ? group.nodes : group.nodes.slice(scope.start, scope.end) : []
        const coverage = wholeTree ? group?.coverage : scope?.coverage
        const focused = selected || nodes.find(node => ['red', 'yellow'].includes(node.light ?? node.gs?.light))?.taskKey
          || nodes.find(node => node.state !== 'ended' && (!hideNormal || (node.light ?? node.gs?.light) !== 'green'))?.taskKey
          || (!hideNormal && nodes[0]?.taskKey)
        const focusedTask = snapshot?.tasks.find(task => task.key === focused), focusedGS = nodes.find(node => node.taskKey === focused)?.gs
        const freshTree = Boolean(open && scope && current.snapshotVersion > openedVersion.current && !current.error && !snapshot.adapter?.error)
        const treeCurrent = { ...current, error: current.error || (open && !freshTree ? '正在读取完整检查；已有树为上次观察' : '') }
        const cached = Boolean(snapshot && (!connectionScope?.active || treeCurrent.error || snapshot.adapter?.error))
        const entryCounts = freshTree ? scope.counts : summary.value?.counts || scope?.counts
        const entryContext = freshTree ? '' : summary.error ? summary.value || scope ? '上次读取' : '摘要不可用' : summary.value?.gsStatus?.supported === false ? '摘要不支持' : summary.value?.sourceStatus !== 'connected' ? '来源未连接' : !summary.value?.scopeKnown ? '范围未确认' : ''
        const close = () => { setOpen(false); anchor.current?.querySelector('[data-gs-entry]')?.focus() }
        useLayoutEffect(() => {
          const Observer = document.defaultView?.IntersectionObserver
          if (!visible || !tab.visible || !Observer || !anchor.current) return
          const observer = new Observer(entries => setIntersects(entries.some(entry => entry.isIntersecting)))
          observer.observe(anchor.current); entryObservers++
          return () => { observer.disconnect(); entryObservers--; setIntersects(false) }
        }, [visible, tab.visible])
        useEffect(() => { if (!entryVisible || tabSignal?.aborted) setOpen(false) }, [entryVisible, tabSignal])
        useEffect(() => { if (open && entryVisible && !snapshotRequest) void pollSnapshot() }, [open, entryVisible])
        useLayoutEffect(() => {
          if (!open) return
          anchor.current?.querySelector('[role=dialog] button')?.focus()
          const viewport = document.defaultView
          const place = () => {
            const popup = anchor.current?.querySelector('[role=dialog]')
            if (!popup) return
            popup.style.left = ''; popup.style.right = ''
            const bounds = popup.getBoundingClientRect(), origin = anchor.current.getBoundingClientRect()
            if (bounds.left < 8) { popup.style.right = 'auto'; popup.style.left = `${8 - origin.left}px` }
            else if (bounds.right > viewport.innerWidth - 8) popup.style.right = `${origin.right - viewport.innerWidth + 8}px`
          }
          place()
          const outside = event => { if (!anchor.current?.contains(event.target)) setOpen(false) }
          const escape = event => { if (event.key === 'Escape') close() }
          const hidden = () => { if (document.visibilityState === 'hidden') setOpen(false) }
          document.addEventListener('pointerdown', outside); document.addEventListener('keydown', escape); document.addEventListener('visibilitychange', hidden); viewport.addEventListener('resize', place); viewListeners += 4
          return () => { document.removeEventListener('pointerdown', outside); document.removeEventListener('keydown', escape); document.removeEventListener('visibilitychange', hidden); viewport.removeEventListener('resize', place); viewListeners -= 4 }
        }, [open])
        return h('span', { className: 'sentinel-gs', ref: anchor },
          h('button', { type: 'button', 'data-gs-entry': sessionId, 'aria-label': `GS 当前会话检查${entryContext ? `；${entryContext}，当前无法确认` : ''}`, 'aria-expanded': open,
            onClick: () => { if (!open) openedVersion.current = state.snapshotVersion; setOpen(value => !value); select(null); setWholeTree(false) } }, 'GS ', entryContext && h('span', { 'data-gs-count-context': true }, '· ', entryContext, ' '), h(GSCounts, { counts: entryCounts })),
          open && h('section', { className: `sentinel-gs-popup sentinel-gs-${placement}`, role: 'dialog', 'aria-label': 'GS 当前会话检查', 'data-gs-scope': wholeTree ? scope?.rootSessionId : sessionId },
            h(MonitorHeader, { scope: wholeTree ? '主会话整棵树' : '当前会话及委派后代', tab: monitorTab, change: setMonitorTab,
              counts: wholeTree ? group?.counts : scope?.counts, close },
              cached && scope && h('small', { 'data-gs-count-context': true }, '上次读取计数 · 当前无法确认')),
            h('section', { role: 'tabpanel', 'aria-label': '当前问题', 'data-gs-issues': true, hidden: monitorTab !== 'issues' },
              monitorTab === 'issues' && freshTree && snapshot && nodes.some(node => ['red', 'yellow'].includes(node.light ?? node.gs?.light ?? snapshot.tasks.find(task => task.key === node.taskKey)?.gs?.light)) &&
                h(ObservationTree, { nodes, snapshot, current: treeCurrent, selected, select: key => { select(key); setMonitorTab('details') }, hideNormal: true, issuesOnly: true })),
            h('section', { role: 'tabpanel', 'aria-label': '详情', hidden: monitorTab !== 'details' }, monitorTab === 'details' && h(React.Fragment, null,
              focusedTask && (!hideNormal || focusedTask.gs?.light !== 'green') && h(ObservationDetail, { key: focusedTask.key, task: focusedTask, gs: focusedGS || focusedTask.gs, snapshot, current: treeCurrent }),
              h(HideNormal, { value: hideNormal, change: setHideNormal }),
              scope && h('p', null, scope.path.map(item => item.title).join(' / ')),
              scope?.rootSessionId !== sessionId && scope?.rootSessionId && h('button', { type: 'button', 'data-gs-return-root': true, onClick: () => { setWholeTree(value => !value); select(null) } }, wholeTree ? '返回当前会话范围' : '返回主会话整棵树'),
              current.error && h('p', { role: 'alert' }, current.error),
              summary.value?.gsStatus?.supported === false && h('p', null, '共享核心尚不支持轻量 GS 摘要；关闭浮层时计数不可判断。'),
              snapshot?.adapter?.gsStatus?.supported === false && h('p', null, '共享核心尚不支持 GS 状态与等待规则；相关检查覆盖不可判断。'),
              !scope && h('p', null, snapshot ? '当前会话的层级材料尚未取得；覆盖不可判断。' : '正在读取当前会话；覆盖不可判断。'),
              scope && !coverage?.complete && h('p', null, '层级覆盖不完整；未知后代数量不估算。'),
              snapshot?.sources.find(source => source.key === scope?.sourceKey)?.status === 'disconnected' && h('p', { 'data-gs-disconnected': true }, '来源失联；所属节点当前不可判断。'),
              coverage?.branchGaps?.length > 0 && h('details', null, h('summary', null, '查看层级缺口'), coverage.branchGaps.map((gap, index) => h('p', { key: index }, gap.sessionId, '：', gap.reason))),
              snapshot && h('details', { 'data-gs-scope-tree': true }, h('summary', null, '代理与所属会话'),
                h(ObservationTree, { nodes, snapshot, current: treeCurrent, selected: focused, select, hideNormal }))))))
      }
      function GSFooter({ sessionId, visible = true, signal, useTabInfo }) {
        const anchor = useRef(null), [usable, setUsable] = useState(false)
        const tab = useTabInfo ? useTabInfo().tab : { visible, signal }
        useLayoutEffect(() => {
          if (!sessionId || !visible || !tab.visible || signal?.aborted || tab.signal?.aborted) { setUsable(false); return }
          const ancestors = []
          for (let node = anchor.current; node; node = node.parentElement) ancestors.push(node)
          const viewport = document.defaultView
          let active = true
          const update = () => active && setUsable(ancestors.length > 0 && ancestors.every(node => {
            const style = viewport.getComputedStyle(node)
            return !node.hidden && style.display !== 'none' && !['hidden', 'collapse'].includes(style.visibility)
          }))
          update()
          const observer = new viewport.MutationObserver(update)
          for (const node of ancestors) observer.observe(node, { attributes: true, attributeFilter: ['class', 'style', 'hidden'] })
          viewport.addEventListener('resize', update); footerObservers++
          const stop = () => { if (!active) return; active = false; observer.disconnect(); viewport.removeEventListener('resize', update); footerObservers-- }
          const cancel = () => { stop(); setUsable(false) }
          signal?.addEventListener('abort', cancel, { once: true }); if (tab.signal !== signal) tab.signal?.addEventListener('abort', cancel, { once: true })
          return () => { stop(); signal?.removeEventListener('abort', cancel); if (tab.signal !== signal) tab.signal?.removeEventListener('abort', cancel) }
        }, [sessionId, visible, tab.visible, signal, tab.signal])
        useLayoutEffect(() => {
          if (!sessionId || !usable) return
          visibleFooters.set(sessionId, (visibleFooters.get(sessionId) || 0) + 1); footerChanged()
          return () => { const count = (visibleFooters.get(sessionId) || 1) - 1; if (count) visibleFooters.set(sessionId, count); else visibleFooters.delete(sessionId); footerChanged() }
        }, [sessionId, usable])
        return sessionId ? h('span', { ref: anchor }, usable && h(GSView, { key: sessionId, sessionId, visible, signal, useTabInfo })) : null
      }
      function GSHeader({ sessionId, visible, signal, useTabInfo }) {
        const [, update] = useState(0)
        useEffect(() => { const changed = () => update(value => value + 1); footerListeners.add(changed); return () => footerListeners.delete(changed) }, [])
        return sessionId && !visibleFooters.has(sessionId) ? h(GSView, { key: sessionId, sessionId, placement: 'below', visible, signal, useTabInfo }) : null
      }
      function Panel({ useTabInfo } = {}) {
        const tab = useTabInfo ? useTabInfo().tab : { visible: false }
        const current = useMonitor(tab.visible, tab.signal), snapshot = current.snapshot
        const [selected, select] = useState(null), [operation, setOperation] = useState(''), [hideNormal, setHideNormal] = useState(false), [monitorTab, setMonitorTab] = useState('issues')
        const tasks = snapshot?.tasks || []
        const sessionGroups = snapshot?.hierarchyContext?.schemaVersion === 1 ? snapshot.hierarchyContext.groups || [] : []
        const groupedTasks = new Set(sessionGroups.flatMap(group => group.nodes.map(node => node.taskKey)))
        const ungroupedTasks = tasks.filter(task => !groupedTasks.has(task.key))
        const visibleUngroupedTasks = ungroupedTasks.filter(task => !hideNormal || task.gs?.light !== 'green')
        const globalCounts = sessionGroups.reduce((total, group) => {
          for (const color of Object.keys(total)) total[color] += group.counts?.[color] || 0
          return total
        }, { green: 0, yellow: 0, red: 0, gray: 0 })
        let globalRunning = sessionGroups.reduce((sum, group) => sum + (group.runningCount || 0), 0)
        for (const task of ungroupedTasks) {
          if (task.gs?.active !== false && Object.hasOwn(globalCounts, task.gs?.light)) { globalCounts[task.gs.light]++; globalRunning++ }
        }
        const groupRank = group => group.counts?.red ? 0 : group.counts?.yellow ? 1 : group.counts?.gray || !group.coverage?.complete ? 2 : 3
        const visibleGroups = sessionGroups.filter(group => !hideNormal || groupRank(group) < 3 || group.nodes.some(node => node.state === 'ended')).sort((a, b) => groupRank(a) - groupRank(b))
        const task = tasks.find(item => item.key === selected) || tasks.find(item => ['red', 'yellow'].includes(item.gs?.light)) || tasks.find(item => item.status === 'running') || tasks[0]
        const overview = snapshot?.overview || {}
        async function action(method) {
          try { await call(method); setOperation(''); await pollSnapshot() }
          catch (error) { setOperation(errorText(error)) }
        }
        const paused = snapshot?.adapter?.monitoring?.paused ?? snapshot?.monitoring?.state === 'paused'
        const currentObservation = connectionScope?.active && !current.error && !snapshot?.adapter?.error
        const observedStatus = item => {
          const status = taskStatus[item.status] || '状态未知'
          return currentObservation ? status : `当前无法确认；上次状态：${status}`
        }
        const observedActivity = item => {
          const activity = text(item.activity?.summary) || '活动摘要未提供'
          return currentObservation ? activity : `历史活动：${activity}`
        }
        return h('section', { className: 'sentinel-overview', 'aria-label': 'Sentinel 执行监控总览', 'data-gs-snapshot-time': snapshot?.generatedAt },
          h(MonitorHeader, { scope: 'Sentinel · 全局总览', tab: monitorTab, change: setMonitorTab, counts: globalCounts, overview: true },
            h('div', { 'data-gs-overview-context': true }, snapshot && !currentObservation && h('small', { 'data-gs-count-context': true }, '上次读取计数 · 当前无法确认'),
              h('small', null, '会话 ', snapshot?.hierarchyContext?.schemaVersion === 1 ? sessionGroups.filter(group => group.rootSessionId).length : '未知',
                ' · 活动代理 ', snapshot?.hierarchyContext?.schemaVersion === 1 ? globalRunning : overview.runningTasks ?? '未知',
                ' · 关注 ', snapshot?.hierarchyContext?.schemaVersion === 1 ? globalCounts.red : overview.attentionTasks ?? '未知'))),
          h('section', { role: 'tabpanel', 'aria-label': '当前问题', 'data-gs-issues': true, hidden: monitorTab !== 'issues' }, monitorTab === 'issues' && currentObservation && h(React.Fragment, null,
            sessionGroups.filter(group => group.nodes.some(node => ['red', 'yellow'].includes(node.light ?? node.gs?.light ?? tasks.find(task => task.key === node.taskKey)?.gs?.light))).map(group => {
              const main = tasks.find(task => group.nodes.some(node => node.sessionId === group.rootSessionId && node.taskKey === task.key))
              return h('section', { key: group.key, 'data-sentinel-session-group': group.key },
                h('strong', null, text(main?.title) || group.rootSessionId || '外部执行'),
                h(ObservationTree, { nodes: group.nodes, snapshot, current, selected, select: key => { select(key); setMonitorTab('details') }, hideNormal: true, overview: true, issuesOnly: true }))
            }),
            ungroupedTasks.filter(item => ['red', 'yellow'].includes(item.gs?.light)).map(item => h('article', { key: item.key, 'data-gs-row': item.key },
              h('button', { type: 'button', onClick: () => { select(item.key); setMonitorTab('details') } }, text(item.title) || item.id),
              h('small', { 'data-gs-basis': item.key, className: 'sentinel-node-basis' }, text(item.gs?.reason)), h(IssueEvidence, { task: item, gs: item.gs, snapshot }))))),
          h('section', { role: 'tabpanel', 'aria-label': '详情', hidden: monitorTab !== 'details' }, monitorTab === 'details' && h(React.Fragment, null,
          task && (!hideNormal || task.gs?.light !== 'green') && h(ObservationDetail, { key: task.key, task, snapshot, current }),
          current.error && h('p', { role: 'alert', className: 'sentinel-attention' }, current.error),
          snapshot?.adapter?.gsStatus?.supported === false && h('p', null, '共享核心尚不支持 GS 状态与等待规则；相关检查覆盖不可判断。'),
          h('p', null, '观察覆盖：已接入和预期后台的范围；整机来源发现完整性未确认。'),
          ungroupedTasks.length > 0 && h('p', null, '另有未分组观察对象；会话及后代数量不可判断。'),
          sessionGroups.some(group => !group.coverage?.complete) && h('p', { 'data-gs-coverage': true }, '层级覆盖有缺口；未知后代数量不估算。'),
          snapshot?.sources.some(source => source.status === 'disconnected') && h('p', { 'data-gs-disconnected': true }, '来源失联：',
            snapshot.sources.filter(source => source.status === 'disconnected').map(source => text(source.descriptor?.name) || '来源名称未知').join('、'), '；所属节点当前不可判断。'),
          h(HideNormal, { value: hideNormal, change: setHideNormal }),
          h('details', { 'data-gs-scope-tree': true }, h('summary', null, '代理与所属会话'),
          h('nav', { 'aria-label': '会话分组' }, visibleGroups.map(group => {
            const main = tasks.find(task => group.nodes.some(node => node.sessionId === group.rootSessionId && node.taskKey === task.key))
            return h('section', { key: group.key, 'data-sentinel-session-group': group.key },
              h('details', { 'data-gs-group-tree': true, open: group.nodes.some(node => node.taskKey === selected) || groupRank(group) < 3 && ((group.runningCount || 0) > 0 || group.nodes.some(node => node.state !== 'ended' && node.light !== null)) },
                h('summary', null, h('span', { className: 'sentinel-node-name' }, text(main?.title) || group.rootSessionId || '外部执行'),
                  !currentObservation && h('small', { 'data-gs-count-context': true }, '上次读取计数 · 当前无法确认'),
                  h('small', null, '活动代理 ', group.runningCount ?? '未知', ' · 关注 ', group.counts?.red ?? '未知'), h(GSCounts, { counts: group.counts }),
                  !group.coverage?.complete && h('small', null, '层级覆盖不完整')),
                group.coverage?.branchGaps?.length > 0 && h('details', null, h('summary', null, '查看层级缺口'), group.coverage.branchGaps.map((gap, index) => h('p', { key: index }, gap.sessionId, '：', gap.reason))),
                h(ObservationTree, { nodes: group.nodes, snapshot, current, selected: selected || task?.key, select, hideNormal, overview: true })))
          })),
          snapshot?.hierarchyContext?.schemaVersion === 1 && !visibleGroups.length && !visibleUngroupedTasks.length && h('p', { 'data-gs-empty': true }, currentObservation ? hideNormal ? '没有需要关注的节点。' : '目前没有活动会话。' : hideNormal ? '上次观察中没有需要关注的节点；当前无法确认。' : '上次观察中没有活动会话；当前无法确认。'),
          h('nav', { 'aria-label': '未分组观察任务' },
            visibleUngroupedTasks.map(item => h('button', { key: item.key, type: 'button',
              'aria-pressed': item.key === task?.key, onClick: () => select(item.key) },
            text(item.title) || item.id, h('small', null, observedStatus(item), ' · ',
              text(snapshot.sources.find(entry => entry.key === item.sourceKey)?.descriptor?.name) || '来源未知'),
            h('small', null, observedActivity(item)))))),
          snapshot && !tasks.length && h('p', null, '已接入来源目前没有已知任务。'),
          h('details', { 'data-gs-monitor-diagnostics': true }, h('summary', null, '观察与诊断'),
          h('div', { className: 'sentinel-controls' },
            h('button', { type: 'button', onClick: () => action(paused ? 'resume' : 'pause') }, paused ? '继续分析' : '暂停分析')),
          operation && h('p', { role: 'alert' }, operation),
          h('small', null, current.notificationError || current.notification || notificationStatus()),
          h('small', null, '暂停期间仍采集；分析和通知暂停。'),
          h('small', null, '关闭面板仍采集；通知要求前端客户端继续存活。'),
          snapshot && h('small', null, '读取时间：', new Date(snapshot.generatedAt).toLocaleTimeString()),
          snapshot && h('details', null, h('summary', null, '查看来源与连接缺口'), snapshot.sources.map(item => h('p', { key: item.key },
            text(item.descriptor?.name) || '来源名称未知', ' · ', item.status === 'connected' ? '已连接' : '连接状态未知或已断开',
            ' · ', [...new Set(item.gaps || [])].map(gapText).join('；'), item.disconnectReason ? ` · ${item.disconnectReason}` : ''))),
          snapshot?.monitoring?.pendingCancelledRequests > 0 && h('p', null, '已取消的分析仍有 ', snapshot.monitoring.pendingCancelledRequests, ' 个底层请求可能晚到；结果将忽略。'),
          h(SourcePlan, { plan: current.sourcePlan, sources: snapshot?.sources || [] }),
          h('details', null, h('summary', null, '查看此客户端资源计数'), h('pre', null, JSON.stringify(diagnostics(), null, 2))))),
          h('footer', null, '本地资料保留 24 小时。告警是有依据的疑点；模型质量仍待独立验收。')))
      }
      const inject = ['slots']
      function apply(ctx) {
        const style = document.createElement('style')
        style.textContent = '.sentinel-overview{padding:14px;font:13px/1.6 system-ui;color:var(--dsw-alias-label-primary);overflow-wrap:anywhere}.sentinel-overview p{margin:8px 0}.sentinel-overview nav{display:flex;flex-direction:column;gap:6px;margin:14px 0}.sentinel-overview button{background:var(--dsw-alias-bg-layer-2);color:inherit;border:1px solid var(--dsw-alias-border-l2);border-radius:6px;padding:8px;text-align:left;cursor:pointer}.sentinel-overview button[aria-pressed=true]{font-weight:600;border-color:currentColor}.sentinel-overview small,.sentinel-overview footer{display:block;font-size:11px;opacity:.8}.sentinel-overview article{border-top:1px solid var(--dsw-alias-border-l2);padding-top:8px}.sentinel-overview details{margin:10px 0}.sentinel-overview summary{cursor:pointer}.sentinel-overview label{display:block;margin:8px 0}.sentinel-overview input,.sentinel-overview select,.sentinel-overview textarea{width:100%;box-sizing:border-box;background:var(--dsw-alias-bg-layer-2);color:inherit;padding:7px;border:1px solid var(--dsw-alias-border-l2);border-radius:5px}.sentinel-controls{display:flex;gap:6px;flex-wrap:wrap}.sentinel-attention{color:#b97925}.sentinel-overview footer{margin-top:18px}'
        style.textContent += '.sentinel-gs{position:relative;display:inline-flex;font:12px/1.4 system-ui;color:var(--dsw-alias-label-primary)}.sentinel-gs button{font:inherit;color:inherit;background:var(--dsw-alias-bg-layer-2);border:1px solid var(--dsw-alias-border-l2);border-radius:6px;padding:4px 6px;cursor:pointer}.sentinel-gs-counts{display:inline-flex;gap:6px}.sentinel-gs-counts>span{display:inline-flex;align-items:center;gap:3px}.sentinel-gs-counts i{width:6px;height:6px;border-radius:50%;background:currentColor}.sentinel-gs-green{color:#378458}.sentinel-gs-yellow{color:#b97925}.sentinel-gs-red{color:#c44b4b}.sentinel-gs-gray{color:#7a8190}.sentinel-gs-popup{box-sizing:border-box;overflow-wrap:anywhere;position:absolute;bottom:calc(100% + 6px);right:0;z-index:1000;width:min(360px,85vw);max-height:60vh;overflow:auto;padding:12px;border:1px solid var(--dsw-alias-border-l2);border-radius:8px;background:var(--dsw-alias-bg-layer-2);box-shadow:0 6px 24px #0003}.sentinel-gs-popup.sentinel-gs-below{bottom:auto;top:calc(100% + 6px)}.sentinel-gs-popup header{display:flex;justify-content:space-between;gap:8px}.sentinel-gs-popup nav{display:flex;flex-direction:column;gap:4px;margin:8px 0}.sentinel-gs-popup small{display:block;font-size:12px}.sentinel-gs-popup p{font-size:12px;margin:6px 0}.sentinel-gs-popup article{border-top:1px solid var(--dsw-alias-border-l2);padding-top:8px}'
        style.textContent += '.sentinel-tree,.sentinel-tree-node,[data-gs-row]{min-width:0;max-width:100%;box-sizing:border-box}.sentinel-tree button{width:100%;min-width:0;text-align:left;overflow-wrap:anywhere;box-sizing:border-box}.sentinel-node-name{font-size:13px;line-height:1.5}.sentinel-overview .sentinel-node-basis,.sentinel-gs-popup .sentinel-node-basis{font-size:12px;line-height:1.5;opacity:1;color:var(--dsw-alias-label-primary)}.sentinel-gs-counts{flex-wrap:wrap}.sentinel-hide-normal{display:flex;align-items:center;gap:6px;font-size:12px;margin:8px 0}.sentinel-overview .sentinel-hide-normal input{width:auto}.sentinel-tree details{margin:4px 0}.sentinel-tree summary{font-size:12px;cursor:pointer}.sentinel-tree article{font-size:12px;overflow-wrap:anywhere}.sentinel-gs-popup button:focus-visible,.sentinel-overview button:focus-visible{outline:2px solid currentColor;outline-offset:2px}.sentinel-gs-popup header>*{min-width:0}'
        style.textContent += '.sentinel-monitor-header{position:sticky;top:0;z-index:2;background:var(--dsw-alias-bg-layer-2);padding:8px 0;margin:0 0 12px;min-width:0}.sentinel-monitor-title,.sentinel-monitor-lamps{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.sentinel-monitor-title{justify-content:space-between}.sentinel-monitor-header .sentinel-monitor-tabs{display:flex;flex-direction:row;gap:6px;margin:8px 0 0}.sentinel-monitor-tabs button{flex:1;text-align:center}.sentinel-monitor-tabs [aria-selected=true]{font-weight:600;border-color:currentColor}.sentinel-gs-popup .sentinel-monitor-header{display:block}.sentinel-issue-evidence small{font-size:11px;line-height:1.45;margin:3px 0;overflow-wrap:anywhere}.sentinel-issue-evidence [data-sentinel-citation]{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.sentinel-focus-summary{border-left:3px solid var(--dsw-alias-border-l2);padding-left:10px}.sentinel-focus-summary>strong{font-size:15px}.sentinel-monitor-header .sentinel-gs-counts{flex-wrap:nowrap}.sentinel-gs-popup [role=tabpanel],.sentinel-overview [role=tabpanel]{min-width:0}[data-gs-issues]:empty{min-height:40px}'
        document.head.appendChild(style)
        ctx.effect(() => () => { style.remove() })
        ctx.inject(['remote', 'typert'], child => {
          child.effect(async () => {
            try { return await child.remote.$mount({ package: id, descriptors }) }
            catch (error) { notifyState({ error: `观察接口挂载失败：${errorText(error)}` }); return () => {} }
          })
          child.inject(['remote.sentinel'], connection => {
            const scope = { remote: connection.get('remote.sentinel'), active: true, abort: new AbortController(), pending: new Set() }
            connectionScope = scope
            notifyState({ notification: notificationStatus(), notificationError: '', config: null })
            for (const connect of settingsBindings) connect(scope)
            for (const binding of viewBindings.values()) attachView(binding)
            for (const binding of summaryBindings.values()) { binding.update(previous => ({ ...previous, error: '正在读取 GS 摘要' })); attachSummary(binding) }
            void pollNotifications(); reconcileSnapshot(); reconcileSummaries()
            notificationTimer = setInterval(() => { void pollNotifications() }, 1000)
            connection.effect(() => () => {
              if (!scope.active) return
              scope.active = false; scope.abort.abort(); for (const abort of scope.pending) abort.abort()
              for (const [notification, callback] of notificationCallbacks) if (callback.scope === scope) releaseNotification(notification)
              if (connectionScope === scope) {
                clearInterval(notificationTimer); notificationTimer = undefined; connectionScope = undefined
                for (const connect of settingsBindings) connect(undefined)
                reconcileSnapshot(); notifyState({ error: '观察连接已关闭', snapshot: null, sourcePlan: null, config: null })
                for (const binding of summaryBindings.values()) binding.update(previous => ({ ...previous, error: '观察连接已关闭' }))
                for (const binding of summaryBindings.values()) detachSummary(binding)
                reconcileSummaries()
                for (const binding of viewBindings.values()) detachView(binding)
              }
            })
          })
        })
        ctx.slots.inject('conversation.composer.dock', () => ctx.slots.register({ name: 'conversation.composer.dock', id: `${id}.gs`, order: 100, label: 'GS' }, GSFooter))
        ctx.slots.inject('settings.section', () => ctx.slots.register({ name: 'settings.section', id, order: 85, label: () => 'Sentinel', inject: () => ({}) }, SettingsPanel))
        ctx.slots.inject('conversation.session.header.utilities', () => ctx.slots.register({ name: 'conversation.session.header.utilities', id: `${id}.gs`, order: 100, label: 'GS' }, GSHeader))
        ctx.inject(['sidebarRightTabs'], child => {
          const dispose = child.get('sidebarRightTabs').register({ id, kind: 'ghost-agent-sentinel', title: () => 'Sentinel',
            guide: [{ id, order: 85, title: () => 'Sentinel', description: () => '已接入后台的执行监控总览' }] })
          child.effect(() => () => dispose())
          child.slots.inject('sidebar.right.pane.tab', () => child.slots.register({ name: 'sidebar.right.pane.tab', key: id, inject: () => ({}) }, Panel))
        })
      }
      return { inject, apply, diagnostics, default: { inject, apply } }
    },
  })
})()
