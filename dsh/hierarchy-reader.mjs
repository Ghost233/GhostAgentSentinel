// Read-only directory discovery is driven by native changes, never by viewers.
export function createHierarchyReader(ctx, { observe, recordGap, signal, timeoutMs = 1000, maxSessions = 2000, maxReads = 32, maxColdReads = 32, maxEvents = 500, maxColdBytes = 262144, maxRoundEvents = 2000, maxRoundDirectoryEntries = 2000, maxRoundMs = 1000, maxDepth = 64 } = {}) {
  const queue = new Map(), cache = new Map(), parents = new Map(), pendingActivations = new Map()
  const day = 24 * 60 * 60 * 1000
  let closed = false, pending, controller, timer, coldReads = 0, released = 0, reads = 0, failures = 0, rounds = 0, peakRoundEvents = 0, peakRoundReads = 0, peakRoundMs = 0
  let pendingColdReads = 0, activeLeases = 0, lateLeasesReleased = 0, metadataRefusals = 0, unknownMetadataReads = 0, admittedPhysicalBytes = 0, coldObservationMs = 0, peakColdObservationMs = 0
  let boundedPageReads = 0, pagePhysicalBytes = 0, pageDecodedBytes = 0, pageDirectoryEntries = 0, peakRoundDirectoryEntries = 0, cursorReleaseFailures = 0
  const gap = reason => recordGap?.(reason)
  function releaseCursor(cursor) {
    if (!cursor) return
    try { ctx.get('sessionQuery')?.releaseObservationPageCursor?.(cursor) }
    catch { failures++; cursorReleaseFailures++; gap('冷目录续读游标释放失败；资源状态未确认') }
  }
  function dropCache(id) { releaseCursor(cache.get(id)?.pageCursor); cache.delete(id) }
  function sweep() {
    const cutoff = Date.now() - day
    for (const [id, value] of cache) if (value.at <= cutoff && ctx.get('agents')?.get(id)?.status !== 'running') dropCache(id)
    for (const [id, value] of parents) if (value.at <= cutoff) parents.delete(id)
    for (const [id, value] of pendingActivations) if (value.capturedAt <= cutoff) pendingActivations.delete(id)
    for (const [id, value] of queue) if (value.at <= cutoff) { queue.delete(id); gap('排队目录超过 24 小时；该分支尚未完成') }
  }
  function schedule(sessionId, depth = 0, at = Date.now()) {
    if (closed || typeof sessionId !== 'string' || !sessionId || queue.has(sessionId)) return
    if (depth > maxDepth) { gap('委派目录深度超过读取预算；更深分支尚未完成'); return }
    if (queue.size >= maxSessions) { gap('委派目录队列超过读取预算；未完成分支仍不可判断'); return }
    queue.set(sessionId, { depth, at })
  }
  function noteCatalog(sessionId, entry, at = entry?.observedAt ?? Date.now()) {
    const childId = entry?.childId ?? entry?.id
    if (closed || typeof sessionId !== 'string' || typeof childId !== 'string') return
    if (at <= Date.now() - day) { gap('目录所属依据超过 24 小时；旧关系未重新续期'); return }
    const existing = parents.get(childId)
    if (childId === sessionId || (existing && existing.sessionId !== sessionId)) { gap('委派目录包含循环或多个父节点；关系未覆盖'); return }
    parents.set(childId, { sessionId, at })
    for (const [identity, fact] of pendingActivations) if (fact.activation.id === childId) {
      const parent = ctx.get('agents')?.get(sessionId)
      if (parent || sessionId) { pendingActivations.delete(identity); observe(fact.type, { activation: fact.activation, parent, child: ctx.get('agents')?.get(childId), capturedAt: fact.capturedAt }) }
    }
    if (parents.size > maxSessions) { parents.delete(parents.keys().next().value); gap('目录所属索引超过预算；旧路径未覆盖') }
  }
  function noteActivation(type, activation, child, nativeParent) {
    if (closed || typeof activation?.id !== 'string' || typeof activation?.runId !== 'string') return
    const parentId = child?.session?.header?.parentSession ?? parents.get(activation.id)?.sessionId
    const parent = nativeParent ?? (parentId ? ctx.get('agents')?.get(parentId) : undefined)
    if (parent || parentId) { observe(type, { activation, parent, child }); return }
    const identity = `${activation.runId}/${type}`
    pendingActivations.set(identity, { type, activation: { id: activation.id, runId: activation.runId, ...(typeof activation.local === 'boolean' ? { local: activation.local } : {}) }, capturedAt: pendingActivations.get(identity)?.capturedAt ?? Date.now() })
    if (pendingActivations.size > maxSessions) { pendingActivations.delete(pendingActivations.keys().next().value); gap('原生执行父节点等待索引超过预算；未核对执行不可判断') }
  }
  function remember(sessionId, entry) {
    const previous = cache.get(sessionId)
    if (previous?.pageCursor !== entry.pageCursor) releaseCursor(previous?.pageCursor)
    cache.delete(sessionId); cache.set(sessionId, { ...entry, at: previous?.cursor === entry.cursor ? previous.at : Date.now() })
    while (cache.size > maxSessions) { dropCache(cache.keys().next().value); gap('目录增量索引超过保留预算；旧分支未持续覆盖') }
  }
  async function read(sessionId, round, queued) {
    const live = ctx.get('sessions')?.get(sessionId), previous = cache.get(sessionId)
    // A detached snapshot has no incremental carrier. Re-read only after it becomes live.
    if (!live && previous && !previous.pageHasMore) return
    if (live && previous?.cursor === live.seq - 1) return
    const remainingMs = maxRoundMs - (performance.now() - round.started)
    if (remainingMs <= 0) { schedule(sessionId, queued.depth, queued.at); return }
    let lease, facts, started = Date.now()
    controller = new AbortController()
    const abort = () => controller?.abort(signal?.reason)
    signal?.addEventListener('abort', abort, { once: true })
    if (signal?.aborted) abort()
    const readMs = Math.max(1, Math.floor(Math.min(timeoutMs, remainingMs)))
    timer = setTimeout(() => controller?.abort(new Error('委派只读观察超过读取预算')), readMs)
    try {
      if (live) {
        const inheritedEventCount = live.inheritedEventCount ?? 0
        const after = Number.isSafeInteger(previous?.cursor) ? previous.cursor + 1 : 0
        const start = Math.max(after, inheritedEventCount), end = Math.min(live.seq, start + Math.min(maxEvents, maxRoundEvents - round.events)), cursor = end - 1
        const events = []
        for (let seq = start; seq < end; seq++) {
          const event = live.eventAt(seq)
          if (event) events.push(['user/message', 'approval/asked', 'approval/decided', 'subagent/catalog'].includes(event.type) ? event : { seq: event.seq, type: event.type, time: event.time })
        }
        round.events += end - start
        facts = { sessionId, header: live.header, inheritedEventCount, events,
          catalog: events.filter(event => event.type === 'subagent/catalog').map(event => ({ id: event.data?.childId, mode: event.data?.mode, observedAt: event.time })),
          cursor, readComplete: end === live.seq, gaps: end < live.seq ? ['原生目录页面尚未读完；剩余分支等待增量读取'] : [] }
      } else if (typeof ctx.get('sessionQuery')?.observeSessionPage === 'function') {
        if (round.coldReads >= maxColdReads) { schedule(sessionId, queued.depth, queued.at); return }
        const query = ctx.get('sessionQuery'), observationStarted = performance.now()
        coldReads++; round.coldReads++; pendingColdReads++
        try {
          lease = await query.observeSessionPage(sessionId, { cursor: previous?.pageCursor, signal: controller.signal,
            budget: { maxDirectoryEntries: Math.min(maxSessions, maxRoundDirectoryEntries - round.directoryEntries), maxPhysicalBytes: maxColdBytes, maxDecodedBytes: maxColdBytes, maxEvents: Math.min(maxEvents, maxRoundEvents - round.events), deadlineMs: readMs } })
          activeLeases++; boundedPageReads++
        } finally {
          pendingColdReads--; const elapsed = performance.now() - observationStarted; coldObservationMs += elapsed; peakColdObservationMs = Math.max(peakColdObservationMs, elapsed)
        }
        pagePhysicalBytes += lease.usage.physicalBytes; pageDecodedBytes += lease.usage.decodedBytes; pageDirectoryEntries += lease.usage.directoryEntries
        round.directoryEntries += lease.usage.directoryEntries
        round.events += lease.usage.events
        if (closed || controller.signal.aborted) { releaseCursor(lease.cursor); return }
        const cut = lease.inheritedEventCount, page = lease.events, own = cut === null ? [] : page.filter(event => event.seq >= cut)
        facts = { sessionId, header: lease.header, inheritedEventCount: cut, events: own,
          catalog: own.filter(event => event.type === 'subagent/catalog').map(event => ({ id: event.data?.childId, mode: event.data?.mode, observedAt: event.time })),
          cursor: page.at(-1)?.seq ?? previous?.cursor ?? -1, readComplete: lease.complete && !lease.hasMore && cut !== null,
          gaps: [...lease.gaps, ...(cut === null ? ['继承切点尚未取得；本页未用于自有材料或后代归属'] : []), ...(lease.hasMore ? ['冷目录页面尚未读完；剩余分支等待继续读取'] : [])] }
      } else {
        const reason = 'SDK 缺少有预算的只读会话分页；此冷分支未读取，后代不可判断'
        gap(reason)
        observe('session/observed', { sessionId, events: [], cursor: -1, readComplete: false, gaps: [reason] })
        remember(sessionId, { cursor: null }); return
      }
      if (closed || controller.signal.aborted) return
      if (Date.now() - started > readMs) { facts.readComplete = false; facts.gaps.push('同步目录回放超过读取预算；不能确认即时取消') }
      const entries = Array.isArray(facts.catalog) ? facts.catalog : []
      if (entries.length > maxSessions) { facts.catalog = entries.slice(0, maxSessions); facts.readComplete = false; facts.gaps.push('目录成员超过读取预算；剩余分支未完成') }
      const receipt = observe('session/observed', facts)
      const acceptedThrough = Number.isSafeInteger(receipt?.acceptedThrough) ? Math.min(facts.cursor, receipt.acceptedThrough) : facts.cursor
      remember(sessionId, { cursor: acceptedThrough, ...(lease?.source === 'stored' ? { pageCursor: lease.hasMore ? lease.cursor : undefined, pageHasMore: lease.hasMore } : {}) })
      if (lease?.source === 'stored' && lease.hasMore) schedule(sessionId, queued.depth, queued.at)
      if (lease?.source === 'stored' && !lease.hasMore) releaseCursor(lease.cursor)
      if (live && acceptedThrough < live.seq - 1) schedule(sessionId, queued.depth, queued.at)
      reads++
      for (const entry of entries.slice(0, maxSessions)) {
        if (typeof entry.id !== 'string') continue
        const parent = parents.get(entry.id)?.sessionId
        if (entry.id === sessionId || (parent && parent !== sessionId)) { gap('委派目录包含循环或多个父节点；关系未覆盖'); continue }
        noteCatalog(sessionId, entry)
        if (entry.mode === 'external') continue
        let ancestor = sessionId, seen = new Set([entry.id])
        while (ancestor && !seen.has(ancestor)) { seen.add(ancestor); ancestor = parents.get(ancestor)?.sessionId }
        if (ancestor) { gap('委派目录存在循环；分支关系不可判断'); continue }
        if (entry.observedAt > Date.now() - day) schedule(entry.id, queued.depth + 1, queued.at)
      }
    } catch (error) {
      failures++
      if (!closed) { gap(`委派目录未完成：${sessionId}：${controller.signal.aborted ? '只读观察已取消' : String(error?.code || error?.message || '读取失败').slice(0, 160)}`); remember(sessionId, { cursor: null }) }
    } finally {
      try { if (lease) { lease[Symbol.dispose](); released++; activeLeases--; if (closed || controller.signal.aborted) lateLeasesReleased++ } } catch { failures++; gap('只读观察句柄释放失败；资源状态未确认') }
      clearTimeout(timer); timer = undefined; signal?.removeEventListener('abort', abort); controller = undefined
    }
  }
  function drain() {
    if (closed || pending) return pending
    sweep()
    if (!queue.size) { if (pendingActivations.size) gap('原生执行缺少可核对的父目录；所属分支不可判断'); return }
    pending = (async () => {
      const round = { started: performance.now(), events: 0, directoryEntries: 0, coldReads: 0, reads: 0 }
      for (let count = 0; !closed && !signal?.aborted && queue.size && count < maxReads; count++) {
        if (round.events >= maxRoundEvents || round.directoryEntries >= maxRoundDirectoryEntries || performance.now() - round.started >= maxRoundMs) break
        const [sessionId, queued] = queue.entries().next().value; queue.delete(sessionId)
        round.reads++
        await read(sessionId, round, queued)
      }
      rounds++; peakRoundEvents = Math.max(peakRoundEvents, round.events); peakRoundReads = Math.max(peakRoundReads, round.reads); peakRoundMs = Math.max(peakRoundMs, performance.now() - round.started)
      peakRoundDirectoryEntries = Math.max(peakRoundDirectoryEntries, round.directoryEntries)
    })().finally(() => { pending = undefined })
    return pending
  }
  return { schedule, drain, noteCatalog, noteActivation, parentOf: id => parents.get(id)?.sessionId, diagnostics() { sweep(); return { queued: queue.size, cached: cache.size, parents: parents.size, pendingActivations: pendingActivations.size, pending: Number(Boolean(pending)), timers: Number(Boolean(timer)), coldReads, reads, released, failures, pendingColdReads, activeLeases, lateLeasesReleased, metadataRefusals, unknownMetadataReads, admittedPhysicalBytes, coldObservationMs, peakColdObservationMs, boundedPageReads, pagePhysicalBytes, pageDecodedBytes, pageDirectoryEntries, cursorReleaseFailures, rounds, peakRoundEvents, peakRoundDirectoryEntries, peakRoundReads, peakRoundMs,
    budgets: { timeoutMs, maxSessions, maxReads, maxColdReads, maxEvents, maxColdBytes, maxRoundEvents, maxRoundDirectoryEntries, maxRoundMs, maxDepth, retentionMs: day, nativeReplayBudgetEnforced: false }, closed } },
    close() { closed = true; controller?.abort(new Error('目录读取关闭')); clearTimeout(timer); timer = undefined; queue.clear(); for (const id of cache.keys()) dropCache(id); parents.clear(); pendingActivations.clear(); return Promise.resolve(pending).then(() => { if (cursorReleaseFailures) throw new Error('冷目录续读游标未确认释放') }) } }
}
