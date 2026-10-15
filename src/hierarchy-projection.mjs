const key = (...parts) => JSON.stringify(parts)
const emptyCounts = () => ({ green: 0, yellow: 0, red: 0, gray: 0 })

// Relations have already been checked against source-owned native evidence.
export function projectHierarchy(sources, tasks) {
  const groups = [], contexts = [], scopes = [], tasksByKey = new Map(tasks.map(task => [task.key, task]))
  for (const source of sources) {
    const context = source.hierarchyContext
    if (!context) continue
    const sourceReasons = [...new Set([...(source.descriptor?.gaps ?? []), ...(source.gapRecords ?? []).map(gap => gap.reason)])]
    contexts.push({ sourceKey: source.key, revision: context.revision, coverage: context.coverage })
    const parents = new Map(), externalParents = new Map(), children = new Map()
    for (const relation of context.relations) {
      if (relation.kind !== 'delegation') continue
      if (relation.childSessionId) {
        parents.set(relation.childSessionId, relation.parentSessionId)
        const list = children.get(relation.parentSessionId) ?? []
        list.push(relation.childSessionId); children.set(relation.parentSessionId, list)
      } else if (relation.childActivationRef) externalParents.set(key(relation.childActivationRef.taskId, relation.childActivationRef.lifecycleId), relation.parentSessionId)
    }
    const root = sessionId => { const seen = new Set(); while (parents.has(sessionId) && !seen.has(sessionId)) { seen.add(sessionId); sessionId = parents.get(sessionId) } return sessionId }
    const byRoot = new Map(), bySession = new Map(), external = new Map(), gapRoots = new Map()
    const rememberRoot = (id, rootSessionId) => { const roots = gapRoots.get(id) ?? new Set(); roots.add(rootSessionId); gapRoots.set(id, roots) }
    const activations = new Map(context.activations.map(item => [key(item.taskId, item.lifecycleId), item]))
    const bindings = new Map(context.instructionBindings.filter(item => item.phase === 'consumed' && item.origin !== 'runtime-notice').map(item => [key(item.taskId, item.lifecycleId), item]))
    for (const task of tasks) {
      if (task.sourceKey !== source.key) continue
      const identity = key(task.id, task.lifecycleId), activation = activations.get(identity)
      if (!activation) continue
      const parentSessionId = activation.sessionId ? parents.get(activation.sessionId) ?? null : externalParents.get(identity) ?? null
      const rootSessionId = root(activation.sessionId ?? parentSessionId)
      const groupKey = key(source.key, rootSessionId ?? task.key)
      let group = byRoot.get(groupKey)
      if (!group) { group = { key: groupKey, sourceKey: source.key, rootSessionId, nodes: [], runningCount: 0, counts: emptyCounts(), coverage: context.coverage }; byRoot.set(groupKey, group); groups.push(group) }
      task.hierarchy = { sessionId: activation.sessionId, parentSessionId, rootSessionId, runId: activation.runId, state: activation.state, instructionOrigin: task.instructionContext?.binding?.origin ?? bindings.get(identity)?.origin ?? 'unknown' }
      const node = { taskKey: task.key, ...task.hierarchy, external: activation.sessionId === null, gs: task.gs }
      rememberRoot(task.id, rootSessionId)
      if (activation.sessionId) rememberRoot(activation.sessionId, rootSessionId)
      const active = activation.state !== 'ended' && task.gs?.light != null
      node.light = active ? task.gs.light : null
      if (active) { group.runningCount++; group.counts[node.light]++ }
      if (activation.sessionId) {
        const list = bySession.get(activation.sessionId) ?? []; list.push(node); bySession.set(activation.sessionId, list)
      } else if (parentSessionId) {
        const list = external.get(parentSessionId) ?? []; list.push(node); external.set(parentSessionId, list)
      } else group.nodes.push({ ...node, depth: 0 })
    }
    for (const { sessionId } of context.sessions) if (parents.has(sessionId) && bySession.has(root(sessionId))) rememberRoot(sessionId, root(sessionId))
    const descendant = (sessionId, ancestor) => { const seen = new Set(); while (sessionId && !seen.has(sessionId)) { if (sessionId === ancestor) return true; seen.add(sessionId); sessionId = parents.get(sessionId) } return false }
    for (const group of byRoot.values()) {
      if (!group.rootSessionId) continue
      const stack = [{ sessionId: group.rootSessionId, depth: 0, exit: false }], visited = new Set(), ranges = new Map()
      while (stack.length) {
        const current = stack.pop()
        if (current.exit) { ranges.get(current.sessionId).end = group.nodes.length; continue }
        if (visited.has(current.sessionId)) continue
        visited.add(current.sessionId)
        ranges.set(current.sessionId, { start: group.nodes.length })
        group.nodes.push(...(bySession.get(current.sessionId) ?? []).map(node => ({ ...node, depth: current.depth })))
        group.nodes.push(...(external.get(current.sessionId) ?? []).map(node => ({ ...node, depth: current.depth + 1 })))
        stack.push({ ...current, exit: true })
        for (const sessionId of [...(children.get(current.sessionId) ?? [])].reverse()) stack.push({ sessionId, depth: current.depth + 1, exit: false })
      }
      const positions = new Map(group.nodes.map((node, index) => [tasksByKey.get(node.taskKey)?.id, index]))
      const prefix = [emptyCounts()]
      for (const node of group.nodes) { const count = { ...prefix.at(-1) }; if (node.light) count[node.light]++; prefix.push(count) }
      for (const [sessionId, range] of ranges) {
        const path = [], seen = new Set(); let ancestor = sessionId
        while (ancestor && !seen.has(ancestor) && path.length < 64) {
          seen.add(ancestor)
          const node = bySession.get(ancestor)?.at(-1), task = node && tasksByKey.get(node.taskKey)
          path.unshift({ sessionId: ancestor, title: task?.title || ancestor }); ancestor = parents.get(ancestor)
        }
        const branchGaps = context.coverage.branchGaps.filter(gap => {
          const target = ranges.get(gap.sessionId), position = positions.get(gap.sessionId), owners = gapRoots.get(gap.sessionId)
          if (!owners || owners.has(null)) return true
          if (target || position !== undefined) return Boolean(target && descendant(gap.sessionId, sessionId)) || position !== undefined && position >= range.start && position < range.end
          return owners.has(group.rootSessionId)
        })
        const scopeNodes = group.nodes.slice(range.start, range.end), activeNodes = scopeNodes.filter(node => node.light)
        const observedNodes = activeNodes.length ? activeNodes : scopeNodes
        for (const reason of sourceReasons) if (!observedNodes.length || observedNodes.some(node => node.gs?.coverage.gaps.includes(reason))) branchGaps.push({ sessionId, reason })
        if (ancestor) branchGaps.push({ sessionId, reason: '所属路径超过显示预算；保留根会话入口' })
        const counts = emptyCounts(); for (const color of Object.keys(counts)) counts[color] = prefix[range.end][color] - prefix[range.start][color]
        const coverage = { complete: !source.hierarchyCaptureBudgetExceeded && (context.coverage.complete || context.coverage.branchGaps.length > 0) && branchGaps.length === 0, branchGaps }
        if (sessionId === group.rootSessionId) group.coverage = coverage
        scopes.push({ key: key(source.key, sessionId), sourceKey: source.key, sessionId, groupKey: group.key, rootSessionId: group.rootSessionId,
          ...range, path, counts, coverage })
      }
    }
  }
  return { schemaVersion: 1, sources: contexts, groups, scopes }
}
