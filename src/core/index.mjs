import { createNativeOriginFences } from './native-origin-fences.mjs';
import { nativeForkIdentity } from '../fork-proof.mjs';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, chmod } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { chmodSync } from 'node:fs';
import { normalizeEnrollment } from '../enrollment-config.mjs';
import { validateHierarchy, projectHierarchy } from '../hierarchy.mjs';
import { createInstructionLookup, instructionContext, eventRef, referenceKey } from '../instruction-context.mjs';
import { gsStatus, userWait, childWait, waitExempts } from '../gs-status.mjs';
import { analysisTimeConditions } from '../analysis/index.mjs';
import { inferenceFailureInfo } from '../analysis/failure-info.mjs';
import { normalizeConversationMonitoring, normalizeCollectionOutcome, normalizeFailureScope, normalizeNativeNotificationEligibility, normalizeNativeNotificationQuery, normalizeNativeNotificationResult } from '../protocol.mjs';
import { createFailureReceipts, failureErrorInfo } from './failure-receipts.mjs';
import { createNativeNotificationStore } from './native-notifications.mjs';
import { createNativeCaptureProgress } from './native-capture-progress.mjs';

const DAY = 24 * 60 * 60 * 1000;
const CLAIM_MS = 60_000;
const GROUP_MS = 3_000;
const HIERARCHY_CAPTURE_LIMIT = 10_000;
const CONVERSATION_MONITORING_LIMIT = 1_000;
const HIERARCHY_CAPTURE_GAP = 'Hierarchy evidence identity retention budget exceeded';
const CHECK_IDS = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const copy = value => structuredClone(value);
const identity = (...parts) => JSON.stringify(parts);
const fingerprint = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const historical = event => event.data?.history === true || event.data?.inherited === true;
const unknownChecks = (reason, judgmentState = 'insufficient') => CHECK_IDS.map(id => ({ id, status: 'unknown', judgmentState, applicability: { state: 'unknown', reason, evidenceIds: [] }, reason, evidenceIds: [] }));

// This module owns monitoring state only. It never controls a source task or a model process.
export async function createCore({
  dataDir, analyzer = null, now = Date.now, cooldownMs = 60_000,
  repeatMs = 600_000, leaseMs = 15_000, idleGraceMs = 2_000, onIdle,
}) {
  await mkdir(dataDir, { recursive: true, mode: 0o700 });
  await chmod(dataDir, 0o700);
  const databasePath = join(dataDir, 'observations.sqlite');
  const db = new DatabaseSync(databasePath);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; CREATE TABLE IF NOT EXISTS records (collection TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(collection,key))');
  await chmod(databasePath, 0o600);
  for (const suffix of ['-wal', '-shm']) {
    try { chmodSync(databasePath + suffix, 0o600); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value');
  const remove = db.prepare('DELETE FROM records WHERE collection=? AND key=?');
  const beforeNativeWrite = db.prepare('SELECT value FROM records WHERE collection=? AND key=?');
  const failureStore = createFailureReceipts({ db, now });
  const rows = db.prepare('SELECT collection,key,value FROM records').all();
  const sources = new Map();
  const tasks = new Map();
  const events = new Map();
  const notifications = new Map();
  const conversationSettings = new Map();
  const conversationRevision = randomUUID();
  const collections = { sources, tasks, events, notifications, conversationSettings };
  let paused = false;
  let enrollment = { entries: [], configuredAt: null, managed: false };
  for (const row of rows) {
    const value = JSON.parse(row.value);
    if (row.collection === 'settings' && row.key === 'monitoring') paused = value.paused === true;
    else if (row.collection === 'settings' && row.key === 'enrollment') enrollment = { ...value, managed: value.managed ?? (value.configuredAt !== null) };
    else collections[row.collection]?.set(row.key, value);
  }
  const nativeProgress = createNativeCaptureProgress({ db, seed: (function* () {
    for (const source of sources.values()) if (source.descriptor.policy === 'chatgpt-main/1') for (const capture of source.hierarchyEvidenceCaptures ?? []) if (capture.nativeHistorical !== undefined) yield { registrationId: source.descriptor.registrationId, nativeRecordId: capture.nativeOriginId, firstCapturedAt: capture.capturedAt, locator: { sourceKey: source.key, taskId: capture.taskId, lifecycleId: capture.lifecycleId, eventId: capture.eventId } };
  })() });
  let nativeOrigins;
  try { nativeOrigins = createNativeOriginFences({ db }); } catch (error) { db.close(); throw error; }
  let closed = false;
  let active = null;
  let pendingCancelledRequests = 0;
  let cancelledRound = null;
  const analysisAdmission = () => {
    const unsettledCalls = (active ? 1 : 0) + pendingCancelledRequests;
    return { limit: 1, unsettledCalls, blocked: unsettledCalls >= 1 };
  };
  let sequence = Math.max(0, ...Array.from(events.values(), event => event.sequence));
  let hadConnection = true;
  let idleTimer = null;
  let wakeTimer = null;
  let idleNotified = false;
  const queue = [];
  const queued = new Set();
  const pendingPrunedTasks = new Set();
  const pendingPrunedSources = new Set();
  let nativeNotifications;
  let nativeJournal = null;
  const rememberNativeRow = (collection, key) => {
    if (!nativeJournal) return;
    const id = identity(collection, key);
    if (!nativeJournal.rows.has(id)) nativeJournal.rows.set(id, { collection, key, value: beforeNativeWrite.get(collection, key)?.value ?? null });
  };
  const save = (collection, value) => { rememberNativeRow(collection, value.key ?? value.id); if (collection === 'notifications') nativeNotifications?.touch(value); return write.run(collection, value.key ?? value.id, JSON.stringify(value)); };
  const erase = (collection, key) => { rememberNativeRow(collection, key); if (collection === 'notifications') nativeNotifications?.removed(notifications.get(key)); remove.run(collection, key); collections[collection].delete(key); };
  const taskEvents = (task, read) => read ? read.eventsByTask.get(task.key) ?? [] : Array.from(events.values()).filter(event => event.taskKey === task.key).sort((a, b) => a.capturedAt - b.capturedAt || a.sequence - b.sequence);
  const projectionRead = () => {
    const bySource = new Map(), eventsByTask = new Map();
    for (const task of tasks.values()) {
      if (!bySource.has(task.sourceKey)) bySource.set(task.sourceKey, { tasks: [], events: [], instruction: null });
      bySource.get(task.sourceKey).tasks.push(task);
      eventsByTask.set(task.key, []);
    }
    for (const event of events.values()) {
      eventsByTask.get(event.taskKey)?.push(event);
      bySource.get(tasks.get(event.taskKey)?.sourceKey)?.events.push(event);
    }
    for (const retained of eventsByTask.values()) retained.sort((a, b) => a.capturedAt - b.capturedAt || a.sequence - b.sequence);
    return { bySource, eventsByTask };
  };
  const connected = source => source?.status === 'connected';
  const findTask = (sourceKey, id, lifecycleId) => Array.from(tasks.values()).find(task => identity(task.sourceKey, task.id, task.lifecycleId) === identity(sourceKey, id, lifecycleId));
  const liveIdentity = task => task.status === 'running' && task.baselineConfirmed && connected(sources.get(task.sourceKey));
  const usable = task => liveIdentity(task) && (!(sources.get(task.sourceKey)?.hierarchyContext || sources.get(task.sourceKey)?.descriptor.policy === 'chatgpt-main/1') || task.requestId != null);
  const observationOnly = task => sources.get(task.sourceKey)?.descriptor.mode === 'observation-only';
  const conversationFor = task => {
    const source = sources.get(task.sourceKey);
    if (source?.descriptor.policy !== 'chatgpt-main/1') return null;
    const registrationId = source.descriptor.registrationId, taskId = task.id;
    const setting = conversationSettings.get(identity(registrationId, taskId));
    return { registrationId, taskId, paused: setting?.paused ?? false, revision: setting?.revision ?? fingerprint([conversationRevision, registrationId, taskId]) };
  };
  const conversationPaused = task => conversationFor(task)?.paused === true;
  const conversationScope = value => {
    const stored = conversationSettings.get(identity(value.registrationId, value.taskId));
    if (stored) return { registrationId: stored.registrationId, taskId: stored.taskId, paused: stored.paused, revision: stored.revision };
    const scoped = [...tasks.values()].find(task => task.id === value.taskId && sources.get(task.sourceKey)?.descriptor.registrationId === value.registrationId && sources.get(task.sourceKey)?.descriptor.policy === 'chatgpt-main/1');
    if (!scoped) throw new Error('Unknown ChatGPT conversation scope');
    return conversationFor(scoped);
  };
  nativeNotifications = createNativeNotificationStore({ db, now, leaseMs, notifications, sources, save, erase, monitoringPaused: () => paused,
    chatPaused: (sourceKey, mainId) => conversationSettings.get(identity(sources.get(sourceKey)?.descriptor.registrationId, mainId))?.paused === true,
    taskNoticeEligible: notification => notificationMembers(notification).some(member => memberAlerts(member).length > 0),
    accepted: notification => {
      for (const member of notificationMembers(notification)) {
        const task = tasks.get(member.taskKey);
        for (const alert of task?.alerts ?? []) if (member.alertIds.includes(alert.id) && alert.notification?.id === notification.id) {
          alert.lastSubmittedAt = now();
          alert.notification = { id: notification.id, status: 'submitted', delivery: 'system-accepted', seen: 'unknown', at: now() };
        }
        if (task) save('tasks', task);
      }
    } });
  for (let cursor = 0, page = 0; page < 11; page++) {
    const retained = failureStore.list({ afterSequence: cursor, limit: 100 });
    for (const receipt of retained.receipts) nativeNotifications.observeFailure(receipt);
    if (!retained.hasMore) break;
    cursor = retained.nextSequence;
  }
  const contextFor = (task, read) => {
    const owner = sources.get(task.sourceKey), context = owner?.hierarchyContext;
    if (!context) return null;
    const current = now();
    const source = read?.bySource.get(task.sourceKey) ?? { tasks: [...tasks.values()].filter(item => item.sourceKey === task.sourceKey), events: [...events.values()].filter(event => tasks.get(event.taskKey)?.sourceKey === task.sourceKey) };
    if (!source.instruction || current < source.instructionAt || current >= source.instruction.validUntil) source.instruction = createInstructionLookup(context, source.tasks, source.events, current, owner.descriptor.policy);
    source.instructionAt = current;
    source.instruction.forkCaptureBudgetExceeded = owner.forkCaptureBudgetExceeded === true;
    const value = instructionContext(task, context, source.tasks, source.events, current, source.instruction);
    value.gaps = [...new Set([...value.gaps, ...(value.binding?.retentionGaps ?? [])])];
    return value;
  };
  const publicEvents = (task, context = contextFor(task), read) => {
    return taskEvents(task, read).map(({ key, taskKey, sequence: eventSequence, ...event }) => {
      const value = copy(event);
      if (context && value.kind === 'input') value.authorizationEligible = context.eligible(value);
      return value;
    });
  };
  const resultKeyFor = (task, context, read) => {
    context ??= contextFor(task, read);
    const coverage = coverageFor(task, read), input = { task: { id: task.id, lifecycleId: task.lifecycleId, status: task.status, currentRequestId: task.requestId, requestStartedAt: task.requestStartedAt }, source: sources.get(task.sourceKey)?.descriptor, events: publicEvents(task, context, read).filter(event => !historical(event)), coverage };
    return fingerprint([task.revision, resultScopeFor(task, read), typeof analyzer?.timeConditions === 'function' ? analyzer.timeConditions(input, now()) : analysisTimeConditions(input, now()), context && [context.binding, context.inheritedEvents.map(event => [eventRef(event), event.capturedAt]), context.gaps]]);
  };
  const resultScopeFor = (task, read) => {
    const source = sources.get(task.sourceKey), activation = source?.hierarchyContext?.activations.find(item => item.taskId === task.id && item.lifecycleId === task.lifecycleId);
    const parent = source?.hierarchyContext?.relations.find(item => item.kind === 'delegation' && (item.childSessionId === task.sessionId || item.childActivationRef?.taskId === task.id && item.childActivationRef?.lifecycleId === task.lifecycleId));
    return fingerprint([task.requestId, task.contextFingerprint, task.status, activation?.sessionId, activation?.runId, parent?.parentSessionId, source?.descriptor, coverageFor(task, read)]);
  };
  const resultDispositionFor = (task, context, read) => {
    if (!task.resultScope || !task.analysis.summary) return task.resultScope != null || task.analysis.completedAt != null || ['analysis_expired', 'evidence_expired'].includes(task.analysis.error?.code) ? 'invalid' : 'none';
    if (!liveIdentity(task) || task.resultScope !== resultScopeFor(task, read) || (task.analysis.summaryAt ?? task.analysis.completedAt) <= now() - DAY || task.analysis.checks.some(check => check.judgmentState === 'failed') || task.analysis.error && task.analysis.error.code !== 'cancelled') return 'invalid';
    if (task.resultKey !== resultKeyFor(task, context, read)) return 'pending';
    return task.analysis.state === 'idle' ? 'reused' : 'current';
  };

  function rememberGap(owner, gap) {
    const latest = new Map();
    for (const value of [...(owner.gapRecords ?? []), gap]) {
      if ((latest.get(value.reason)?.at ?? -1) <= value.at) latest.set(value.reason, value);
    }
    owner.gapRecords = [...latest.values()];
    owner.gaps = [...new Set(owner.gapRecords.map(value => value.reason))];
  }

  function initializeHierarchyCaptures(source) {
    if (!source?.hierarchyContext || source.hierarchyEvidenceCaptures) return;
    const context = source.hierarchyContext;
    const references = [...context.activations.flatMap(item => item.stateRefs), ...context.relations.flatMap(item => item.evidenceRefs), ...context.instructionBindings.flatMap(item => [item.inputRef, ...item.originRefs, ...item.ancestorLimitRefs]), ...(context.waitBindings ?? []).flatMap(item => item.evidenceRefs),
      ...Array.from(events.values()).filter(event => tasks.get(event.taskKey)?.sourceKey === source.key).map(eventRef)];
    const captures = new Map();
    for (const reference of references) {
      if (captures.has(referenceKey(reference))) continue;
      if (captures.size === HIERARCHY_CAPTURE_LIMIT) {
        source.hierarchyCaptureBudgetExceeded = true;
        source.hierarchyCaptureMigrationBlocked = true;
        rememberGap(source, { reason: HIERARCHY_CAPTURE_GAP, at: now() });
        break;
      }
      const event = events.get(identity(source.key, reference.taskId, reference.lifecycleId, reference.eventId));
      captures.set(referenceKey(reference), { ...reference, capturedAt: event?.capturedAt ?? now() - DAY });
    }
    source.hierarchyEvidenceCaptures = [...captures.values()];
    save('sources', source);
  }

  function cleanQueue() {
    for (let index = queue.length - 1; index >= 0; index--) {
      const task = tasks.get(queue[index]);
      if (!paused && !closed && task && usable(task) && !observationOnly(task) && !conversationPaused(task)) continue;
      queued.delete(queue[index]);
      queue.splice(index, 1);
      if (task && task.analysis.queuedAt !== null) {
        task.analysis.queuedAt = null;
        if (task.analysis.state === 'queued') task.analysis.state = task.analysis.completedAt ? 'stale' : 'unknown';
        save('tasks', task);
      }
    }
    const retained = new Set(queue);
    for (const key of queued) if (!retained.has(key)) queued.delete(key);
  }

  const saveEnrollment = () => { rememberNativeRow('settings', 'enrollment'); return write.run('settings', 'enrollment', JSON.stringify(enrollment)); };
  function enrollEnabledAdapter(descriptor) {
    if (descriptor.enroll !== true || enrollment.managed || enrollment.entries.some(entry => entry.registrationId === descriptor.registrationId)) return;
    const [entry] = normalizeEnrollment([{ registrationId: descriptor.registrationId, name: descriptor.name, ...(descriptor.profile === undefined ? {} : { profile: descriptor.profile }), enabled: true }]);
    // A full plan must not prevent an additional known source from being observed.
    if (enrollment.entries.length >= 100) return;
    enrollment.entries.push({ ...entry, lastSeenAt: null });
    enrollment.configuredAt ??= now();
    saveEnrollment();
  }

  function noteSourceSeen(source) {
    const entry = enrollment.entries.find(value => value.registrationId === source.descriptor.registrationId);
    if (!entry || (entry.lastSeenAt ?? -1) >= source.lastSeenAt) return;
    entry.lastSeenAt = source.lastSeenAt;
    saveEnrollment();
  }

  function enrollmentSnapshot() {
    const entries = enrollment.entries.map(entry => {
      const matching = Array.from(sources.values()).filter(source => source.descriptor.registrationId === entry.registrationId);
      const live = matching.filter(connected);
      const status = !entry.enabled ? 'disabled' : live.length > 0 ? live.every(source => source.baselineConfirmed) ? 'connected' : 'connecting' : entry.lastSeenAt === null ? 'pending' : 'disconnected';
      return { ...copy(entry), status, connectedSources: live.length, disconnectedSources: matching.length - live.length, sourceKeys: matching.map(source => source.key) };
    });
    const expected = entries.filter(entry => entry.enabled);
    const planned = new Set(entries.map(entry => entry.registrationId));
    const gaps = [];
    if (enrollment.configuredAt === null) gaps.push('Expected source configurations have not been set');
    if (expected.length === 0) gaps.push('No enabled source configurations');
    for (const entry of expected) {
      if (entry.connectedSources === 0) gaps.push(`${entry.name}: configured source is not connected`);
      for (const key of entry.sourceKeys) {
        const source = sources.get(key);
        if (!connected(source)) {
          if (entry.connectedSources > 0) gaps.push(`${entry.name}: a previously observed host is disconnected; its current activity is unknown`);
          continue;
        }
        if (!source.baselineConfirmed) gaps.push(`${entry.name}: source baseline is unconfirmed`);
        for (const gap of [...(source.descriptor.gaps ?? []), ...source.gaps]) gaps.push(`${entry.name}: ${gap}`);
        for (const task of tasks.values()) {
          if (task.sourceKey !== key || !['running', 'unknown'].includes(task.status)) continue;
          for (const gap of coverageFor(task).gaps) gaps.push(`${entry.name} / ${task.title}: ${gap}`);
        }
      }
    }
    return {
      entries, configured: expected.length, connected: expected.filter(entry => entry.connectedSources > 0).length,
      missing: expected.filter(entry => entry.connectedSources === 0).length,
      unlisted: Array.from(sources.values()).filter(source => !planned.has(source.descriptor.registrationId)).map(source => ({ sourceKey: source.key, registrationId: source.descriptor.registrationId, name: source.descriptor.name, status: source.status, baselineConfirmed: source.baselineConfirmed })),
      coverage: { scope: 'configured', complete: gaps.length === 0, gaps: [...new Set(gaps)] },
      discovery: { complete: false, gaps: ['Automatic discovery of all local sources has not been verified'] },
    };
  }

  function invalidateAlerts(task, reason) {
    for (const alert of task.alerts) {
      if (alert.status === 'attention' || alert.status === 'unknown' && reason !== 'unknown') { alert.status = reason; alert.updatedAt = now(); }
    }
    for (const notification of notifications.values()) {
      if (notificationMembers(notification).some(member => member.taskKey === task.key) && ['pending', 'claimed'].includes(notification.status) && !notificationEligible(notification)) {
        notification.status = 'cancelled';
        notification.error = reason;
        save('notifications', notification);
      }
    }
  }

  function cancelRound(reason, onlyTask, retainTransport = false) {
    const error = new Error(reason);
    if (!retainTransport && cancelledRound && (!onlyTask || cancelledRound.taskKey === onlyTask.key)) cancelledRound.transportController.abort(error);
    if (!active || (onlyTask && active.taskKey !== onlyTask.key)) return;
    const round = active;
    active = null;
    round.cancelled = true;
    pendingCancelledRequests++;
    cancelledRound = round;
    const controller = round.controller;
    // The provider may keep computing. The sealed core round no longer needs its inputs.
    round.input = null;
    round.controller = null;
    if (!retainTransport) round.transportController.abort(error);
    controller.abort(error);
    const task = tasks.get(round.taskKey);
    if (task) {
      task.analysis.state = 'interrupted';
      task.analysis.error = { code: 'cancelled', message: reason };
      task.analysis.cancelledComputeMayContinue = true;
      task.analysis.nextEligibleAt = now();
      invalidateAlerts(task, 'unknown');
      save('tasks', task);
    }
  }

  function resetSource(source, reason) {
    source.status = 'disconnected';
    source.baselineConfirmed = false;
    source.disconnectReason = reason;
    rememberGap(source, { reason, at: now() });
    save('sources', source);
    for (const task of tasks.values()) {
      if (task.sourceKey !== source.key) continue;
      const interrupted = task.analysis.state === 'analyzing';
      cancelRound(reason, task);
      if (task.status !== 'unknown') task.lastKnownStatus = task.status;
      task.status = 'unknown';
      task.baselineConfirmed = false;
      task.analysis.state = interrupted ? 'interrupted' : task.analysis.completedAt ? 'stale' : 'unknown';
      if (interrupted) task.analysis.error = { code: 'analysis_interrupted', message: reason };
      invalidateAlerts(task, 'source-disconnected');
      save('tasks', task);
    }
  }

  // Bootstrap legacy capture facts before the first retention pass can delete their bodies.
  for (const source of sources.values()) initializeHierarchyCaptures(source);
  // Stored activity is history, not proof that the original host is still running.
  for (const source of sources.values()) resetSource(source, 'Core restarted; source baseline is unknown');
  for (const notification of notifications.values()) {
    if (['pending', 'claimed'].includes(notification.status) && !(notification.kind === 'monitoring-failure' && notification.status === 'pending')) {
      notification.status = 'cancelled';
      notification.error = 'Core restarted; delivery eligibility must be confirmed again';
      save('notifications', notification);
    }
  }

  function coverageFor(task, read) {
    const source = sources.get(task.sourceKey);
    let scopedEvents = taskEvents(task, read);
    if (source?.descriptor.policy === 'chatgpt-main/1') {
      scopedEvents = scopedEvents.filter(event => !historical(event));
      if (task.requestId != null) {
        const request = scopedEvents.findIndex(event => event.kind === 'input' && event.id === task.requestId);
        if (request >= 0) scopedEvents = scopedEvents.slice(request);
      }
    }
    const inCurrentScope = gap => task.requestStartedAt == null || (gap.endAt ?? gap.at) >= task.requestStartedAt;
    const gaps = [
      ...(source?.descriptor.gaps ?? []),
      ...(source?.gapRecords ?? []).filter(inCurrentScope).map(gap => gap.reason),
      ...(task.gapRecords ?? []).filter(inCurrentScope).map(gap => gap.reason),
    ];
    if (source?.hierarchyCaptureBudgetExceeded) gaps.push(HIERARCHY_CAPTURE_GAP);
    if (!task.baselineConfirmed) gaps.push('Task baseline is unconfirmed');
    if (source?.descriptor.policy === 'chatgpt-main/1' && task.requestId == null) gaps.push('Current user request is unverified');
    if (!connected(source)) gaps.push('Source is disconnected');
    if (scopedEvents.some(event => event.data?.truncated && event.data?.inputOrigin !== 'runtime-notice' && (task.requestStartedAt == null || event.capturedAt >= task.requestStartedAt))) gaps.push('Some observed excerpts are truncated');
    if (scopedEvents.some(event => event.data?.redacted && (task.requestStartedAt == null || event.capturedAt >= task.requestStartedAt))) gaps.push('Some observed content was omitted for privacy');
    return { complete: gaps.length === 0, gaps: [...new Set(gaps)] };
  }

  function activityFor(task, retained, coverage, current) {
    const request = retained.findIndex(event => event.id === task.requestId && event.kind === 'input');
    const boundary = !connected(sources.get(task.sourceKey)) ? '来源失联，以下仅为历史观察：' : !task.baselineConfirmed ? '任务基线未知，以下仅为历史观察：' : task.status !== 'running' ? '任务当前不在运行或状态未知，以下仅为历史观察：' : !coverage.complete ? '观察覆盖不足，无法确认当前活动；最近观察：' : '';
    if (request < 0 || !task.currentRequest.trim()) return { summary: `${boundary}当前请求未知，无法确认当前活动`, observedAt: null, evidenceIds: [] };
    const scope = retained.slice(request).filter(event => !historical(event)), byId = new Map(scope.map(event => [event.id, event]));
    const replacements = new Map(), refinements = new Set();
    const interrupted = event => event.data?.interrupted === true || event.data?.aborted === true || event.data?.timedOut === true || Boolean(event.data?.signal);
    for (const event of scope) {
      const original = byId.get(event.data?.refinesEventId);
      if (event.kind !== 'tool-result' || original?.kind !== 'tool-result' || original.id === event.id || event.data?.resultSource !== 'native-shell-canonical' || !event.data.callId || event.data.callId !== original.data?.callId) continue;
      const previous = replacements.get(original.id) ?? original;
      replacements.set(original.id, { ...original, text: event.text, data: { ...previous.data, ...event.data, interrupted: interrupted(previous) || interrupted(event), isError: previous.data?.isError === true || event.data.isError === true }, activityEvidence: [...(previous.activityEvidence ?? [original.id]), event.id] });
      refinements.add(event.id);
    }
    const canonical = scope.filter(event => !refinements.has(event.id)).map(event => replacements.get(event.id) ?? event);
    const open = [];
    for (const event of canonical) {
      if (!event.data?.callId) continue;
      if (event.kind === 'tool-start') open.push(event);
      else if (event.kind === 'tool-result' && (!event.data.refinesEventId || event.activityEvidence)) {
        const index = open.findIndex(start => start.data.callId === event.data.callId);
        if (index >= 0) open.splice(index, 1);
      }
    }
    if (!boundary && open.length) {
      const oldest = open[0], seconds = Math.max(0, Math.floor((current - oldest.capturedAt) / 1000));
      return { summary: `工具 ${oldest.data.toolName || '调用'} 尚未观察到结束，已观察至少 ${seconds} 秒${open.length > 1 ? `；共 ${open.length} 个未配对开始记录` : ''}`, observedAt: open.reduce((latest, event) => Math.max(latest, event.capturedAt), 0), evidenceIds: open.map(event => event.id) };
    }
    const latest = canonical.filter(event => ['output', 'tool-start', 'tool-result'].includes(event.kind)).at(-1);
    if (!latest) return { summary: `${boundary}当前请求已采集，尚无动作记录`, observedAt: retained[request].capturedAt, evidenceIds: [retained[request].id] };
    const label = latest.kind === 'output' ? '代理输出' : latest.kind === 'tool-start' ? '工具开始' : interrupted(latest) ? '工具结果（执行中断）' : latest.data?.isError === true ? '工具结果（工具报告错误）' : '工具结果';
    const linkage = latest.kind !== 'output' && (!latest.data?.callId || (latest.data.refinesEventId && !latest.activityEvidence)) ? '；调用关联未知' : '';
    const exit = latest.kind === 'tool-result' ? Number.isFinite(latest.data?.exitCode) ? `；退出码 ${latest.data.exitCode}` : '；退出码未知' : '';
    return { summary: `${boundary}${label}：${latest.text}${exit}${linkage}`, observedAt: latest.capturedAt, evidenceIds: latest.activityEvidence ?? [latest.id] };
  }

  function prune() {
    const current = now();
    const expiredNativeProgress = nativeProgress.prune(current);
    failureStore.prune(current);
    const cutoff = current - DAY;
    const expiredByTask = new Map();
    for (const event of events.values()) {
      if (event.capturedAt > cutoff) continue;
      if (!expiredByTask.has(event.taskKey)) expiredByTask.set(event.taskKey, new Set());
      expiredByTask.get(event.taskKey).add(event.id);
      erase('events', event.key);
    }
    for (const task of tasks.values()) {
      let changed = pendingPrunedTasks.has(task.key);
      const expired = new Set(expiredByTask.get(task.key) ?? []);
      const retained = new Set(taskEvents(task).map(event => event.id));
      const references = [task.requestId, ...(task.analysis.inputEventIds ?? []),
        ...task.analysis.checks.flatMap(check => check.evidenceIds ?? []),
        ...task.alerts.flatMap(alert => [...(alert.evidenceIds ?? []), ...(alert.recoveryEvidenceIds ?? [])])];
      for (const id of references) if (id && !retained.has(id)) expired.add(id);
      if (expired.size > 0) {
        pendingPrunedTasks.add(task.key);
        cancelRound('Analysis evidence expired after 24 hours', task);
        rememberGap(task, { reason: 'Earlier evidence expired after 24 hours', at: now(), endAt: now() - DAY });
        if (expired.has(task.requestId)) { task.currentRequest = ''; task.requestId = null; task.contextFingerprint = null; }
        if (task.analysis.inputEventIds?.some(id => expired.has(id)) || task.analysis.checks?.some(check => check.evidenceIds?.some(id => expired.has(id)))) {
          task.analysis.state = 'stale';
          task.analysis.error = { code: 'evidence_expired', message: 'Evidence supporting this analysis expired' };
          task.analysis.summary = null;
          task.analysis.summaryAt = null;
          task.resultKey = null;
          task.resultScope = null;
          task.analysis.inputEventIds = [];
          task.analysis.checks = unknownChecks('Supporting evidence expired');
          invalidateAlerts(task, 'evidence-expired');
        }
        changed = true;
      }
      for (const alert of task.alerts) {
        const missingReference = [...(alert.evidenceRefs ?? []), ...(alert.recoveryEvidenceRefs ?? [])].some(reference => !events.has(identity(task.sourceKey, reference.taskId, reference.lifecycleId, reference.eventId)));
        if ((alert.evidenceIds ?? []).some(id => expired.has(id)) || (alert.recoveryEvidenceIds ?? []).some(id => expired.has(id)) || missingReference && alert.status !== 'evidence-expired') {
          alert.status = 'evidence-expired';
          alert.reason = 'Supporting evidence expired after 24 hours';
          alert.evidenceIds = [];
          alert.recoveryEvidenceIds = [];
          alert.updatedAt = now();
          changed = true;
        }
      }
      const expiredContext = (task.analysis.contextEvidenceRefs ?? []).some(reference => !events.has(identity(task.sourceKey, reference.taskId, reference.lifecycleId, reference.eventId)));
      if (expiredContext) {
        cancelRound('Inherited instruction evidence expired after 24 hours', task);
        task.analysis.state = 'stale'; task.analysis.summary = null; task.analysis.summaryAt = null;
        task.resultKey = null; task.resultScope = null; task.contextFingerprint = null;
        task.analysis.error = { code: 'evidence_expired', message: 'Inherited evidence supporting this analysis expired' };
        task.analysis.checks = unknownChecks('Inherited supporting evidence expired'); task.analysis.contextEvidenceRefs = [];
        invalidateAlerts(task, 'evidence-expired'); changed = true;
      }
      if ((task.analysis.summaryAt ?? task.analysis.completedAt) !== null && (task.analysis.summaryAt ?? task.analysis.completedAt) <= cutoff) {
        task.analysis.summary = null;
        task.analysis.checks = unknownChecks('Analysis expired after 24 hours');
        task.analysis.completedAt = null;
        task.analysis.summaryAt = null;
        task.analysis.inputEventIds = [];
        task.analysis.through = null;
        task.analysis.state = 'stale';
        task.analysis.error = { code: 'analysis_expired', message: 'Analysis expired after 24 hours' };
        task.resultKey = null;
        task.resultScope = null;
        changed = true;
      }
      if (task.notificationBinding && !task.notificationBinding.expired && task.notificationBinding.evidenceRefs.some(reference => {
        const event = events.get(identity(task.sourceKey, reference.taskId, reference.lifecycleId, reference.eventId));
        return !event || event.capturedAt <= cutoff;
      })) { task.notificationBinding.evidenceRefs = []; task.notificationBinding.expired = true; changed = true; }
      const retainedAlerts = task.alerts.filter(alert => alert.firstAt > cutoff);
      if (retainedAlerts.length !== task.alerts.length) { task.alerts = retainedAlerts; changed = true; }
      const retainedGaps = (task.gapRecords ?? []).filter(gap => gap.at > cutoff);
      if (retainedGaps.length !== (task.gapRecords ?? []).length) {
        task.gapRecords = retainedGaps;
        task.gaps = [...new Set(retainedGaps.map(gap => gap.reason))];
        changed = true;
      }
      if (changed) pendingPrunedTasks.add(task.key);
      if (!liveIdentity(task) && task.updatedAt <= cutoff && taskEvents(task).length === 0) {
        erase('tasks', task.key);
        pendingPrunedTasks.delete(task.key);
      } else if (changed) {
        save('tasks', task);
        pendingPrunedTasks.delete(task.key);
      }
    }
    for (const notification of notifications.values()) {
      if (notification.createdAt <= cutoff || notification.kind !== 'monitoring-failure' && !notificationMembers(notification).some(member => tasks.has(member.taskKey)) || notification.evidenceThrough <= cutoff) erase('notifications', notification.id);
    }
    for (const source of sources.values()) {
      const context = source.hierarchyContext;
      const retainedReference = reference => {
          const event = events.get(identity(source.key, reference.taskId, reference.lifecycleId, reference.eventId));
          return event && event.capturedAt > cutoff;
      };
      const contextReferences = context ? [...context.activations.flatMap(activation => activation.stateRefs), ...context.relations.flatMap(relation => relation.evidenceRefs), ...context.instructionBindings.flatMap(binding => [binding.inputRef, ...binding.originRefs, ...binding.ancestorLimitRefs]), ...(context.waitBindings ?? []).flatMap(binding => binding.evidenceRefs)] : [];
      if (context && (source.hierarchyMaterialAt <= cutoff || contextReferences.some(reference => !retainedReference(reference)) || source.hierarchyMaterialAt == null && contextReferences.length === 0)) {
        let expiredProof = false;
        const activations = context.activations.filter(activation => findTask(source.key, activation.taskId, activation.lifecycleId)).map(activation => {
          const stateRefs = activation.stateRefs.filter(retainedReference);
          if (stateRefs.length === activation.stateRefs.length) return activation;
          expiredProof = true;
          return { ...activation, stateRefs, state: activation.state === 'ended' ? 'ended' : 'unknown' };
        });
        const relevantSessions = new Set(activations.flatMap(activation => activation.sessionId ? [activation.sessionId] : []));
        const identities = new Set(activations.map(activation => identity(activation.taskId, activation.lifecycleId)));
        const parents = new Map(context.relations.filter(relation => relation.kind === 'delegation' && relation.childSessionId).map(relation => [relation.childSessionId, relation]));
        for (const relation of context.relations) if (relation.childActivationRef && identities.has(identity(relation.childActivationRef.taskId, relation.childActivationRef.lifecycleId))) relevantSessions.add(relation.parentSessionId);
        for (const sessionId of relevantSessions) if (parents.has(sessionId)) relevantSessions.add(parents.get(sessionId).parentSessionId);
        const relations = context.relations.filter(relation => relation.childSessionId ? relevantSessions.has(relation.childSessionId) && relevantSessions.has(relation.parentSessionId) : identities.has(identity(relation.childActivationRef.taskId, relation.childActivationRef.lifecycleId))).map(relation => {
          const evidenceRefs = relation.evidenceRefs.filter(retainedReference);
          if (evidenceRefs.length !== relation.evidenceRefs.length) expiredProof = true;
          return { ...relation, evidenceRefs };
        });
        const instructionBindings = context.instructionBindings.filter(binding => identities.has(identity(binding.taskId, binding.lifecycleId)) && retainedReference(binding.inputRef) && binding.originRefs.every(retainedReference)).map(binding => {
          const ancestorLimitRefs = binding.ancestorLimitRefs.filter(retainedReference);
          return ancestorLimitRefs.length === binding.ancestorLimitRefs.length ? binding : { ...binding, ancestorLimitRefs, retentionGaps: [...new Set([...(binding.retentionGaps ?? []), 'ancestor_limit_origin_or_retention_missing'])] };
        });
        const waitBindings = (context.waitBindings ?? []).filter(binding => identities.has(identity(binding.taskId, binding.lifecycleId)) && binding.evidenceRefs.every(retainedReference));
        const sessions = context.sessions.filter(session => relevantSessions.has(session.sessionId));
        const next = { ...context, sessions, activations, relations, instructionBindings, ...(context.waitBindings ? { waitBindings } : {}) };
        if (expiredProof) next.coverage = { complete: false, branchGaps: [{ sessionId: sessions[0]?.sessionId ?? 'unknown', reason: 'Retained hierarchy execution evidence expired after 24 hours' }] };
        if (JSON.stringify(next) !== JSON.stringify(context)) { source.hierarchyContext = next; delete source.hierarchyMaterialKey; pendingPrunedSources.add(source.key); }
      }
      const nativeRuns = (source.hierarchyRunBindings ?? []).filter(binding => binding.capturedAt > cutoff || liveIdentity(findTask(source.key, binding.taskId, binding.lifecycleId) ?? {}));
      if (nativeRuns.length !== (source.hierarchyRunBindings?.length ?? 0)) { source.hierarchyRunBindings = nativeRuns; pendingPrunedSources.add(source.key); }
      const retainedGaps = (source.gapRecords ?? []).filter(gap => gap.at > cutoff || source.hierarchyCaptureBudgetExceeded && gap.reason === HIERARCHY_CAPTURE_GAP);
      if (retainedGaps.length !== (source.gapRecords ?? []).length || pendingPrunedSources.has(source.key)) {
        pendingPrunedSources.add(source.key);
        source.gapRecords = retainedGaps;
        source.gaps = [...new Set(retainedGaps.map(gap => gap.reason))];
        save('sources', source);
        pendingPrunedSources.delete(source.key);
      }
      if (!connected(source) && source.lastSeenAt <= cutoff && !Array.from(tasks.values()).some(task => task.sourceKey === source.key)) { failureStore.clearSource(source.key, current); erase('sources', source.key); }
    }
    if (expiredByTask.size > 0 || expiredNativeProgress > 0) db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  }

  function notificationScope(task) {
    const binding = task.notificationBinding;
    if (binding && !binding.expired && binding.evidenceRefs.every(reference => {
      const event = events.get(identity(task.sourceKey, reference.taskId, reference.lifecycleId, reference.eventId));
      return event && event.capturedAt > now() - DAY;
    })) {
      const { evidenceRefs, executionRunId, ...scope } = binding;
      return scope;
    }
    return { rootSessionId: null, rootIdentity: null, coverage: { complete: false, gaps: [binding ? 'notification_root_evidence_expired' : 'notification_root_activation_unconfirmed'] } };
  }

  function bindNotificationRoot(task, context) {
    const own = context.activations.find(item => item.taskId === task.id && item.lifecycleId === task.lifecycleId);
    if (!own || task.notificationBinding?.executionRunId === own.runId) return;
    let root = own;
    const evidenceRefs = [];
    let rootCatalog = [];
    while (root) {
      const relation = context.relations.find(item => item.kind === 'delegation' && (root.sessionId ? item.childSessionId === root.sessionId : item.childActivationRef?.taskId === root.taskId && item.childActivationRef?.lifecycleId === root.lifecycleId));
      if (!relation) break;
      const parents = context.activations.filter(item => item.sessionId === relation.parentSessionId && relation.evidenceRefs.some(reference => reference.taskId === item.taskId && reference.lifecycleId === item.lifecycleId));
      if (parents.length !== 1) return;
      rootCatalog = relation.evidenceRefs.map(reference => events.get(identity(task.sourceKey, reference.taskId, reference.lifecycleId, reference.eventId))).filter(event => event?.taskId === parents[0].taskId && event.taskLifecycleId === parents[0].lifecycleId && event.data?.nativeType === 'subagent/catalog' && event.data.childSessionId === (relation.childSessionId ?? relation.childActivationRef?.taskId));
      evidenceRefs.push(...rootCatalog.map(eventRef));
      root = parents[0];
    }
    if (!root?.sessionId || root.state === 'unknown' || !root.stateRefs.length) return;
    if (root.runId && rootCatalog.length) {
      const starts = root.stateRefs.map(reference => events.get(identity(task.sourceKey, reference.taskId, reference.lifecycleId, reference.eventId))).filter(event => event?.data?.nativeType === 'subagent/start' && event.data.runId === root.runId && event.data.childSessionId === root.sessionId);
      if (!starts.some(start => rootCatalog.some(catalog => catalog.capturedAt >= start.capturedAt))) return;
    }
    const owner = findTask(task.sourceKey, root.taskId, root.lifecycleId);
    if (!owner) return;
    task.notificationBinding = { executionRunId: own.runId, rootSessionId: root.sessionId, rootIdentity: { taskKey: owner.key, lifecycleId: root.lifecycleId, runId: root.runId }, evidenceRefs: [...evidenceRefs, ...root.stateRefs], coverage: { complete: true, gaps: [] } };
    save('tasks', task);
  }

  function notificationMembers(notification) {
    return notification.members ?? [{ taskKey: notification.taskKey, requestId: notification.requestId, alertIds: notification.alertIds }];
  }

  function memberAlerts(member) {
    const task = tasks.get(member.taskKey);
    if (!task || !usable(task) || paused || conversationPaused(task) || task.requestId !== member.requestId) return [];
    if (['stale', 'failed', 'interrupted', 'unknown'].includes(task.analysis.state) || task.analysis.checks.some(check => check.judgmentState === 'failed')) return [];
    const retained = new Set(taskEvents(task).map(event => event.id));
    const waiting = userWait(task, taskEvents(task), now());
    return member.alertIds.flatMap(id => {
      const alert = task.alerts.find(value => value.id === id);
      return alert?.status === 'attention' && !waitExempts(alert, waiting, taskEvents(task)) && alert.evidenceIds.length > 0 && alert.evidenceIds.every(value => retained.has(value)) ? [alert] : [];
    });
  }

  function notificationEligible(notification, current) {
    if (notification.kind === 'monitoring-failure') return nativeNotifications.scopeEligible(notification, current);
    return notificationMembers(notification).some(member => memberAlerts(member).length > 0);
  }

  function refreshNotification(notification) {
    if (notification.kind === 'monitoring-failure') return;
    notification.members = notificationMembers(notification).flatMap(member => {
      const alerts = memberAlerts(member), task = tasks.get(member.taskKey);
      return alerts.length ? [{ taskKey: task.key, requestId: member.requestId, name: task.title, checkIds: alerts.map(alert => alert.checkId), alertIds: alerts.map(alert => alert.id), reasons: alerts.map(alert => alert.reason) }] : [];
    });
    const first = notification.members[0];
    if (!first) return;
    const scope = notificationScope(tasks.get(first.taskKey)), groupKey = identity(notification.sourceKey, scope.rootIdentity ?? first.taskKey), separated = new Map();
    notification.members = notification.members.filter(member => {
      const memberScope = notificationScope(tasks.get(member.taskKey)), memberGroup = identity(notification.sourceKey, memberScope.rootIdentity ?? member.taskKey);
      if (memberGroup === groupKey) return true;
      if (!separated.has(memberGroup)) separated.set(memberGroup, { scope: memberScope, members: [] });
      separated.get(memberGroup).members.push(member);
      return false;
    });
    Object.assign(notification, scope, { groupKey });
    for (const [memberGroup, separatedGroup] of separated) {
      const split = { ...notification, ...separatedGroup.scope, id: randomUUID(), groupKey: memberGroup, members: separatedGroup.members };
      refreshNotification(split);
      notifications.set(split.id, split);
      save('notifications', split);
    }
    notification.taskKey = first.taskKey;
    notification.checkIds = [...new Set(notification.members.flatMap(member => member.checkIds))];
    notification.alertIds = notification.members.flatMap(member => member.alertIds);
    const sourceName = String(sources.get(notification.sourceKey)?.descriptor.name ?? '来源名称未知').slice(0, 40);
    const header = `观察身份：${first.taskKey}\n来源：${notification.sourceKey} · ${sourceName}\n`;
    notification.title = `${String(first.name).slice(0, 80)}: needs attention`;
    const lineBudget = Math.max(0, Math.floor((1_200 - header.length) / notification.members.length) - 1);
    notification.body = (header + (notification.members.length === 1 ? first.reasons.join('\n') : notification.members.map(member => `${String(member.name).slice(0, 24)} (${member.taskKey.slice(0, 8)}): ${member.reasons.join('；')}`.slice(0, lineBudget)).join('\n'))).slice(0, 1_200);
  }

  function expireClaim(notification) {
    if (notification.status !== 'claimed' || now() - notification.claimedAt < CLAIM_MS) return;
    notification.status = 'expired';
    notification.delivery = 'unknown';
    notification.error = 'Notification claim expired before acknowledgement; delivery is unknown';
    save('notifications', notification);
    for (const member of notificationMembers(notification)) {
      const task = tasks.get(member.taskKey);
      for (const alert of task?.alerts ?? []) {
        if (alert.notification?.id !== notification.id) continue;
        alert.notification = { id: notification.id, status: 'expired', delivery: 'unknown', seen: 'unknown', error: notification.error, at: now() };
      }
      if (task) save('tasks', task);
    }
  }

  function makeNotification(task, alerts) {
    if (alerts.length === 0) return;
    const current = now();
    const due = alerts.filter(alert => {
      if (waitExempts(alert, userWait(task, taskEvents(task), current), taskEvents(task))) return false;
      if (Array.from(notifications.values()).some(value => notificationMembers(value).some(member => member.alertIds.includes(alert.id)) && ['pending', 'claimed'].includes(value.status))) return false;
      const lastAttempt = Math.max(alert.lastSubmittedAt ?? 0, alert.lastAttemptAt ?? 0);
      return lastAttempt === 0 || current - lastAttempt >= Math.max(600_000, repeatMs);
    });
    if (due.length === 0) return;
    const scope = notificationScope(task), groupKey = identity(task.sourceKey, scope.rootIdentity ?? task.key);
    let notification = Array.from(notifications.values()).find(value => value.groupKey === groupKey && value.status === 'pending' && current < value.readyAt);
    if (!notification) notification = { id: randomUUID(), sourceKey: task.sourceKey, groupKey, ...scope, members: [], evidenceThrough: task.analysis.through, createdAt: current, readyAt: current + GROUP_MS, status: 'pending', delivery: 'unknown', seen: 'unknown', claimedAt: null };
    let member = notification.members.find(value => value.taskKey === task.key && value.requestId === task.requestId);
    if (!member) { member = { taskKey: task.key, requestId: task.requestId, alertIds: [] }; notification.members.push(member); }
    member.alertIds.push(...due.map(alert => alert.id));
    notification.evidenceThrough = Math.max(notification.evidenceThrough, task.analysis.through);
    refreshNotification(notification);
    if (!nativeNotifications.prepareTask(notification, task)) return;
    notifications.set(notification.id, notification);
    save('notifications', notification);
  }

  function updateAlerts(task, result, observedAt) {
    const current = now();
    for (const check of result.checks) {
      let alert = task.alerts.find(value => value.checkId === check.id && value.requestId === task.requestId && !['superseded', 'ended', 'evidence-expired', 'recovered'].includes(value.status));
      if (check.status === 'attention') {
        const oldIds = new Set(alert?.evidenceIds ?? []), oldRefs = new Set((alert?.evidenceRefs ?? []).map(referenceKey));
        const sameWaitEvidence = check.id === 'long_wait' && alert?.status === 'attention' && new Set(check.evidenceIds).size === oldIds.size && check.evidenceIds.every(id => oldIds.has(id)) && new Set((check.evidenceRefs ?? []).map(referenceKey)).size === oldRefs.size && (check.evidenceRefs ?? []).every(reference => oldRefs.has(referenceKey(reference)));
        const conditionAt = check.conditionAt ?? (sameWaitEvidence ? alert.observedAt ?? observedAt : observedAt);
        if (!alert) {
          alert = { id: randomUUID(), checkId: check.id, requestId: task.requestId, firstAt: current, lastSubmittedAt: null, lastAttemptAt: null };
          task.alerts.push(alert);
        }
        Object.assign(alert, { status: 'attention', reason: check.reason, evidenceIds: check.evidenceIds, evidenceRefs: check.evidenceRefs, observedAt: conditionAt, updatedAt: current, evidenceThrough: task.analysis.through });
      } else if (alert) {
        const previouslyObserved = new Set(alert.evidenceIds ?? []);
        // Rewording or silence is not recovery. A clear result needs new retained evidence.
        const candidates = check.id === 'long_wait' ? check.evidenceIds : check.recoveryFor === alert.id ? check.recoveryEvidenceIds ?? [] : [];
        const hasNewResult = candidates.some(id => !previouslyObserved.has(id) && taskEvents(task).some(event => event.id === id && event.kind === 'tool-result' && !event.data?.history && !event.data?.interrupted && !event.data?.aborted && !event.data?.timedOut && !event.data?.signal && (check.id !== 'long_wait' || taskEvents(task).filter(previous => previouslyObserved.has(previous.id) && previous.kind === 'tool-start').every(previous => previous.data?.callId && previous.data.callId === event.data?.callId))));
        if (check.status === 'clear' && hasNewResult) {
          alert.status = 'recovered'; alert.updatedAt = current;
          alert.recoveryEvidenceIds = check.evidenceIds; alert.recoveryEvidenceRefs = check.evidenceRefs;
        }
      }
    }
    makeNotification(task, task.alerts.filter(alert => alert.status === 'attention'));
  }

  function validateResult(result, round) {
    if (!result || !result.summary || !Array.isArray(result.checks) || result.checks.length !== CHECK_IDS.length) throw new Error('Analyzer result must contain summary and all six checks');
    const eventIds = new Set(round.input.events.map(event => event.id));
    const referenceIds = new Set([...round.input.events, ...(round.input.instructionContext?.inheritedEvents ?? [])].map(event => referenceKey(eventRef(event))));
    const checks = new Map();
    if (!['currentTask', 'currentState', 'recentProgress', 'attention'].every(field => typeof result.summary[field] === 'string')) throw new Error('Analyzer summary fields must be strings');
    for (const check of result.checks) {
      if (!CHECK_IDS.includes(check.id) || checks.has(check.id) || !['attention', 'clear', 'unknown'].includes(check.status) || !Array.isArray(check.evidenceIds) || check.evidenceIds.some(id => !eventIds.has(id))) throw new Error('Analyzer returned invalid or cross-task evidence');
      if (check.evidenceRefs && (!Array.isArray(check.evidenceRefs) || check.evidenceRefs.some(reference => !referenceIds.has(referenceKey(reference))))) throw new Error('Analyzer returned unverified context evidence');
      if (check.status === 'attention' && check.evidenceIds.length === 0) throw new Error('Attention requires retained evidence');
      if (check.conditionAt !== undefined && (!Number.isSafeInteger(check.conditionAt) || check.conditionAt < 0 || check.id !== 'long_wait' || check.status !== 'attention')) throw new Error('Analyzer returned invalid wait condition time');
      if (check.applicability && (!['triggered', 'not-triggered', 'unknown'].includes(check.applicability.state) || typeof check.applicability.reason !== 'string' || !Array.isArray(check.applicability.evidenceIds) || check.applicability.evidenceIds.some(id => !eventIds.has(id)) || check.applicability.state !== 'unknown' && check.applicability.evidenceIds.length === 0 || check.status === 'attention' && check.applicability.state !== 'triggered')) throw new Error('Analyzer returned invalid applicability evidence');
      if (check.recoveryFor !== undefined && (!round.input.task.activeAlerts?.some(alert => alert.id === check.recoveryFor && alert.checkId === check.id) || !Array.isArray(check.recoveryEvidenceIds) || !check.recoveryEvidenceIds.length || check.recoveryEvidenceIds.some(id => !check.evidenceIds.includes(id) || !round.input.events.some(event => event.id === id && event.kind === 'tool-result' && !event.data?.history && !event.data?.interrupted && !event.data?.aborted && !event.data?.timedOut && !event.data?.signal)))) throw new Error('Analyzer returned invalid recovery scope or result evidence');
      check.applicability ??= { state: check.status !== 'unknown' ? 'triggered' : 'unknown', reason: 'Legacy analyzer trigger coverage', evidenceIds: check.evidenceIds };
      check.judgmentState = round.input.source.policy === 'chatgpt-main/1' && check.status === 'unknown' && check.judgmentState === 'failed' ? 'failed' : check.applicability.state === 'not-triggered' ? 'not-triggered' : check.status === 'unknown' ? 'insufficient' : check.status;
      if (round.input.source.policy === 'chatgpt-main/1' && check.id === 'long_wait' && check.status === 'attention') {
        check.status = 'unknown';
        check.judgmentState = 'insufficient';
        check.reason = '等待原因未确认；观察时长本身不能确认停滞';
        check.applicability = { state: 'unknown', reason: check.reason, evidenceIds: check.evidenceIds };
        delete check.conditionAt;
        delete check.recoveryFor;
        delete check.recoveryEvidenceIds;
      }
      checks.set(check.id, check);
    }
    const value = { ...copy(result), checks: CHECK_IDS.map(id => checks.get(id)) };
    if (round.input.source.policy === 'chatgpt-main/1') value.summary.attention = value.checks.filter(check => check.status === 'attention').map(check => check.reason).join('；') || '本轮未形成有足够依据的提醒；不可判断项目仍显示未知';
    return value;
  }

  function completeRound(round, result, error) {
    if (round.cancelled) {
      pendingCancelledRequests = Math.max(0, pendingCancelledRequests - 1);
      if (cancelledRound === round) cancelledRound = null;
      if (!closed) tick();
    }
    if (closed || active !== round || round.cancelled) return;
    active = null;
    const task = tasks.get(round.taskKey);
    if (!task || !usable(task) || observationOnly(task) || conversationPaused(task) || task.requestId !== round.requestId) return;
    if ([...round.input.events, ...(round.input.instructionContext?.inheritedEvents ?? [])].some(event => event.capturedAt <= now() - DAY)) {
      error = Object.assign(new Error('Analysis inputs expired before the response completed'), { code: 'evidence_expired' });
    }
    const completedAt = now();
    task.analysis.completedAt = completedAt;
    task.analysis.through = round.through;
    task.analysis.throughRevision = round.revision;
    task.analysis.nextEligibleAt = completedAt + cooldownMs;
    task.analysis.cancelledComputeMayContinue = false;
    if (error) {
      task.resultKey = null;
      task.resultScope = null;
      task.analysis.state = 'failed';
      task.analysis.error = { code: error.code ?? 'analysis_failed', stage: error.stage ?? null, message: error.message ?? String(error) };
      if (error.code === 'network') {
        let details;
        try { details = inferenceFailureInfo(error.details); } catch {}
        if (details) task.analysis.error.details = details;
      }
      task.analysis.checks = unknownChecks('The current analysis failed; prior conclusions are not current', 'failed');
    } else {
      task.resultKey = round.resultKey;
      task.resultScope = round.resultScope;
      task.analysis.error = null;
      task.analysis.summary = result.summary;
      task.analysis.summaryAt = completedAt;
      task.analysis.inputEventIds = round.input.events.map(event => event.id);
      task.analysis.contextEvidenceRefs = (round.input.instructionContext?.inheritedEvents ?? []).map(eventRef);
      task.analysis.checks = result.checks;
      task.analysis.coverage = result.coverage;
      task.analysis.usage = result.usage ?? null;
      task.analysis.quality = result.quality ?? 'unvalidated';
      task.analysis.state = result.coverage?.dependencies?.length > 0 ? 'unavailable' : 'cooldown';
      updateAlerts(task, result, round.observedAt);
    }
    const failedCheck = !error && sources.get(task.sourceKey)?.descriptor.policy === 'chatgpt-main/1' ? result.checks.find(check => check.judgmentState === 'failed') : null;
    const settledError = error ?? failedCheck?.error ?? (failedCheck ? { code: 'analysis_failed' } : null);
    if (failedCheck) {
      task.resultKey = null; task.resultScope = null;
      task.analysis.state = 'failed';
      task.analysis.error = { ...failureErrorInfo(settledError), message: 'One or more analysis checks failed' };
    }
    if (sources.get(task.sourceKey)?.descriptor.policy === 'chatgpt-main/1' && (error || !result.coverage?.dependencies?.length)) {
      const settled = failureStore.settle({ sourceKey: task.sourceKey, mainId: task.id, lifecycleId: task.lifecycleId, roundId: round.id,
        domain: 'analysis', outcome: settledError ? 'failed' : 'succeeded', attempted: true, settled: true, settledAt: completedAt, error: settledError });
      nativeNotifications.observeFailure(settled.receipt);
    }
    save('tasks', task);
    tick();
  }

  function startNext() {
    if (closed || paused || active || pendingCancelledRequests > 0 || !analyzer) return;
    while (queue.length > 0) {
      const key = queue.shift();
      queued.delete(key);
      const task = tasks.get(key);
      if (!task || !usable(task) || observationOnly(task) || conversationPaused(task)) continue;
      if (now() < (task.analysis.nextEligibleAt ?? 0)) continue;
      const capturedEvents = publicEvents(task).filter(event => !historical(event));
      const instruction = contextFor(task);
      const roundContext = instruction && copy({ binding: instruction.binding, inheritedEvents: instruction.inheritedEvents.map(({ key, taskKey, sequence, ...event }) => event), gaps: instruction.gaps });
      const source = sources.get(task.sourceKey);
      const round = {
        id: randomUUID(),
        taskKey: key, requestId: task.requestId, revision: task.revision, observedAt: now(), resultKey: resultKeyFor(task), resultScope: resultScopeFor(task),
        through: capturedEvents.length ? Math.max(...capturedEvents.map(event => event.capturedAt)) : null,
        controller: new AbortController(), transportController: new AbortController(), cancelled: false,
        input: { task: copy({ key: task.key, id: task.id, lifecycleId: task.lifecycleId, title: task.title, status: task.status, currentRequest: task.currentRequest, currentRequestId: task.requestId, requestStartedAt: task.requestStartedAt, lastActivity: task.lastActivity, activeAlerts: task.alerts.filter(alert => alert.status === 'attention' && alert.requestId === task.requestId).map(({ id, checkId, reason, evidenceIds }) => ({ id, checkId, reason, evidenceIds })) }), source: copy(source.descriptor), events: capturedEvents, coverage: coverageFor(task), ...(roundContext ? { instructionContext: roundContext } : {}) },
      };
      active = round;
      task.analysis.state = 'analyzing';
      task.analysis.startedAt = round.observedAt;
      task.analysis.queuedAt = null;
      task.analysis.error = null;
      save('tasks', task);
      const currentAnalyzer = analyzer;
      Promise.resolve().then(() => round.cancelled ? null : currentAnalyzer.analyze(round.input, { signal: round.controller.signal, transportSignal: round.transportController.signal }))
        .then(result => round.cancelled ? null : validateResult(result, round))
        .then(result => completeRound(round, result, null), error => completeRound(round, null, error));
      break;
    }
  }

  function scheduleIdle() {
    const noSources = !Array.from(sources.values()).some(connected);
    if (!hadConnection || !noSources || closed) {
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = null;
      idleNotified = false;
      return;
    }
    cancelRound('Last source disconnected');
    if (!idleTimer && !idleNotified) {
      idleTimer = setTimeout(() => {
        idleTimer = null;
        if (closed || Array.from(sources.values()).some(connected)) return;
        idleNotified = true;
        Promise.resolve().then(() => onIdle?.()).catch(() => {});
      }, idleGraceMs);
      idleTimer.unref();
    }
  }

  function tick() {
    if (closed) return;
    const current = now();
    for (const source of sources.values()) {
      if (connected(source) && current - source.lastSeenAt >= leaseMs) resetSource(source, 'Source heartbeat lease expired');
    }
    prune();
    cleanQueue();
    for (const task of tasks.values()) {
      if (conversationPaused(task)) {
        if (task.analysis.state !== 'paused' || task.analysis.queuedAt !== null) {
          task.analysis.state = 'paused'; task.analysis.queuedAt = null; save('tasks', task);
        }
        continue;
      }
      if (observationOnly(task)) {
        if (task.analysis.state !== 'observation-only') {
          task.analysis.state = 'observation-only';
          task.analysis.queuedAt = null;
          task.analysis.nextEligibleAt = null;
          task.analysis.error = null;
          save('tasks', task);
        }
        continue;
      }
      if (!usable(task) || paused || active?.taskKey === task.key) continue;
      if (!analyzer) {
        task.analysis.state = 'unavailable';
        task.analysis.error = { code: 'analyzer_unconfigured', message: 'Inference dependencies are not configured' };
        task.analysis.checks = unknownChecks('Inference dependencies are not configured');
        continue;
      }
      if (current < (task.analysis.nextEligibleAt ?? 0)) continue;
      if (task.resultKey === resultKeyFor(task) && task.analysis.summary && !task.analysis.error && task.analysis.completedAt > current - DAY && task.analysis.checks.filter(check => check.status === 'attention').every(check => task.alerts.some(alert => alert.checkId === check.id && alert.requestId === task.requestId && alert.status === 'attention'))) {
        task.analysis.state = 'idle';
        continue;
      }
      if (!queued.has(task.key)) {
        queued.add(task.key);
        queue.push(task.key);
        task.analysis.queuedAt = current;
      }
      if (task.analysis.state !== 'queued') {
        task.analysis.state = 'queued';
        save('tasks', task);
      }
    }
    for (const notification of notifications.values()) {
      expireClaim(notification);
      if (['pending', 'claimed'].includes(notification.status) && !notificationEligible(notification, current)) {
        notification.status = 'cancelled';
        notification.error = 'Current alert eligibility is unknown';
        save('notifications', notification);
      }
    }
    // Native maintenance uses this tick's time; legacy task checks keep their own clock reads.
    nativeNotifications.reconcile(current);
    for (const task of tasks.values()) if (usable(task) && !observationOnly(task) && !paused && !conversationPaused(task) && !['unknown', 'failed', 'stale', 'interrupted'].includes(task.analysis.state) && !task.analysis.checks.some(check => check.judgmentState === 'failed')) makeNotification(task, task.alerts.filter(alert => alert.status === 'attention'));
    scheduleIdle();
    startNext();
    if (wakeTimer) clearTimeout(wakeTimer);
    wakeTimer = null;
    const times = Array.from(tasks.values()).filter(task => usable(task) && !observationOnly(task) && !conversationPaused(task) && task.analysis.nextEligibleAt > current).map(task => task.analysis.nextEligibleAt);
    times.push(...Array.from(notifications.values()).filter(notification => notification.status === 'pending' && notification.readyAt > current).map(notification => notification.readyAt));
    if (times.length > 0) {
      wakeTimer = setTimeout(tick, Math.max(1, Math.min(...times) - current));
      wakeTimer.unref();
    }
  }

  const interval = setInterval(tick, Math.max(25, Math.min(1_000, leaseMs / 2)));
  interval.unref();
  tick();
  const ensureOpen = () => { if (closed) throw new Error('Monitoring core is closed'); };
  function applyGSProjection(publicTasks, current) {
    for (const task of publicTasks) {
      const source = sources.get(task.sourceKey);
      const scopedTask = { ...task, events: task.events.filter(event => !historical(event)), requestId: tasks.get(task.key).requestId };
      task.gs = gsStatus(scopedTask, source, current, childWait(scopedTask, source, publicTasks, current));
      if (conversationPaused(task) && source.status === 'connected' && task.gs.light !== null) task.gs = { ...task.gs, light: 'gray', label: '本聊天分析已暂停', reason: '执行观察继续；仅手动恢复本聊天分析，恢复后按当前材料重新排队' };
      task.lastActivity = task.gs.silence.since;
    }
  }
  const getSource = sourceKey => {
    const source = sources.get(sourceKey);
    initializeHierarchyCaptures(source);
    if (connected(source) && now() - source.lastSeenAt >= leaseMs) resetSource(source, 'Source heartbeat lease expired');
    if (!connected(source)) throw new Error('Source is not registered or connected');
    return source;
  };
  const newTask = (sourceKey, descriptor) => ({
    key: randomUUID(), sourceKey, id: descriptor.id, lifecycleId: descriptor.lifecycleId,
    title: descriptor.title, status: descriptor.status ?? 'unknown', lastKnownStatus: descriptor.status ?? 'unknown', sessionId: descriptor.sessionId ?? null,
    parentId: descriptor.parentId ?? null, gaps: descriptor.gaps ?? [], baselineConfirmed: false,
    gapRecords: (descriptor.gaps ?? []).map(reason => ({ reason, at: now() })),
    currentRequest: '', requestId: null, requestStartedAt: null, updatedAt: now(), lastActivity: null, revision: 0,
    analysis: { state: 'unknown', through: null, throughRevision: 0, completedAt: null, nextEligibleAt: null, error: null, summary: null, checks: unknownChecks('No completed analysis'), queuedAt: null, startedAt: null }, alerts: [],
  });
  function admitRequest(task, event) {
    if (historical(event)) return;
    const currentInput = taskEvents(task).find(value => value.id === task.requestId);
    if (event.kind !== 'input' || (currentInput && (event.capturedAt < currentInput.capturedAt || event.capturedAt === currentInput.capturedAt && !(event.data?.inputPhase === 'consumed' && Number.isSafeInteger(event.data?.inputSequence) && event.data.inputSequence > (currentInput.data?.inputSequence ?? Infinity))))) return;
    cancelRound('Current request changed during analysis', task, true);
    invalidateAlerts(task, 'superseded');
    task.currentRequest = event.text; task.requestId = event.id; task.requestStartedAt = event.capturedAt;
    task.analysis.state = 'unknown'; task.analysis.summary = null; task.analysis.checks = unknownChecks('Current request has not been analyzed');
    task.analysis.nextEligibleAt = Math.max(now(), task.analysis.nextEligibleAt ?? 0);
  }

  return {
    async configureSources(entries) {
      ensureOpen();
      tick();
      const normalized = normalizeEnrollment(entries);
      const previous = new Map(enrollment.entries.map(entry => [entry.registrationId, entry]));
      enrollment = { configuredAt: now(), managed: true, entries: normalized.map(entry => {
        const seen = Array.from(sources.values()).filter(source => source.descriptor.registrationId === entry.registrationId).map(source => source.lastSeenAt);
        const lastSeenAt = previous.get(entry.registrationId)?.lastSeenAt ?? null;
        return { ...entry, lastSeenAt: seen.length ? Math.max(lastSeenAt ?? 0, ...seen) : lastSeenAt };
      }) };
      saveEnrollment();
      return enrollmentSnapshot();
    },
    async sourcePlan() {
      ensureOpen();
      tick();
      return enrollmentSnapshot();
    },
    async registerSource(descriptor) {
      ensureOpen();
      if (descriptor.policy === 'chatgpt-main/1') nativeOrigins.checkVersion(descriptor.registrationId, descriptor.nativeOriginVersion ?? 1);
      enrollEnabledAdapter(descriptor);
      let source = Array.from(sources.values()).find(value => identity(value.descriptor.registrationId, value.descriptor.hostRunId) === identity(descriptor.registrationId, descriptor.hostRunId));
      if (!source) {
        source = { key: randomUUID(), descriptor: copy(descriptor), status: 'connected', lastSeenAt: now(), gaps: [], gapRecords: [], baselineConfirmed: false };
        sources.set(source.key, source);
      } else {
        if (now() - source.lastSeenAt >= leaseMs) resetSource(source, 'Source lease expired before re-registration');
        const modeChanged = source.descriptor.mode !== descriptor.mode;
        const policyChanged = source.descriptor.policy !== descriptor.policy;
        Object.assign(source, { descriptor: copy(descriptor), status: 'connected', lastSeenAt: now(), disconnectReason: null });
        if (policyChanged) {
          delete source.hierarchyContext;
          delete source.hierarchyMaterialKey;
        }
        if (modeChanged || policyChanged) for (const task of tasks.values()) if (task.sourceKey === source.key) {
          cancelRound('Source observation mode or policy changed', task);
          invalidateAlerts(task, 'unknown');
          if (policyChanged) { task.currentRequest = ''; task.requestId = null; task.requestStartedAt = null; task.contextFingerprint = null; }
          task.resultKey = null;
          task.resultScope = null;
          Object.assign(task.analysis, { state: observationOnly(task) ? 'observation-only' : 'unknown',
            summary: null, summaryAt: null, completedAt: null, startedAt: null, through: null, throughRevision: 0,
            queuedAt: null, nextEligibleAt: null, error: null, inputEventIds: [], contextEvidenceRefs: [],
            checks: unknownChecks('Source observation mode or policy changed; prior judgments are not current') });
          save('tasks', task);
        }
      }
      hadConnection = true;
      save('sources', source);
      noteSourceSeen(source);
      tick();
      return source.key;
    },
    async nativeOriginFence(sourceKey, key) { ensureOpen(); const source = getSource(sourceKey); if (source.descriptor.policy !== 'chatgpt-main/1' || source.descriptor.nativeOriginVersion !== 2 || !/^[a-f0-9]{64}$/.test(key ?? '')) throw Object.assign(new Error('invalid native origin scope'), { code: 'native_origin_invalid' }); return nativeOrigins.read(source.descriptor.registrationId, key); },
    async heartbeat(sourceKey) {
      ensureOpen();
      const source = getSource(sourceKey);
      source.lastSeenAt = now();
      save('sources', source);
      noteSourceSeen(source);
      tick();
      return { sourceKey, lastSeenAt: source.lastSeenAt };
    },
    async ingest(sourceKey, batch) {
      ensureOpen();
      const source = getSource(sourceKey);
      if (source.hierarchyCaptureMigrationBlocked) throw Object.assign(new Error(HIERARCHY_CAPTURE_GAP), { code: 'hierarchy_context_invalid' });
      if (source.descriptor.policy !== 'chatgpt-main/1' && batch.events?.some(event => event.data?.forkHistoryPending === true)) throw Object.assign(new Error('pending fork input requires ChatGPT main policy'), { code: 'hierarchy_context_invalid' });
      const originPlan = source.descriptor.policy === 'chatgpt-main/1' ? nativeOrigins.project(source.descriptor.registrationId, source.descriptor.nativeOriginVersion ?? 1, batch.nativeOrigins, batch.events) : null;
      const nativeCaptureTime = source.descriptor.policy === 'chatgpt-main/1' && batch.events?.some(event => event.data?.nativeRecordId) ? now() : null;
      let nativePlan;
      if (nativeCaptureTime !== null) {
        try { nativePlan = nativeProgress.project(source.descriptor.registrationId, batch.events, nativeCaptureTime, locator => locator && events.get(identity(locator.sourceKey, locator.taskId, locator.lifecycleId, locator.eventId))); }
        catch (error) {
          if (['native_capture_capacity', 'native_capture_body_unavailable'].includes(error.code)) { rememberGap(source, { reason: error.code, at: nativeCaptureTime }); save('sources', source); }
          throw error;
        }
        batch = { ...batch, events: nativePlan.events };
      }
      // Compact native proof identity is independent of its observation lifecycle.
      // Keep the first body/container and never give replayed history a new clock.
      const nativeCaptures = new Map((source.hierarchyEvidenceCaptures ?? []).filter(value => value.nativeOriginId).map(value => [value.nativeOriginId, { ...value }]));
      const persistedNativeIds = new Set(nativeCaptures.keys());
      const nativeScopes = new Map(), nativeBodies = new Map(), earlierNativeCaptures = new Map();
      for (const capture of source.hierarchyEvidenceCaptures ?? []) {
        if (!capture.nativeOriginId) continue;
        const event = events.get(identity(sourceKey, capture.taskId, capture.lifecycleId, capture.eventId));
        const first = nativeCaptures.get(capture.nativeOriginId);
        if (first.capturedAt > capture.capturedAt) nativeCaptures.set(capture.nativeOriginId, { ...capture });
        nativeScopes.set(identity(capture.nativeOriginId, capture.taskId, capture.nativeHistorical ?? Boolean(event && historical(event))), capture);
        if (event) nativeBodies.set(referenceKey(capture), event);
      }
      if (source.descriptor.policy === 'chatgpt-main/1') batch = { ...batch, events: (batch.events ?? []).filter(event => {
        const nativeId = nativeForkIdentity(event), first = nativeId && nativeCaptures.get(nativeId);
        if (event.data?.nativeRecordId && nativeId) {
          const scope = identity(nativeId, event.taskId, historical(event)), previous = nativeScopes.get(scope);
          if (first) {
            const body = nativeBodies.get(referenceKey(first));
            if (first.capturedAt <= nativeCaptureTime - DAY || persistedNativeIds.has(nativeId) && !body) return false;
            if (previous && referenceKey(previous) !== referenceKey(eventRef(event))) return false;
            if (body && (body.text !== event.text || Boolean(body.data?.truncated) !== Boolean(event.data?.truncated) || Boolean(body.data?.redacted) !== Boolean(event.data?.redacted))) throw Object.assign(new Error('Native record body conflicts with its first capture'), { code: 'hierarchy_context_invalid' });
            const earliest = Math.min(first.capturedAt, event.capturedAt);
            if (earliest < first.capturedAt) { earlierNativeCaptures.set(nativeId, earliest); nativeCaptures.set(nativeId, { ...first, capturedAt: earliest }); }
            event.capturedAt = earliest;
            if (body?.evidence) event.evidence = body.evidence;
          } else if (event.capturedAt <= nativeCaptureTime) nativeCaptures.set(nativeId, { ...eventRef(event), capturedAt: event.capturedAt });
          if (!previous) nativeScopes.set(scope, { ...eventRef(event), capturedAt: event.capturedAt });
          nativeBodies.set(referenceKey(eventRef(event)), event);
          return true;
        }
        if (first && persistedNativeIds.has(nativeId) && !events.has(identity(sourceKey, first.taskId, first.lifecycleId, first.eventId))) return false;
        if (nativeId && !first && event.capturedAt <= now()) nativeCaptures.set(nativeId, { ...eventRef(event), capturedAt: event.capturedAt });
        return !first || first.capturedAt > now() - DAY && referenceKey(first) === referenceKey(eventRef(event));
      }) };
      const incoming = batch.hierarchyContext, previous = source.hierarchyContext;
      let acceptedRuns, acceptedCaptures;
      if (incoming) {
        if (previous && incoming.revision < previous.revision) return { accepted: 0, duplicate: 0, expired: 0, paused, hierarchyContext: { schemaVersion: 1, acceptedRevision: previous.revision } };
        if (previous && incoming.revision === previous.revision && JSON.stringify(incoming) !== JSON.stringify(previous)) throw Object.assign(new Error('hierarchy revision content conflict'), { code: 'hierarchy_context_invalid' });
        const captures = new Map((source.hierarchyEvidenceCaptures ?? []).map(value => [referenceKey(value), value]));
        for (const event of batch.events ?? []) if (captures.get(referenceKey(eventRef(event)))?.capturedAt <= now() - DAY) {
          throw Object.assign(new Error('Expired hierarchy evidence cannot acquire a new first capture timestamp'), { code: 'hierarchy_context_invalid' });
        }
        const prospectiveTasks = new Map(Array.from(tasks.values()).filter(task => task.sourceKey === sourceKey).map(task => [identity(task.id, task.lifecycleId), task]));
        for (const task of batch.tasks ?? []) prospectiveTasks.set(identity(task.id, task.lifecycleId), task);
        const prospectiveEvents = new Map(Array.from(events.values()).filter(event => tasks.get(event.taskKey)?.sourceKey === sourceKey).map(event => [identity(event.taskId, event.taskLifecycleId, event.id), event]));
        for (const event of batch.events ?? []) if (!prospectiveEvents.has(identity(event.taskId, event.taskLifecycleId, event.id))) prospectiveEvents.set(identity(event.taskId, event.taskLifecycleId, event.id), event);
        acceptedRuns = validateHierarchy(incoming, { tasks: [...prospectiveTasks.values()], events: [...prospectiveEvents.values()], now: now(), acceptedRuns: source.hierarchyRunBindings ?? [], sourcePolicy: source.descriptor.policy });
        const references = [...incoming.activations.flatMap(item => item.stateRefs), ...incoming.relations.flatMap(item => item.evidenceRefs), ...incoming.instructionBindings.flatMap(item => [item.inputRef, ...item.originRefs, ...item.ancestorLimitRefs]), ...(incoming.waitBindings ?? []).flatMap(item => item.evidenceRefs)];
        for (const reference of references) {
          const capture = captures.get(referenceKey(reference));
          if (capture && capture.capturedAt <= now() - DAY) throw Object.assign(new Error('Expired hierarchy evidence cannot acquire a new first capture timestamp'), { code: 'hierarchy_context_invalid' });
          if (!capture) captures.set(referenceKey(reference), { ...reference, capturedAt: prospectiveEvents.get(identity(reference.taskId, reference.lifecycleId, reference.eventId)).capturedAt });
        }
        if (captures.size > HIERARCHY_CAPTURE_LIMIT) {
          source.hierarchyCaptureBudgetExceeded = true;
          rememberGap(source, { reason: HIERARCHY_CAPTURE_GAP, at: now() });
          save('sources', source);
          throw Object.assign(new Error(HIERARCHY_CAPTURE_GAP), { code: 'hierarchy_context_invalid' });
        }
        acceptedCaptures = [...captures.values()];
      }
      if (!incoming && source.hierarchyEvidenceCaptures) {
        const expiredCaptures = new Set(source.hierarchyEvidenceCaptures.filter(value => value.capturedAt <= now() - DAY).map(referenceKey));
        if ((batch.events ?? []).some(event => expiredCaptures.has(referenceKey(eventRef(event))))) throw Object.assign(new Error('Expired hierarchy evidence cannot acquire a new first capture timestamp'), { code: 'hierarchy_context_invalid' });
      }
      // Only native ChatGPT batches need the new progress dependency. Persist
      // its SQL first, but publish neither its memory nor the batch until both
      // sides release this savepoint. Legacy/DSH ingestion keeps its old path.
      let transaction = null, applyNativeProgress = null, applyNativeOrigins = null;
      if (nativePlan || originPlan) {
        transaction = { rows: new Map(), sequence, activeTaskKey: active?.taskKey ?? null };
        db.exec('SAVEPOINT native_ingest'); nativeJournal = transaction;
      }
      try {
        if (originPlan) applyNativeOrigins = originPlan.stage(nativePlan?.hasProgress ?? false);
        if (nativePlan) applyNativeProgress = nativePlan.stage((batch.events ?? []).map(event => ({ event: events.get(identity(sourceKey, event.taskId, event.taskLifecycleId, event.id)) ?? event, locator: { sourceKey, taskId: event.taskId, lifecycleId: event.taskLifecycleId, eventId: event.id } })));
        if (source.descriptor.policy === 'chatgpt-main/1') {
          const captures = new Map((acceptedCaptures ?? source.hierarchyEvidenceCaptures ?? []).map(value => [referenceKey(value), value]));
          for (const event of batch.events ?? []) {
            const nativeOriginId = nativeForkIdentity(event);
            if (!nativeOriginId || event.capturedAt > now()) continue;
            const reference = eventRef(event), id = referenceKey(reference);
            if (captures.has(id)) { captures.get(id).nativeOriginId = nativeOriginId; if (event.data?.nativeRecordId) captures.get(id).nativeHistorical = historical(event); continue; }
            // Reserve existing ledger space for current explicit execution proofs.
            if (captures.size >= HIERARCHY_CAPTURE_LIMIT - 2000) { source.forkCaptureBudgetExceeded = true; continue; }
            captures.set(id, { ...reference, capturedAt: event.capturedAt, nativeOriginId, ...(event.data?.nativeRecordId ? { nativeHistorical: historical(event) } : {}) });
          }
          acceptedCaptures = [...captures.values()];
          source.hierarchyEvidenceCaptures = acceptedCaptures;
        }
        // A delayed earlier source capture may shorten, never extend, the shared
        // native clock. Commit this only after the whole incoming proof validates.
        for (const [nativeId, capturedAt] of nativePlan?.earlier ?? []) earlierNativeCaptures.set(nativeId, Math.min(earlierNativeCaptures.get(nativeId) ?? Infinity, capturedAt));
        if (earlierNativeCaptures.size) {
          for (const capture of acceptedCaptures) if (earlierNativeCaptures.has(capture.nativeOriginId)) capture.capturedAt = Math.min(capture.capturedAt, earlierNativeCaptures.get(capture.nativeOriginId));
          const changed = new Set();
          for (const event of events.values()) {
            const eventSource = sources.get(tasks.get(event.taskKey)?.sourceKey);
            if (eventSource?.descriptor.policy !== 'chatgpt-main/1' || eventSource.descriptor.registrationId !== source.descriptor.registrationId) continue;
            const earliest = earlierNativeCaptures.get(nativeForkIdentity(event));
            if (earliest === undefined || event.capturedAt <= earliest) continue;
            event.capturedAt = earliest; save('events', event); changed.add(event.taskKey);
          }
          for (const other of sources.values()) if (other !== source && other.descriptor.policy === 'chatgpt-main/1' && other.descriptor.registrationId === source.descriptor.registrationId) {
            let changed = false;
            for (const capture of other.hierarchyEvidenceCaptures ?? []) if (earlierNativeCaptures.has(capture.nativeOriginId) && capture.capturedAt > earlierNativeCaptures.get(capture.nativeOriginId)) { capture.capturedAt = earlierNativeCaptures.get(capture.nativeOriginId); changed = true; }
            if (changed) save('sources', other);
          }
          for (const key of changed) {
            const task = tasks.get(key), retained = taskEvents(task).filter(event => !historical(event));
            task.lastActivity = retained.length ? Math.max(...retained.map(event => event.capturedAt)) : null;
            task.revision++; save('tasks', task);
          }
        }
        source.lastSeenAt = now();
        if (batch.gaps) {
          for (const reason of batch.gaps) {
            rememberGap(source, { reason, at: now() });
          }
        }
        if (batch.tasks) source.baselineConfirmed = true;
        save('sources', source);
        noteSourceSeen(source);
        const counts = { accepted: 0, duplicate: 0, expired: 0, paused };
        for (const descriptor of batch.tasks ?? []) {
          let task = findTask(sourceKey, descriptor.id, descriptor.lifecycleId);
          if (!task) { task = newTask(sourceKey, descriptor); rememberNativeRow('tasks', task.key); tasks.set(task.key, task); }
          const before = JSON.stringify([task.title, task.status, task.sessionId, task.parentId, task.gaps, task.baselineConfirmed]);
          task.title = descriptor.title;
          task.status = descriptor.status;
          if (task.status !== 'unknown') task.lastKnownStatus = task.status;
          task.sessionId = descriptor.sessionId ?? task.sessionId;
          task.parentId = descriptor.parentId ?? task.parentId;
          for (const reason of descriptor.gaps ?? []) {
            if (!task.gaps.includes(reason)) rememberGap(task, { reason, at: now() });
          }
          task.gaps = [...new Set([...(task.gaps ?? []), ...(descriptor.gaps ?? [])])].filter(reason => reason !== 'Task baseline is not yet available');
          task.gapRecords = (task.gapRecords ?? []).filter(gap => gap.reason !== 'Task baseline is not yet available');
          task.baselineConfirmed = !(observationOnly(task) || source.descriptor.policy === 'chatgpt-main/1') || descriptor.status !== 'unknown';
          const changed = before !== JSON.stringify([task.title, task.status, task.sessionId, task.parentId, task.gaps, task.baselineConfirmed]);
          if (changed) {
            task.updatedAt = now();
            task.revision++;
            if (task.status !== 'running') { cancelRound('Source task is no longer running', task); invalidateAlerts(task, task.status === 'unknown' ? 'unknown' : 'ended'); }
            save('tasks', task);
          }
        }
        for (const original of batch.events ?? []) {
          if (original.capturedAt > now()) {
            rememberGap(source, { reason: 'Rejected event with a future capture timestamp', at: now() });
            save('sources', source);
            counts.expired++;
            continue;
          }
          let task = findTask(sourceKey, original.taskId, original.taskLifecycleId);
          if (!task) {
            task = newTask(sourceKey, { id: original.taskId, lifecycleId: original.taskLifecycleId, title: original.taskId, status: 'unknown' });
            rememberNativeRow('tasks', task.key);
            task.gaps = ['Task baseline is not yet available'];
            task.gapRecords = [{ reason: 'Task baseline is not yet available', at: now() }];
            tasks.set(task.key, task);
          }
          const eventKey = identity(sourceKey, original.taskId, original.taskLifecycleId, original.id);
          if (events.has(eventKey)) {
            if (events.get(eventKey).capturedAt <= now() - DAY) counts.expired++;
            else counts.duplicate++;
            continue;
          }
          if (original.capturedAt <= now() - DAY) { counts.expired++; continue; }
          const event = { ...copy(original), key: eventKey, taskKey: task.key, sequence: ++sequence };
          events.set(eventKey, event);
          save('events', event);
          counts.accepted++;
          task.updatedAt = now();
          if (!historical(event)) task.lastActivity = Math.max(task.lastActivity ?? 0, event.capturedAt);
          task.revision++;
          if (!incoming && !source.hierarchyContext && source.descriptor.policy !== 'chatgpt-main/1') admitRequest(task, event);
          if (event.kind === 'gap' && !historical(event)) {
            rememberGap(task, { reason: event.text, at: event.capturedAt });
          }
          if (!historical(event) && event.kind === 'status' && ['running', 'idle', 'disposed', 'unknown'].includes(event.data?.status) && (source.descriptor.policy !== 'chatgpt-main/1' || task.baselineConfirmed)) {
            task.status = event.data.status;
            if (task.status !== 'unknown') task.lastKnownStatus = task.status;
            if (task.status !== 'running') { cancelRound('Source task is no longer running', task); invalidateAlerts(task, task.status === 'unknown' ? 'unknown' : 'ended'); }
          }
          save('tasks', task);
        }
        if (incoming) {
          const materialKey = fingerprint([incoming.sessions, incoming.activations, incoming.relations, incoming.instructionBindings, incoming.waitBindings, incoming.coverage]);
          if (materialKey !== source.hierarchyMaterialKey) { source.hierarchyMaterialKey = materialKey; source.hierarchyMaterialAt = now(); }
          source.hierarchyContext = copy(incoming);
          source.hierarchyRunBindings = acceptedRuns;
          source.hierarchyEvidenceCaptures = acceptedCaptures;
          for (const activation of incoming.activations) {
            const prior = previous?.activations.find(item => item.taskId === activation.taskId && item.lifecycleId === activation.lifecycleId);
            const task = findTask(sourceKey, activation.taskId, activation.lifecycleId);
            if (task && prior?.runId && activation.runId && activation.runId !== prior.runId) {
              delete task.notificationBinding;
              cancelRound('Native execution activation changed', task, true); invalidateAlerts(task, 'superseded');
              task.excludedInstructionIds = [...new Set([...(task.excludedInstructionIds ?? []), ...previous.instructionBindings.filter(binding => binding.taskId === task.id && binding.lifecycleId === task.lifecycleId && binding.phase === 'consumed').map(binding => binding.inputRef.eventId)])].slice(-1000);
              task.currentRequest = ''; task.requestId = null; task.requestStartedAt = null;
              task.analysis.state = 'unknown'; task.analysis.summary = null; task.analysis.checks = unknownChecks('New native execution has no consumed instruction'); task.revision++; save('tasks', task);
            }
          }
          for (const task of tasks.values()) if (task.sourceKey === sourceKey) bindNotificationRoot(task, incoming);
          for (const binding of incoming.instructionBindings) if (binding.phase === 'consumed' && binding.origin !== 'runtime-notice' && (source.descriptor.policy !== 'chatgpt-main/1' || binding.origin === 'user-entry')) {
            const task = findTask(sourceKey, binding.taskId, binding.lifecycleId), event = events.get(identity(sourceKey, binding.inputRef.taskId, binding.inputRef.lifecycleId, binding.inputRef.eventId));
            if (task && event && !task.excludedInstructionIds?.includes(event.id)) { admitRequest(task, event); save('tasks', task); }
          }
          for (const task of tasks.values()) if (task.sourceKey === sourceKey) {
            const context = contextFor(task), fingerprint = JSON.stringify([context.binding, context.inheritedEvents.map(event => [eventRef(event), event.capturedAt]), context.gaps]);
            if (task.contextFingerprint !== fingerprint) {
              cancelRound('Instruction context changed during analysis', task, true);
              invalidateAlerts(task, 'unknown');
              task.contextFingerprint = fingerprint; task.revision++;
              task.analysis.state = 'unknown'; task.analysis.summary = null; task.analysis.checks = unknownChecks('Current instruction context has not been analyzed');
              save('tasks', task);
            }
          }
          save('sources', source); counts.hierarchyContext = { schemaVersion: 1, acceptedRevision: incoming.revision };
        }
        if (transaction) { db.exec('RELEASE native_ingest'); nativeJournal = null; transaction = null; applyNativeProgress?.(); applyNativeOrigins?.(); }
        if (batch.nativeOrigins) counts.nativeOrigins = batch.nativeOrigins.map(proof => nativeOrigins.read(source.descriptor.registrationId, proof.key));
        tick();
        return counts;
      } catch (error) {
        if (transaction) {
          nativeJournal = null;
          db.exec('ROLLBACK TO native_ingest'); db.exec('RELEASE native_ingest');
          for (const { collection, key, value } of transaction.rows.values()) {
            if (collections[collection]) { if (value === null) collections[collection].delete(key); else collections[collection].set(key, JSON.parse(value)); }
            else if (collection === 'settings' && key === 'enrollment' && value !== null) enrollment = JSON.parse(value);
          }
          sequence = transaction.sequence;
          // Cancellation of an own model call cannot be undone. Restored
          // content stays intact; its result remains conservatively unknown.
          if (transaction.activeTaskKey && active?.taskKey !== transaction.activeTaskKey) {
            const task = tasks.get(transaction.activeTaskKey);
            if (task) { task.analysis.state = 'interrupted'; task.analysis.summary = null; task.resultKey = null; task.resultScope = null; save('tasks', task); }
          }
        }
        throw error;
      }
    },
    async unregisterSource(sourceKey, reason = 'Source disconnected') {
      ensureOpen();
      const source = sources.get(sourceKey);
      if (source) resetSource(source, reason);
      tick();
      return { sourceKey, status: 'disconnected' };
    },
    async snapshot() {
      ensureOpen();
      tick();
      const current = now(), read = projectionRead();
      const publicTasks = Array.from(tasks.values()).map(task => {
        const { baselineConfirmed, revision, requestId, contextFingerprint, excludedInstructionIds, resultKey, resultScope, notificationBinding, ...value } = copy(task);
        value.notificationScope = notificationScope(task);
        const instruction = contextFor(task, read);
        value.events = publicEvents(task, instruction, read);
        if (instruction) value.instructionContext = copy({ binding: instruction.binding, ancestorLimitRefs: instruction.inheritedEvents.filter(event => event.contextRole === 'ancestor-limit').map(eventRef), userLimitRefs: instruction.inheritedEvents.filter(event => event.contextRole === 'user-limit').map(eventRef), gaps: instruction.gaps });
        value.coverage = coverageFor(task, read);
        value.activity = activityFor(task, value.events, value.coverage, current);
        value.analysis.lagMs = task.lastActivity === null ? null : Math.max(0, task.lastActivity - (task.analysis.through ?? task.lastActivity));
        value.analysis.waitMs = task.analysis.queuedAt === null ? 0 : Math.max(0, current - task.analysis.queuedAt);
        value.analysis.resultDisposition = resultDispositionFor(task, instruction, read);
        const conversation = conversationFor(task);
        if (conversation) value.conversationMonitoring = conversation;
        if (sources.get(task.sourceKey)?.descriptor.policy === 'chatgpt-main/1') value.monitoringHealth = failureStore.health({ sourceKey: task.sourceKey, mainId: task.id, lifecycleId: task.lifecycleId }, current);
        return value;
      });
      applyGSProjection(publicTasks, current);
      const hierarchyContext = projectHierarchy([...sources.values()], publicTasks);
      return {
        protocol: 1, generatedAt: current,
        monitoring: { state: paused ? 'paused' : 'active', paused, queueLength: queue.filter(key => usable(tasks.get(key) ?? {})).length, activeTaskKey: active?.taskKey ?? null, pendingCancelledRequests, admission: analysisAdmission(), retentionMs: DAY },
        overview: { runningTasks: publicTasks.filter(task => task.status === 'running').length, knownSources: sources.size, connectedSources: Array.from(sources.values()).filter(connected).length, attentionTasks: publicTasks.filter(task => task.alerts.some(alert => alert.status === 'attention')).length, coverage: { complete: false, gaps: ['Automatic discovery of all local sources has not been verified'] } },
        enrollment: enrollmentSnapshot(),
        hierarchyContext,
        sources: Array.from(sources.values()).map(source => ({ ...copy(source), ...(source.descriptor.policy === 'chatgpt-main/1' ? { monitoringHealth: failureStore.health({ sourceKey: source.key, mainId: null, lifecycleId: null }, current) } : {}) })), tasks: publicTasks,
      };
    },
    async gsSummary({ sourceKey, sessionId }) {
      ensureOpen();
      if (![sourceKey, sessionId].every(value => typeof value === 'string' && value.trim() && value.length <= 2000)) throw new Error('Invalid GS summary scope');
      tick();
      const current = now(), read = projectionRead(), source = sources.get(sourceKey), sourceStatus = source?.status ?? 'unknown';
      const scopedTasks = Array.from(tasks.values()).filter(task => task.sourceKey === sourceKey).map(task => {
        const context = contextFor(task, read);
        return { key: task.key, sourceKey, id: task.id, lifecycleId: task.lifecycleId, sessionId: task.sessionId,
          status: task.status, lastKnownStatus: task.lastKnownStatus, currentRequest: task.currentRequest, requestStartedAt: task.requestStartedAt,
          events: taskEvents(task, read).map(({ id, taskId, taskLifecycleId, kind, capturedAt, data }) => ({ id, taskId, taskLifecycleId, kind, capturedAt, data })),
          alerts: task.alerts, analysis: { completedAt: task.analysis.completedAt, inputEventIds: task.analysis.inputEventIds, checks: task.analysis.checks, coverage: task.analysis.coverage, error: task.analysis.error, resultDisposition: resultDispositionFor(task, context, read) },
          coverage: coverageFor(task, read), instructionContext: context ? { binding: context.binding, gaps: context.gaps } : null };
      });
      applyGSProjection(scopedTasks, current);
      const projection = projectHierarchy(source ? [source] : [], scopedTasks), scope = projection.scopes.find(value => value.sessionId === sessionId);
      const group = scope && projection.groups.find(value => value.key === scope.groupKey);
      const nodes = group?.nodes.slice(scope.start, scope.end) ?? [];
      const emptySourceGaps = nodes.length ? [] : [...(source?.descriptor.gaps ?? []), ...(source?.gapRecords ?? []).map(gap => gap.reason), ...(source?.hierarchyCaptureBudgetExceeded ? [HIERARCHY_CAPTURE_GAP] : [])];
      const gaps = [...new Set([...(sourceStatus === 'connected' ? [] : [`source_${sourceStatus}`]), ...(!scope ? ['gs_scope_unconfirmed'] : []), ...(scope?.coverage.branchGaps.map(gap => gap.reason) ?? []), ...emptySourceGaps, ...nodes.filter(node => node.light).flatMap(node => node.gs.coverage.gaps)])];
      return { protocol: 1, generatedAt: current, sourceKey, sessionId, sourceStatus, scopeKnown: Boolean(scope),
        counts: scope ? copy(scope.counts) : { green: 0, yellow: 0, red: 0, gray: 0 },
        coverage: { complete: Boolean(scope?.coverage.complete) && sourceStatus === 'connected' && gaps.length === 0, gaps: gaps.length <= 100 ? gaps : [...gaps.slice(0, 99), 'gs_summary_gap_limit'] }, gsStatus: { supported: true } };
    },
    async diagnostics() {
      if (!closed) tick();
      const timers = { lease: closed ? 0 : 1, wakeup: wakeTimer ? 1 : 0, idle: idleTimer ? 1 : 0 };
      const admission = analysisAdmission();
      timers.total = timers.lease + timers.wakeup + timers.idle;
      const contexts = [...sources.values()].flatMap(source => source.hierarchyContext ? [source.hierarchyContext] : []);
      const hierarchyReferences = contexts.reduce((count, context) => count + context.activations.reduce((sum, activation) => sum + activation.stateRefs.length, 0) + context.relations.reduce((sum, relation) => sum + relation.evidenceRefs.length, 0) + context.instructionBindings.reduce((sum, binding) => sum + 1 + binding.originRefs.length + binding.ancestorLimitRefs.length, 0) + (context.waitBindings ?? []).reduce((sum, binding) => sum + binding.evidenceRefs.length, 0), 0);
      const metadata = { hierarchySessions: contexts.reduce((count, context) => count + context.sessions.length, 0), hierarchyActivations: contexts.reduce((count, context) => count + context.activations.length, 0), hierarchyRelations: contexts.reduce((count, context) => count + context.relations.length, 0), hierarchyReferences,
        nativeRunBindings: [...sources.values()].reduce((count, source) => count + (source.hierarchyRunBindings?.length ?? 0), 0), analysisReuseKeys: [...tasks.values()].reduce((count, task) => count + Number(task.resultKey != null) + Number(task.resultScope != null), 0), taskContextFingerprints: [...tasks.values()].filter(task => task.contextFingerprint != null).length,
        notificationBindings: [...tasks.values()].filter(task => task.notificationBinding).length, notificationBindingEvidenceRefs: [...tasks.values()].reduce((count, task) => count + (task.notificationBinding?.evidenceRefs.length ?? 0), 0), notificationMemberships: [...notifications.values()].reduce((count, notification) => count + notificationMembers(notification).length, 0) };
      return {
        protocol: 1, pid: process.pid, generatedAt: now(), closed, failureReceipts: failureStore.diagnostics(), nativeNotifications: nativeNotifications.diagnostics(),
        runtime: { connectedSources: closed ? 0 : Array.from(sources.values()).filter(connected).length, activeAnalyses: active ? 1 : 0, cancelledUnsettledRequests: pendingCancelledRequests, unsettledAnalysisCalls: admission.unsettledCalls, analysisAdmissionLimit: admission.limit, analysisAdmissionBlocked: admission.blocked },
        temporary: { queueKeys: queue.length, queueMemberships: queued.size, timers, pendingNotificationWindows: [...notifications.values()].filter(notification => notification.status === 'pending').length },
        retained: { retentionMs: DAY, sources: sources.size, tasks: tasks.size, events: events.size, alerts: Array.from(tasks.values()).reduce((count, task) => count + task.alerts.length, 0), notifications: notifications.size, sourceGapRecords: Array.from(sources.values()).reduce((count, source) => count + (source.gapRecords?.length ?? 0), 0), taskGapRecords: Array.from(tasks.values()).reduce((count, task) => count + (task.gapRecords?.length ?? 0), 0), hierarchyEvidenceIdentities: Array.from(sources.values()).reduce((count, source) => count + (source.hierarchyEvidenceCaptures?.length ?? 0), 0), hierarchyEvidenceIdentityLimitPerSource: HIERARCHY_CAPTURE_LIMIT, nativeOriginFences: nativeOrigins.diagnostics(), nativeCaptureProgress: nativeProgress.diagnostics(locator => locator && events.get(identity(locator.sourceKey, locator.taskId, locator.lifecycleId, locator.eventId))), ...metadata },
        configuration: { sourcePlanEntries: enrollment.entries.length, conversationMonitoringScopes: conversationSettings.size, conversationMonitoringLimit: CONVERSATION_MONITORING_LIMIT, pausedConversations: [...conversationSettings.values()].filter(setting => setting.paused).length }, memory: process.memoryUsage(),
      };
    },
    async conversationMonitoring(value) {
      ensureOpen();
      return conversationScope(normalizeConversationMonitoring(value));
    },
    async failureReceipts(scope) {
      ensureOpen();
      scope = normalizeFailureScope(scope);
      const source = sources.get(scope.sourceKey);
      if (source?.descriptor.policy !== 'chatgpt-main/1') throw new Error('Failure receipts require a known ChatGPT source');
      return failureStore.list(scope);
    },
    async setNativeNotificationEligibility(value) {
      ensureOpen();
      return nativeNotifications.setEligibility(normalizeNativeNotificationEligibility(value));
    },
    async nativeNotificationRecords(value) {
      ensureOpen();
      return nativeNotifications.records(normalizeNativeNotificationQuery(value));
    },
    async recordNativeNotificationResult(value) {
      ensureOpen();
      return nativeNotifications.recordResult(normalizeNativeNotificationResult(value));
    },
    async recordCollectionOutcome(value) {
      ensureOpen();
      const outcome = normalizeCollectionOutcome(value), source = sources.get(outcome.sourceKey);
      if (source?.descriptor.policy !== 'chatgpt-main/1') throw new Error('Collection outcomes require a known ChatGPT source');
      if (outcome.mainId !== null && outcome.lifecycleId !== null && ![...tasks.values()].some(task => task.sourceKey === outcome.sourceKey && task.id === outcome.mainId && task.lifecycleId === outcome.lifecycleId)) throw new Error('Collection outcome scope is not a verified task');
      const settled = failureStore.settle({ ...outcome, domain: 'collection', attempted: true, settled: true });
      nativeNotifications.observeFailure(settled.receipt);
      return settled;
    },
    async setConversationMonitoring(value) {
      ensureOpen();
      const control = normalizeConversationMonitoring(value, true), current = conversationScope(control);
      if (current.paused === control.paused) return current;
      if (current.revision !== control.revision) throw Object.assign(new Error('Conversation monitoring state changed'), { code: 'conversation_monitoring_conflict' });
      if (!conversationSettings.has(identity(control.registrationId, control.taskId)) && conversationSettings.size >= CONVERSATION_MONITORING_LIMIT) throw Object.assign(new Error('Conversation monitoring preference capacity reached; existing choices remain unchanged'), { code: 'conversation_monitoring_capacity' });
      const setting = { key: identity(control.registrationId, control.taskId), registrationId: control.registrationId, taskId: control.taskId, paused: control.paused, revision: randomUUID() };
      save('conversationSettings', setting); conversationSettings.set(setting.key, setting);
      for (const task of tasks.values()) {
        if (sources.get(task.sourceKey)?.descriptor.policy !== 'chatgpt-main/1' || sources.get(task.sourceKey)?.descriptor.registrationId !== control.registrationId || task.id !== control.taskId) continue;
        if (control.paused) cancelRound('Conversation analysis is paused', task, true);
        task.analysis.queuedAt = null;
        task.analysis.state = control.paused ? 'paused' : 'unknown';
        task.analysis.nextEligibleAt = Math.max(now(), task.analysis.nextEligibleAt ?? 0);
        task.resultKey = null; task.resultScope = null;
        invalidateAlerts(task, 'paused'); save('tasks', task);
      }
      tick();
      return conversationScope(control);
    },
    async pause() {
      ensureOpen();
      paused = true;
      write.run('settings', 'monitoring', JSON.stringify({ paused }));
      cancelRound('Monitoring is paused');
      queue.length = 0;
      queued.clear();
      for (const task of tasks.values()) {
        task.analysis.queuedAt = null;
        if (usable(task)) task.analysis.state = 'paused';
        invalidateAlerts(task, 'unknown');
        save('tasks', task);
      }
      return { paused };
    },
    async resume() {
      ensureOpen();
      paused = false;
      write.run('settings', 'monitoring', JSON.stringify({ paused }));
      tick();
      return { paused };
    },
    async setAnalyzer(nextAnalyzer) {
      ensureOpen();
      cancelRound('Inference configuration changed');
      analyzer = nextAnalyzer;
      queue.length = 0;
      queued.clear();
      for (const task of tasks.values()) {
        task.analysis.queuedAt = null;
        task.resultKey = null;
        task.resultScope = null;
        task.analysis.nextEligibleAt = Math.max(now(), task.analysis.nextEligibleAt ?? 0);
        invalidateAlerts(task, 'unknown');
        save('tasks', task);
      }
      tick();
    },
    async claimNotifications(sourceKey) {
      ensureOpen();
      const source = sources.get(sourceKey);
      if (source?.descriptor.policy !== 'chatgpt-main/1' || !nativeNotifications.qualified(sourceKey)) getSource(sourceKey);
      tick();
      const claimed = [];
      for (const notification of notifications.values()) {
        if (notification.sourceKey !== sourceKey || notification.status !== 'pending' || now() < (notification.readyAt ?? notification.createdAt + GROUP_MS) || !notificationEligible(notification)) continue;
        if (notification.kind === 'monitoring-failure' && !nativeNotifications.qualified(sourceKey)) continue;
        refreshNotification(notification);
        notification.status = 'claimed';
        notification.claimedAt = now();
        nativeNotifications.claim(notification);
        for (const member of notification.members) {
          const task = tasks.get(member.taskKey);
          for (const alert of task?.alerts ?? []) if (member.alertIds.includes(alert.id)) { alert.lastAttemptAt = now(); alert.notification = { id: notification.id, status: 'claimed', delivery: 'unknown', seen: 'unknown', at: now() }; }
          if (task) save('tasks', task);
        }
        save('notifications', notification);
        const { requestId, alertIds, createdAt, status, delivery, seen, claimedAt, groupKey, readyAt, ...publicValue } = notification;
        claimed.push(copy({ ...publicValue, members: notification.members.map(({ requestId, ...member }) => member) }));
      }
      return claimed;
    },
    async ackNotification(id, result) {
      ensureOpen();
      const notification = notifications.get(id);
      if (!notification) throw new Error('Unknown notification');
      if (!['submitted', 'failed'].includes(result.status)) throw new Error('Notification result must be submitted or failed');
      expireClaim(notification);
      const asyncFailure = notification.status === 'submitted' && result.status === 'failed';
      if (notification.status !== 'claimed' && !asyncFailure) return { id, status: notification.status, delivery: notification.delivery, seen: notification.seen };
      if (notification.kind === 'task-alert' || notification.kind === 'monitoring-failure') throw new Error('Native notifications require a matching native attempt result');
      notification.status = result.status;
      notification.delivery = result.status === 'submitted' ? 'system-accepted' : 'failed';
      notification.error = result.error ?? null;
      notification.ackedAt = now();
      for (const member of notificationMembers(notification)) {
        const task = tasks.get(member.taskKey);
        for (const alert of task?.alerts ?? []) if (member.alertIds.includes(alert.id) && alert.notification?.id === id) {
          if (result.status === 'submitted') alert.lastSubmittedAt = now();
          alert.notification = { id, status: result.status, delivery: notification.delivery, seen: 'unknown', error: notification.error, at: now() };
        }
        if (task) save('tasks', task);
      }
      save('notifications', notification);
      return { id, status: notification.status, delivery: notification.delivery, seen: 'unknown' };
    },
    async close() {
      if (closed) return;
      cancelRound('Monitoring core closed');
      closed = true;
      queue.length = 0;
      queued.clear();
      pendingPrunedTasks.clear();
      pendingPrunedSources.clear();
      clearInterval(interval);
      if (wakeTimer) clearTimeout(wakeTimer);
      if (idleTimer) clearTimeout(idleTimer);
      wakeTimer = null;
      idleTimer = null;
      failureStore.close();
      db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
      db.close();
    },
  };
}
