import { DatabaseSync } from 'node:sqlite';
import { mkdir, chmod } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { chmodSync } from 'node:fs';
import { normalizeEnrollment } from '../enrollment-config.mjs';

const DAY = 24 * 60 * 60 * 1000;
const CLAIM_MS = 60_000;
const CHECK_IDS = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const copy = value => structuredClone(value);
const identity = (...parts) => JSON.stringify(parts);
const unknownChecks = reason => CHECK_IDS.map(id => ({ id, status: 'unknown', reason, evidenceIds: [] }));

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
  const rows = db.prepare('SELECT collection,key,value FROM records').all();
  const sources = new Map();
  const tasks = new Map();
  const events = new Map();
  const notifications = new Map();
  const collections = { sources, tasks, events, notifications };
  let paused = false;
  let enrollment = { entries: [], configuredAt: null, managed: false };
  for (const row of rows) {
    const value = JSON.parse(row.value);
    if (row.collection === 'settings' && row.key === 'monitoring') paused = value.paused === true;
    else if (row.collection === 'settings' && row.key === 'enrollment') enrollment = { ...value, managed: value.managed ?? (value.configuredAt !== null) };
    else collections[row.collection]?.set(row.key, value);
  }
  let closed = false;
  let active = null;
  let pendingCancelledRequests = 0;
  let sequence = Math.max(0, ...Array.from(events.values(), event => event.sequence));
  let hadConnection = true;
  let idleTimer = null;
  let wakeTimer = null;
  let idleNotified = false;
  const queue = [];
  const queued = new Set();
  const pendingPrunedTasks = new Set();
  const pendingPrunedSources = new Set();
  const save = (collection, value) => write.run(collection, value.key ?? value.id, JSON.stringify(value));
  const erase = (collection, key) => { remove.run(collection, key); collections[collection].delete(key); };
  const taskEvents = task => Array.from(events.values()).filter(event => event.taskKey === task.key).sort((a, b) => a.capturedAt - b.capturedAt || a.sequence - b.sequence);
  const connected = source => source?.status === 'connected';
  const usable = task => task.status === 'running' && task.baselineConfirmed && connected(sources.get(task.sourceKey));
  const publicEvents = task => taskEvents(task).map(({ key, taskKey, sequence: eventSequence, ...event }) => copy(event));

  function rememberGap(owner, gap) {
    const latest = new Map();
    for (const value of [...(owner.gapRecords ?? []), gap]) {
      if ((latest.get(value.reason)?.at ?? -1) <= value.at) latest.set(value.reason, value);
    }
    owner.gapRecords = [...latest.values()];
    owner.gaps = [...new Set(owner.gapRecords.map(value => value.reason))];
  }

  function cleanQueue() {
    for (let index = queue.length - 1; index >= 0; index--) {
      const task = tasks.get(queue[index]);
      if (!paused && !closed && task && usable(task)) continue;
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

  const saveEnrollment = () => write.run('settings', 'enrollment', JSON.stringify(enrollment));
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
      if (alert.status === 'attention') { alert.status = reason; alert.updatedAt = now(); }
    }
    for (const notification of notifications.values()) {
      if (notification.taskKey === task.key && ['pending', 'claimed'].includes(notification.status)) {
        notification.status = 'cancelled';
        notification.error = reason;
        save('notifications', notification);
      }
    }
  }

  function cancelRound(reason, onlyTask) {
    if (!active || (onlyTask && active.taskKey !== onlyTask.key)) return;
    const round = active;
    active = null;
    round.cancelled = true;
    pendingCancelledRequests++;
    const controller = round.controller;
    // The provider may keep computing. The sealed core round no longer needs its inputs.
    round.input = null;
    round.controller = null;
    controller.abort(new Error(reason));
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
      task.status = 'unknown';
      task.baselineConfirmed = false;
      task.analysis.state = interrupted ? 'interrupted' : task.analysis.completedAt ? 'stale' : 'unknown';
      if (interrupted) task.analysis.error = { code: 'analysis_interrupted', message: reason };
      invalidateAlerts(task, 'unknown');
      save('tasks', task);
    }
  }

  // Stored activity is history, not proof that the original host is still running.
  for (const source of sources.values()) resetSource(source, 'Core restarted; source baseline is unknown');
  for (const notification of notifications.values()) {
    if (['pending', 'claimed'].includes(notification.status)) {
      notification.status = 'cancelled';
      notification.error = 'Core restarted; delivery eligibility must be confirmed again';
      save('notifications', notification);
    }
  }

  function coverageFor(task) {
    const source = sources.get(task.sourceKey);
    const inCurrentScope = gap => task.requestStartedAt == null || (gap.endAt ?? gap.at) >= task.requestStartedAt;
    const gaps = [
      ...(source?.descriptor.gaps ?? []),
      ...(source?.gapRecords ?? []).filter(inCurrentScope).map(gap => gap.reason),
      ...(task.gapRecords ?? []).filter(inCurrentScope).map(gap => gap.reason),
    ];
    if (!task.baselineConfirmed) gaps.push('Task baseline is unconfirmed');
    if (!connected(source)) gaps.push('Source is disconnected');
    if (taskEvents(task).some(event => event.data?.truncated && (task.requestStartedAt == null || event.capturedAt >= task.requestStartedAt))) gaps.push('Some observed excerpts are truncated');
    if (taskEvents(task).some(event => event.data?.redacted && (task.requestStartedAt == null || event.capturedAt >= task.requestStartedAt))) gaps.push('Some observed content was omitted for privacy');
    return { complete: gaps.length === 0, gaps: [...new Set(gaps)] };
  }

  function activityFor(task, retained, coverage, current) {
    const request = retained.findIndex(event => event.id === task.requestId && event.kind === 'input');
    const boundary = !connected(sources.get(task.sourceKey)) ? '来源失联，以下仅为历史观察：' : !task.baselineConfirmed ? '任务基线未知，以下仅为历史观察：' : task.status !== 'running' ? '任务当前不在运行或状态未知，以下仅为历史观察：' : !coverage.complete ? '观察覆盖不足，无法确认当前活动；最近观察：' : '';
    if (request < 0 || !task.currentRequest.trim()) return { summary: `${boundary}当前请求未知，无法确认当前活动`, observedAt: null, evidenceIds: [] };
    const scope = retained.slice(request), byId = new Map(scope.map(event => [event.id, event]));
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
    const cutoff = now() - DAY;
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
        if (expired.has(task.requestId)) { task.currentRequest = ''; task.requestId = null; }
        if (task.analysis.inputEventIds?.some(id => expired.has(id)) || task.analysis.checks?.some(check => check.evidenceIds?.some(id => expired.has(id)))) {
          task.analysis.state = 'stale';
          task.analysis.error = { code: 'evidence_expired', message: 'Evidence supporting this analysis expired' };
          task.analysis.summary = null;
          task.analysis.summaryAt = null;
          task.analysis.inputEventIds = [];
          task.analysis.checks = unknownChecks('Supporting evidence expired');
          invalidateAlerts(task, 'evidence-expired');
        }
        for (const alert of task.alerts) {
          if ((alert.evidenceIds ?? []).some(id => expired.has(id)) || (alert.recoveryEvidenceIds ?? []).some(id => expired.has(id))) {
            alert.status = 'evidence-expired';
            alert.reason = 'Supporting evidence expired after 24 hours';
            alert.evidenceIds = [];
            alert.recoveryEvidenceIds = [];
            alert.updatedAt = now();
          }
        }
        changed = true;
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
        changed = true;
      }
      const retainedAlerts = task.alerts.filter(alert => alert.firstAt > cutoff);
      if (retainedAlerts.length !== task.alerts.length) { task.alerts = retainedAlerts; changed = true; }
      const retainedGaps = (task.gapRecords ?? []).filter(gap => gap.at > cutoff);
      if (retainedGaps.length !== (task.gapRecords ?? []).length) {
        task.gapRecords = retainedGaps;
        task.gaps = [...new Set(retainedGaps.map(gap => gap.reason))];
        changed = true;
      }
      if (changed) pendingPrunedTasks.add(task.key);
      if (!usable(task) && task.updatedAt <= cutoff && taskEvents(task).length === 0) {
        erase('tasks', task.key);
        pendingPrunedTasks.delete(task.key);
      } else if (changed) {
        save('tasks', task);
        pendingPrunedTasks.delete(task.key);
      }
    }
    for (const notification of notifications.values()) {
      if (notification.createdAt <= cutoff || !tasks.has(notification.taskKey) || notification.evidenceThrough <= cutoff) erase('notifications', notification.id);
    }
    for (const source of sources.values()) {
      const retainedGaps = (source.gapRecords ?? []).filter(gap => gap.at > cutoff);
      if (retainedGaps.length !== (source.gapRecords ?? []).length || pendingPrunedSources.has(source.key)) {
        pendingPrunedSources.add(source.key);
        source.gapRecords = retainedGaps;
        source.gaps = [...new Set(retainedGaps.map(gap => gap.reason))];
        save('sources', source);
        pendingPrunedSources.delete(source.key);
      }
      if (!connected(source) && source.lastSeenAt <= cutoff && !Array.from(tasks.values()).some(task => task.sourceKey === source.key)) erase('sources', source.key);
    }
    if (expiredByTask.size > 0) db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  }

  function notificationEligible(notification) {
    const task = tasks.get(notification.taskKey);
    if (!task || !usable(task) || paused || task.requestId !== notification.requestId) return false;
    if (['stale', 'failed', 'interrupted', 'unknown'].includes(task.analysis.state)) return false;
    const retained = new Set(taskEvents(task).map(event => event.id));
    return notification.alertIds.every(id => {
      const alert = task.alerts.find(value => value.id === id);
      return alert?.status === 'attention' && alert.evidenceIds.length > 0 && alert.evidenceIds.every(value => retained.has(value));
    });
  }

  function expireClaim(notification) {
    if (notification.status !== 'claimed' || now() - notification.claimedAt < CLAIM_MS) return;
    notification.status = 'expired';
    notification.delivery = 'unknown';
    notification.error = 'Notification claim expired before acknowledgement; delivery is unknown';
    save('notifications', notification);
    const task = tasks.get(notification.taskKey);
    for (const alert of task?.alerts ?? []) {
      if (alert.notification?.id !== notification.id) continue;
      alert.notification = { id: notification.id, status: 'expired', delivery: 'unknown', seen: 'unknown', error: notification.error, at: now() };
    }
    if (task) save('tasks', task);
  }

  function makeNotification(task, alerts) {
    if (alerts.length === 0) return;
    const current = now();
    const due = alerts.filter(alert => {
      if (Array.from(notifications.values()).some(value => value.alertIds.includes(alert.id) && ['pending', 'claimed'].includes(value.status))) return false;
      const lastAttempt = Math.max(alert.lastSubmittedAt ?? 0, alert.lastAttemptAt ?? 0);
      return lastAttempt === 0 || current - lastAttempt >= Math.max(600_000, repeatMs);
    });
    if (due.length === 0) return;
    const sourceName = String(sources.get(task.sourceKey)?.descriptor.name ?? '来源名称未知').slice(0, 40);
    const notification = {
      id: randomUUID(), taskKey: task.key, sourceKey: task.sourceKey,
      requestId: task.requestId, title: `${String(task.title).slice(0, 80)}: needs attention`,
      body: `观察身份：${task.key}\n来源：${task.sourceKey} · ${sourceName}\n${due.map(alert => alert.reason).join('\n')}`.slice(0, 1_200),
      checkIds: due.map(alert => alert.checkId), alertIds: due.map(alert => alert.id),
      evidenceThrough: task.analysis.through, createdAt: current,
      status: 'pending', delivery: 'unknown', seen: 'unknown', claimedAt: null,
    };
    notifications.set(notification.id, notification);
    save('notifications', notification);
  }

  function updateAlerts(task, result) {
    const current = now();
    for (const check of result.checks) {
      let alert = task.alerts.find(value => value.checkId === check.id && value.requestId === task.requestId && !['superseded', 'ended', 'evidence-expired', 'recovered'].includes(value.status));
      if (check.status === 'attention') {
        if (!alert) {
          alert = { id: randomUUID(), checkId: check.id, requestId: task.requestId, firstAt: current, lastSubmittedAt: null, lastAttemptAt: null };
          task.alerts.push(alert);
        }
        Object.assign(alert, { status: 'attention', reason: check.reason, evidenceIds: check.evidenceIds, updatedAt: current, evidenceThrough: task.analysis.through });
      } else if (alert) {
        const previouslyObserved = new Set(alert.evidenceIds ?? []);
        // Rewording or silence is not recovery. A clear result needs new retained evidence.
        const hasNewEvidence = check.evidenceIds.some(id => !previouslyObserved.has(id));
        alert.status = check.status === 'clear' && hasNewEvidence ? 'recovered' : 'unknown';
        alert.updatedAt = current;
        if (alert.status === 'recovered') alert.recoveryEvidenceIds = check.evidenceIds;
      }
    }
    makeNotification(task, task.alerts.filter(alert => alert.status === 'attention'));
  }

  function validateResult(result, round) {
    if (!result || !result.summary || !Array.isArray(result.checks) || result.checks.length !== CHECK_IDS.length) throw new Error('Analyzer result must contain summary and all six checks');
    const eventIds = new Set(round.input.events.map(event => event.id));
    const checks = new Map();
    if (!['currentTask', 'currentState', 'recentProgress', 'attention'].every(field => typeof result.summary[field] === 'string')) throw new Error('Analyzer summary fields must be strings');
    for (const check of result.checks) {
      if (!CHECK_IDS.includes(check.id) || checks.has(check.id) || !['attention', 'clear', 'unknown'].includes(check.status) || !Array.isArray(check.evidenceIds) || check.evidenceIds.some(id => !eventIds.has(id))) throw new Error('Analyzer returned invalid or cross-task evidence');
      if (check.status === 'attention' && check.evidenceIds.length === 0) throw new Error('Attention requires retained evidence');
      checks.set(check.id, check);
    }
    return { ...copy(result), checks: CHECK_IDS.map(id => checks.get(id)) };
  }

  function completeRound(round, result, error) {
    if (round.cancelled) pendingCancelledRequests = Math.max(0, pendingCancelledRequests - 1);
    if (closed || active !== round || round.cancelled) return;
    active = null;
    const task = tasks.get(round.taskKey);
    if (!task || !usable(task) || task.requestId !== round.requestId) return;
    if (round.input.events.some(event => event.capturedAt <= now() - DAY)) {
      error = Object.assign(new Error('Analysis inputs expired before the response completed'), { code: 'evidence_expired' });
    }
    const completedAt = now();
    task.analysis.completedAt = completedAt;
    task.analysis.through = round.through;
    task.analysis.throughRevision = round.revision;
    task.analysis.nextEligibleAt = completedAt + cooldownMs;
    task.analysis.cancelledComputeMayContinue = false;
    if (error) {
      task.analysis.state = 'failed';
      task.analysis.error = { code: error.code ?? 'analysis_failed', stage: error.stage ?? null, message: error.message ?? String(error) };
      task.analysis.checks = unknownChecks('The current analysis failed; prior conclusions are not current');
      invalidateAlerts(task, 'unknown');
    } else {
      task.analysis.error = null;
      task.analysis.summary = result.summary;
      task.analysis.summaryAt = completedAt;
      task.analysis.inputEventIds = round.input.events.map(event => event.id);
      task.analysis.checks = result.checks;
      task.analysis.coverage = result.coverage;
      task.analysis.usage = result.usage ?? null;
      task.analysis.quality = result.quality ?? 'unvalidated';
      task.analysis.state = result.coverage?.dependencies?.length > 0 ? 'unavailable' : 'cooldown';
      updateAlerts(task, result);
    }
    save('tasks', task);
    tick();
  }

  function startNext() {
    if (closed || paused || active || !analyzer) return;
    while (queue.length > 0) {
      const key = queue.shift();
      queued.delete(key);
      const task = tasks.get(key);
      if (!task || !usable(task)) continue;
      if (now() < (task.analysis.nextEligibleAt ?? 0)) continue;
      const capturedEvents = publicEvents(task);
      const source = sources.get(task.sourceKey);
      const round = {
        taskKey: key, requestId: task.requestId, revision: task.revision,
        through: capturedEvents.length ? Math.max(...capturedEvents.map(event => event.capturedAt)) : null,
        controller: new AbortController(), cancelled: false,
        input: { task: copy({ key: task.key, id: task.id, lifecycleId: task.lifecycleId, title: task.title, status: task.status, currentRequest: task.currentRequest, currentRequestId: task.requestId, lastActivity: task.lastActivity }), source: copy(source.descriptor), events: capturedEvents, coverage: coverageFor(task) },
      };
      active = round;
      task.analysis.state = 'analyzing';
      task.analysis.startedAt = now();
      task.analysis.queuedAt = null;
      task.analysis.error = null;
      save('tasks', task);
      const currentAnalyzer = analyzer;
      Promise.resolve().then(() => round.cancelled ? null : currentAnalyzer.analyze(round.input, { signal: round.controller.signal }))
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
      if (!usable(task) || paused || active?.taskKey === task.key) continue;
      if (!analyzer) {
        task.analysis.state = 'unavailable';
        task.analysis.error = { code: 'analyzer_unconfigured', message: 'Inference dependencies are not configured' };
        task.analysis.checks = unknownChecks('Inference dependencies are not configured');
        continue;
      }
      if (current < (task.analysis.nextEligibleAt ?? 0)) continue;
      if (!queued.has(task.key)) {
        queued.add(task.key);
        queue.push(task.key);
        task.analysis.state = 'queued';
        task.analysis.queuedAt = current;
        save('tasks', task);
      }
    }
    for (const notification of notifications.values()) {
      expireClaim(notification);
      if (['pending', 'claimed'].includes(notification.status) && !notificationEligible(notification)) {
        notification.status = 'cancelled';
        notification.error = 'Current alert eligibility is unknown';
        save('notifications', notification);
      }
    }
    scheduleIdle();
    startNext();
    if (wakeTimer) clearTimeout(wakeTimer);
    wakeTimer = null;
    const times = Array.from(tasks.values()).filter(task => usable(task) && task.analysis.nextEligibleAt > current).map(task => task.analysis.nextEligibleAt);
    if (times.length > 0) {
      wakeTimer = setTimeout(tick, Math.max(1, Math.min(...times) - current));
      wakeTimer.unref();
    }
  }

  const interval = setInterval(tick, Math.max(25, Math.min(1_000, leaseMs / 2)));
  interval.unref();
  tick();
  const ensureOpen = () => { if (closed) throw new Error('Monitoring core is closed'); };
  const getSource = sourceKey => {
    const source = sources.get(sourceKey);
    if (connected(source) && now() - source.lastSeenAt >= leaseMs) resetSource(source, 'Source heartbeat lease expired');
    if (!connected(source)) throw new Error('Source is not registered or connected');
    return source;
  };
  const newTask = (sourceKey, descriptor) => ({
    key: randomUUID(), sourceKey, id: descriptor.id, lifecycleId: descriptor.lifecycleId,
    title: descriptor.title, status: descriptor.status ?? 'unknown', sessionId: descriptor.sessionId ?? null,
    parentId: descriptor.parentId ?? null, gaps: descriptor.gaps ?? [], baselineConfirmed: false,
    gapRecords: (descriptor.gaps ?? []).map(reason => ({ reason, at: now() })),
    currentRequest: '', requestId: null, requestStartedAt: null, updatedAt: now(), lastActivity: null, revision: 0,
    analysis: { state: 'unknown', through: null, throughRevision: 0, completedAt: null, nextEligibleAt: null, error: null, summary: null, checks: unknownChecks('No completed analysis'), queuedAt: null, startedAt: null }, alerts: [],
  });
  const findTask = (sourceKey, id, lifecycleId) => Array.from(tasks.values()).find(task => identity(task.sourceKey, task.id, task.lifecycleId) === identity(sourceKey, id, lifecycleId));

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
      enrollEnabledAdapter(descriptor);
      let source = Array.from(sources.values()).find(value => identity(value.descriptor.registrationId, value.descriptor.hostRunId) === identity(descriptor.registrationId, descriptor.hostRunId));
      if (!source) {
        source = { key: randomUUID(), descriptor: copy(descriptor), status: 'connected', lastSeenAt: now(), gaps: [], gapRecords: [], baselineConfirmed: false };
        sources.set(source.key, source);
      } else {
        if (now() - source.lastSeenAt >= leaseMs) resetSource(source, 'Source lease expired before re-registration');
        Object.assign(source, { descriptor: copy(descriptor), status: 'connected', lastSeenAt: now(), disconnectReason: null });
      }
      hadConnection = true;
      save('sources', source);
      noteSourceSeen(source);
      tick();
      return source.key;
    },
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
        if (!task) { task = newTask(sourceKey, descriptor); tasks.set(task.key, task); }
        const before = JSON.stringify([task.title, task.status, task.sessionId, task.parentId, task.gaps, task.baselineConfirmed]);
        task.title = descriptor.title;
        task.status = descriptor.status;
        task.sessionId = descriptor.sessionId ?? task.sessionId;
        task.parentId = descriptor.parentId ?? task.parentId;
        for (const reason of descriptor.gaps ?? []) {
          if (!task.gaps.includes(reason)) rememberGap(task, { reason, at: now() });
        }
        task.gaps = [...new Set([...(task.gaps ?? []), ...(descriptor.gaps ?? [])])].filter(reason => reason !== 'Task baseline is not yet available');
        task.gapRecords = (task.gapRecords ?? []).filter(gap => gap.reason !== 'Task baseline is not yet available');
        task.baselineConfirmed = true;
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
        task.lastActivity = Math.max(task.lastActivity ?? 0, event.capturedAt);
        task.revision++;
        const currentInput = taskEvents(task).find(value => value.id === task.requestId);
        if (event.kind === 'input' && (!currentInput || event.capturedAt > currentInput.capturedAt)) {
          cancelRound('Current request changed during analysis', task);
          invalidateAlerts(task, 'superseded');
          task.currentRequest = event.text;
          task.requestId = event.id;
          task.requestStartedAt = event.capturedAt;
          task.analysis.state = 'unknown';
          task.analysis.summary = null;
          task.analysis.checks = unknownChecks('Current request has not been analyzed');
          task.analysis.nextEligibleAt = Math.max(now(), task.analysis.nextEligibleAt ?? 0);
        }
        if (event.kind === 'gap') {
          rememberGap(task, { reason: event.text, at: event.capturedAt });
        }
        if (event.kind === 'status' && ['running', 'idle', 'disposed', 'unknown'].includes(event.data?.status)) {
          task.status = event.data.status;
          if (task.status !== 'running') { cancelRound('Source task is no longer running', task); invalidateAlerts(task, task.status === 'unknown' ? 'unknown' : 'ended'); }
        }
        save('tasks', task);
      }
      tick();
      return counts;
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
      const current = now();
      const publicTasks = Array.from(tasks.values()).map(task => {
        const { baselineConfirmed, revision, requestId, ...value } = copy(task);
        value.events = publicEvents(task);
        value.coverage = coverageFor(task);
        value.activity = activityFor(task, value.events, value.coverage, current);
        value.analysis.lagMs = task.lastActivity === null ? null : Math.max(0, task.lastActivity - (task.analysis.through ?? task.lastActivity));
        value.analysis.waitMs = task.analysis.queuedAt === null ? 0 : Math.max(0, current - task.analysis.queuedAt);
        return value;
      });
      return {
        protocol: 1, generatedAt: current,
        monitoring: { state: paused ? 'paused' : 'active', paused, queueLength: queue.filter(key => usable(tasks.get(key) ?? {})).length, activeTaskKey: active?.taskKey ?? null, pendingCancelledRequests, retentionMs: DAY },
        overview: { runningTasks: publicTasks.filter(task => task.status === 'running').length, knownSources: sources.size, connectedSources: Array.from(sources.values()).filter(connected).length, attentionTasks: publicTasks.filter(task => task.alerts.some(alert => alert.status === 'attention')).length, coverage: { complete: false, gaps: ['Automatic discovery of all local sources has not been verified'] } },
        enrollment: enrollmentSnapshot(),
        sources: Array.from(sources.values()).map(source => copy(source)), tasks: publicTasks,
      };
    },
    async diagnostics() {
      if (!closed) tick();
      const timers = { lease: closed ? 0 : 1, wakeup: wakeTimer ? 1 : 0, idle: idleTimer ? 1 : 0 };
      timers.total = timers.lease + timers.wakeup + timers.idle;
      return {
        protocol: 1, pid: process.pid, generatedAt: now(), closed,
        runtime: { connectedSources: closed ? 0 : Array.from(sources.values()).filter(connected).length, activeAnalyses: active ? 1 : 0, cancelledUnsettledRequests: pendingCancelledRequests },
        temporary: { queueKeys: queue.length, queueMemberships: queued.size, timers },
        retained: { retentionMs: DAY, sources: sources.size, tasks: tasks.size, events: events.size, alerts: Array.from(tasks.values()).reduce((count, task) => count + task.alerts.length, 0), notifications: notifications.size, sourceGapRecords: Array.from(sources.values()).reduce((count, source) => count + (source.gapRecords?.length ?? 0), 0), taskGapRecords: Array.from(tasks.values()).reduce((count, task) => count + (task.gapRecords?.length ?? 0), 0) },
        configuration: { sourcePlanEntries: enrollment.entries.length }, memory: process.memoryUsage(),
      };
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
        task.analysis.nextEligibleAt = now();
        invalidateAlerts(task, 'unknown');
        save('tasks', task);
      }
      tick();
    },
    async claimNotifications(sourceKey) {
      ensureOpen();
      getSource(sourceKey);
      tick();
      const claimed = [];
      for (const notification of notifications.values()) {
        if (notification.sourceKey !== sourceKey || notification.status !== 'pending' || !notificationEligible(notification)) continue;
        notification.status = 'claimed';
        notification.claimedAt = now();
        const task = tasks.get(notification.taskKey);
        for (const alert of task?.alerts ?? []) if (notification.alertIds.includes(alert.id)) alert.lastAttemptAt = now();
        for (const alert of task?.alerts ?? []) if (notification.alertIds.includes(alert.id)) alert.notification = { id: notification.id, status: 'claimed', delivery: 'unknown', seen: 'unknown', at: now() };
        if (task) save('tasks', task);
        save('notifications', notification);
        const { requestId, alertIds, createdAt, status, delivery, seen, claimedAt, ...publicValue } = notification;
        claimed.push(copy(publicValue));
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
      notification.status = result.status;
      notification.delivery = result.status === 'submitted' ? 'system-accepted' : 'failed';
      notification.error = result.error ?? null;
      notification.ackedAt = now();
      if (result.status === 'submitted') {
        const task = tasks.get(notification.taskKey);
        for (const alert of task?.alerts ?? []) if (notification.alertIds.includes(alert.id)) alert.lastSubmittedAt = now();
        if (task) save('tasks', task);
      }
      const task = tasks.get(notification.taskKey);
      for (const alert of task?.alerts ?? []) if (notification.alertIds.includes(alert.id) && alert.notification?.id === id) alert.notification = { id, status: result.status, delivery: notification.delivery, seen: 'unknown', error: notification.error, at: now() };
      if (task) save('tasks', task);
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
      db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
      db.close();
    },
  };
}
