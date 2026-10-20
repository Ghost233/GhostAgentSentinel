import { randomUUID } from 'node:crypto';

const DAY = 86_400_000, LIMIT = 1024;
const META = 'native-notification-metadata', LINKS = 'native-notification-links';
const BASELINES = 'native-initial-baselines', MONITORED = 'native-initial-monitored', ROLLUPS = 'native-initial-rollups', GROUP_LIMIT = 256;
const copy = value => structuredClone(value);

// Native facts extend the core's notification records; DSH records are untouched.
export function createNativeNotificationStore({ db, now, leaseMs, notifications, sources, save, erase, monitoringPaused, chatPaused, taskNoticeEligible, accepted, readReceipt, notificationEnabled = () => true, notificationHistoryGap = () => false }) {
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value');
  const remove = db.prepare('DELETE FROM records WHERE collection=? AND key=?');
  const saved = db.prepare('SELECT value FROM records WHERE collection=? AND key=?').get('settings', META);
  const metadata = saved ? JSON.parse(saved.value) : {};
  let classificationGap = metadata.classificationGap === true;
  const links = new Map(db.prepare('SELECT value FROM records WHERE collection=?').all(LINKS).map(row => { const value = JSON.parse(row.value); return [value.id, value]; }));
  const load = collection => new Map(db.prepare('SELECT value FROM records WHERE collection=?').all(collection).map(row => { const value = JSON.parse(row.value); return [collection === ROLLUPS ? value.id : value.key ?? value.id, value]; }));
  const baselines = load(BASELINES), monitored = load(MONITORED), rollups = load(ROLLUPS);
  const eligibility = new Map();
  const persistInitial = (collection, value, index) => { const key = collection === ROLLUPS ? value.id : value.key ?? value.id; write.run(collection, key, JSON.stringify(value)); index.set(key, value); };
  const markRollupIncomplete = id => { const value = rollups.get(id); if (value && !value.incomplete && !value.closedAt) persistInitial(ROLLUPS, { ...value, incomplete: true }, rollups); };
  for (const value of rollups.values()) if (value.attempt?.state === 'submitting') persistInitial(ROLLUPS, { ...value, attempt: { ...value.attempt, state: 'unknown' } }, rollups);
  function registerEnrollment(descriptor, pristine) {
    const proof = descriptor.initialEnrollmentBaseline;
    if (!proof || descriptor.policy !== 'chatgpt-main/1') return;
    const previous = baselines.get(descriptor.registrationId);
    if (previous || !proof.initialize || !pristine) return;
    if (baselines.size >= GROUP_LIMIT) { classificationGap = true; persist(); return; }
    persistInitial(BASELINES, { key: descriptor.registrationId, id: proof.id, complete: true }, baselines);
  }
  function markMonitored(registrationId, mainId) {
    const baseline = baselines.get(registrationId), key = JSON.stringify([registrationId, mainId]);
    if (!baseline || monitored.has(key)) return;
    if (monitored.size >= LIMIT) { classificationGap = true; persist(); if (baseline.complete) persistInitial(BASELINES, { ...baseline, complete: false }, baselines); return; }
    persistInitial(MONITORED, { key, registrationId, mainId }, monitored);
  }
  function enrollmentPhase(owner, outcome, hasMonitored) {
    if (outcome.mainId === null) return 'unknown';
    const registrationId = owner.descriptor.registrationId;
    if (outcome.lifecycleId !== null || hasMonitored || monitored.has(JSON.stringify([registrationId, outcome.mainId])) || outcome.enrollmentPhase === 'established') return 'established';
    const baseline = baselines.get(registrationId);
    return outcome.enrollmentPhase === 'initial-unanchored' && baseline?.complete && baseline.id === owner.descriptor.initialEnrollmentBaseline?.id ? 'initial-unanchored' : 'unknown';
  }
  function rollupKey(receipt, owner) {
    return JSON.stringify([owner.descriptor.registrationId, receipt.error.stage, receipt.error.category, receipt.error.code, receipt.error.codeHash ?? null, receipt.error.status ?? null]);
  }
  function reconcileRollups() {
    for (const value of rollups.values()) {
      if (value.closedAt || value.incomplete) continue;
      const records = [...notifications.values()].filter(record => record.rollupId === value.id), ids = [...new Set(records.flatMap(record => record.receiptIds))];
      const facts = ids.map(id => readReceipt(id));
      if (ids.length < value.membersSeen || facts.some(value => !value)) markRollupIncomplete(value.id);
      else if (ids.length && facts.every(value => value.state === 'recovered')) persistInitial(ROLLUPS, { ...value, closedAt: now() }, rollups);
    }
  }
  function rollupRecord(value) {
    const group = rollups.get(value.rollupId);
    if (!group) return null;
    const records = [...notifications.values()].filter(record => record.rollupId === group.id), active = new Set(records.filter(record => record.sourceKey === value.sourceKey && record.hostRunId === value.hostRunId && record.receiptIds.some(id => readReceipt(id)?.state === 'active')).map(record => record.mainId));
    return { id: group.id, retainedMembers: new Set(records.map(record => record.mainId)).size, activeCountScope: 'record-source-generation', knownActiveMembers: active.size, incomplete: group.incomplete, state: group.closedAt ? 'recovered' : group.incomplete ? 'incomplete' : 'active', firstAttemptConsumed: group.consumed, summaryNotificationId: group.attempt?.notificationId ?? null, summarySubmissionState: group.attempt?.state ?? 'not-attempted' };
  }
  function beginRollupSubmission(value, current) {
    const group = rollups.get(value.rollupId);
    if (!group) return true;
    if (group.consumed) {
      if (group.attempt?.notificationId === value.id && group.attempt.attemptId === value.native.attemptId) return false;
      if (group.attempt?.state === 'submitting') {
        value.status = 'pending'; value.claimedAt = null; value.waitReason = 'rollup-in-flight';
        value.native.suppression = { reason: 'rollup-in-flight', observedAt: current, deferred: true };
      } else { value.status = 'grouped'; value.delivery = 'grouped'; value.waitReason = null; if (value.native.suppression) value.native.suppression.deferred = false; }
      return false;
    }
    persistInitial(ROLLUPS, { ...group, consumed: true, attempt: { notificationId: value.id, attemptId: value.native.attemptId, sourceKey: value.sourceKey, hostRunId: value.hostRunId, state: 'submitting', observedAt: current } }, rollups);
    return true;
  }
  function settleRollupAttempt(value, state, observedAt) {
    const group = rollups.get(value.rollupId);
    if (group?.attempt?.notificationId !== value.id || group.attempt.attemptId !== value.native?.attemptId) return;
    if (group.attempt.state !== 'accepted' || state === 'accepted') persistInitial(ROLLUPS, { ...group, attempt: { ...group.attempt, state, observedAt } }, rollups);
  }
  let sequence = Math.max(metadata.sequence ?? 0, ...[...notifications.values()].map(value => value.nativeSequence ?? 0));
  let prunedThrough = metadata.prunedThrough ?? 0, receiptPrunedThrough = metadata.receiptPrunedThrough ?? 0, historyGap = metadata.historyGap === true;
  const persist = () => write.run('settings', META, JSON.stringify({ sequence, prunedThrough, receiptPrunedThrough, historyGap, classificationGap }));
  const native = value => value?.kind === 'task-alert' || value?.kind === 'monitoring-failure';
  function touch(value) { if (native(value)) { value.nativeSequence = ++sequence; persist(); } }
  function removed(value) { if (value?.rollupId) markRollupIncomplete(value.rollupId); if (native(value)) { prunedThrough = Math.max(prunedThrough, value.nativeSequence ?? 0); persist(); } }
  function prune(current) {
    for (const link of links.values()) if (link.firstFailedAt <= (current ?? now()) - DAY) { if (link.rollupId) markRollupIncomplete(link.rollupId); remove.run(LINKS, link.id); links.delete(link.id); }
    for (const value of rollups.values()) if (value.closedAt && value.closedAt <= (current ?? now()) - DAY) { remove.run(ROLLUPS, value.id); rollups.delete(value.id); }
  }
  const needsCancellation = value => Boolean(value.native?.nativeRequestId && value.native.pendingCancellation?.pending !== false && value.native.firstDeliveredAt == null && (value.historyPruned || ['submitting', 'unknown', 'accepted', 'failed'].includes(value.native.submission?.state)));
  const submissionCapacity = value => [...notifications.values()].filter(notice => (sources.get(notice.sourceKey)?.descriptor.registrationId ?? notice.registrationId) === (sources.get(value.sourceKey)?.descriptor.registrationId ?? value.registrationId) && needsCancellation(notice)).length < 1000;
  function source(sourceKey) {
    const value = sources.get(sourceKey);
    if (value?.descriptor.policy !== 'chatgpt-main/1') throw new Error('Native notifications require a known ChatGPT source');
    return value;
  }
  function qualified(sourceKey, current) {
    const value = sources.get(sourceKey), observed = eligibility.get(sourceKey);
    return Boolean(value && observed && value.descriptor.hostRunId === observed.hostRunId && observed.appRunning === true && observed.pluginEnabled === true && observed.observedAt <= (current ?? now()) && (current ?? now()) - observed.observedAt < leaseMs);
  }
  function scopeEligible(value, current) {
    if (value.historyPruned) return false;
    const owner = sources.get(value.sourceKey);
    if (!notificationEnabled(owner?.descriptor.registrationId, value.kind)) return false;
    if (!owner || owner.descriptor.policy !== 'chatgpt-main/1' || owner.descriptor.hostRunId !== value.hostRunId || monitoringPaused() || value.evidenceThrough <= (current ?? now()) - DAY) return false;
    if (value.mainId !== null && chatPaused(value.sourceKey, value.mainId)) return false;
    if (value.kind === 'task-alert') return taskNoticeEligible(value);
    // A settled monitoring failure describes the observer. Its first notice
    // keeps that original scope across recovery and execution changes.
    return true;
  }
  function cancel(value, reason, current) {
    let changed = false;
    if (['pending', 'claimed'].includes(value.status)) { value.status = 'cancelled'; value.error = reason; changed = true; }
    if (value.native?.nativeRequestId && value.native.pendingCancellation?.pending !== false && !value.native.cancelRequestedAt) { value.native.cancelRequestedAt = current ?? now(); changed = true; }
    if (changed) save('notifications', value);
  }
  function reconcile(current) {
    prune(current); reconcileRollups();
    for (const value of notifications.values()) {
      if (!native(value) || value.historyPruned) continue;
      const facts = eligibility.get(value.sourceKey);
      const stopped = facts && !qualified(value.sourceKey, current);
      if (!scopeEligible(value, current) || stopped) cancel(value, stopped ? 'Native notification qualification is unavailable' : 'Native notification scope is no longer eligible', current);
      const waitReason = value.status !== 'pending' ? null : !qualified(value.sourceKey, current) ? 'qualification-unconfirmed' : value.native?.suppression?.deferred ? value.native.suppression.reason : null;
      if (value.waitReason !== waitReason) { value.waitReason = waitReason; save('notifications', value); }
    }
  }
  function prepareTask(value, task) {
    const owner = sources.get(task.sourceKey);
    if (owner?.descriptor.policy !== 'chatgpt-main/1') return true;
    Object.assign(value, { kind: 'task-alert', hostRunId: owner.descriptor.hostRunId, mainId: task.id, lifecycleId: task.lifecycleId, receiptIds: [], humanSeen: 'unknown' });
    return true;
  }
  function observeFailure(receipt) {
    prune();
    if (!receipt || links.has(receipt.id) || receipt.sequence <= receiptPrunedThrough || receipt.firstFailedAt <= now() - DAY) return;
    const owner = sources.get(receipt.sourceKey);
    if (owner?.descriptor.policy !== 'chatgpt-main/1') return;
    const current = now(), initial = receipt.enrollmentPhase === 'initial-unanchored' && receipt.domain === 'collection' && receipt.mainId !== null && receipt.lifecycleId === null;
    let rollup = initial ? [...rollups.values()].find(value => value.key === rollupKey(receipt, owner) && !value.closedAt) : null;
    if (initial && !rollup && rollups.size < GROUP_LIMIT) rollup = { id: randomUUID(), key: rollupKey(receipt, owner), registrationId: owner.descriptor.registrationId, consumed: false, incomplete: false, membersSeen: 0, closedAt: null, attempt: null };
    if (rollup && [...notifications.values()].some(value => value.rollupId === rollup.id && (value.sourceKey !== receipt.sourceKey || value.hostRunId !== owner.descriptor.hostRunId))) rollup = { ...rollup, incomplete: true };
    if (initial && !rollup) { historyGap = true; persist(); }
    const groupKey = JSON.stringify(['monitoring-failure', receipt.sourceKey, receipt.mainId, receipt.lifecycleId, ...(rollup ? [rollup.id] : [])]);
    let value = [...notifications.values()].find(value => value.kind === 'monitoring-failure' && value.groupKey === groupKey && value.status === 'pending' && value.receiptIds.length < 100 && current - value.createdAt < 3000);
    value = value ? copy(value) : { id: randomUUID(), kind: 'monitoring-failure', sourceKey: receipt.sourceKey, hostRunId: owner.descriptor.hostRunId, mainId: receipt.mainId, lifecycleId: receipt.lifecycleId, groupKey, members: [], receiptIds: [], firstRoundIds: [], evidenceThrough: receipt.firstFailedAt, createdAt: current, readyAt: current, status: 'pending', delivery: 'unknown', seen: 'unknown', humanSeen: 'unknown', claimedAt: null };
    if (!notificationEnabled(owner.descriptor.registrationId, 'monitoring-failure')) { value.status = 'cancelled'; value.error = 'Installation notification preference is disabled'; }
    if (rollup) value.rollupId = rollup.id;
    value.receiptIds.push(receipt.id); value.firstRoundIds.push(receipt.firstRoundId);
    value.title = 'GhostSentinel · 监控状态提醒';
    value.body = `来源：${value.sourceKey}\n主聊天：${value.mainId ?? '身份未确认'}\n执行：${value.lifecycleId ?? '身份未确认'}\n已记录 ${value.receiptIds.length} 项监控故障（${receipt.domain} / ${receipt.error.category} / ${receipt.error.code}）；请在会话检查核对故障回执，当前是否恢复另行显示。`.slice(0, 1200);
    if (rollup) value.body = '首次接入部分聊天时读取失败；同类问题将在会话检查中持续汇总。每个聊天仍保留独立故障回执，当前是否恢复另行显示。';
    const link = { id: receipt.id, sequence: receipt.sequence, firstFailedAt: receipt.firstFailedAt, notificationId: value.id, ...(rollup ? { rollupId: rollup.id } : {}) };
    const previous = { sequence, receiptPrunedThrough, historyGap };
    let evictedRollup = null;
    db.exec('SAVEPOINT native_failure_notice');
    try {
      save('notifications', value); write.run(LINKS, link.id, JSON.stringify(link));
      if (rollup) write.run(ROLLUPS, rollup.id, JSON.stringify({ ...rollup, membersSeen: rollup.membersSeen + 1 }));
      if (links.size >= LIMIT) {
        const oldest = [...links.values()].sort((a, b) => a.sequence - b.sequence)[0];
        if (oldest.rollupId && rollups.has(oldest.rollupId)) {
          const previous = rollups.get(oldest.rollupId);
          evictedRollup = { ...previous, ...(rollup?.id === previous.id ? { membersSeen: rollup.membersSeen + 1 } : {}), incomplete: true };
          write.run(ROLLUPS, evictedRollup.id, JSON.stringify(evictedRollup));
        }
        remove.run(LINKS, oldest.id); receiptPrunedThrough = Math.max(receiptPrunedThrough, oldest.sequence); historyGap = true; persist();
      }
      db.exec('RELEASE native_failure_notice');
      if (links.size >= LIMIT) { const oldest = [...links.values()].sort((a, b) => a.sequence - b.sequence)[0]; links.delete(oldest.id); }
      notifications.set(value.id, value); links.set(link.id, link);
      if (evictedRollup) rollups.set(evictedRollup.id, evictedRollup);
      if (rollup) rollups.set(rollup.id, { ...rollup, membersSeen: rollup.membersSeen + 1, ...(evictedRollup?.id === rollup.id ? { incomplete: true } : {}) });
    } catch (error) { db.exec('ROLLBACK TO native_failure_notice'); db.exec('RELEASE native_failure_notice'); ({ sequence, receiptPrunedThrough, historyGap } = previous); throw error; }
  }
  function publicRecord(value) {
    return copy({ id: value.id, sequence: value.nativeSequence, sourceKey: value.sourceKey, hostRunId: value.hostRunId, mainId: value.mainId, lifecycleId: value.lifecycleId, kind: value.kind, ...(value.rollupId ? { rollup: rollupRecord(value) } : {}), title: String(value.title ?? '').slice(0, 200), body: String(value.body ?? '').slice(0, 1200), receiptIds: value.receiptIds ?? [], firstRoundIds: value.firstRoundIds ?? [], checkIds: value.checkIds ?? [], alertIds: value.alertIds ?? [], members: (value.members ?? []).map(member => ({ taskKey: member.taskKey, checkIds: member.checkIds ?? [], alertIds: member.alertIds ?? [] })), createdAt: value.createdAt, evidenceThrough: value.evidenceThrough, status: value.status, delivery: value.delivery, humanSeen: 'unknown', waitReason: value.waitReason ?? null, errorCode: value.native?.errorCode ?? (value.error ? 'notification_unavailable' : null), native: value.native ?? null });
  }
  function claim(value) {
    if (!native(value)) return;
    if (value.native?.suppression?.deferred && value.native.submission.state === 'not-attempted') return;
    value.native = { attemptId: randomUUID(), nativeRequestId: null, bundleId: 'com.ghostsentinel.notifications', authorization: { state: 'unknown', observedAt: null }, submission: { state: 'not-attempted', observedAt: null } };
  }
  function recordResult(result) {
    if (result.observedAt > now() || result.deliveryDate > result.observedAt) throw new Error('Native notification observation is in the future');
    reconcile();
    const original = notifications.get(result.notificationId), value = original?.rollupId ? copy(original) : original, attempt = value?.native;
    if (!native(value) || !attempt || attempt.attemptId !== result.attemptId) throw new Error('Native notification attempt does not match');
    if (!/^ghostsentinel:[a-f0-9]{24}:.+/.test(result.nativeRequestId) || attempt.nativeRequestId && attempt.nativeRequestId !== result.nativeRequestId) throw new Error('Native notification request does not match');
    if (value.historyPruned) {
      if (result.event !== 'pending-cancelled') throw new Error('Native notification history was pruned');
      if (result.pending) { attempt.pendingCancellation = { pending: true, observedAt: result.observedAt }; save('notifications', value); }
      else erase('notifications', value.id);
      return { record: null, maySubmit: false };
    }
    if (['accepted', 'submission-failed', 'submission-unknown', 'user-action'].includes(result.event) && attempt.submission.state === 'not-attempted') throw new Error('Native submission has not started');
    attempt.nativeRequestId = result.nativeRequestId;
    const transaction = value.rollupId ? { rollups: new Map([...rollups].map(([key, value]) => [key, copy(value)])), sequence, prunedThrough, receiptPrunedThrough, historyGap } : null;
    if (transaction) db.exec('SAVEPOINT native_rollup_result');
    try {
    let maySubmit = false;
    const observedAt = result.observedAt;
    if (result.event === 'authorization') {
      if (observedAt >= (attempt.authorization.observedAt ?? 0)) attempt.authorization = { state: result.authorization, observedAt };
    } else if (result.event === 'suppressed') {
      const deferred = ['pending', 'claimed'].includes(value.status) && ['project-unconfirmed', 'qualification-unconfirmed'].includes(result.reason) && attempt.submission.state === 'not-attempted' && qualified(value.sourceKey) && scopeEligible(value);
      attempt.suppression = { reason: result.reason, observedAt, deferred };
      if (deferred) { value.status = 'pending'; value.claimedAt = null; value.waitReason = result.reason; }
      else cancel(value, 'Native notification was suppressed');
    } else if (result.event === 'submitting') {
      maySubmit = value.status === 'claimed' && scopeEligible(value) && qualified(value.sourceKey) && submissionCapacity(value) && ['authorized', 'provisional'].includes(attempt.authorization.state) && attempt.submission.state === 'not-attempted';
      if (maySubmit) maySubmit = beginRollupSubmission(value, observedAt);
      if (maySubmit) {
        attempt.submission = { state: 'submitting', observedAt };
        if (attempt.suppression?.deferred) attempt.suppression = { ...attempt.suppression, deferred: false, resolvedAt: observedAt };
        value.waitReason = null;
      }
    } else if (result.event === 'not-submitted') {
      const group = rollups.get(value.rollupId);
      if (value.status !== 'cancelled' || attempt.submission.state !== 'submitting' || group && (group.attempt?.notificationId !== value.id || group.attempt.attemptId !== attempt.attemptId || group.attempt.state !== 'submitting')) throw new Error('Native request was not proven unsubmitted');
      attempt.submission = { state: 'not-submitted', observedAt };
      if (group) persistInitial(ROLLUPS, { ...group, consumed: false, attempt: null }, rollups);
    } else if (['accepted', 'submission-failed', 'submission-unknown'].includes(result.event)) {
      if (!['submitting', 'unknown', 'accepted', 'failed'].includes(attempt.submission.state)) throw new Error('Native submission has not started');
      if (attempt.submission.state !== 'accepted' && (attempt.submission.observedAt ?? 0) <= observedAt || result.event === 'accepted') {
        const state = result.event === 'accepted' ? 'accepted' : result.event === 'submission-failed' ? 'failed' : 'unknown';
        attempt.submission = { state, observedAt };
        settleRollupAttempt(value, state, observedAt);
        attempt.errorCode = result.errorCode ?? null;
        if (state === 'accepted') {
          value.delivery = 'system-accepted';
          if (value.status === 'claimed' && scopeEligible(value) && qualified(value.sourceKey)) { value.status = 'submitted'; accepted(value); }
        } else if (value.status === 'claimed') { value.status = state === 'failed' ? 'failed' : 'expired'; value.delivery = state === 'failed' ? 'failed' : 'unknown'; }
      }
    } else if (result.event === 'pending-cancelled') {
      if (observedAt >= (attempt.pendingCancellation?.observedAt ?? 0)) attempt.pendingCancellation = { pending: result.pending, observedAt };
    } else if (result.event === 'delivered-snapshot') {
      if (observedAt >= (attempt.deliveredSnapshot?.observedAt ?? 0)) attempt.deliveredSnapshot = { present: result.present, observedAt, ...(result.deliveryDate !== undefined ? { deliveryDate: result.deliveryDate } : {}) };
      if (result.present && attempt.firstDeliveredAt === undefined) attempt.firstDeliveredAt = result.deliveryDate ?? observedAt;
    } else if (result.event === 'user-action') {
      if (observedAt >= (attempt.userAction?.observedAt ?? 0)) attempt.userAction = { action: result.action, observedAt };
    }
    if (!scopeEligible(value) || !qualified(value.sourceKey)) cancel(value, 'Native notification qualification or scope is unavailable');
    save('notifications', value);
    if (transaction) { db.exec('RELEASE native_rollup_result'); notifications.set(value.id, value); }
    return { record: publicRecord(value), maySubmit };
    } catch (error) {
      if (!transaction) throw error;
      let rollback;
      try { db.exec('ROLLBACK TO native_rollup_result'); db.exec('RELEASE native_rollup_result'); } catch (failure) { rollback = failure; }
      rollups.clear(); for (const [key, value] of transaction.rollups) rollups.set(key, value);
      ({ sequence, prunedThrough, receiptPrunedThrough, historyGap } = transaction);
      if (rollback) throw new AggregateError([error, rollback], 'Native rollup result and rollback failed');
      throw error;
    }
  }
  return { touch, removed, registerEnrollment, markMonitored, enrollmentPhase, prepareTask, observeFailure, scopeEligible, qualified, reconcile, cancel, publicRecord, claim, recordResult, needsCancellation,
    prepareTouches(values) {
      let next = sequence;
      for (const value of values) if (native(value)) value.nativeSequence = ++next;
      return { stage() {
        if (next !== sequence) write.run('settings', META, JSON.stringify({ sequence: next, prunedThrough, receiptPrunedThrough, historyGap, classificationGap }));
        return () => { sequence = next; };
      } };
    },
    prepareClearSources(sourceKeys, notificationIds, receiptIds, registrationId) {
      const selected = [...links].filter(([id, link]) => notificationIds.has(link.notificationId) || receiptIds.has(id)), selectedEligibility = [...eligibility.keys()].filter(key => sourceKeys.has(key));
      const initial = [[BASELINES, baselines], [MONITORED, monitored], [ROLLUPS, rollups]].map(([collection, index]) => ({ collection, index, keys: [...index].filter(([key, value]) => collection === BASELINES ? key === registrationId : value.registrationId === registrationId).map(([key]) => key) }));
      return { removed: { nativeNotificationLinks: selected.length, nativeEligibilityScopes: selectedEligibility.length, initialEnrollmentBaselines: initial[0].keys.length, initialMonitoredScopes: initial[1].keys.length, initialFailureRollups: initial[2].keys.length }, stage() {
        for (const [id] of selected) remove.run(LINKS, id);
        for (const item of initial) for (const key of item.keys) remove.run(item.collection, key);
        return () => { for (const [id] of selected) links.delete(id); for (const key of selectedEligibility) eligibility.delete(key); for (const item of initial) for (const key of item.keys) item.index.delete(key); };
      } };
    },
    setEligibility(facts) {
      const owner = source(facts.sourceKey);
      if (owner.descriptor.hostRunId !== facts.hostRunId) throw new Error('Native notification source generation does not match');
      if (!eligibility.has(facts.sourceKey) && eligibility.size >= 256) {
        const expired = [...eligibility.keys()].find(key => !qualified(key));
        if (!expired) throw new Error('Native notification qualification capacity reached');
        for (const value of notifications.values()) if (native(value) && value.sourceKey === expired) cancel(value, 'Native notification qualification expired');
        eligibility.delete(expired);
      }
      eligibility.set(facts.sourceKey, { ...facts, observedAt: now() }); reconcile();
      return { ...facts, observedAt: now(), expiresAt: now() + leaseMs };
    },
    records({ sourceKey, afterSequence = 0, limit = 100 }) {
      const owner = source(sourceKey); reconcile();
      const values = [...notifications.values()].filter(value => native(value) && (value.historyPruned ? value.registrationId === owner.descriptor.registrationId : value.sourceKey === sourceKey) && value.nativeSequence > afterSequence).sort((a, b) => a.nativeSequence - b.nativeSequence), page = values.slice(0, limit);
      return { records: page.filter(value => !value.historyPruned).map(publicRecord), cancellations: page.filter(value => value.historyPruned).map(copy), nextSequence: page.at(-1)?.nativeSequence ?? afterSequence, hasMore: values.length > page.length, historyGap: notificationHistoryGap(owner.descriptor.registrationId), maximumRecords: 1000, qualification: qualified(sourceKey) ? 'confirmed' : 'unconfirmed' };
    },
    diagnostics: () => ({ nextExpiryAt: links.size ? Math.min(...[...links.values()].map(value => value.firstFailedAt + DAY)) : null, records: [...notifications.values()].filter(value => native(value) && !value.historyPruned).length, cancellationFences: [...notifications.values()].filter(value => value.historyPruned).length, failureLinks: links.size, eligibilitySources: eligibility.size, maximumEligibilitySources: 256, maximumRecords: 1000, recordLimitScope: 'installation', maximumFailureLinks: LIMIT, maximumReceiptsPerNotice: 100, initialEnrollment: { classificationGap, baselines: baselines.size, monitoredScopes: monitored.size, rollups: rollups.size, maximumBaselines: GROUP_LIMIT, maximumRollups: GROUP_LIMIT, maximumMonitoredScopes: LIMIT, incompleteRollups: [...rollups.values()].filter(value => value.incomplete).length, consumedRollups: [...rollups.values()].filter(value => value.consumed).length }, latestSequence: sequence, prunedThroughSequence: prunedThrough, historyGap }),
  };
}
