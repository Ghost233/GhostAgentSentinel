import { randomUUID } from 'node:crypto';

const DAY = 86_400_000, LIMIT = 1024;
const META = 'native-notification-metadata', LINKS = 'native-notification-links';
const copy = value => structuredClone(value);

// Native facts extend the core's notification records; DSH records are untouched.
export function createNativeNotificationStore({ db, now, leaseMs, notifications, sources, tasks, save, erase, monitoringPaused, chatPaused, taskNoticeEligible, accepted }) {
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value');
  const remove = db.prepare('DELETE FROM records WHERE collection=? AND key=?');
  const saved = db.prepare('SELECT value FROM records WHERE collection=? AND key=?').get('settings', META);
  const metadata = saved ? JSON.parse(saved.value) : {};
  const links = new Map(db.prepare('SELECT value FROM records WHERE collection=?').all(LINKS).map(row => { const value = JSON.parse(row.value); return [value.id, value]; }));
  const eligibility = new Map();
  let sequence = Math.max(metadata.sequence ?? 0, ...[...notifications.values()].map(value => value.nativeSequence ?? 0));
  let prunedThrough = metadata.prunedThrough ?? 0, receiptPrunedThrough = metadata.receiptPrunedThrough ?? 0, historyGap = metadata.historyGap === true;
  const persist = () => write.run('settings', META, JSON.stringify({ sequence, prunedThrough, receiptPrunedThrough, historyGap }));
  const native = value => value?.kind === 'task-alert' || value?.kind === 'monitoring-failure';
  function touch(value) { if (native(value)) { value.nativeSequence = ++sequence; persist(); } }
  function removed(value) { if (native(value)) { prunedThrough = Math.max(prunedThrough, value.nativeSequence ?? 0); persist(); } }
  function prune(current) {
    for (const link of links.values()) if (link.firstFailedAt <= (current ?? now()) - DAY) { remove.run(LINKS, link.id); links.delete(link.id); }
    for (const value of notifications.values()) if (native(value) && value.evidenceThrough <= (current ?? now()) - DAY) erase('notifications', value.id);
  }
  function capacity() {
    const retained = [...notifications.values()].filter(native);
    if (retained.length < LIMIT) return true;
    const oldest = retained.filter(value => !['pending', 'claimed'].includes(value.status) && (!value.native?.nativeRequestId || value.native.pendingCancellation?.pending === false)).sort((a, b) => a.createdAt - b.createdAt)[0];
    if (oldest) { erase('notifications', oldest.id); return true; }
    historyGap = true; persist(); return false;
  }
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
    const owner = sources.get(value.sourceKey);
    if (!owner || owner.descriptor.policy !== 'chatgpt-main/1' || owner.descriptor.hostRunId !== value.hostRunId || monitoringPaused() || value.evidenceThrough <= (current ?? now()) - DAY) return false;
    if (value.mainId !== null && chatPaused(value.sourceKey, value.mainId)) return false;
    if (value.kind === 'task-alert') return taskNoticeEligible(value);
    if (value.lifecycleId !== null) {
      const matches = [...tasks.values()].filter(task => task.sourceKey === value.sourceKey && task.id === value.mainId);
      const target = matches.find(task => task.lifecycleId === value.lifecycleId);
      if (!target || target.status !== 'running' || matches.some(task => task.lifecycleId !== value.lifecycleId && task.status === 'running')) return false;
    }
    return true;
  }
  function cancel(value, reason, current) {
    let changed = false;
    if (['pending', 'claimed'].includes(value.status)) { value.status = 'cancelled'; value.error = reason; changed = true; }
    if (value.native?.nativeRequestId && value.native.pendingCancellation?.pending !== false && !value.native.cancelRequestedAt) { value.native.cancelRequestedAt = current ?? now(); changed = true; }
    if (changed) save('notifications', value);
  }
  function reconcile(current) {
    prune(current);
    for (const value of notifications.values()) {
      if (!native(value)) continue;
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
    if (!native(value) && !capacity()) return false;
    Object.assign(value, { kind: 'task-alert', hostRunId: owner.descriptor.hostRunId, mainId: task.id, lifecycleId: task.lifecycleId, receiptIds: [], humanSeen: 'unknown' });
    return true;
  }
  function observeFailure(receipt) {
    prune();
    if (!receipt || links.has(receipt.id) || receipt.sequence <= receiptPrunedThrough || receipt.firstFailedAt <= now() - DAY) return;
    const owner = sources.get(receipt.sourceKey);
    if (owner?.descriptor.policy !== 'chatgpt-main/1') return;
    const current = now(), groupKey = JSON.stringify(['monitoring-failure', receipt.sourceKey, receipt.mainId, receipt.lifecycleId]);
    let value = [...notifications.values()].find(value => value.kind === 'monitoring-failure' && value.groupKey === groupKey && value.status === 'pending' && value.receiptIds.length < 100 && current - value.createdAt < 3000);
    if (!value && !capacity()) return;
    value = value ? copy(value) : { id: randomUUID(), kind: 'monitoring-failure', sourceKey: receipt.sourceKey, hostRunId: owner.descriptor.hostRunId, mainId: receipt.mainId, lifecycleId: receipt.lifecycleId, groupKey, members: [], receiptIds: [], firstRoundIds: [], evidenceThrough: receipt.firstFailedAt, createdAt: current, readyAt: current, status: 'pending', delivery: 'unknown', seen: 'unknown', humanSeen: 'unknown', claimedAt: null };
    value.receiptIds.push(receipt.id); value.firstRoundIds.push(receipt.firstRoundId);
    value.title = 'GhostSentinel · 监控状态提醒';
    value.body = `来源：${value.sourceKey}\n主聊天：${value.mainId ?? '身份未确认'}\n执行：${value.lifecycleId ?? '身份未确认'}\n已记录 ${value.receiptIds.length} 项监控故障（${receipt.domain} / ${receipt.error.category} / ${receipt.error.code}）；请在会话检查核对故障回执，当前是否恢复另行显示。`.slice(0, 1200);
    const link = { id: receipt.id, sequence: receipt.sequence, firstFailedAt: receipt.firstFailedAt, notificationId: value.id };
    const previous = { sequence, receiptPrunedThrough, historyGap };
    db.exec('SAVEPOINT native_failure_notice');
    try {
      save('notifications', value); write.run(LINKS, link.id, JSON.stringify(link));
      if (links.size >= LIMIT) {
        const oldest = [...links.values()].sort((a, b) => a.sequence - b.sequence)[0];
        remove.run(LINKS, oldest.id); receiptPrunedThrough = Math.max(receiptPrunedThrough, oldest.sequence); historyGap = true; persist();
      }
      db.exec('RELEASE native_failure_notice');
      if (links.size >= LIMIT) { const oldest = [...links.values()].sort((a, b) => a.sequence - b.sequence)[0]; links.delete(oldest.id); }
      notifications.set(value.id, value); links.set(link.id, link);
    } catch (error) { db.exec('ROLLBACK TO native_failure_notice'); db.exec('RELEASE native_failure_notice'); ({ sequence, receiptPrunedThrough, historyGap } = previous); throw error; }
  }
  function publicRecord(value) {
    return copy({ id: value.id, sequence: value.nativeSequence, sourceKey: value.sourceKey, hostRunId: value.hostRunId, mainId: value.mainId, lifecycleId: value.lifecycleId, kind: value.kind, title: String(value.title ?? '').slice(0, 200), body: String(value.body ?? '').slice(0, 1200), receiptIds: value.receiptIds ?? [], firstRoundIds: value.firstRoundIds ?? [], checkIds: value.checkIds ?? [], alertIds: value.alertIds ?? [], members: (value.members ?? []).map(member => ({ taskKey: member.taskKey, checkIds: member.checkIds ?? [], alertIds: member.alertIds ?? [] })), createdAt: value.createdAt, evidenceThrough: value.evidenceThrough, status: value.status, delivery: value.delivery, humanSeen: 'unknown', waitReason: value.waitReason ?? null, errorCode: value.native?.errorCode ?? (value.error ? 'notification_unavailable' : null), native: value.native ?? null });
  }
  function claim(value) {
    if (!native(value)) return;
    if (value.native?.suppression?.deferred && value.native.submission.state === 'not-attempted') return;
    value.native = { attemptId: randomUUID(), nativeRequestId: null, bundleId: 'com.ghostsentinel.notifications', authorization: { state: 'unknown', observedAt: null }, submission: { state: 'not-attempted', observedAt: null } };
  }
  function recordResult(result) {
    if (result.observedAt > now() || result.deliveryDate > result.observedAt) throw new Error('Native notification observation is in the future');
    reconcile();
    const value = notifications.get(result.notificationId), attempt = value?.native;
    if (!native(value) || !attempt || attempt.attemptId !== result.attemptId) throw new Error('Native notification attempt does not match');
    if (!/^ghostsentinel:[a-f0-9]{24}:.+/.test(result.nativeRequestId) || attempt.nativeRequestId && attempt.nativeRequestId !== result.nativeRequestId) throw new Error('Native notification request does not match');
    if (['accepted', 'submission-failed', 'submission-unknown', 'user-action'].includes(result.event) && attempt.submission.state === 'not-attempted') throw new Error('Native submission has not started');
    attempt.nativeRequestId = result.nativeRequestId;
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
      maySubmit = value.status === 'claimed' && scopeEligible(value) && qualified(value.sourceKey) && ['authorized', 'provisional'].includes(attempt.authorization.state) && attempt.submission.state === 'not-attempted';
      if (maySubmit) {
        attempt.submission = { state: 'submitting', observedAt };
        if (attempt.suppression?.deferred) attempt.suppression = { ...attempt.suppression, deferred: false, resolvedAt: observedAt };
        value.waitReason = null;
      }
    } else if (['accepted', 'submission-failed', 'submission-unknown'].includes(result.event)) {
      if (!['submitting', 'unknown', 'accepted', 'failed'].includes(attempt.submission.state)) throw new Error('Native submission has not started');
      if (attempt.submission.state !== 'accepted' && (attempt.submission.observedAt ?? 0) <= observedAt || result.event === 'accepted') {
        const state = result.event === 'accepted' ? 'accepted' : result.event === 'submission-failed' ? 'failed' : 'unknown';
        attempt.submission = { state, observedAt };
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
    return { record: publicRecord(value), maySubmit };
  }
  return { touch, removed, prepareTask, observeFailure, scopeEligible, qualified, reconcile, cancel, publicRecord, claim, recordResult,
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
      source(sourceKey); reconcile();
      const values = [...notifications.values()].filter(value => native(value) && value.sourceKey === sourceKey && value.nativeSequence > afterSequence).sort((a, b) => a.nativeSequence - b.nativeSequence), page = values.slice(0, limit);
      return { records: page.map(publicRecord), nextSequence: page.at(-1)?.nativeSequence ?? afterSequence, hasMore: values.length > page.length, historyGap: historyGap || afterSequence < prunedThrough, maximumRecords: LIMIT, qualification: qualified(sourceKey) ? 'confirmed' : 'unconfirmed' };
    },
    diagnostics: () => ({ records: [...notifications.values()].filter(native).length, failureLinks: links.size, eligibilitySources: eligibility.size, maximumEligibilitySources: 256, maximumRecords: LIMIT, maximumFailureLinks: LIMIT, maximumReceiptsPerNotice: 100, latestSequence: sequence, prunedThroughSequence: prunedThrough, historyGap }),
  };
}
