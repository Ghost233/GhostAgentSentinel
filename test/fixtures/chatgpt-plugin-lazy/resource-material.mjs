export function resourceMaterial({ at, cwd, producerVersion }) {
  const row = (type, payload, offset = 0) => JSON.stringify({ type, timestamp: new Date(at + offset).toISOString(), payload }) + '\n'
  return row('session_meta', { id: 'resource-main', cli_version: producerVersion, originator: 'Codex Desktop', source: 'vscode', cwd })
    + row('event_msg', { type: 'task_started', turn_id: 'resource-turn', turn_attribution: { turn_id: 'resource-turn', turn_trigger: 'composer' } })
    + row('event_msg', { type: 'item_completed', thread_id: 'resource-main', turn_id: 'resource-turn', item: { type: 'UserMessage', id: 'owned-input', content: [{ type: 'text', text: 'Read this synthetic resource fixture only' }] } }, 1)
}
